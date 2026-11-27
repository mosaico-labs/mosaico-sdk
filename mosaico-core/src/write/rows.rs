use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::error::{Error, Result};
use crate::ontology::TIMESTAMP_COLUMN;
use crate::ontology::{Encode, RecordBuilder};
use crate::write::topic::FlushPolicy;

const CHUNK_BYTES: usize = 1 << 20;
const TIMESTAMP_BYTES: usize = 8;

pub fn record_schema<T: Encode>() -> SchemaRef {
    let timestamp = Arc::new(Field::new(TIMESTAMP_COLUMN, DataType::Int64, false));
    let fields: Vec<_> = std::iter::once(timestamp)
        .chain(T::fields().iter().cloned())
        .collect();
    Arc::new(Schema::new(fields))
}

pub(super) struct Chunk {
    pub(super) batch: RecordBatch,
    pub(super) since: Option<Instant>,
    pub(super) urgent: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Ready {
    No,
    Chunk,
    Policy,
}

pub(super) trait Rows: Send {
    fn as_any(&mut self) -> &mut dyn Any;
    fn ready(&self, policy: &FlushPolicy, target: usize) -> Ready;
    fn take(&mut self, urgent: bool) -> Result<Option<Chunk>>;
}

#[derive(Default)]
struct Fill {
    rows: usize,
    bytes: usize,
    since: Option<Instant>,
}

impl Fill {
    fn add(&mut self, rows: usize, bytes: usize) {
        self.since.get_or_insert_with(Instant::now);
        self.rows += rows;
        self.bytes += bytes;
    }

    fn ready(&self, policy: &FlushPolicy, target: usize) -> Ready {
        if self.rows == 0 {
            return Ready::No;
        }
        let by_policy = policy
            .max_bytes
            .is_some_and(|bytes| self.bytes >= bytes.min(target))
            || policy.max_rows.is_some_and(|rows| self.rows >= rows)
            || policy
                .max_age
                .zip(self.since)
                .is_some_and(|(age, since)| since.elapsed() >= age);
        if by_policy {
            Ready::Policy
        } else if self.bytes >= CHUNK_BYTES.min(target) {
            Ready::Chunk
        } else {
            Ready::No
        }
    }

    fn reset(&mut self) -> Option<Instant> {
        self.rows = 0;
        self.bytes = 0;
        self.since.take()
    }
}

pub(super) struct RowBuffer<T: Encode> {
    schema: SchemaRef,
    builder: T::Builder,
    timestamps: Vec<i64>,
    fill: Fill,
}

impl<T: Encode> RowBuffer<T> {
    pub(super) fn new() -> Self {
        Self {
            schema: record_schema::<T>(),
            builder: T::Builder::default(),
            timestamps: Vec::new(),
            fill: Fill::default(),
        }
    }

    pub(super) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(super) fn append(&mut self, timestamp_ns: i64, value: &T, size: usize) {
        self.timestamps.push(timestamp_ns);
        self.builder.append(value);
        self.fill.add(1, size + TIMESTAMP_BYTES);
    }
}

impl<T: Encode> Rows for RowBuffer<T> {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn ready(&self, policy: &FlushPolicy, target: usize) -> Ready {
        self.fill.ready(policy, target)
    }

    fn take(&mut self, urgent: bool) -> Result<Option<Chunk>> {
        if self.timestamps.is_empty() {
            return Ok(None);
        }
        let since = self.fill.reset();
        let timestamps: ArrayRef = Arc::new(Int64Array::from(std::mem::take(&mut self.timestamps)));
        let columns = std::iter::once(timestamps)
            .chain(self.builder.finish())
            .collect();
        let batch = RecordBatch::try_new(self.schema.clone(), columns).map_err(|e| {
            Error::schema(format!("cannot build '{}' records", T::TAG)).with_source(e)
        })?;
        Ok(Some(Chunk {
            batch,
            since,
            urgent,
        }))
    }
}

pub(super) struct BatchRows {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    fill: Fill,
}

impl BatchRows {
    pub(super) fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            batches: Vec::new(),
            fill: Fill::default(),
        }
    }

    pub(super) fn accepts(&self, schema: &SchemaRef) -> bool {
        Arc::ptr_eq(&self.schema, schema) || self.schema == *schema
    }

    pub(super) fn append(&mut self, batch: RecordBatch, size: usize) {
        self.fill.add(batch.num_rows(), size);
        self.batches.push(batch);
    }
}

impl Rows for BatchRows {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn ready(&self, policy: &FlushPolicy, target: usize) -> Ready {
        self.fill.ready(policy, target)
    }

    fn take(&mut self, urgent: bool) -> Result<Option<Chunk>> {
        if self.batches.is_empty() {
            return Ok(None);
        }
        let since = self.fill.reset();
        let batches = std::mem::take(&mut self.batches);
        let batch = arrow_select::concat::concat_batches(&self.schema, &batches)
            .map_err(|e| Error::schema("cannot join the pushed records").with_source(e))?;
        Ok(Some(Chunk {
            batch,
            since,
            urgent,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn row(timestamp_ns: i64) -> RecordBatch {
        let column: ArrayRef = Arc::new(Int64Array::from(vec![timestamp_ns]));
        RecordBatch::try_from_iter([(TIMESTAMP_COLUMN, column)]).unwrap()
    }

    #[test]
    fn rows_join_in_order() {
        let first = row(1);
        let mut rows = BatchRows::new(first.schema());
        rows.append(first, 10);
        rows.append(row(2), 10);
        assert_eq!(rows.ready(&FlushPolicy::default(), 1 << 30), Ready::No);
        let chunk = rows.take(false).unwrap().unwrap();
        let timestamps = chunk
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(timestamps.values(), &[1, 2]);
        assert!(chunk.since.is_some());
        assert!(rows.take(false).unwrap().is_none());
    }

    #[test]
    fn rows_follow_flush_policy() {
        let first = row(1);
        let mut rows = BatchRows::new(first.schema());
        rows.append(first, 10);
        let by_rows = FlushPolicy {
            max_rows: Some(1),
            ..FlushPolicy::default()
        };
        assert_eq!(rows.ready(&by_rows, 1 << 30), Ready::Policy);
        let by_bytes = FlushPolicy {
            max_bytes: Some(1000),
            ..FlushPolicy::default()
        };
        assert_eq!(rows.ready(&by_bytes, 1 << 30), Ready::No);
        assert_eq!(rows.ready(&by_bytes, 10), Ready::Policy);
        assert_eq!(rows.ready(&FlushPolicy::default(), 10), Ready::Chunk);
        let by_age = FlushPolicy {
            max_age: Some(Duration::ZERO),
            ..FlushPolicy::default()
        };
        assert_eq!(rows.ready(&by_age, 1 << 30), Ready::Policy);
    }

    #[test]
    fn other_schema_not_joined() {
        let first = row(1);
        let rows = BatchRows::new(first.schema());
        assert!(rows.accepts(&row(2).schema()));
        let other: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let other = RecordBatch::try_from_iter([("other", other)]).unwrap();
        assert!(!rows.accepts(&other.schema()));
    }
}
