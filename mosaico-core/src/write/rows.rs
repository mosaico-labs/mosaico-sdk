use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::error::{Error, Result};
use crate::ontology::{Encode, RecordBuilder};
use crate::write::topic::{FlushPolicy, TIMESTAMP};

const CHUNK_BYTES: usize = 1 << 20;
const TIMESTAMP_BYTES: usize = 8;

pub fn record_schema<T: Encode>() -> SchemaRef {
    let timestamp = Arc::new(Field::new(TIMESTAMP, DataType::Int64, false));
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

pub(super) struct RowBuffer<T: Encode> {
    schema: SchemaRef,
    builder: T::Builder,
    timestamps: Vec<i64>,
    bytes: usize,
    since: Option<Instant>,
}

impl<T: Encode> RowBuffer<T> {
    pub(super) fn new() -> Self {
        Self {
            schema: record_schema::<T>(),
            builder: T::Builder::default(),
            timestamps: Vec::new(),
            bytes: 0,
            since: None,
        }
    }

    pub(super) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(super) fn append(&mut self, timestamp_ns: i64, value: &T, size: usize) {
        self.since.get_or_insert_with(Instant::now);
        self.timestamps.push(timestamp_ns);
        self.builder.append(value);
        self.bytes += size + TIMESTAMP_BYTES;
    }
}

impl<T: Encode> Rows for RowBuffer<T> {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn ready(&self, policy: &FlushPolicy, target: usize) -> Ready {
        if self.timestamps.is_empty() {
            return Ready::No;
        }
        let by_policy = policy
            .max_bytes
            .is_some_and(|bytes| self.bytes >= bytes.min(target))
            || policy
                .max_rows
                .is_some_and(|rows| self.timestamps.len() >= rows)
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

    fn take(&mut self, urgent: bool) -> Result<Option<Chunk>> {
        if self.timestamps.is_empty() {
            return Ok(None);
        }
        self.bytes = 0;
        let since = self.since.take();
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
