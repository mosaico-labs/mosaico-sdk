use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StateLock, PoisonError};
use std::time::Duration;

use arrow_array::{Array, Int64Array, RecordBatch, RecordBatchReader};
use tokio::sync::Mutex;

use crate::comm::action::ActionKind;
use crate::comm::connection::Connection;
use crate::error::{Error, ErrorKind, Result};
use crate::ontology::Encode;
use crate::write::batch_writer::BatchWriter;
use crate::write::rows::{Chunk, Ready, RowBuffer, Rows};
use crate::write::session::interrupted_error;

pub(super) const TIMESTAMP: &str = "timestamp_ns";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicErrorPolicy {
    Raise,
    Finalize,
    Ignore,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TopicWriterStatus {
    Active,
    Finalized,
    FinalizedWithError,
    IgnoredLastError,
    Raised,
    RecordTooLarge,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FlushPolicy {
    pub max_bytes: Option<usize>,
    pub max_rows: Option<usize>,
    pub max_age: Option<Duration>,
}

#[derive(Clone)]
pub struct TopicWriter {
    topic: Arc<TopicState>,
}

impl TopicWriter {
    pub(super) fn new(topic: Arc<TopicState>) -> Self {
        Self { topic }
    }

    pub fn name(&self) -> &str {
        &self.topic.name
    }

    pub fn dropped_records(&self) -> u64 {
        self.topic.dropped.load(Ordering::Relaxed)
    }

    pub fn status(&self) -> TopicWriterStatus {
        self.topic.state().status
    }

    pub fn last_error(&self) -> Option<String> {
        self.topic.state().last_error.clone()
    }

    pub async fn push_batch(&self, batch: RecordBatch) -> Result<()> {
        let mut guard = self.topic.slot.lock().await;
        let Some(slot) = guard.as_mut() else {
            return Err(self.topic.finalized());
        };
        let result = self.topic.push_batch(slot, batch).await;
        self.topic.record(result.as_ref().map(|()| true));
        result
    }

    pub async fn push<T: Encode>(&self, timestamp_ns: i64, value: &T) -> Result<()> {
        let mut guard = self.topic.slot.lock().await;
        let Some(slot) = guard.as_mut() else {
            return Err(self.topic.finalized());
        };
        let result = self.topic.push_record(slot, timestamp_ns, value).await;
        self.topic.record(result.as_ref().map(|sent| *sent));
        result.map(drop)
    }

    pub fn accepted(&self) {
        let mut state = self.topic.state();
        if !matches!(
            state.status,
            TopicWriterStatus::Finalized | TopicWriterStatus::FinalizedWithError
        ) {
            *state = State {
                status: TopicWriterStatus::Active,
                last_error: None,
            };
        }
    }

    pub async fn record_too_large(&self, size: usize) -> Result<()> {
        let guard = self.topic.slot.lock().await;
        let Some(slot) = guard.as_ref() else {
            return Err(self.topic.finalized());
        };
        self.topic
            .drop_record(size, slot.writer.record_limit())
            .await;
        Ok(())
    }

    pub async fn reject(&self, error: Error) -> Error {
        let guard = self.topic.slot.lock().await;
        if guard.is_none() {
            return self.topic.finalized();
        }
        self.topic
            .set_state(TopicWriterStatus::Raised, Some(error.to_string()));
        error
    }

    pub async fn handle_error(&self, reason: &str) -> bool {
        let mut guard = self.topic.slot.lock().await;
        match self.topic.on_error {
            TopicErrorPolicy::Raise => {
                self.topic
                    .set_state(TopicWriterStatus::Raised, Some(reason.to_owned()));
                false
            }
            TopicErrorPolicy::Ignore => {
                self.topic.report(reason).await;
                self.topic
                    .set_state(TopicWriterStatus::IgnoredLastError, Some(reason.to_owned()));
                true
            }
            TopicErrorPolicy::Finalize => {
                let Some(slot) = guard.take() else {
                    return false;
                };
                self.topic.report(reason).await;
                let _ = slot.writer.finish().await;
                self.topic.set_state(
                    TopicWriterStatus::FinalizedWithError,
                    Some(reason.to_owned()),
                );
                true
            }
        }
    }

    pub async fn flush(&self) -> Result<()> {
        let mut guard = self.topic.slot.lock().await;
        let Some(slot) = guard.as_mut() else {
            return Err(self.topic.finalized());
        };
        let result = match self.topic.send_rows(slot, true).await {
            Ok(()) => slot.writer.flush().await,
            Err(error) => Err(error),
        };
        if let Err(error) = &result {
            self.topic
                .set_state(TopicWriterStatus::Raised, Some(error.to_string()));
        }
        result
    }

    pub async fn set_flush_policy(&self, policy: FlushPolicy) -> Result<()> {
        let mut guard = self.topic.slot.lock().await;
        let Some(slot) = guard.as_mut() else {
            return Err(self.topic.finalized());
        };
        slot.writer.set_policy(policy)?;
        slot.policy = policy;
        let ready = slot.rows.as_ref().map_or(Ready::No, |rows| {
            rows.ready(&policy, slot.writer.target_bytes())
        });
        match ready {
            Ready::No => Ok(()),
            Ready::Chunk | Ready::Policy => self.topic.send_rows(slot, true).await,
        }
    }

    pub async fn push_stream(&self, reader: impl RecordBatchReader + Send) -> Result<()> {
        for batch in reader {
            let batch = batch.map_err(|e| {
                Error::new(ErrorKind::InvalidArgument, "cannot read batch").with_source(e)
            })?;
            self.push_batch(batch).await?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(super) enum Close<'a> {
    Finish,
    Report(&'a str),
    Discard(&'a str),
}

struct Slot {
    writer: BatchWriter,
    rows: Option<Box<dyn Rows>>,
    ready: Option<Chunk>,
    policy: FlushPolicy,
}

pub(super) struct TopicState {
    name: String,
    locator: String,
    connection: Connection,
    on_error: TopicErrorPolicy,
    dropped: Arc<AtomicU64>,
    slot: Mutex<Option<Slot>>,
    last_timestamp: AtomicI64,
    state: StateLock<State>,
}

struct State {
    status: TopicWriterStatus,
    last_error: Option<String>,
}

impl TopicState {
    pub(super) fn new(
        name: String,
        locator: String,
        connection: Connection,
        on_error: TopicErrorPolicy,
        writer: BatchWriter,
    ) -> Self {
        Self {
            name,
            locator,
            connection,
            on_error,
            dropped: writer.dropped(),
            slot: Mutex::new(Some(Slot {
                writer,
                rows: None,
                ready: None,
                policy: FlushPolicy::default(),
            })),
            last_timestamp: AtomicI64::new(i64::MIN),
            state: StateLock::new(State {
                status: TopicWriterStatus::Active,
                last_error: None,
            }),
        }
    }

    pub(super) fn name(&self) -> &str {
        &self.name
    }

    fn finalized(&self) -> Error {
        Error::new(
            ErrorKind::Closed,
            format!("topic '{}' is finalized", self.name),
        )
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn set_state(&self, status: TopicWriterStatus, last_error: Option<String>) {
        *self.state() = State { status, last_error };
    }

    fn record(&self, result: std::result::Result<bool, &Error>) {
        match result {
            Ok(true) => self.set_state(TopicWriterStatus::Active, None),
            Ok(false) => {}
            Err(error) => self.set_state(TopicWriterStatus::Raised, Some(error.to_string())),
        }
    }

    async fn push_batch(&self, slot: &mut Slot, batch: RecordBatch) -> Result<()> {
        self.send_rows(slot, false).await?;
        let range = timestamp_range(&batch)?;
        let batch = slot.writer.conform(batch).await?;
        let Some((first, last)) = range else {
            return Ok(());
        };
        check_follows(
            &self.name,
            self.last_timestamp.load(Ordering::Relaxed),
            first,
        )?;
        slot.writer.enqueue(batch, None, false).await?;
        self.last_timestamp.store(last, Ordering::Relaxed);
        Ok(())
    }

    async fn push_record<T: Encode>(
        &self,
        slot: &mut Slot,
        timestamp_ns: i64,
        value: &T,
    ) -> Result<bool> {
        if slot.writer.interrupted() {
            return Err(interrupted_error());
        }
        check_follows(
            &self.name,
            self.last_timestamp.load(Ordering::Relaxed),
            timestamp_ns,
        )?;
        let size = value.size();
        let limit = slot.writer.record_limit();
        if size > limit {
            self.drop_record(size, limit).await;
            return Ok(false);
        }
        self.send_ready(slot).await?;
        let other_type = slot
            .rows
            .as_mut()
            .is_some_and(|rows| !rows.as_any().is::<RowBuffer<T>>());
        if other_type {
            self.send_rows(slot, false).await?;
            slot.rows = None;
        }
        let rows = match &mut slot.rows {
            Some(rows) => rows,
            None => {
                let buffer = RowBuffer::<T>::new();
                slot.writer.check_shape(buffer.schema())?;
                slot.rows.insert(Box::new(buffer))
            }
        };
        let Some(buffer) = rows.as_any().downcast_mut::<RowBuffer<T>>() else {
            return Err(Error::new(
                ErrorKind::Stream,
                "record buffer of another type",
            ));
        };
        buffer.append(timestamp_ns, value, size);
        self.last_timestamp.store(timestamp_ns, Ordering::Relaxed);
        match rows.ready(&slot.policy, slot.writer.target_bytes()) {
            Ready::No => {}
            Ready::Chunk => slot.ready = rows.take(false)?,
            Ready::Policy => slot.ready = rows.take(true)?,
        }
        self.send_ready(slot).await?;
        Ok(true)
    }

    async fn send_rows(&self, slot: &mut Slot, urgent: bool) -> Result<()> {
        self.send_ready(slot).await?;
        if let Some(rows) = slot.rows.as_mut() {
            slot.ready = rows.take(urgent)?;
        }
        self.send_ready(slot).await
    }

    async fn send_ready(&self, slot: &mut Slot) -> Result<()> {
        let Some(chunk) = &slot.ready else {
            return Ok(());
        };
        let batch = slot.writer.conform(chunk.batch.clone()).await?;
        slot.writer
            .enqueue(batch, chunk.since, chunk.urgent)
            .await?;
        slot.ready = None;
        Ok(())
    }

    async fn drop_record(&self, size: usize, limit: usize) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
        let message = format!(
            "record dropped for topic '{}': its size ({size} bytes) exceeds the message size limit ({limit} bytes) and it cannot be split further",
            self.name
        );
        let _ = self
            .connection
            .notify_error(
                ActionKind::TopicNotificationCreate,
                &self.locator,
                message.clone(),
            )
            .await;
        self.set_state(TopicWriterStatus::RecordTooLarge, Some(message));
    }

    pub(super) async fn close(&self, close: Close<'_>) -> Result<()> {
        let mut guard = self.slot.lock().await;
        let Some(mut slot) = guard.take() else {
            return Ok(());
        };
        let reason = match close {
            Close::Finish => None,
            Close::Report(reason) => {
                self.report(reason).await;
                Some(reason)
            }
            Close::Discard(reason) => {
                drop(slot);
                self.set_state(
                    TopicWriterStatus::FinalizedWithError,
                    Some(reason.to_owned()),
                );
                return Ok(());
            }
        };
        let sent = match reason {
            None => self.send_rows(&mut slot, false).await,
            Some(_) => Ok(()),
        };
        let result = sent.and(slot.writer.finish().await);
        match (&result, reason) {
            (Ok(()), None) => self.set_state(TopicWriterStatus::Finalized, None),
            (Err(error), _) => self.set_state(
                TopicWriterStatus::FinalizedWithError,
                Some(error.to_string()),
            ),
            (Ok(()), Some(reason)) => self.set_state(
                TopicWriterStatus::FinalizedWithError,
                Some(reason.to_owned()),
            ),
        }
        result
    }

    async fn report(&self, reason: &str) {
        let message = format!("topic '{}' failed: {reason}", self.locator);
        let _ = self
            .connection
            .notify_error(ActionKind::TopicNotificationCreate, &self.locator, message)
            .await;
    }
}

fn timestamp_range(batch: &RecordBatch) -> Result<Option<(i64, i64)>> {
    let timestamps = batch
        .column_by_name(TIMESTAMP)
        .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| Error::schema(format!("batch must have a '{TIMESTAMP}' Int64 column")))?;
    if timestamps.null_count() > 0 {
        return Err(Error::new(
            ErrorKind::InvalidArgument,
            format!("'{TIMESTAMP}' must not contain nulls"),
        ));
    }
    let values = timestamps.values();
    if values.windows(2).any(|pair| pair[0] > pair[1]) {
        return Err(Error::new(
            ErrorKind::InvalidArgument,
            format!(
                "'{TIMESTAMP}' must be non-decreasing: found a timestamp lower than the previous one"
            ),
        ));
    }
    Ok(values.first().copied().zip(values.last().copied()))
}

fn check_follows(topic: &str, previous: i64, first: i64) -> Result<()> {
    if first < previous {
        return Err(Error::new(
            ErrorKind::InvalidArgument,
            format!(
                "'{TIMESTAMP}' must be non-decreasing across pushes on topic '{topic}': got {first}, lower than the last pushed timestamp {previous}"
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch};

    use super::*;

    fn batch(timestamps: Vec<Option<i64>>) -> RecordBatch {
        let column: ArrayRef = Arc::new(Int64Array::from(timestamps));
        RecordBatch::try_from_iter([(TIMESTAMP, column)]).unwrap()
    }

    fn kind(result: Result<impl std::fmt::Debug>) -> ErrorKind {
        result.unwrap_err().kind()
    }

    #[test]
    fn equal_timestamps_are_accepted_within_a_batch() {
        let range = timestamp_range(&batch(vec![Some(1), Some(1), Some(2), Some(2)]));
        assert_eq!(range.unwrap(), Some((1, 2)));
        assert_eq!(timestamp_range(&batch(vec![])).unwrap(), None);
    }

    #[test]
    fn decreasing_or_null_timestamps_are_rejected() {
        let decreasing = timestamp_range(&batch(vec![Some(2), Some(1)]));
        assert_eq!(kind(decreasing), ErrorKind::InvalidArgument);
        let nulls = timestamp_range(&batch(vec![Some(1), None]));
        assert_eq!(kind(nulls), ErrorKind::InvalidArgument);
    }

    #[test]
    fn a_push_may_not_start_before_the_last_pushed_timestamp() {
        assert!(check_follows("/t", i64::MIN, i64::MIN).is_ok());
        assert!(check_follows("/t", 5, 5).is_ok());
        assert!(check_follows("/t", 5, 6).is_ok());
        let error = check_follows("/t", 5, 4).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        assert!(
            error
                .to_string()
                .contains("got 4, lower than the last pushed timestamp 5")
        );
    }
}
