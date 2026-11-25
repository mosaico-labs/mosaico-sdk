use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StateLock, MutexGuard, PoisonError};
use std::time::Duration;

use arrow_array::{Array, Int64Array, RecordBatch};
use futures::{Stream, StreamExt};
use tokio::sync::{Mutex, watch};
use tokio::task::AbortHandle;

use crate::comm::action::ActionKind;
use crate::comm::budget::limits;
use crate::comm::connection::Connection;
use crate::error::{Error, ErrorKind, Result};
use crate::ontology::{Encode, Format};
use crate::write::batch_writer::{BatchWriter, estimate};
use crate::write::rows::{BatchRows, Chunk, Ready, RowBuffer, Rows};
use crate::write::session::interrupted_error;

pub(super) const TIMESTAMP: &str = "timestamp_ns";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TopicLevelErrorPolicy {
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
#[non_exhaustive]
pub struct FlushPolicy {
    pub max_bytes: Option<usize>,
    pub max_rows: Option<usize>,
    pub max_age: Option<Duration>,
}

impl FlushPolicy {
    pub fn max_bytes(mut self, bytes: usize) -> Self {
        self.max_bytes = Some(bytes);
        self
    }

    pub fn max_rows(mut self, rows: usize) -> Self {
        self.max_rows = Some(rows);
        self
    }

    pub fn max_age(mut self, age: Duration) -> Self {
        self.max_age = Some(age);
        self
    }
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
        &self.topic.reporter.name
    }

    pub fn dropped_record_count(&self) -> u64 {
        self.topic.reporter.status.dropped()
    }

    pub fn status(&self) -> TopicWriterStatus {
        self.topic.reporter.status.lock().status
    }

    pub fn last_error(&self) -> Option<String> {
        self.topic.reporter.status.lock().last_error.clone()
    }

    pub async fn push_batch(&self, batch: RecordBatch) -> Result<()> {
        let mut guard = self.topic.slot.lock().await;
        let slot = self.topic.open(&mut guard).await?;
        let result = self.topic.push_batch(slot, batch).await.map(|()| true);
        self.topic.settle(&mut guard, result).await.map(drop)
    }

    pub async fn push<T: Encode>(&self, timestamp_ns: i64, value: &T) -> Result<()> {
        let mut guard = self.topic.slot.lock().await;
        let slot = self.topic.open(&mut guard).await?;
        let result = self.topic.push_record(slot, timestamp_ns, value).await;
        self.topic.settle(&mut guard, result).await.map(drop)
    }

    pub async fn push_row(&self, row: RecordBatch) -> Result<()> {
        let mut guard = self.topic.slot.lock().await;
        let slot = self.topic.open(&mut guard).await?;
        let result = self.topic.push_row(slot, row).await;
        self.topic.settle(&mut guard, result).await.map(drop)
    }

    #[doc(hidden)]
    pub async fn reject(&self, error: Error) -> Error {
        let guard = self.topic.slot.lock().await;
        if guard.is_none() {
            return self.topic.ended().await;
        }
        self.topic
            .set_state(TopicWriterStatus::Raised, Some(error.to_string()));
        error
    }

    pub async fn handle_error(&self, reason: &str) -> Result<bool> {
        let mut guard = self.topic.slot.lock().await;
        if guard.is_none() {
            let _ = self.topic.closed().await;
            return Ok(self.topic.on_error == TopicLevelErrorPolicy::Ignore);
        }
        match self.topic.on_error {
            TopicLevelErrorPolicy::Raise => {
                self.topic
                    .set_state(TopicWriterStatus::Raised, Some(reason.to_owned()));
                Ok(false)
            }
            TopicLevelErrorPolicy::Ignore => {
                self.topic.reporter.report(reason).await;
                self.topic
                    .set_state(TopicWriterStatus::IgnoredLastError, Some(reason.to_owned()));
                Ok(true)
            }
            TopicLevelErrorPolicy::Finalize => self
                .topic
                .close_slot(&mut guard, Close::Report(reason))
                .await
                .map(|()| true),
        }
    }

    pub async fn finalize(&self, error: Option<&str>) -> Result<()> {
        let close = match error {
            Some(reason) => Close::Report(reason),
            None => Close::Finish,
        };
        self.topic.close(close).await
    }

    pub async fn flush(&self) -> Result<()> {
        let mut guard = self.topic.slot.lock().await;
        let slot = self.topic.open(&mut guard).await?;
        let result = match self.topic.send_rows(slot, true).await {
            Ok(()) => slot.writer.flush().await,
            Err(error) => Err(error),
        };
        self.topic
            .settle(&mut guard, result.map(|()| false))
            .await?;
        Ok(())
    }

    pub async fn set_flush_policy(&self, policy: FlushPolicy) -> Result<()> {
        let mut guard = self.topic.slot.lock().await;
        let slot = self.topic.open(&mut guard).await?;
        if let Err(error) = slot.writer.set_policy(policy) {
            return self.topic.settle(&mut guard, Err(error)).await.map(drop);
        }
        slot.policy = policy;
        let ready = slot.rows.as_ref().map_or(Ready::No, |rows| {
            rows.ready(&policy, slot.writer.target_bytes())
        });
        let result = match ready {
            Ready::No => Ok(false),
            Ready::Chunk | Ready::Policy => self.topic.send_rows(slot, true).await.map(|()| false),
        };
        self.topic.settle(&mut guard, result).await.map(drop)
    }

    pub async fn push_stream<S, E>(&self, batches: S) -> Result<()>
    where
        S: Stream<Item = std::result::Result<RecordBatch, E>>,
        E: std::error::Error + Send + Sync + 'static,
    {
        let mut batches = std::pin::pin!(batches);
        while let Some(batch) = batches.next().await {
            match batch {
                Ok(batch) => self.push_batch(batch).await?,
                Err(e) => {
                    let error =
                        Error::new(ErrorKind::InvalidArgument, "cannot read batch").with_source(e);
                    return Err(self.reject(error).await);
                }
            }
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

pub(crate) struct Reporter {
    name: String,
    locator: String,
    connection: Connection,
    limit: usize,
    status: Status,
}

impl Reporter {
    pub(super) fn new(name: String, locator: String, connection: Connection) -> Self {
        let (limit, _) = limits(connection.info());
        Self {
            name,
            locator,
            connection,
            limit,
            status: Status::new(),
        }
    }

    pub(crate) fn locator(&self) -> &str {
        &self.locator
    }

    pub(crate) async fn record_dropped(&self, count: u64, largest: usize, at_push: bool) {
        let message = format!(
            "{count} record(s) dropped for topic '{}': each is larger alone than the message size limit ({} bytes), the largest is {largest} bytes",
            self.name, self.limit
        );
        self.status.record_dropped(count, message.clone(), at_push);
        self.notify(message).await;
    }

    async fn report(&self, reason: &str) {
        self.notify(format!("topic '{}' failed: {reason}", self.locator))
            .await;
    }

    async fn notify(&self, message: String) {
        let _ = self
            .connection
            .notify_error(ActionKind::TopicNotificationCreate, &self.locator, message)
            .await;
    }
}

pub(super) struct TopicState {
    ontology_tag: String,
    format: Format,
    on_error: TopicLevelErrorPolicy,
    reporter: Arc<Reporter>,
    slot: Mutex<Option<Slot>>,
    last_timestamp: AtomicI64,
    upload_failure: StateLock<Option<(Error, bool)>>,
    closing: StateLock<Option<Closing>>,
}

struct Closing {
    task: AbortHandle,
    done: watch::Receiver<bool>,
    outcome: Arc<StateLock<Option<Result<()>>>>,
    end_sent: Arc<AtomicBool>,
}

struct Status {
    state: StateLock<State>,
    dropped: AtomicU64,
}

struct State {
    status: TopicWriterStatus,
    last_error: Option<String>,
}

impl Status {
    fn new() -> Self {
        Self {
            state: StateLock::new(State {
                status: TopicWriterStatus::Active,
                last_error: None,
            }),
            dropped: AtomicU64::new(0),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn set(&self, status: TopicWriterStatus, last_error: Option<String>) {
        let mut state = self.lock();
        if terminal(state.status) && !terminal(status) {
            return;
        }
        *state = State { status, last_error };
    }

    fn close(&self) {
        let mut state = self.lock();
        if !terminal(state.status) {
            *state = State {
                status: TopicWriterStatus::Finalized,
                last_error: None,
            };
        }
    }

    fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    fn record_dropped(&self, count: u64, message: String, at_push: bool) {
        self.dropped.fetch_add(count, Ordering::Relaxed);
        let mut state = self.lock();
        let replace = match at_push {
            true => !terminal(state.status),
            false => matches!(
                state.status,
                TopicWriterStatus::Active | TopicWriterStatus::RecordTooLarge
            ),
        };
        if replace {
            *state = State {
                status: TopicWriterStatus::RecordTooLarge,
                last_error: Some(message),
            };
        }
    }
}

impl TopicState {
    pub(super) fn new(
        ontology_tag: String,
        format: Format,
        on_error: TopicLevelErrorPolicy,
        reporter: Arc<Reporter>,
        writer: BatchWriter,
    ) -> Self {
        Self {
            ontology_tag,
            format,
            on_error,
            reporter,
            slot: Mutex::new(Some(Slot {
                writer,
                rows: None,
                ready: None,
                policy: FlushPolicy::default(),
            })),
            last_timestamp: AtomicI64::new(i64::MIN),
            upload_failure: StateLock::new(None),
            closing: StateLock::new(None),
        }
    }

    pub(super) fn name(&self) -> &str {
        &self.reporter.name
    }

    pub(super) fn locator(&self) -> &str {
        &self.reporter.locator
    }

    pub(super) fn upload_failure(&self) -> Option<Error> {
        self.failed_upload().map(|(error, _)| error)
    }

    pub(super) fn failed_upload(&self) -> Option<(Error, bool)> {
        self.upload_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|(error, end_sent)| (error.duplicate(), *end_sent))
    }

    fn upload_error(&self, cause: Error, end_sent: bool) -> (Error, Option<String>) {
        if cause.kind() == ErrorKind::Closed {
            return (cause, None);
        }
        let reason = cause.to_string();
        let error = Error::new(
            cause.kind(),
            format!("the upload of topic '{}' failed", self.reporter.name),
        )
        .with_source(cause);
        *self
            .upload_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some((error.duplicate(), end_sent));
        (
            error,
            (self.on_error != TopicLevelErrorPolicy::Raise).then_some(reason),
        )
    }

    fn start_closing(&self, writer: BatchWriter) {
        let end_sent = writer.end_sent();
        let outcome = Arc::new(StateLock::new(None));
        let (finished, done) = watch::channel(false);
        let task = tokio::spawn({
            let outcome = outcome.clone();
            async move {
                let result = writer.finish().await;
                *outcome.lock().unwrap_or_else(PoisonError::into_inner) = Some(result);
                finished.send_replace(true);
            }
        })
        .abort_handle();
        *self.closing.lock().unwrap_or_else(PoisonError::into_inner) = Some(Closing {
            task,
            done,
            outcome,
            end_sent,
        });
    }

    fn discard_closing(&self, reason: &str) {
        let closing = self
            .closing
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(closing) = closing {
            closing.task.abort();
            self.set_state(
                TopicWriterStatus::FinalizedWithError,
                Some(reason.to_owned()),
            );
        }
    }

    async fn closed(&self) -> Result<()> {
        let mut done = match &*self.closing.lock().unwrap_or_else(PoisonError::into_inner) {
            Some(closing) => closing.done.clone(),
            None => return Ok(()),
        };
        let _ = done.wait_for(|finished| *finished).await;
        let Some(closing) = self
            .closing
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        else {
            return Ok(());
        };
        let outcome = closing
            .outcome
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| Err(Error::new(ErrorKind::Stream, "topic writer task failed")));
        let cause = match outcome {
            Ok(()) => {
                self.reporter.status.close();
                return Ok(());
            }
            Err(cause) => cause,
        };
        let (error, report) = self.upload_error(cause, closing.end_sent.load(Ordering::Relaxed));
        self.set_state(
            TopicWriterStatus::FinalizedWithError,
            Some(error.to_string()),
        );
        if let Some(reason) = report {
            self.reporter.report(&reason).await;
        }
        Err(error)
    }

    fn finalized(&self) -> Error {
        let state = self.reporter.status.lock();
        let message = match (&state.status, &state.last_error) {
            (TopicWriterStatus::FinalizedWithError, Some(reason)) => {
                format!("topic '{}' is finalized: {reason}", self.reporter.name)
            }
            _ => format!("topic '{}' is finalized", self.reporter.name),
        };
        Error::new(ErrorKind::Closed, message)
    }

    async fn ended(&self) -> Error {
        match self.closed().await {
            Ok(()) => self.finalized(),
            Err(error) => error,
        }
    }

    async fn open<'a>(&self, slot: &'a mut Option<Slot>) -> Result<&'a mut Slot> {
        if slot.as_ref().is_some_and(|open| open.writer.failed()) {
            return Err(self.fail(slot).await);
        }
        if slot.is_none() {
            return Err(self.ended().await);
        }
        slot.as_mut().ok_or_else(|| self.finalized())
    }

    async fn settle(&self, slot: &mut Option<Slot>, result: Result<bool>) -> Result<bool> {
        if result.is_err() && slot.as_ref().is_some_and(|open| open.writer.failed()) {
            return Err(self.fail(slot).await);
        }
        self.record(result.as_ref().map(|sent| *sent));
        result
    }

    async fn fail(&self, slot: &mut Option<Slot>) -> Error {
        if let Some(open) = slot.take() {
            self.start_closing(open.writer);
        }
        self.ended().await
    }

    pub(super) fn set_state(&self, status: TopicWriterStatus, last_error: Option<String>) {
        self.reporter.status.set(status, last_error);
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
            &self.reporter.name,
            self.last_timestamp.load(Ordering::Relaxed),
            first,
        )?;
        slot.writer
            .enqueue(batch, None, false, || {
                self.last_timestamp.store(last, Ordering::Relaxed)
            })
            .await
    }

    async fn push_record<T: Encode>(
        &self,
        slot: &mut Slot,
        timestamp_ns: i64,
        value: &T,
    ) -> Result<bool> {
        self.admit(slot, timestamp_ns)?;
        let reuse = slot
            .rows
            .as_mut()
            .is_some_and(|rows| rows.as_any().is::<RowBuffer<T>>());
        let fresh: Option<Box<dyn Rows>> = if reuse {
            None
        } else {
            if T::TAG != self.ontology_tag || T::FORMAT != self.format {
                return Err(Error::invalid_argument(format!(
                    "topic '{}' stores {} ({:?}), not {} ({:?})",
                    self.reporter.name,
                    self.ontology_tag,
                    self.format,
                    T::TAG,
                    T::FORMAT
                )));
            }
            let buffer = RowBuffer::<T>::new();
            slot.writer.check_shape(buffer.schema())?;
            Some(Box::new(buffer))
        };
        let size = value.size();
        if !self.fits(slot.writer.record_limit(), size).await {
            return Ok(false);
        }
        let rows = self.switch_rows(slot, fresh).await?;
        let Some(buffer) = rows.as_any().downcast_mut::<RowBuffer<T>>() else {
            return Err(other_buffer());
        };
        buffer.append(timestamp_ns, value, size);
        self.appended(slot, timestamp_ns).await
    }

    async fn push_row(&self, slot: &mut Slot, row: RecordBatch) -> Result<bool> {
        if row.num_rows() != 1 {
            return Err(Error::invalid_argument(format!(
                "expected one record, got {} rows",
                row.num_rows()
            )));
        }
        let Some((timestamp_ns, _)) = timestamp_range(&row)? else {
            return Ok(false);
        };
        self.admit(slot, timestamp_ns)?;
        let schema = row.schema();
        let reuse = slot.rows.as_mut().is_some_and(|rows| {
            rows.as_any()
                .downcast_ref::<BatchRows>()
                .is_some_and(|buffer| buffer.accepts(&schema))
        });
        let fresh: Option<Box<dyn Rows>> = if reuse {
            None
        } else {
            slot.writer.check_shape(&schema)?;
            Some(Box::new(BatchRows::new(schema)))
        };
        slot.writer.check_nulls(&row)?;
        let size = estimate(&row);
        if !self.fits(slot.writer.record_limit(), size).await {
            return Ok(false);
        }
        let rows = self.switch_rows(slot, fresh).await?;
        let Some(buffer) = rows.as_any().downcast_mut::<BatchRows>() else {
            return Err(other_buffer());
        };
        buffer.append(row, size);
        self.appended(slot, timestamp_ns).await
    }

    fn admit(&self, slot: &Slot, timestamp_ns: i64) -> Result<()> {
        if slot.writer.interrupted() {
            return Err(interrupted_error());
        }
        check_follows(
            &self.reporter.name,
            self.last_timestamp.load(Ordering::Relaxed),
            timestamp_ns,
        )
    }

    async fn fits(&self, limit: usize, size: usize) -> bool {
        if size <= limit {
            return true;
        }
        self.reporter.record_dropped(1, size, true).await;
        false
    }

    async fn switch_rows<'a>(
        &self,
        slot: &'a mut Slot,
        fresh: Option<Box<dyn Rows>>,
    ) -> Result<&'a mut Box<dyn Rows>> {
        self.send_ready(slot).await?;
        if let Some(fresh) = fresh {
            if slot.rows.is_some() {
                self.send_rows(slot, false).await?;
            }
            slot.rows = Some(fresh);
        }
        slot.rows.as_mut().ok_or_else(other_buffer)
    }

    async fn appended(&self, slot: &mut Slot, timestamp_ns: i64) -> Result<bool> {
        self.last_timestamp.store(timestamp_ns, Ordering::Relaxed);
        if let Some(rows) = slot.rows.as_mut() {
            match rows.ready(&slot.policy, slot.writer.target_bytes()) {
                Ready::No => {}
                Ready::Chunk => slot.ready = rows.take(false)?,
                Ready::Policy => slot.ready = rows.take(true)?,
            }
        }
        self.send_ready(slot).await?;
        Ok(true)
    }

    async fn send_rows(&self, slot: &mut Slot, urgent: bool) -> Result<()> {
        self.send_ready(slot).await?;
        if let Some(rows) = slot.rows.as_mut() {
            slot.ready = rows.take(urgent)?;
        }
        self.send_ready(slot).await?;
        slot.writer.drain().await
    }

    async fn send_ready(&self, slot: &mut Slot) -> Result<()> {
        let Some(chunk) = &slot.ready else {
            return Ok(());
        };
        let (since, urgent) = (chunk.since, chunk.urgent);
        let batch = slot.writer.conform(chunk.batch.clone()).await?;
        let ready = &mut slot.ready;
        slot.writer
            .enqueue(batch, since, urgent, || *ready = None)
            .await
    }

    pub(super) async fn close(&self, close: Close<'_>) -> Result<()> {
        let mut guard = self.slot.lock().await;
        self.close_slot(&mut guard, close).await
    }

    async fn close_slot(&self, slot: &mut Option<Slot>, close: Close<'_>) -> Result<()> {
        let Some(open) = slot.as_mut() else {
            if let Close::Discard(reason) = close {
                self.discard_closing(reason);
                return Ok(());
            }
            return self.closed().await;
        };
        let (sent, reason) = match close {
            Close::Finish => (self.send_rows(open, false).await, None),
            Close::Report(reason) => {
                self.reporter.report(reason).await;
                (Ok(()), Some(reason))
            }
            Close::Discard(reason) => {
                *slot = None;
                self.set_state(
                    TopicWriterStatus::FinalizedWithError,
                    Some(reason.to_owned()),
                );
                return Ok(());
            }
        };
        let Some(open) = slot.take() else {
            return sent;
        };
        match reason {
            None => self.set_state(TopicWriterStatus::Finalized, None),
            Some(reason) => self.set_state(
                TopicWriterStatus::FinalizedWithError,
                Some(reason.to_owned()),
            ),
        }
        self.start_closing(open.writer);
        let result = match self.closed().await {
            Ok(()) => sent,
            Err(error) if error.kind() == ErrorKind::Closed => sent.and(Err(error)),
            Err(error) => Err(error),
        };
        if let Err(error) = &result {
            self.set_state(
                TopicWriterStatus::FinalizedWithError,
                Some(error.to_string()),
            );
        }
        result
    }
}

fn terminal(status: TopicWriterStatus) -> bool {
    matches!(
        status,
        TopicWriterStatus::Finalized | TopicWriterStatus::FinalizedWithError
    )
}

fn other_buffer() -> Error {
    Error::new(ErrorKind::Stream, "record buffer of another type")
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
    use arrow_schema::{DataType, Field, Fields, Schema};

    use super::*;
    use crate::ontology::{Ontology, RecordBuilder};
    use crate::write::session::Interrupter;

    fn offline_writer(on_error: TopicLevelErrorPolicy) -> TopicWriter {
        let connection = Connection::offline(1 << 20);
        let schema = Arc::new(Schema::new(vec![Field::new(
            TIMESTAMP,
            DataType::Int64,
            false,
        )]));
        let reporter = Arc::new(Reporter::new(
            "/t".to_owned(),
            "run/t".to_owned(),
            connection.clone(),
        ));
        let writer = BatchWriter::open(
            &connection,
            reporter.clone(),
            String::new(),
            schema,
            connection.scope().token(),
            Interrupter::new(),
        );
        let topic = TopicState::new(
            "Empty".to_owned(),
            Format::Default,
            on_error,
            reporter,
            writer,
        );
        TopicWriter::new(Arc::new(topic))
    }

    #[tokio::test]
    async fn error_after_finalize() {
        for policy in [
            TopicLevelErrorPolicy::Raise,
            TopicLevelErrorPolicy::Ignore,
            TopicLevelErrorPolicy::Finalize,
        ] {
            let writer = offline_writer(policy);
            writer.topic.close(Close::Discard("closed")).await.unwrap();
            let suppressed = writer.handle_error("late push").await.unwrap();
            assert_eq!(suppressed, policy == TopicLevelErrorPolicy::Ignore);
            assert_eq!(writer.status(), TopicWriterStatus::FinalizedWithError);
            assert_eq!(writer.last_error().as_deref(), Some("closed"));
        }
    }

    #[tokio::test]
    async fn finalize_policy_failed_close() {
        let writer = offline_writer(TopicLevelErrorPolicy::Finalize);
        let error = writer.handle_error("sensor lost").await.unwrap_err();
        assert_eq!(writer.status(), TopicWriterStatus::FinalizedWithError);
        assert_eq!(writer.last_error(), Some(error.to_string()));
        assert!(!writer.handle_error("again").await.unwrap());
    }

    #[tokio::test]
    async fn finalize_once() {
        for reason in [None, Some("camera unplugged")] {
            let writer = offline_writer(TopicLevelErrorPolicy::Raise);
            let error = writer.finalize(reason).await.unwrap_err();
            assert_eq!(writer.status(), TopicWriterStatus::FinalizedWithError);
            assert_eq!(writer.last_error(), Some(error.to_string()));
            writer.finalize(None).await.unwrap();
            assert_eq!(writer.status(), TopicWriterStatus::FinalizedWithError);
            assert_eq!(writer.last_error(), Some(error.to_string()));
            let pushed = writer.push_batch(batch(vec![Some(1)])).await;
            assert_eq!(kind(pushed), ErrorKind::Closed);
        }
    }

    #[tokio::test]
    async fn stream_read_error_sets_raised() {
        let writer = offline_writer(TopicLevelErrorPolicy::Raise);
        let failing = futures::stream::iter([Err::<RecordBatch, _>(std::io::Error::other(
            "camera unplugged",
        ))]);
        let error = writer.push_stream(failing).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        assert_eq!(writer.status(), TopicWriterStatus::Raised);
        assert_eq!(
            writer.last_error().as_deref(),
            Some("cannot read batch: camera unplugged")
        );
    }

    async fn wait_for_the_upload_to_fail(writer: &TopicWriter) {
        for _ in 0..500 {
            let guard = writer.topic.slot.lock().await;
            if guard.as_ref().is_some_and(|slot| slot.writer.failed()) {
                return;
            }
            drop(guard);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the upload did not fail");
    }

    #[tokio::test]
    async fn dead_upload_finalizes_topic() {
        for policy in [
            TopicLevelErrorPolicy::Raise,
            TopicLevelErrorPolicy::Ignore,
            TopicLevelErrorPolicy::Finalize,
        ] {
            let writer = offline_writer(policy);
            wait_for_the_upload_to_fail(&writer).await;
            let row = batch(vec![Some(1)]);
            let error = writer.push_row(row.clone()).await.unwrap_err();
            assert_ne!(error.kind(), ErrorKind::Closed);
            assert!(
                error
                    .to_string()
                    .starts_with("the upload of topic '/t' failed: ")
            );
            assert_eq!(writer.status(), TopicWriterStatus::FinalizedWithError);
            assert_eq!(writer.last_error(), Some(error.to_string()));
            let recorded = writer.topic.upload_failure().unwrap();
            assert_eq!(recorded.to_string(), error.to_string());
            assert_eq!(recorded.kind(), error.kind());
            let again = writer.push_row(row).await.unwrap_err();
            assert_eq!(again.kind(), ErrorKind::Closed);
            assert_eq!(
                again.to_string(),
                format!("topic '/t' is finalized: {error}")
            );
            assert_eq!(
                writer.handle_error(&again.to_string()).await.unwrap(),
                policy == TopicLevelErrorPolicy::Ignore
            );
            assert_eq!(writer.status(), TopicWriterStatus::FinalizedWithError);
            assert_eq!(writer.last_error(), Some(error.to_string()));
            assert_eq!(kind(writer.flush().await), ErrorKind::Closed);
            writer.finalize(None).await.unwrap();
        }
    }

    #[tokio::test]
    async fn upload_failure_at_close() {
        let writer = offline_writer(TopicLevelErrorPolicy::Raise);
        wait_for_the_upload_to_fail(&writer).await;
        let error = writer.finalize(None).await.unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("the upload of topic '/t' failed: ")
        );
        assert_eq!(writer.last_error(), Some(error.to_string()));
        assert_eq!(
            writer.topic.upload_failure().map(|e| e.to_string()),
            Some(error.to_string())
        );
    }

    #[tokio::test]
    async fn cancelled_close_status() {
        use futures::FutureExt;

        let cancelled = offline_writer(TopicLevelErrorPolicy::Raise);
        assert!(cancelled.finalize(None).now_or_never().is_none());
        let failed = offline_writer(TopicLevelErrorPolicy::Ignore);
        wait_for_the_upload_to_fail(&failed).await;
        let row = batch(vec![Some(1)]);
        assert!(failed.push_row(row.clone()).now_or_never().is_none());
        for writer in [cancelled, failed] {
            assert!(writer.topic.slot.lock().await.is_none());
            let error = writer.finalize(None).await.unwrap_err();
            assert!(
                error
                    .to_string()
                    .starts_with("the upload of topic '/t' failed: ")
            );
            assert_eq!(writer.status(), TopicWriterStatus::FinalizedWithError);
            assert_eq!(writer.last_error(), Some(error.to_string()));
            let pushed = writer.push_row(row.clone()).await.unwrap_err();
            assert_eq!(
                pushed.to_string(),
                format!("topic '/t' is finalized: {error}")
            );
        }
    }

    #[tokio::test]
    async fn discard_skips_pending_close() {
        use futures::FutureExt;

        let writer = offline_writer(TopicLevelErrorPolicy::Raise);
        assert!(writer.finalize(None).now_or_never().is_none());
        let discarded = writer.topic.close(Close::Discard("deleted")).now_or_never();
        assert!(matches!(discarded, Some(Ok(()))));
        assert_eq!(writer.status(), TopicWriterStatus::FinalizedWithError);
        assert_eq!(writer.last_error().as_deref(), Some("deleted"));
        writer.finalize(None).await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_push_status() {
        use futures::FutureExt;

        let writer = offline_writer(TopicLevelErrorPolicy::Ignore);
        wait_for_the_upload_to_fail(&writer).await;
        let row = batch(vec![Some(1)]);
        assert!(writer.push_row(row.clone()).now_or_never().is_none());
        let error = writer.push_row(row.clone()).await.unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("the upload of topic '/t' failed: ")
        );
        assert_eq!(writer.status(), TopicWriterStatus::FinalizedWithError);
        assert_eq!(writer.last_error(), Some(error.to_string()));
        let pushed = writer.push_row(row).await.unwrap_err();
        assert_eq!(
            pushed.to_string(),
            format!("topic '/t' is finalized: {error}")
        );
    }

    #[tokio::test]
    async fn interrupted_upload_not_failed() {
        let writer = offline_writer(TopicLevelErrorPolicy::Raise);
        let (closed, report) = writer.topic.upload_error(interrupted_error(), false);
        assert_eq!(closed.kind(), ErrorKind::Closed);
        assert!(report.is_none());
        assert!(writer.topic.upload_failure().is_none());
        let (failed, report) = writer
            .topic
            .upload_error(Error::new(ErrorKind::Connection, "broken pipe"), true);
        assert!(report.is_none());
        assert_eq!(
            failed.to_string(),
            "the upload of topic '/t' failed: broken pipe"
        );
        assert!(writer.topic.upload_failure().is_some());
    }

    #[test]
    fn final_status_is_sticky() {
        let status = Status::new();
        status.set(
            TopicWriterStatus::FinalizedWithError,
            Some("dead".to_owned()),
        );
        status.set(TopicWriterStatus::Raised, Some("late".to_owned()));
        status.set(TopicWriterStatus::Active, None);
        assert_eq!(status.lock().status, TopicWriterStatus::FinalizedWithError);
        assert_eq!(status.lock().last_error.as_deref(), Some("dead"));
        status.set(TopicWriterStatus::Finalized, None);
        assert_eq!(status.lock().status, TopicWriterStatus::Finalized);
    }

    #[test]
    fn flush_policy_builder() {
        let policy = FlushPolicy::default()
            .max_bytes(10)
            .max_rows(2)
            .max_age(Duration::from_millis(5));
        assert_eq!(
            (policy.max_bytes, policy.max_rows, policy.max_age),
            (Some(10), Some(2), Some(Duration::from_millis(5)))
        );
    }

    #[test]
    fn late_drop_keeps_newer_error() {
        for (before, at_push, after) in [
            (
                TopicWriterStatus::Active,
                false,
                TopicWriterStatus::RecordTooLarge,
            ),
            (
                TopicWriterStatus::RecordTooLarge,
                false,
                TopicWriterStatus::RecordTooLarge,
            ),
            (TopicWriterStatus::Raised, false, TopicWriterStatus::Raised),
            (
                TopicWriterStatus::IgnoredLastError,
                false,
                TopicWriterStatus::IgnoredLastError,
            ),
            (
                TopicWriterStatus::Raised,
                true,
                TopicWriterStatus::RecordTooLarge,
            ),
            (
                TopicWriterStatus::IgnoredLastError,
                true,
                TopicWriterStatus::RecordTooLarge,
            ),
            (
                TopicWriterStatus::Finalized,
                true,
                TopicWriterStatus::Finalized,
            ),
            (
                TopicWriterStatus::FinalizedWithError,
                false,
                TopicWriterStatus::FinalizedWithError,
            ),
        ] {
            let status = Status::new();
            status.set(before, Some("before".to_owned()));
            status.record_dropped(2, "dropped".to_owned(), at_push);
            assert_eq!(status.lock().status, after, "{before:?} at push {at_push}");
            let last = match after {
                TopicWriterStatus::RecordTooLarge => "dropped",
                _ => "before",
            };
            assert_eq!(status.lock().last_error.as_deref(), Some(last));
            assert_eq!(status.dropped(), 2);
        }
    }

    #[tokio::test]
    async fn dropped_record_message() {
        let reporter = Reporter::new(
            "/t".to_owned(),
            "run/t".to_owned(),
            Connection::offline(1 << 20),
        );
        reporter.record_dropped(3, 2_000_000, false).await;
        assert_eq!(
            reporter.status.lock().last_error,
            Some(format!(
                "3 record(s) dropped for topic '/t': each is larger alone than the message size limit ({} bytes), the largest is 2000000 bytes",
                reporter.limit
            ))
        );
    }

    fn batch(timestamps: Vec<Option<i64>>) -> RecordBatch {
        let column: ArrayRef = Arc::new(Int64Array::from(timestamps));
        RecordBatch::try_from_iter([(TIMESTAMP, column)]).unwrap()
    }

    fn kind(result: Result<impl std::fmt::Debug>) -> ErrorKind {
        result.unwrap_err().kind()
    }

    #[test]
    fn equal_timestamps_accepted() {
        let range = timestamp_range(&batch(vec![Some(1), Some(1), Some(2), Some(2)]));
        assert_eq!(range.unwrap(), Some((1, 2)));
        assert_eq!(timestamp_range(&batch(vec![])).unwrap(), None);
    }

    #[test]
    fn rejects_bad_timestamps() {
        let decreasing = timestamp_range(&batch(vec![Some(2), Some(1)]));
        assert_eq!(kind(decreasing), ErrorKind::InvalidArgument);
        let nulls = timestamp_range(&batch(vec![Some(1), None]));
        assert_eq!(kind(nulls), ErrorKind::InvalidArgument);
    }

    #[test]
    fn push_after_last_timestamp() {
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

    #[derive(Default)]
    struct NoColumns;

    impl<T> RecordBuilder<T> for NoColumns {
        fn append(&mut self, _: &T) {}

        fn finish(&mut self) -> Vec<ArrayRef> {
            Vec::new()
        }
    }

    macro_rules! empty_record {
        ($name:ident, $tag:literal, $format:expr, $size:expr) => {
            struct $name;

            impl Ontology for $name {
                const TAG: &'static str = $tag;
                type Columns = ();
                type View<'a> = ();

                fn columns(_: &RecordBatch) -> Result<()> {
                    Ok(())
                }

                fn view(_: &(), _: usize) {}

                fn from_view(_: &()) -> Result<Self> {
                    Ok(Self)
                }
            }

            impl Encode for $name {
                const FORMAT: Format = $format;
                type Builder = NoColumns;

                fn fields() -> Fields {
                    Fields::empty()
                }

                fn size(&self) -> usize {
                    $size
                }
            }
        };
    }

    empty_record!(Empty, "Empty", Format::Default, 8);
    empty_record!(Other, "Other", Format::Default, 8);
    empty_record!(HugeOther, "Other", Format::Default, 1 << 21);
    empty_record!(RaggedEmpty, "Empty", Format::Ragged, 8);

    #[tokio::test]
    async fn push_checks_tag_and_format() {
        let writer = offline_writer(TopicLevelErrorPolicy::Raise);
        let error = writer.push(1, &Other).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        assert_eq!(
            error.to_string(),
            "topic '/t' stores Empty (Default), not Other (Default)"
        );
        let error = writer.push(1, &RaggedEmpty).await.unwrap_err();
        assert_eq!(
            error.to_string(),
            "topic '/t' stores Empty (Default), not Empty (Ragged)"
        );
        let error = writer.push(1, &HugeOther).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        assert_eq!(writer.dropped_record_count(), 0);
        writer.push(1, &Empty).await.unwrap();
        writer.push(2, &Empty).await.unwrap();
    }

    #[tokio::test]
    async fn oversized_row_checked_first() {
        let writer = offline_writer(TopicLevelErrorPolicy::Raise);
        let timestamps: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let payload: ArrayRef = Arc::new(arrow_array::BinaryArray::from(vec![
            vec![0u8; 1 << 21].as_slice(),
        ]));
        let row =
            RecordBatch::try_from_iter([(TIMESTAMP, timestamps), ("payload", payload)]).unwrap();
        assert!(writer.push_row(row).await.is_err());
        assert_eq!(writer.dropped_record_count(), 0);
    }
}
