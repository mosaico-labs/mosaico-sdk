use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use mosaico_core::{Format, Timestamp, TimestampRange};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyList};
use tokio::sync::Mutex;

use crate::ahead::{Busy, Feed, Merged};
use crate::arrow::ffi::{BlockingReader, RawBatch, SchemaHolder, stream_capsule};
use crate::errors::{ServerError, StreamClosedError, invalid, sequence_error, to_py};
use crate::runtime::{ProcessBound, enter, run, run_now, wait, wait_now};

type SessionTuple = (String, i64, Option<i64>, Vec<String>, bool);

fn range(start: Option<i64>, end: Option<i64>) -> TimestampRange {
    TimestampRange::new(start.map(Timestamp::from_ns), end.map(Timestamp::from_ns))
}

#[pyclass(frozen, module = "mosaico_rs._core")]
pub(crate) struct SequenceHandler(pub(crate) ProcessBound<mosaico_core::SequenceHandler>);

#[pymethods]
impl SequenceHandler {
    #[getter]
    fn name(&self) -> PyResult<String> {
        Ok(self.0.get()?.name().to_owned())
    }

    #[getter]
    fn created_timestamp(&self) -> PyResult<i64> {
        Ok(self.0.get()?.created_timestamp().as_ns())
    }

    #[getter]
    fn user_metadata(&self) -> PyResult<Option<String>> {
        let handler = self.0.get()?;
        handler.user_metadata().map_err(to_py)?;
        Ok(handler.user_metadata_json().map(str::to_owned))
    }

    #[getter]
    fn sessions(&self) -> PyResult<Vec<SessionTuple>> {
        Ok(self
            .0
            .get()?
            .sessions()
            .iter()
            .map(|s| {
                (
                    s.locator.clone(),
                    s.created_timestamp.as_ns(),
                    s.completed_timestamp.map(Timestamp::as_ns),
                    s.topics.clone(),
                    s.locked,
                )
            })
            .collect())
    }

    #[getter]
    fn topics(&self) -> PyResult<Vec<String>> {
        Ok(self.0.get()?.topics().map(str::to_owned).collect())
    }

    #[getter]
    fn timestamp_ns_min(&self) -> PyResult<Option<i64>> {
        Ok(self.0.get()?.timestamp_ns_min().map(Timestamp::as_ns))
    }

    #[getter]
    fn timestamp_ns_max(&self) -> PyResult<Option<i64>> {
        Ok(self.0.get()?.timestamp_ns_max().map(Timestamp::as_ns))
    }

    fn topic_handler(&self, topic: &str) -> PyResult<TopicHandler> {
        let handler = self.0.get()?.topic_handler(topic).map_err(to_py)?;
        Ok(TopicHandler(ProcessBound::new(handler)))
    }

    #[pyo3(signature = (topics, start_timestamp_ns = None, end_timestamp_ns = None, join = false))]
    fn get_window_streamer(
        &self,
        py: Python<'_>,
        topics: Vec<Bound<'_, PyAny>>,
        start_timestamp_ns: Option<i64>,
        end_timestamp_ns: Option<i64>,
        join: bool,
    ) -> PyResult<WindowStream> {
        let handler = self.0.get()?;
        let mut selected = Vec::with_capacity(topics.len());
        for topic in &topics {
            let Ok(given) = topic.cast::<TopicHandler>() else {
                return Err(invalid(format!(
                    "topics must hold TopicHandler objects, not {topic}"
                )));
            };
            selected.push(given.get().0.get()?.clone());
        }
        let streamer = run(
            py,
            handler.get_window_streamer_for(selected, range(start_timestamp_ns, end_timestamp_ns)),
        )?;
        let schemas = streamer.schemas();
        let msg_count = streamer.msg_count();
        let timestamp_ns_min = streamer.timestamp_ns_min().map(Timestamp::as_ns);
        let timestamp_ns_max = streamer.timestamp_ns_max().map(Timestamp::as_ns);
        let handles = ProcessBound::new(streamer.close_handles());
        let feed = Feed::new(streamer, join);
        let busy = feed.busy();
        Ok(WindowStream {
            schemas,
            msg_count,
            timestamp_ns_min,
            timestamp_ns_max,
            handles,
            checked: AtomicU64::new(u64::MAX),
            busy,
            inner: ProcessBound::new(Mutex::new(Delivered::new(feed))),
        })
    }

    fn reload(&self, py: Python<'_>) -> PyResult<SequenceHandler> {
        let mut handler = self.0.get()?.clone();
        wait(py, handler.reload())?.map_err(sequence_error)?;
        Ok(SequenceHandler(ProcessBound::new(handler)))
    }

    fn close(&self) -> PyResult<()> {
        self.0.get()?.close();
        Ok(())
    }
}

#[pyclass(frozen, module = "mosaico_rs._core")]
pub(crate) struct TopicHandler(pub(crate) ProcessBound<mosaico_core::TopicHandler>);

#[pymethods]
impl TopicHandler {
    #[getter]
    fn sequence_name(&self) -> PyResult<String> {
        Ok(self.0.get()?.sequence_name().to_owned())
    }

    #[getter]
    fn name(&self) -> PyResult<String> {
        Ok(self.0.get()?.name().to_owned())
    }

    #[getter]
    fn ontology_tag(&self) -> PyResult<String> {
        Ok(self.0.get()?.ontology_tag().to_owned())
    }

    #[getter]
    fn user_metadata(&self) -> PyResult<Option<String>> {
        let handler = self.0.get()?;
        handler.user_metadata().map_err(to_py)?;
        Ok(handler.user_metadata_json().map(str::to_owned))
    }

    #[getter]
    fn timestamp_ns_min(&self) -> PyResult<Option<i64>> {
        Ok(self.0.get()?.timestamp_ns_min().map(Timestamp::as_ns))
    }

    #[getter]
    fn timestamp_ns_max(&self) -> PyResult<Option<i64>> {
        Ok(self.0.get()?.timestamp_ns_max().map(Timestamp::as_ns))
    }

    #[getter]
    fn msg_count(&self) -> PyResult<u64> {
        Ok(self.0.get()?.msg_count())
    }

    #[getter]
    fn created_timestamp(&self) -> PyResult<i64> {
        Ok(self.0.get()?.created_timestamp().as_ns())
    }

    #[getter]
    fn completed_timestamp(&self) -> PyResult<Option<i64>> {
        Ok(self.0.get()?.completed_timestamp().map(Timestamp::as_ns))
    }

    #[getter]
    fn locked(&self) -> PyResult<bool> {
        Ok(self.0.get()?.locked())
    }

    #[getter]
    fn serialization_format(&self) -> PyResult<&'static str> {
        match self.0.get()?.serialization_format().map_err(to_py)? {
            Format::Default => Ok("default"),
            Format::Ragged => Ok("ragged"),
            Format::Image => Ok("image"),
            other => Err(ServerError::new_err(format!(
                "unknown serialization format {other:?}"
            ))),
        }
    }

    #[getter]
    fn chunks_number(&self) -> PyResult<u64> {
        Ok(self.0.get()?.chunks_number())
    }

    #[getter]
    fn total_size_bytes(&self) -> PyResult<u64> {
        Ok(self.0.get()?.total_size_bytes())
    }

    fn ontology_schema(&self, py: Python<'_>) -> PyResult<SchemaHolder> {
        let handler = self.0.get()?;
        let fields = run(py, handler.ontology_schema())?;
        Ok(SchemaHolder(Arc::new(Schema::new(fields))))
    }

    #[pyo3(signature = (start_timestamp_ns = None, end_timestamp_ns = None, join = false))]
    fn get_batch_streamer(
        &self,
        py: Python<'_>,
        start_timestamp_ns: Option<i64>,
        end_timestamp_ns: Option<i64>,
        join: bool,
    ) -> PyResult<BatchStream> {
        let handler = self.0.get()?;
        let streamer = run(
            py,
            handler.get_batch_streamer(range(start_timestamp_ns, end_timestamp_ns)),
        )?;
        let schema = streamer.schema().clone();
        let handle = ProcessBound::new(streamer.close_handle());
        let feed = Feed::new(streamer, join);
        let busy = feed.busy();
        Ok(BatchStream {
            schema,
            handle,
            busy,
            inner: ProcessBound::new(Mutex::new(Some(Delivered::new(feed)))),
        })
    }

    fn close(&self) -> PyResult<()> {
        self.0.get()?.close();
        Ok(())
    }
}

struct Delivered<S, T> {
    source: S,
    count: u64,
    last: Option<T>,
}

impl<S, T: Clone> Delivered<S, T> {
    fn new(source: S) -> Self {
        Self {
            source,
            count: 0,
            last: None,
        }
    }

    async fn next<F>(&mut self, seq: u64, fetch: F) -> mosaico_core::Result<Option<T>>
    where
        F: AsyncFnOnce(&mut S) -> mosaico_core::Result<Option<T>>,
    {
        if seq != 0 && seq == self.count {
            return Ok(self.last.clone());
        }
        if seq != self.count + 1 {
            return Err(mosaico_core::Error::invalid_argument(format!(
                "item {seq} requested after item {}",
                self.count
            )));
        }
        let next = match fetch(&mut self.source).await {
            Ok(next) => next,
            Err(e) => {
                self.last = None;
                return Err(e);
            }
        };
        self.count = seq;
        self.last = next.clone();
        Ok(next)
    }
}

type BatchFeed = Delivered<Feed<mosaico_core::TopicBatchStreamer>, RecordBatch>;

#[pyclass(frozen, module = "mosaico_rs._core")]
pub(crate) struct BatchStream {
    schema: SchemaRef,
    handle: ProcessBound<mosaico_core::CloseHandle>,
    busy: Busy,
    inner: ProcessBound<Mutex<Option<BatchFeed>>>,
}

#[pymethods]
impl BatchStream {
    #[getter]
    fn schema(&self) -> SchemaHolder {
        SchemaHolder(self.schema.clone())
    }

    #[pyo3(signature = (seq, contended = false))]
    fn next_batch(&self, py: Python<'_>, seq: u64, contended: bool) -> PyResult<Option<RawBatch>> {
        if self.closed()? {
            self.release()?;
            return Err(StreamClosedError::new_err("stream closed"));
        }
        let _entered = enter(self as *const Self as usize)?;
        let inner = self.inner.get()?;
        let next = wait_now(py, async {
            let mut state = inner.lock().await;
            let delivered = state.as_mut()?;
            Some(
                delivered
                    .next(seq, async |feed| feed.next(contended).await)
                    .await,
            )
        })?
        .ok_or_else(|| {
            StreamClosedError::new_err(
                "the stream was exported with __arrow_c_stream__: read it from the exported reader",
            )
        })?
        .map_err(to_py)?;
        Ok(next.map(RawBatch::new))
    }

    fn is_closed(&self) -> PyResult<bool> {
        self.closed()
    }

    fn close(&self) -> PyResult<()> {
        self.handle.get()?.close();
        self.release()
    }

    fn export<'py>(&self, py: Python<'py>, seen: u64) -> PyResult<Bound<'py, PyCapsule>> {
        self.export_after(py, Some(seen))
    }

    #[pyo3(signature = (requested_schema = None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        self.export_after(py, None)
    }
}

impl BatchStream {
    fn closed(&self) -> PyResult<bool> {
        Ok(self.handle.get()?.is_closed() && !self.busy.holds_open())
    }

    fn export_after<'py>(
        &self,
        py: Python<'py>,
        seen: Option<u64>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        if self.closed()? {
            self.release()?;
            return Err(StreamClosedError::new_err("stream closed"));
        }
        let _entered = enter(self as *const Self as usize)?;
        let inner = self.inner.get()?;
        let taken = wait(py, async { inner.lock().await.take() })?;
        let Some(delivered) = taken else {
            return Err(StreamClosedError::new_err(
                "the stream was already exported with __arrow_c_stream__",
            ));
        };
        let pending = delivered
            .last
            .filter(|_| seen.is_some_and(|seen| delivered.count == seen.saturating_add(1)));
        let Some(source) = delivered.source.into_direct() else {
            return Err(invalid("a stream opened with join=True cannot be exported"));
        };
        let reader = BlockingReader::new(self.schema.clone(), source, pending);
        stream_capsule(py, Box::new(reader))
    }

    fn release(&self) -> PyResult<()> {
        if let Ok(mut inner) = self.inner.get()?.try_lock()
            && let Some(delivered) = inner.as_mut()
        {
            delivered.source.close();
            delivered.last = None;
        }
        Ok(())
    }
}

type Window<'py> = (Vec<(u16, RawBatch, Bound<'py, PyList>)>, Vec<u16>);

#[pyclass(frozen, module = "mosaico_rs._core")]
pub(crate) struct WindowStream {
    schemas: Vec<SchemaRef>,
    msg_count: u64,
    timestamp_ns_min: Option<i64>,
    timestamp_ns_max: Option<i64>,
    handles: ProcessBound<Vec<mosaico_core::CloseHandle>>,
    checked: AtomicU64,
    busy: Busy,
    inner: ProcessBound<Mutex<Delivered<Feed<mosaico_core::SequenceWindowStreamer>, Merged>>>,
}

#[pymethods]
impl WindowStream {
    #[getter]
    fn schemas(&self) -> Vec<SchemaHolder> {
        self.schemas.iter().cloned().map(SchemaHolder).collect()
    }

    #[getter]
    fn msg_count(&self) -> u64 {
        self.msg_count
    }

    #[getter]
    fn timestamp_ns_min(&self) -> Option<i64> {
        self.timestamp_ns_min
    }

    #[getter]
    fn timestamp_ns_max(&self) -> Option<i64> {
        self.timestamp_ns_max
    }

    #[pyo3(signature = (seq, contended = false))]
    fn next_window<'py>(
        &self,
        py: Python<'py>,
        seq: u64,
        contended: bool,
    ) -> PyResult<Option<Window<'py>>> {
        if self.is_closed()? {
            self.close()?;
            return Err(StreamClosedError::new_err("stream closed"));
        }
        let _entered = enter(self as *const Self as usize)?;
        let inner = self.inner.get()?;
        let next = run_now(py, async {
            inner
                .lock()
                .await
                .next(seq, async |feed| feed.next(contended).await)
                .await
        })?;
        let Some(window) = next else {
            return Ok(None);
        };
        let mut batches = Vec::with_capacity(window.parts.len());
        for part in window.parts {
            let timestamps = PyList::new(py, part.timestamps.values().iter().copied())?;
            batches.push((part.topic, RawBatch::new(part.batch), timestamps));
        }
        Ok(Some((batches, window.order)))
    }

    fn is_closed(&self) -> PyResult<bool> {
        Ok(
            mosaico_core::CloseHandle::any_closed(self.handles.get()?, &self.checked)
                && !self.busy.holds_open(),
        )
    }

    fn close(&self) -> PyResult<()> {
        self.handles.get()?.iter().for_each(|h| h.close());
        if let Ok(mut inner) = self.inner.get()?.try_lock() {
            inner.source.close();
            inner.last = None;
        }
        Ok(())
    }
}
