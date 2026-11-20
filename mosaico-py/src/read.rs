use arrow_schema::SchemaRef;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use tokio::sync::Mutex;

use crate::arrow::ffi::{BlockingReader, RawBatch, SchemaHolder, stream_capsule};
use crate::errors::{StreamClosedError, to_py};
use crate::runtime::{ProcessBound, run, wait};

type SessionTuple = (String, i64, Option<i64>, Vec<String>, bool);

#[pyclass(frozen, module = "mosaico_rs._core")]
pub(crate) struct SequenceHandler(pub(crate) ProcessBound<mosaico_core::SequenceHandler>);

#[pymethods]
impl SequenceHandler {
    #[getter]
    fn name(&self) -> PyResult<String> {
        Ok(self.0.get()?.name().to_owned())
    }

    #[getter]
    fn created_at_ns(&self) -> PyResult<i64> {
        Ok(self.0.get()?.created_at_ns())
    }

    #[getter]
    fn user_metadata(&self) -> PyResult<Option<String>> {
        Ok(self.0.get()?.user_metadata().map(str::to_owned))
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
                    s.created_at_ns,
                    s.completed_at_ns,
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
        Ok(self.0.get()?.timestamp_ns_min())
    }

    #[getter]
    fn timestamp_ns_max(&self) -> PyResult<Option<i64>> {
        Ok(self.0.get()?.timestamp_ns_max())
    }

    fn topic_handler(&self, topic: &str) -> PyResult<TopicHandler> {
        let handler = self.0.get()?.topic_handler(topic).map_err(to_py)?;
        Ok(TopicHandler(ProcessBound::new(handler)))
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
    fn sequence(&self) -> PyResult<String> {
        Ok(self.0.get()?.sequence().to_owned())
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
    fn timestamp_ns_min(&self) -> PyResult<Option<i64>> {
        Ok(self.0.get()?.timestamp_ns_min())
    }

    #[getter]
    fn timestamp_ns_max(&self) -> PyResult<Option<i64>> {
        Ok(self.0.get()?.timestamp_ns_max())
    }

    #[pyo3(signature = (start_ns = None, end_ns = None))]
    fn get_batch_streamer(
        &self,
        py: Python<'_>,
        start_ns: Option<i64>,
        end_ns: Option<i64>,
    ) -> PyResult<BatchStream> {
        let handler = self.0.get()?;
        let streamer = run(py, handler.get_batch_streamer(start_ns, end_ns))?;
        Ok(BatchStream {
            schema: streamer.schema().clone(),
            handle: ProcessBound::new(streamer.close_handle()),
            inner: ProcessBound::new(Mutex::new(Some(streamer))),
        })
    }

    fn close(&self) -> PyResult<()> {
        self.0.get()?.close();
        Ok(())
    }
}

#[pyclass(frozen, module = "mosaico_rs._core")]
pub(crate) struct BatchStream {
    schema: SchemaRef,
    handle: ProcessBound<mosaico_core::CloseHandle>,
    inner: ProcessBound<Mutex<Option<mosaico_core::BatchStreamer>>>,
}

#[pymethods]
impl BatchStream {
    #[getter]
    fn schema(&self) -> SchemaHolder {
        SchemaHolder(self.schema.clone())
    }

    fn next_batch(&self, py: Python<'_>) -> PyResult<Option<RawBatch>> {
        let inner = self.inner.get()?;
        let next = wait(py, async {
            match inner.lock().await.as_mut() {
                Some(streamer) => Some(streamer.next_batch().await),
                None => None,
            }
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
        Ok(self.handle.get()?.is_closed())
    }

    fn close(&self) -> PyResult<()> {
        self.handle.get()?.close();
        Ok(())
    }

    #[pyo3(signature = (requested_schema = None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let inner = self.inner.get()?;
        let taken = wait(py, async { inner.lock().await.take() })?;
        let Some(streamer) = taken else {
            return Err(StreamClosedError::new_err(
                "the stream was already exported with __arrow_c_stream__",
            ));
        };
        let reader = BlockingReader::new(self.schema.clone(), streamer);
        stream_capsule(py, Box::new(reader))
    }
}
