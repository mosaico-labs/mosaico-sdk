use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as SchemaLock, PoisonError};

use mosaico_core::{
    Encode, Error, FlushPolicy, Format, Interrupter, TopicErrorPolicy, TopicWriterStatus,
};
use pyo3::exceptions::PyBaseException;
use pyo3::prelude::*;
use tokio::sync::Mutex;

use crate::arrow::ffi::{Imported, KnownSchema, import_batch, import_schema};
use crate::errors::{
    MosaicoError, StreamClosedError, invalid, parse_seconds, sequence_error, to_py,
};
use crate::record;
use crate::runtime::{Closing, ProcessBound, run, run_now, wait, wait_closing};

#[pyclass(frozen, module = "mosaico_rs._core")]
pub(crate) struct SequenceWriter {
    name: String,
    session_locator: String,
    interrupter: Interrupter,
    left_open: AtomicBool,
    inner: ProcessBound<Mutex<Option<mosaico_core::SequenceWriter>>>,
}

impl SequenceWriter {
    pub(crate) fn new(writer: mosaico_core::SequenceWriter) -> Self {
        Self {
            name: writer.name().to_owned(),
            session_locator: writer.session_locator().to_owned(),
            interrupter: writer.interrupter(),
            left_open: AtomicBool::new(false),
            inner: ProcessBound::new(Mutex::new(Some(writer))),
        }
    }

    fn closed(
        &self,
        py: Python<'_>,
        closing: Closing<Option<mosaico_core::Result<()>>>,
    ) -> PyResult<Option<PyErr>> {
        let left_open = |result: &mosaico_core::Result<()>| {
            if result.as_ref().is_err_and(Error::left_open) {
                self.left_open.store(true, Ordering::Relaxed);
            }
        };
        match closing {
            Closing::Done(result) => {
                let result = result.ok_or_else(ended)?;
                left_open(&result);
                result.map(|()| None).map_err(sequence_error)
            }
            Closing::Interrupted(result, signal) => {
                let result = result.ok_or_else(ended)?;
                left_open(&result);
                match result {
                    Ok(()) => Ok(Some(signal)),
                    Err(error) => Err(interrupted(py, signal, error, sequence_error)),
                }
            }
            Closing::Abandoned(signal) => {
                self.left_open.store(true, Ordering::Relaxed);
                Err(signal)
            }
        }
    }
}

fn ended() -> PyErr {
    StreamClosedError::new_err("the write session has already ended")
}

fn interrupted(py: Python<'_>, signal: PyErr, error: Error, convert: fn(Error) -> PyErr) -> PyErr {
    let left_open = error.left_open();
    let error = convert(error);
    if left_open || !error.is_instance_of::<StreamClosedError>(py) {
        signal.set_cause(py, Some(error));
    }
    signal
}

fn push_value<T: Encode>(
    py: Python<'_>,
    writer: &mosaico_core::TopicWriter,
    timestamp_ns: i64,
    value: PyResult<T>,
) -> PyResult<()> {
    match value {
        Ok(value) => run_now(py, writer.push(timestamp_ns, &value)),
        Err(error) => {
            let message = error
                .value(py)
                .str()
                .map_or_else(|_| error.to_string(), |text| text.to_string());
            let error = Error::invalid_argument(message);
            Err(to_py(wait(py, writer.reject(error))?))
        }
    }
}

fn count(name: &str, value: Option<i64>) -> PyResult<Option<usize>> {
    value
        .map(|value| {
            usize::try_from(value).map_err(|_| {
                invalid(format!(
                    "{name} must be a non-negative integer, got {value}"
                ))
            })
        })
        .transpose()
}

#[pymethods]
impl SequenceWriter {
    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn session_locator(&self) -> &str {
        &self.session_locator
    }

    #[getter]
    fn left_open(&self) -> bool {
        self.left_open.load(Ordering::Relaxed)
    }

    #[pyo3(signature = (name, ontology_tag, format, schema, metadata, on_error))]
    #[allow(clippy::too_many_arguments)]
    fn topic_create(
        &self,
        py: Python<'_>,
        name: &str,
        ontology_tag: &str,
        format: &str,
        schema: &Bound<'_, PyAny>,
        metadata: Option<Vec<u8>>,
        on_error: &str,
    ) -> PyResult<TopicWriter> {
        let format = match format {
            "default" => Format::Default,
            "ragged" => Format::Ragged,
            "image" => Format::Image,
            other => return Err(invalid(format!("unknown serialization format '{other}'"))),
        };
        let on_error = match on_error {
            "raise" => TopicErrorPolicy::Raise,
            "finalize" => TopicErrorPolicy::Finalize,
            "ignore" => TopicErrorPolicy::Ignore,
            other => {
                return Err(invalid(format!(
                    "topic error policy must be 'raise', 'finalize' or 'ignore', not '{other}'"
                )));
            }
        };
        let schema = import_schema(schema)?;
        let inner = self.inner.get()?;
        let created = wait_closing(
            py,
            async {
                match inner.lock().await.as_mut() {
                    Some(writer) => Some(
                        writer
                            .topic_create(name, ontology_tag, format, schema, metadata, on_error)
                            .await,
                    ),
                    None => None,
                }
            },
            &self.interrupter,
        )?;
        let writer = match created {
            Closing::Done(created) => created.ok_or_else(ended)?.map_err(sequence_error)?,
            Closing::Interrupted(Some(Err(error)), signal) => {
                return Err(interrupted(py, signal, error, sequence_error));
            }
            Closing::Interrupted(_, signal) | Closing::Abandoned(signal) => return Err(signal),
        };
        Ok(TopicWriter::new(writer, self.interrupter.clone()))
    }

    fn finish(&self, py: Python<'_>) -> PyResult<Option<Py<PyBaseException>>> {
        let inner = self.inner.get()?;
        let closing = wait_closing(
            py,
            async {
                let writer = inner.lock().await.take();
                match writer {
                    Some(writer) => Some(writer.finish().await),
                    None => None,
                }
            },
            &self.interrupter,
        )?;
        Ok(self
            .closed(py, closing)?
            .map(|signal| signal.into_value(py)))
    }

    #[pyo3(signature = (reason, interrupted = false))]
    fn abort(&self, py: Python<'_>, reason: &str, interrupted: bool) -> PyResult<()> {
        if interrupted {
            self.interrupter.interrupt();
        }
        let inner = self.inner.get()?;
        let closing = wait_closing(
            py,
            async {
                let writer = inner.lock().await.take();
                match writer {
                    Some(writer) => Some(writer.abort(reason).await),
                    None => None,
                }
            },
            &self.interrupter,
        )?;
        match self.closed(py, closing)? {
            Some(signal) => Err(signal),
            None => Ok(()),
        }
    }
}

#[pyclass(frozen, module = "mosaico_rs._core")]
pub(crate) struct TopicWriter(
    ProcessBound<mosaico_core::TopicWriter>,
    SchemaLock<Option<Arc<KnownSchema>>>,
    Interrupter,
);

impl TopicWriter {
    fn new(writer: mosaico_core::TopicWriter, interrupter: Interrupter) -> Self {
        Self(
            ProcessBound::new(writer),
            SchemaLock::new(None),
            interrupter,
        )
    }

    fn known_schema(&self) -> std::sync::MutexGuard<'_, Option<Arc<KnownSchema>>> {
        self.1.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn import(&self, batch: &Bound<'_, PyAny>) -> Result<Imported, String> {
        let known = self.known_schema().clone();
        let imported = import_batch(batch, known)?;
        *self.known_schema() = Some(imported.known().clone());
        Ok(imported)
    }
}

#[pymethods]
impl TopicWriter {
    #[getter]
    fn name(&self) -> PyResult<String> {
        Ok(self.0.get()?.name().to_owned())
    }

    #[getter]
    fn status(&self) -> PyResult<&'static str> {
        match self.0.get()?.status() {
            TopicWriterStatus::Active => Ok("active"),
            TopicWriterStatus::Finalized => Ok("finalized"),
            TopicWriterStatus::FinalizedWithError => Ok("finalized_with_error"),
            TopicWriterStatus::IgnoredLastError => Ok("ignored_last_error"),
            TopicWriterStatus::Raised => Ok("raised_exception"),
            TopicWriterStatus::RecordTooLarge => Ok("record_too_large"),
            other => Err(MosaicoError::new_err(format!(
                "unknown topic writer status {other:?}"
            ))),
        }
    }

    #[getter]
    fn last_error(&self) -> PyResult<Option<String>> {
        Ok(self.0.get()?.last_error())
    }

    #[getter]
    fn dropped_records(&self) -> PyResult<u64> {
        Ok(self.0.get()?.dropped_records())
    }

    fn push_batch(&self, py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<()> {
        let writer = self.0.get()?;
        let batch = self.import(batch);
        run(py, async {
            match batch.and_then(Imported::owned) {
                Ok(batch) => writer.push_batch(batch).await,
                Err(message) => Err(writer.reject(Error::invalid_argument(message)).await),
            }
        })
    }

    fn push_row(&self, py: Python<'_>, row: &Bound<'_, PyAny>) -> PyResult<()> {
        let writer = self.0.get()?;
        let row = self.import(row);
        run_now(py, async {
            match row.and_then(Imported::owned) {
                Ok(row) => writer.push_row(row).await,
                Err(message) => Err(writer.reject(Error::invalid_argument(message)).await),
            }
        })
    }

    fn push(
        &self,
        py: Python<'_>,
        kind: &str,
        timestamp_ns: i64,
        data: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let writer = self.0.get()?;
        match kind {
            "imu" => push_value(py, writer, timestamp_ns, record::imu(data)),
            "image" => push_value(py, writer, timestamp_ns, record::image(data)),
            "compressed_image" => {
                push_value(py, writer, timestamp_ns, record::compressed_image(data))
            }
            other => Err(invalid(format!("unknown record kind '{other}'"))),
        }
    }

    fn handle_error(&self, py: Python<'_>, reason: &str) -> PyResult<bool> {
        let writer = self.0.get()?;
        match wait_closing(py, writer.handle_error(reason), &self.2)? {
            Closing::Done(result) => result.map_err(to_py),
            Closing::Interrupted(Err(error), signal) => Err(interrupted(py, signal, error, to_py)),
            Closing::Interrupted(Ok(_), signal) | Closing::Abandoned(signal) => Err(signal),
        }
    }

    #[pyo3(signature = (error = None))]
    fn finalize(&self, py: Python<'_>, error: Option<&str>) -> PyResult<()> {
        let writer = self.0.get()?;
        match wait_closing(py, writer.finalize(error), &self.2)? {
            Closing::Done(result) => result.map_err(to_py),
            Closing::Interrupted(Err(error), signal) => Err(interrupted(py, signal, error, to_py)),
            Closing::Interrupted(Ok(()), signal) | Closing::Abandoned(signal) => Err(signal),
        }
    }

    fn reject(&self, py: Python<'_>, message: &str) -> PyResult<Py<PyBaseException>> {
        let writer = self.0.get()?;
        let error = wait(py, writer.reject(Error::invalid_argument(message)))?;
        Ok(to_py(error).into_value(py))
    }

    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        let writer = self.0.get()?;
        run(py, writer.flush())
    }

    #[pyo3(signature = (max_bytes = None, max_rows = None, max_age = None))]
    fn set_flush_policy(
        &self,
        py: Python<'_>,
        max_bytes: Option<i64>,
        max_rows: Option<i64>,
        max_age: Option<f64>,
    ) -> PyResult<()> {
        let mut policy = FlushPolicy::default();
        policy.max_bytes = count("max_bytes", max_bytes)?;
        policy.max_rows = count("max_rows", max_rows)?;
        policy.max_age = max_age.map(|s| parse_seconds("max_age", s)).transpose()?;
        let writer = self.0.get()?;
        run(py, writer.set_flush_policy(policy))
    }
}

#[pyfunction]
pub(crate) fn topic_name(name: &str) -> String {
    mosaico_core::topic_name(name)
}
