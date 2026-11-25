use mosaico_core::{Compression, ConnectOptions, Notification, SessionLevelErrorPolicy};
use pyo3::prelude::*;

use crate::errors::{invalid, parse_seconds, sequence_error};
use crate::read::{SequenceHandler, TopicHandler};
use crate::runtime::{ProcessBound, run, wait};
use crate::write::SequenceWriter;

#[pyclass(frozen, module = "mosaico_rs._core")]
pub(crate) struct Client(ProcessBound<mosaico_core::MosaicoClient>);

#[pymethods]
impl Client {
    #[staticmethod]
    #[pyo3(signature = (host, port, timeout, tls = false, tls_cert = None, api_key = None, compression = None))]
    #[allow(clippy::too_many_arguments)]
    fn connect(
        py: Python<'_>,
        host: &str,
        port: i64,
        timeout: f64,
        tls: bool,
        tls_cert: Option<&[u8]>,
        api_key: Option<String>,
        compression: Option<&str>,
    ) -> PyResult<Self> {
        let port = u16::try_from(port)
            .ok()
            .filter(|&port| port != 0)
            .ok_or_else(|| invalid(format!("invalid port {port}: expected 1-65535")))?;
        let compression = match compression {
            None => Compression::None,
            Some("lz4") => Compression::Lz4,
            Some("zstd") => Compression::Zstd,
            Some("gzip") => Compression::Gzip,
            Some(other) => {
                return Err(invalid(format!(
                    "compression must be None, 'lz4', 'zstd' or 'gzip', not '{other}'"
                )));
            }
        };
        let mut options = ConnectOptions::new(host, port);
        options.connect_timeout = parse_seconds("timeout", timeout)?;
        options.tls = tls;
        options.tls_cert = tls_cert.map(<[u8]>::to_vec);
        options.api_key = api_key;
        options.compression = compression;
        let client = run(py, mosaico_core::MosaicoClient::connect(options))?;
        Ok(Self(ProcessBound::new(client)))
    }

    #[getter]
    fn server_version(&self) -> PyResult<String> {
        Ok(self.0.get()?.server_info().version.clone())
    }

    #[getter]
    fn max_batch_size_bytes(&self) -> PyResult<u64> {
        Ok(self.0.get()?.server_info().max_batch_size_bytes())
    }

    fn close(&self) -> PyResult<()> {
        self.0.get()?.close();
        Ok(())
    }

    fn sequence_handler(&self, py: Python<'_>, name: &str) -> PyResult<SequenceHandler> {
        let client = self.0.get()?;
        let handler = wait(py, client.sequence_handler(name))?.map_err(sequence_error)?;
        Ok(SequenceHandler(ProcessBound::new(handler)))
    }

    fn topic_handler(&self, py: Python<'_>, sequence: &str, topic: &str) -> PyResult<TopicHandler> {
        let client = self.0.get()?;
        let handler = run(py, client.topic_handler(sequence, topic))?;
        Ok(TopicHandler(ProcessBound::new(handler)))
    }

    #[pyo3(signature = (name, metadata, on_error))]
    fn sequence_create(
        &self,
        py: Python<'_>,
        name: &str,
        metadata: Option<Vec<u8>>,
        on_error: &str,
    ) -> PyResult<SequenceWriter> {
        let client = self.0.get()?;
        let policy = session_policy(on_error)?;
        let writer =
            wait(py, client.sequence_create(name, metadata, policy))?.map_err(sequence_error)?;
        Ok(SequenceWriter::new(writer))
    }

    fn sequence_update(
        &self,
        py: Python<'_>,
        name: &str,
        on_error: &str,
    ) -> PyResult<SequenceWriter> {
        let client = self.0.get()?;
        let policy = session_policy(on_error)?;
        let writer = wait(py, client.sequence_update(name, policy))?.map_err(sequence_error)?;
        Ok(SequenceWriter::new(writer))
    }

    fn list_sequences(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let client = self.0.get()?;
        run(py, client.list_sequences())
    }

    fn sequence_delete(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let client = self.0.get()?;
        wait(py, client.sequence_delete(name))?.map_err(sequence_error)
    }

    fn session_delete(&self, py: Python<'_>, locator: &str) -> PyResult<()> {
        let client = self.0.get()?;
        wait(py, client.session_delete(locator))?.map_err(sequence_error)
    }

    fn sequence_exists(&self, py: Python<'_>, name: &str) -> PyResult<bool> {
        let client = self.0.get()?;
        run(py, client.sequence_exists(name))
    }

    fn topic_exists(&self, py: Python<'_>, sequence: &str, topic: &str) -> PyResult<bool> {
        let client = self.0.get()?;
        run(py, client.topic_exists(sequence, topic))
    }

    fn list_sequence_notifications(
        &self,
        py: Python<'_>,
        sequence: &str,
    ) -> PyResult<Vec<NotificationTuple>> {
        let client = self.0.get()?;
        let list = wait(py, client.list_sequence_notifications(sequence))?;
        Ok(list
            .map_err(sequence_error)?
            .into_iter()
            .map(tuple)
            .collect())
    }

    fn clear_sequence_notifications(&self, py: Python<'_>, sequence: &str) -> PyResult<()> {
        let client = self.0.get()?;
        wait(py, client.clear_sequence_notifications(sequence))?.map_err(sequence_error)
    }

    fn list_topic_notifications(
        &self,
        py: Python<'_>,
        sequence: &str,
        topic: &str,
    ) -> PyResult<Vec<NotificationTuple>> {
        let client = self.0.get()?;
        let list = run(py, client.list_topic_notifications(sequence, topic))?;
        Ok(list.into_iter().map(tuple).collect())
    }

    fn clear_topic_notifications(
        &self,
        py: Python<'_>,
        sequence: &str,
        topic: &str,
    ) -> PyResult<()> {
        let client = self.0.get()?;
        run(py, client.clear_topic_notifications(sequence, topic))
    }
}

type NotificationTuple = (String, Option<String>, String, String, String);

fn tuple(notification: Notification) -> NotificationTuple {
    (
        notification.sequence_name,
        notification.topic_name,
        notification.kind,
        notification.message,
        notification.created_at,
    )
}

fn session_policy(value: &str) -> PyResult<SessionLevelErrorPolicy> {
    match value {
        "report" => Ok(SessionLevelErrorPolicy::Report),
        "delete" => Ok(SessionLevelErrorPolicy::Delete),
        other => Err(invalid(format!(
            "session error policy must be 'report' or 'delete', not '{other}'"
        ))),
    }
}
