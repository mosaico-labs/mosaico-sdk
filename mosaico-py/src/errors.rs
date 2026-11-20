use std::time::Duration;

use mosaico_core::{Error, ErrorKind};
use pyo3::prelude::*;

pyo3::import_exception!(mosaico_rs._errors, MosaicoError);
pyo3::import_exception!(mosaico_rs._errors, ConnectError);
pyo3::import_exception!(mosaico_rs._errors, SequenceNotFoundError);
pyo3::import_exception!(mosaico_rs._errors, TopicNotFoundError);
pyo3::import_exception!(mosaico_rs._errors, InvalidArgumentError);
pyo3::import_exception!(mosaico_rs._errors, AlreadyExistsError);
pyo3::import_exception!(mosaico_rs._errors, PermissionDeniedError);
pyo3::import_exception!(mosaico_rs._errors, ServerError);
pyo3::import_exception!(mosaico_rs._errors, StreamError);
pyo3::import_exception!(mosaico_rs._errors, SchemaError);
pyo3::import_exception!(mosaico_rs._errors, StreamClosedError);

pub(crate) fn to_py(error: Error) -> PyErr {
    let message = error.to_string();
    match error.kind() {
        ErrorKind::Connection => ConnectError::new_err(message),
        ErrorKind::NotFound => TopicNotFoundError::new_err(message),
        ErrorKind::InvalidArgument => InvalidArgumentError::new_err(message),
        ErrorKind::AlreadyExists => AlreadyExistsError::new_err(message),
        ErrorKind::Unauthenticated | ErrorKind::PermissionDenied => {
            PermissionDeniedError::new_err(message)
        }
        ErrorKind::Closed => StreamClosedError::new_err(message),
        ErrorKind::Stream => StreamError::new_err(message),
        ErrorKind::Schema => SchemaError::new_err(message),
        _ => ServerError::new_err(message),
    }
}

pub(crate) fn sequence_error(error: Error) -> PyErr {
    if error.kind() == ErrorKind::NotFound {
        return SequenceNotFoundError::new_err(error.to_string());
    }
    to_py(error)
}

pub(crate) fn invalid(message: impl Into<String>) -> PyErr {
    InvalidArgumentError::new_err(message.into())
}

pub(crate) fn parse_seconds(name: &str, seconds: f64) -> PyResult<Duration> {
    if seconds.is_nan() || seconds <= 0.0 {
        return Err(invalid(format!(
            "{name} must be a positive number of seconds, got {seconds:?}"
        )));
    }
    Duration::try_from_secs_f64(seconds)
        .map_err(|_| invalid(format!("{name} too large: {seconds:?} s")))
}
