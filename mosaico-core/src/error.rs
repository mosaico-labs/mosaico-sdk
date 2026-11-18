use std::error::Error as StdError;
use std::fmt;

use arrow_flight::error::FlightError;
use tonic::{Code, Status};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    Connection,
    NotFound,
    AlreadyExists,
    InvalidArgument,
    Unauthenticated,
    PermissionDenied,
    FailedPrecondition,
    Cancelled,
    Closed,
    Stream,
    Protocol,
    Schema,
    Server,
}

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    message: String,
    source: Option<Box<dyn StdError + Send + Sync>>,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub(crate) fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    pub(crate) fn with_source(mut self, source: impl StdError + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    pub(crate) fn or_not_found(mut self, message: impl FnOnce() -> String) -> Self {
        if self.kind == ErrorKind::NotFound {
            self.message = message();
            self.source = None;
        }
        self
    }

    pub fn schema(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Schema, message)
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)?;
        if let Some(source) = &self.source {
            let cause = root_cause(source.as_ref());
            if !cause.is_empty() && !self.message.contains(&cause) {
                write!(f, ": {cause}")?;
            }
        }
        Ok(())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|e| e as &(dyn StdError + 'static))
    }
}

impl From<Status> for Error {
    fn from(status: Status) -> Self {
        let kind = match status.code() {
            Code::Unavailable => ErrorKind::Connection,
            Code::Unknown if status.source().is_some() => ErrorKind::Connection,
            Code::NotFound => ErrorKind::NotFound,
            Code::AlreadyExists => ErrorKind::AlreadyExists,
            Code::InvalidArgument => ErrorKind::InvalidArgument,
            Code::Unauthenticated => ErrorKind::Unauthenticated,
            Code::PermissionDenied => ErrorKind::PermissionDenied,
            Code::FailedPrecondition => ErrorKind::FailedPrecondition,
            Code::Cancelled if status.source().is_some() => ErrorKind::Connection,
            Code::Cancelled => ErrorKind::Cancelled,
            _ => ErrorKind::Server,
        };
        Error::new(kind, status_text(&status)).with_source(status)
    }
}

impl From<FlightError> for Error {
    fn from(error: FlightError) -> Self {
        match error {
            FlightError::Tonic(status) => Error::from(*status),
            other => Error::new(ErrorKind::Stream, other.to_string()).with_source(other),
        }
    }
}

fn status_text(status: &Status) -> String {
    if status.message().is_empty() {
        format!("{:?}", status.code())
    } else {
        format!("{} ({:?})", status.message(), status.code())
    }
}

fn root_cause(mut error: &(dyn StdError + 'static)) -> String {
    while let Some(source) = error.source() {
        error = source;
    }
    match error.downcast_ref::<Status>() {
        Some(status) => status_text(status),
        None => error.to_string(),
    }
}
