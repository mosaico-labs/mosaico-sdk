use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};

use crate::error::{Error, ErrorKind, Result};
use crate::read::batch_reader::BatchReader;
use crate::scope::{self, Token};
use crate::time::Timestamp;

#[derive(Clone)]
pub struct CloseHandle(Arc<Token>);

impl CloseHandle {
    pub(crate) fn new(token: Arc<Token>) -> Self {
        Self(token)
    }

    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    pub fn close(&self) {
        self.0.close();
    }

    #[doc(hidden)]
    pub fn any_closed(handles: &[CloseHandle], checked: &AtomicU64) -> bool {
        let closes = scope::closes();
        if checked.load(Ordering::Acquire) == closes {
            return false;
        }
        if handles.iter().any(CloseHandle::is_closed) {
            return true;
        }
        checked.store(closes, Ordering::Release);
        false
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ReadInfo {
    pub(crate) msg_count: u64,
    pub(crate) timestamp_ns_min: Option<Timestamp>,
    pub(crate) timestamp_ns_max: Option<Timestamp>,
}

pub struct TopicBatchStreamer {
    schema: SchemaRef,
    source: Option<BatchReader>,
    info: ReadInfo,
    token: Arc<Token>,
}

impl TopicBatchStreamer {
    pub(crate) fn single(reader: BatchReader, info: ReadInfo, token: Arc<Token>) -> Self {
        Self {
            schema: reader.schema().clone(),
            source: Some(reader),
            info,
            token,
        }
    }

    pub(crate) fn empty(token: Arc<Token>) -> Self {
        Self {
            schema: Arc::new(Schema::empty()),
            source: None,
            info: ReadInfo::default(),
            token,
        }
    }

    pub fn close_handle(&self) -> CloseHandle {
        CloseHandle(self.token.clone())
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn msg_count(&self) -> u64 {
        self.info.msg_count
    }

    pub fn timestamp_ns_min(&self) -> Option<Timestamp> {
        self.info.timestamp_ns_min
    }

    pub fn timestamp_ns_max(&self) -> Option<Timestamp> {
        self.info.timestamp_ns_max
    }

    pub(crate) fn info(&self) -> ReadInfo {
        self.info
    }

    pub async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.ensure_open()?;
        let Some(source) = self.source.as_mut() else {
            return Ok(None);
        };
        let item = source.next_batch().await;
        match &item {
            Ok(Some(_)) => {}
            Ok(None) => {
                self.source = None;
                self.ensure_open()?;
            }
            Err(_) => self.close(),
        }
        item
    }

    pub fn close(&mut self) {
        self.token.close();
        self.source = None;
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.source.is_none()
    }

    pub(crate) fn ensure_open(&self) -> Result<()> {
        if self.token.is_closed() {
            return Err(Error::new(ErrorKind::Closed, "stream closed"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};

    use super::*;
    use crate::read::batch_reader::testing::{Tail, streamer};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "timestamp_ns",
            DataType::Int64,
            false,
        )]))
    }

    fn batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
    }

    #[test]
    fn any_closed_sees_close() {
        let scope = crate::scope::Scope::root();
        let handles = vec![
            CloseHandle::new(scope.token()),
            CloseHandle::new(scope.token()),
        ];
        let checked = AtomicU64::new(u64::MAX);
        assert!(!CloseHandle::any_closed(&handles, &checked));
        assert!(!CloseHandle::any_closed(&handles, &checked));
        handles[1].close();
        assert!(CloseHandle::any_closed(&handles, &checked));
    }

    #[tokio::test]
    async fn end_repeats() {
        let mut stream = streamer(schema(), vec![batch(&[1, 2]), batch(&[3])], Tail::End).await;
        assert_eq!(stream.next_batch().await.unwrap().unwrap().num_rows(), 2);
        assert_eq!(stream.next_batch().await.unwrap().unwrap().num_rows(), 1);
        assert!(stream.next_batch().await.unwrap().is_none());
        assert!(stream.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn error_then_closed() {
        let mut stream = streamer(schema(), vec![batch(&[1])], Tail::Fail).await;
        assert!(stream.next_batch().await.unwrap().is_some());
        let failed = stream.next_batch().await.unwrap_err();
        assert_eq!(failed.kind(), ErrorKind::Stream);
        let closed = stream.next_batch().await.unwrap_err();
        assert_eq!(closed.kind(), ErrorKind::Closed);
    }

    #[tokio::test]
    async fn close_stops_wait() {
        let mut stream = streamer(schema(), Vec::new(), Tail::Hang).await;
        let handle = stream.close_handle();
        let waiting = tokio::spawn(async move { stream.next_batch().await.map(|b| b.is_some()) });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished());
        handle.close();
        let ended = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ended.unwrap_err().kind(), ErrorKind::Closed);
        assert!(handle.is_closed());
    }
}
