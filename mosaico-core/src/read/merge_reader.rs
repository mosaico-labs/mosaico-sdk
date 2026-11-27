use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::pin::pin;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::SchemaRef;
use futures::future::{self, Either};

use crate::error::{Error, ErrorKind, Result};
use crate::read::data_streamer::timestamps;
use crate::read::streamer::{CloseHandle, TopicBatchStreamer};
use crate::scope::{self, Token};

pub(crate) const WINDOW_ROWS: usize = 65_536;
const MAX_TOPICS: usize = u16::MAX as usize + 1;

pub(crate) fn check_topic_count(topics: usize) -> Result<()> {
    if topics > MAX_TOPICS {
        return Err(Error::new(
            ErrorKind::InvalidArgument,
            format!("{topics} topics, at most {MAX_TOPICS} in one read"),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct MergeWindow {
    pub batches: Vec<WindowBatch>,
    pub order: Vec<u16>,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WindowBatch {
    pub topic: u16,
    pub batch: RecordBatch,
    pub timestamps: Int64Array,
}

struct Head {
    stream: TopicBatchStreamer,
    timestamps: Option<Int64Array>,
    row: usize,
}

impl Head {
    fn needs_batch(&self) -> bool {
        !self.stream.is_finished() && self.timestamps.as_ref().is_none_or(|t| self.row >= t.len())
    }
}

pub(crate) struct MergeReader {
    heads: Vec<Head>,
    queue: BinaryHeap<Reverse<(i64, u16)>>,
    pending: Vec<WindowBatch>,
    max_rows: usize,
    closed: bool,
    checked: u64,
    token: Arc<Token>,
    labels: Vec<String>,
}

impl MergeReader {
    #[cfg(test)]
    pub(crate) fn new(streams: Vec<TopicBatchStreamer>, max_rows: usize) -> Result<Self> {
        Self::with_token(streams, max_rows, crate::scope::Scope::root().token())
    }

    pub(crate) fn with_token(
        streams: Vec<TopicBatchStreamer>,
        max_rows: usize,
        token: Arc<Token>,
    ) -> Result<Self> {
        check_topic_count(streams.len())?;
        Ok(Self {
            heads: streams
                .into_iter()
                .map(|stream| Head {
                    stream,
                    timestamps: None,
                    row: 0,
                })
                .collect(),
            queue: BinaryHeap::new(),
            pending: Vec::new(),
            max_rows: max_rows.max(1),
            closed: false,
            checked: u64::MAX,
            token,
            labels: Vec::new(),
        })
    }

    pub(crate) fn labeled(mut self, labels: Vec<String>) -> Self {
        self.labels = labels;
        self
    }

    pub(crate) async fn next_window(&mut self) -> Result<Option<MergeWindow>> {
        self.ensure_open()?;
        for topic in 0..self.heads.len() {
            if !self.heads[topic].needs_batch() {
                continue;
            }
            if let Err(e) = self.load(topic).await {
                self.close();
                return Err(match self.labels.get(topic) {
                    Some(label) => e.in_topic(label),
                    None => e,
                });
            }
        }
        let batches = std::mem::take(&mut self.pending);
        let mut order = Vec::new();
        while order.len() < self.max_rows {
            let Some(Reverse((_, topic))) = self.queue.pop() else {
                break;
            };
            order.push(topic);
            let head = &mut self.heads[usize::from(topic)];
            head.row += 1;
            match head.timestamps.as_ref() {
                Some(t) if head.row < t.len() => {
                    self.queue.push(Reverse((t.value(head.row), topic)))
                }
                _ => break,
            }
        }
        if order.is_empty() {
            return Ok(None);
        }
        Ok(Some(MergeWindow { batches, order }))
    }

    pub(crate) fn schemas(&self) -> Vec<SchemaRef> {
        self.heads
            .iter()
            .map(|h| h.stream.schema().clone())
            .collect()
    }

    pub(crate) fn close_handles(&self) -> Vec<CloseHandle> {
        let own = CloseHandle::new(self.token.clone());
        std::iter::once(own)
            .chain(self.heads.iter().map(|h| h.stream.close_handle()))
            .collect()
    }

    pub(crate) fn close(&mut self) {
        self.closed = true;
        self.token.close();
        self.queue.clear();
        self.pending.clear();
        for head in &mut self.heads {
            head.stream.close();
            head.timestamps = None;
        }
    }

    pub(crate) fn ensure_open(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::new(ErrorKind::Closed, "stream closed"));
        }
        let closes = scope::closes();
        if closes == self.checked {
            return Ok(());
        }
        if self.token.is_closed() {
            self.close();
            return Err(Error::new(ErrorKind::Closed, "stream closed"));
        }
        if let Err(e) = self.heads.iter().try_for_each(|h| h.stream.ensure_open()) {
            self.close();
            return Err(e);
        }
        self.checked = closes;
        Ok(())
    }

    async fn load(&mut self, topic: usize) -> Result<()> {
        loop {
            let mut notice = pin!(scope::close_notice());
            notice.as_mut().enable();
            self.ensure_open()?;
            let head = &mut self.heads[topic];
            let next = {
                let wait = pin!(head.stream.next_batch());
                match future::select(wait, notice).await {
                    Either::Left((next, _)) => next?,
                    Either::Right(_) => continue,
                }
            };
            let Some(batch) = next else {
                head.timestamps = None;
                return Ok(());
            };
            if batch.num_rows() == 0 {
                continue;
            }
            let t = timestamps(&batch)?;
            self.queue.push(Reverse((t.value(0), topic as u16)));
            head.timestamps = Some(t.clone());
            head.row = 0;
            self.pending.push(WindowBatch {
                topic: topic as u16,
                batch,
                timestamps: t,
            });
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow_schema::{DataType, Field, Schema, SchemaRef};

    use super::*;
    use crate::read::batch_reader::testing::{Tail, delayed, streamer};
    use crate::scope::Scope;

    fn schema(nullable: bool) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "timestamp_ns",
            DataType::Int64,
            nullable,
        )]))
    }

    fn batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            schema(false),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    async fn topic(batches: &[&[i64]], tail: Tail) -> TopicBatchStreamer {
        streamer(
            schema(false),
            batches.iter().map(|b| batch(b)).collect(),
            tail,
        )
        .await
    }

    async fn rows(reader: &mut MergeReader) -> Result<Vec<(u16, i64)>> {
        let mut current: HashMap<u16, (Int64Array, usize)> = HashMap::new();
        let mut out = Vec::new();
        while let Some(window) = reader.next_window().await? {
            for b in window.batches {
                if let Some((t, i)) = current.get(&b.topic) {
                    assert_eq!(*i, t.len(), "batch replaced before it was used up");
                }
                assert_eq!(b.timestamps, timestamps(&b.batch).unwrap());
                current.insert(b.topic, (b.timestamps, 0));
            }
            for k in window.order {
                let (t, i) = current.get_mut(&k).unwrap();
                out.push((k, t.value(*i)));
                *i += 1;
            }
        }
        Ok(out)
    }

    #[tokio::test]
    async fn merges_by_timestamp() {
        let streams = vec![
            topic(&[&[1, 4], &[7]], Tail::End).await,
            topic(&[&[2, 3, 8]], Tail::End).await,
            topic(&[&[5]], Tail::End).await,
        ];
        let mut reader = MergeReader::new(streams, WINDOW_ROWS).unwrap();
        let merged = rows(&mut reader).await.unwrap();
        assert_eq!(
            merged,
            vec![(0, 1), (1, 2), (1, 3), (0, 4), (2, 5), (0, 7), (1, 8)]
        );
        assert!(reader.next_window().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn ties_follow_topic_order() {
        let streams = vec![
            topic(&[&[2, 2]], Tail::End).await,
            topic(&[&[1, 2], &[2]], Tail::End).await,
            topic(&[&[2]], Tail::End).await,
        ];
        let mut reader = MergeReader::new(streams, WINDOW_ROWS).unwrap();
        let merged = rows(&mut reader).await.unwrap();
        assert_eq!(merged, vec![(1, 1), (0, 2), (0, 2), (1, 2), (1, 2), (2, 2)]);
    }

    #[tokio::test]
    async fn empty_topics_skipped() {
        let token = Scope::root().token();
        let streams = vec![
            TopicBatchStreamer::empty(token),
            topic(&[&[], &[3], &[], &[4]], Tail::End).await,
            topic(&[], Tail::End).await,
        ];
        let mut reader = MergeReader::new(streams, WINDOW_ROWS).unwrap();
        assert_eq!(rows(&mut reader).await.unwrap(), vec![(1, 3), (1, 4)]);
    }

    #[tokio::test]
    async fn topic_cap() {
        let token = Scope::root().token();
        let empty = |n: usize| -> Vec<TopicBatchStreamer> {
            (0..n)
                .map(|_| TopicBatchStreamer::empty(token.clone()))
                .collect()
        };
        let cap = MAX_TOPICS;
        let error = MergeReader::new(empty(cap + 1), WINDOW_ROWS).err().unwrap();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        assert!(error.to_string().contains("65536"), "{error}");
        let mut streams = empty(cap - 2);
        streams.push(topic(&[&[2, 4]], Tail::End).await);
        streams.push(topic(&[&[1, 3]], Tail::End).await);
        let mut reader = MergeReader::new(streams, WINDOW_ROWS).unwrap();
        assert_eq!(
            rows(&mut reader).await.unwrap(),
            vec![
                (u16::MAX, 1),
                (u16::MAX - 1, 2),
                (u16::MAX, 3),
                (u16::MAX - 1, 4)
            ]
        );
    }

    #[tokio::test]
    async fn own_token_closes() {
        let scope = Scope::root();
        let mut reader = MergeReader::with_token(Vec::new(), 10, scope.token()).unwrap();
        assert_eq!(reader.close_handles().len(), 1);
        scope.close();
        let error = reader.next_window().await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Closed);
    }

    #[tokio::test]
    async fn all_empty_ends() {
        let streams = vec![topic(&[], Tail::End).await, topic(&[&[]], Tail::End).await];
        let mut reader = MergeReader::new(streams, WINDOW_ROWS).unwrap();
        assert!(reader.next_window().await.unwrap().is_none());
        let mut none = MergeReader::new(Vec::new(), WINDOW_ROWS).unwrap();
        assert!(none.next_window().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn window_ends_at_batch() {
        let streams = vec![
            topic(&[&[1, 2], &[3]], Tail::End).await,
            topic(&[&[10]], Tail::End).await,
        ];
        let mut reader = MergeReader::new(streams, WINDOW_ROWS).unwrap();
        let first = reader.next_window().await.unwrap().unwrap();
        assert_eq!(
            first
                .batches
                .iter()
                .map(|b| (b.topic, b.batch.num_rows()))
                .collect::<Vec<_>>(),
            vec![(0, 2), (1, 1)]
        );
        assert_eq!(first.order, vec![0, 0]);
        let second = reader.next_window().await.unwrap().unwrap();
        assert_eq!(second.batches.len(), 1);
        assert_eq!(second.order, vec![0]);
        let third = reader.next_window().await.unwrap().unwrap();
        assert!(third.batches.is_empty());
        assert_eq!(third.order, vec![1]);
        assert!(reader.next_window().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn window_row_cap() {
        let streams = vec![
            topic(&[&[1, 3, 5, 7, 9]], Tail::End).await,
            topic(&[&[2, 4, 6, 8, 10]], Tail::End).await,
        ];
        let mut reader = MergeReader::new(streams, 3).unwrap();
        let mut sizes = Vec::new();
        let mut all = Vec::new();
        while let Some(window) = reader.next_window().await.unwrap() {
            sizes.push(window.order.len());
            all.extend(window.order);
        }
        assert!(sizes.iter().all(|&n| n <= 3));
        assert_eq!(all, vec![0, 1, 0, 1, 0, 1, 0, 1, 0, 1]);
    }

    #[tokio::test]
    async fn error_closes_all() {
        let streams = vec![
            topic(&[&[1]], Tail::Fail).await,
            topic(&[&[2, 3]], Tail::Hang).await,
        ];
        let other = streams[1].close_handle();
        let mut reader = MergeReader::new(streams, WINDOW_ROWS).unwrap();
        assert_eq!(reader.next_window().await.unwrap().unwrap().order, vec![0]);
        let failed = reader.next_window().await.unwrap_err();
        assert_eq!(failed.kind(), ErrorKind::Stream);
        assert!(other.is_closed());
        let closed = reader.next_window().await.unwrap_err();
        assert_eq!(closed.kind(), ErrorKind::Closed);
    }

    #[tokio::test]
    async fn null_timestamp_fails() {
        let nulls = RecordBatch::try_new(
            schema(true),
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        let streams = vec![
            streamer(schema(true), vec![nulls], Tail::End).await,
            topic(&[&[2]], Tail::End).await,
        ];
        let mut reader = MergeReader::new(streams, WINDOW_ROWS)
            .unwrap()
            .labeled(vec!["seq/a".to_owned(), "seq/b".to_owned()]);
        let failed = reader.next_window().await.unwrap_err();
        assert_eq!(failed.kind(), ErrorKind::Schema);
        assert!(failed.to_string().starts_with("topic 'seq/a': "));
        let closed = reader.next_window().await.unwrap_err();
        assert_eq!(closed.kind(), ErrorKind::Closed);
        assert_eq!(closed.to_string(), "stream closed");
    }

    #[tokio::test]
    async fn missing_timestamp_fails() {
        let other: SchemaRef = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(other.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let streams = vec![streamer(other, vec![batch], Tail::End).await];
        let mut reader = MergeReader::new(streams, WINDOW_ROWS).unwrap();
        let failed = reader.next_window().await.unwrap_err();
        assert_eq!(failed.kind(), ErrorKind::Schema);
    }

    #[tokio::test]
    async fn closed_stream_stops() {
        let streams = vec![
            topic(&[&[1, 2]], Tail::End).await,
            topic(&[&[3]], Tail::Hang).await,
        ];
        let first = streams[0].close_handle();
        let second = streams[1].close_handle();
        let mut reader = MergeReader::new(streams, 1).unwrap();
        assert_eq!(reader.next_window().await.unwrap().unwrap().order, vec![0]);
        first.close();
        let closed = reader.next_window().await.unwrap_err();
        assert_eq!(closed.kind(), ErrorKind::Closed);
        assert!(second.is_closed());
    }

    #[tokio::test]
    async fn close_stops_wait() {
        let streams = vec![topic(&[], Tail::Hang).await];
        let handle = streams[0].close_handle();
        let mut reader = MergeReader::new(streams, WINDOW_ROWS).unwrap();
        let waiting = tokio::spawn(async move { reader.next_window().await.map(|w| w.is_some()) });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!waiting.is_finished());
        handle.close();
        let ended = tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ended.unwrap_err().kind(), ErrorKind::Closed);
    }
    #[tokio::test]
    async fn retry_after_cancel() {
        let streams = vec![
            topic(&[&[1, 2]], Tail::End).await,
            delayed(schema(false), vec![batch(&[3])], Duration::from_millis(200)).await,
        ];
        let mut reader = MergeReader::new(streams, WINDOW_ROWS).unwrap();
        let cut = tokio::time::timeout(Duration::from_millis(50), reader.next_window()).await;
        assert!(cut.is_err());
        assert_eq!(
            rows(&mut reader).await.unwrap(),
            vec![(0, 1), (0, 2), (1, 3)]
        );
    }

    #[tokio::test]
    async fn other_close_wakes() {
        let streams = vec![
            topic(&[&[1], &[5]], Tail::End).await,
            topic(&[&[2]], Tail::Hang).await,
        ];
        let first = streams[0].close_handle();
        let mut reader = MergeReader::new(streams, WINDOW_ROWS).unwrap();
        assert_eq!(reader.next_window().await.unwrap().unwrap().order, vec![0]);
        assert_eq!(reader.next_window().await.unwrap().unwrap().order, vec![1]);
        let waiting = tokio::spawn(async move { reader.next_window().await.map(|w| w.is_some()) });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished());
        first.close();
        let ended = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ended.unwrap_err().kind(), ErrorKind::Closed);
    }

    #[tokio::test]
    async fn unrelated_close_ignored() {
        let streams = vec![
            topic(&[&[1]], Tail::End).await,
            delayed(schema(false), vec![batch(&[2])], Duration::from_millis(100)).await,
        ];
        let mut reader = MergeReader::new(streams, WINDOW_ROWS).unwrap();
        let waiting = tokio::spawn(async move {
            let window = reader.next_window().await?;
            Ok::<_, Error>(window.map(|w| (w.order, w.batches.len())))
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        Scope::root().token().close();
        let ended = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ended.unwrap(), Some((vec![0], 2)));
    }
}
