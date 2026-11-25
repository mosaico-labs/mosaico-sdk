use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::pin::pin;

use arrow_array::{Int64Array, RecordBatch};
use futures::future::{self, Either};

use crate::error::{Error, ErrorKind, Result};
use crate::read::data_streamer::timestamps;
use crate::read::streamer::TopicBatchStreamer;

pub(crate) const WINDOW_ROWS: usize = 65_536;

#[derive(Debug, Clone)]
pub(crate) struct Window {
    pub(crate) batches: Vec<(u16, RecordBatch)>,
    pub(crate) order: Vec<u16>,
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
    pending: Vec<(u16, RecordBatch)>,
    max_rows: usize,
    closed: bool,
}

impl MergeReader {
    pub(crate) fn new(streams: Vec<TopicBatchStreamer>, max_rows: usize) -> Result<Self> {
        if streams.len() > usize::from(u16::MAX) + 1 {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "{} topics, at most {} in one read",
                    streams.len(),
                    u16::MAX as usize + 1
                ),
            ));
        }
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
        })
    }

    pub(crate) async fn next_window(&mut self) -> Result<Option<Window>> {
        self.ensure_open()?;
        for topic in 0..self.heads.len() {
            if !self.heads[topic].needs_batch() {
                continue;
            }
            if let Err(e) = self.load(topic).await {
                self.close();
                return Err(e);
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
        Ok(Some(Window { batches, order }))
    }

    pub(crate) fn close(&mut self) {
        self.closed = true;
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
        if let Err(e) = self.heads.iter().try_for_each(|h| h.stream.ensure_open()) {
            self.close();
            return Err(e);
        }
        Ok(())
    }

    async fn load(&mut self, topic: usize) -> Result<()> {
        let others: Vec<_> = self
            .heads
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != topic)
            .map(|(_, h)| h.stream.token())
            .collect();
        let head = &mut self.heads[topic];
        loop {
            let next = {
                let wait = pin!(head.stream.next_batch());
                let closed = pin!(async {
                    if others.is_empty() {
                        future::pending::<()>().await;
                    }
                    future::select_all(others.iter().map(|t| Box::pin(t.closed()))).await;
                });
                match future::select(wait, closed).await {
                    Either::Left((next, _)) => next?,
                    Either::Right(_) => return Err(Error::new(ErrorKind::Closed, "stream closed")),
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
            head.timestamps = Some(t);
            head.row = 0;
            self.pending.push((topic as u16, batch));
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
            for (k, b) in window.batches {
                if let Some((t, i)) = current.get(&k) {
                    assert_eq!(*i, t.len(), "batch replaced before it was used up");
                }
                current.insert(k, (timestamps(&b).unwrap(), 0));
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
                .map(|(k, b)| (*k, b.num_rows()))
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
        let mut reader = MergeReader::new(streams, WINDOW_ROWS).unwrap();
        let failed = reader.next_window().await.unwrap_err();
        assert_eq!(failed.kind(), ErrorKind::Schema);
        let closed = reader.next_window().await.unwrap_err();
        assert_eq!(closed.kind(), ErrorKind::Closed);
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
}
