use std::any::Any;
use std::sync::OnceLock;

use arrow_array::{Int64Array, RecordBatch};

use crate::error::{Error, ErrorKind, Result};
use crate::ontology::Ontology;
use crate::read::data_streamer::timestamps;
use crate::read::merge_reader::{MergeReader, WINDOW_ROWS};
use crate::read::streamer::{ReadInfo, TopicBatchStreamer};
use crate::read::topic::TopicHandler;

type Columns = Box<dyn Any + Send + Sync>;

struct Current {
    batch: RecordBatch,
    timestamps: Int64Array,
    row: usize,
    columns: OnceLock<Columns>,
}

pub struct SequenceDataStreamer {
    topics: Vec<TopicHandler>,
    reader: MergeReader,
    current: Vec<Option<Current>>,
    order: Vec<u16>,
    position: usize,
    info: ReadInfo,
}

impl SequenceDataStreamer {
    pub(crate) fn new(topics: Vec<TopicHandler>, streams: Vec<TopicBatchStreamer>) -> Result<Self> {
        Self::with_window(topics, streams, WINDOW_ROWS)
    }

    fn with_window(
        topics: Vec<TopicHandler>,
        streams: Vec<TopicBatchStreamer>,
        window_rows: usize,
    ) -> Result<Self> {
        let infos: Vec<ReadInfo> = streams.iter().map(|s| s.info()).collect();
        let info = ReadInfo {
            msg_count: infos.iter().map(|i| i.msg_count).sum(),
            timestamp_ns_min: infos.iter().filter_map(|i| i.timestamp_ns_min).min(),
            timestamp_ns_max: infos.iter().filter_map(|i| i.timestamp_ns_max).max(),
        };
        let current = topics.iter().map(|_| None).collect();
        Ok(Self {
            topics,
            reader: MergeReader::new(streams, window_rows)?,
            current,
            order: Vec::new(),
            position: 0,
            info,
        })
    }

    pub fn topics(&self) -> &[TopicHandler] {
        &self.topics
    }

    pub fn msg_count(&self) -> u64 {
        self.info.msg_count
    }

    pub fn timestamp_ns_min(&self) -> Option<i64> {
        self.info.timestamp_ns_min
    }

    pub fn timestamp_ns_max(&self) -> Option<i64> {
        self.info.timestamp_ns_max
    }

    pub async fn next(&mut self) -> Result<Option<RowRef<'_>>> {
        if !self.ready().await? {
            return Ok(None);
        }
        let topic = usize::from(self.order[self.position]);
        self.position += 1;
        if self.current[topic].is_none() {
            self.close();
            return Err(Error::new(ErrorKind::Protocol, "merge row without a batch"));
        }
        let Some(current) = self.current[topic].as_mut() else {
            return Ok(None);
        };
        let row = current.row;
        current.row += 1;
        let current = &*current;
        Ok(Some(RowRef {
            timestamp_ns: current.timestamps.value(row),
            topic: &self.topics[topic],
            current,
            row,
        }))
    }

    pub async fn next_timestamp(&mut self) -> Result<Option<i64>> {
        if !self.ready().await? {
            return Ok(None);
        }
        let topic = usize::from(self.order[self.position]);
        match self.current[topic].as_ref() {
            Some(c) => Ok(Some(c.timestamps.value(c.row))),
            None => {
                self.close();
                Err(Error::new(ErrorKind::Protocol, "merge row without a batch"))
            }
        }
    }

    pub fn close(&mut self) {
        self.reader.close();
        self.clear();
    }

    async fn ready(&mut self) -> Result<bool> {
        self.reader.ensure_open()?;
        if self.position < self.order.len() {
            return Ok(true);
        }
        let window = match self.reader.next_window().await {
            Ok(Some(window)) => window,
            Ok(None) => {
                self.clear();
                return Ok(false);
            }
            Err(e) => {
                self.clear();
                return Err(e);
            }
        };
        for (topic, batch) in window.batches {
            self.current[usize::from(topic)] = Some(Current {
                timestamps: timestamps(&batch)?,
                batch,
                row: 0,
                columns: OnceLock::new(),
            });
        }
        self.order = window.order;
        self.position = 0;
        Ok(!self.order.is_empty())
    }

    fn clear(&mut self) {
        self.order = Vec::new();
        self.position = 0;
        self.current.iter_mut().for_each(|c| *c = None);
    }
}

pub struct RowRef<'a> {
    pub timestamp_ns: i64,
    topic: &'a TopicHandler,
    current: &'a Current,
    row: usize,
}

impl<'a> RowRef<'a> {
    pub fn topic(&self) -> &'a TopicHandler {
        self.topic
    }

    pub fn batch(&self) -> &'a RecordBatch {
        &self.current.batch
    }

    pub fn row(&self) -> usize {
        self.row
    }

    pub fn view<T: Ontology>(&self) -> Result<Option<T::View<'a>>> {
        if self.topic.ontology_tag() != T::TAG {
            return Ok(None);
        }
        let current: &'a Current = self.current;
        let columns = match current.columns.get() {
            Some(columns) => columns,
            None => {
                let columns: Columns = Box::new(T::columns(&current.batch)?);
                current.columns.get_or_init(|| columns)
            }
        };
        let columns = columns.downcast_ref::<T::Columns>().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                format!(
                    "topic '{}' was already read as another type with tag '{}'",
                    self.topic.name(),
                    T::TAG
                ),
            )
        })?;
        Ok(Some(T::view(columns, self.row)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};

    use super::*;
    use crate::comm::connection::Connection;
    use crate::read::batch_reader::testing::{Tail, delayed, streamer_with};
    use crate::scope::Scope;

    struct Value;

    impl Ontology for Value {
        const TAG: &'static str = "Value";
        type Columns = Int64Array;
        type View<'a> = i64;

        fn columns(batch: &RecordBatch) -> Result<Int64Array> {
            batch
                .column_by_name("value")
                .and_then(|c| c.as_primitive_opt::<Int64Type>())
                .cloned()
                .ok_or_else(|| Error::schema("missing field value"))
        }

        fn view(columns: &Int64Array, row: usize) -> i64 {
            columns.value(row)
        }

        fn from_view(_: &i64) -> Result<Self> {
            Ok(Value)
        }
    }

    struct Twin;

    impl Ontology for Twin {
        const TAG: &'static str = "Value";
        type Columns = ();
        type View<'a> = ();

        fn columns(_: &RecordBatch) -> Result<()> {
            Ok(())
        }

        fn view(_: &(), _: usize) {}

        fn from_view(_: &()) -> Result<Self> {
            Ok(Twin)
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("timestamp_ns", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn batch(timestamps: &[i64]) -> RecordBatch {
        let values = Int64Array::from_iter_values(timestamps.iter().map(|t| t * 10));
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(values),
            ],
        )
        .unwrap()
    }

    struct Topic {
        name: &'static str,
        tag: &'static str,
        batches: Vec<&'static [i64]>,
        info: ReadInfo,
    }

    fn topic(name: &'static str, tag: &'static str, batches: Vec<&'static [i64]>) -> Topic {
        Topic {
            name,
            tag,
            batches,
            info: ReadInfo::default(),
        }
    }

    async fn open(topics: Vec<Topic>, window_rows: usize) -> (SequenceDataStreamer, Scope) {
        let connection = Connection::offline(1 << 20);
        let scope = Scope::root();
        let mut handlers = Vec::new();
        let mut streams = Vec::new();
        for t in topics {
            let handler = TopicHandler::offline(&connection, scope.child(), "seq", t.name, t.tag);
            let batches = t.batches.iter().map(|b| batch(b)).collect();
            streams
                .push(streamer_with(schema(), batches, Tail::End, t.info, handler.token()).await);
            handlers.push(handler);
        }
        let streamer = SequenceDataStreamer::with_window(handlers, streams, window_rows).unwrap();
        (streamer, scope)
    }

    async fn collect(streamer: &mut SequenceDataStreamer) -> Vec<(String, i64, Option<i64>)> {
        let mut out = Vec::new();
        while let Some(row) = streamer.next().await.unwrap() {
            let value = row.view::<Value>().unwrap();
            out.push((row.topic().name().to_owned(), row.timestamp_ns, value));
        }
        out
    }

    #[tokio::test]
    async fn rows_in_time_order() {
        let (mut streamer, _scope) = open(
            vec![
                topic("/a", "Value", vec![&[1, 4], &[6]]),
                topic("/b", "Other", vec![&[2, 4, 7]]),
            ],
            3,
        )
        .await;
        let rows = collect(&mut streamer).await;
        let expected = [
            ("/a", 1, Some(10)),
            ("/b", 2, None),
            ("/a", 4, Some(40)),
            ("/b", 4, None),
            ("/a", 6, Some(60)),
            ("/b", 7, None),
        ];
        let expected: Vec<_> = expected
            .iter()
            .map(|(n, t, v)| (n.to_string(), *t, *v))
            .collect();
        assert_eq!(rows, expected);
        assert!(streamer.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn raw_row_access() {
        let (mut streamer, _scope) = open(vec![topic("/a", "Value", vec![&[1, 2]])], 8).await;
        streamer.next().await.unwrap();
        let row = streamer.next().await.unwrap().unwrap();
        assert_eq!(row.row(), 1);
        assert_eq!(row.batch().num_rows(), 2);
        assert_eq!(row.view::<Value>().unwrap(), Some(20));
        assert_eq!(row.view::<Value>().unwrap(), Some(20));
    }

    #[tokio::test]
    async fn same_tag_other_type() {
        let (mut streamer, _scope) = open(vec![topic("/a", "Value", vec![&[1]])], 8).await;
        let row = streamer.next().await.unwrap().unwrap();
        assert_eq!(row.view::<Value>().unwrap(), Some(10));
        let error = row.view::<Twin>().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }

    #[tokio::test]
    async fn view_schema_error() {
        let connection = Connection::offline(1 << 20);
        let scope = Scope::root();
        let handler = TopicHandler::offline(&connection, scope.child(), "seq", "/a", "Value");
        let only_time: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "timestamp_ns",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(only_time.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
                .unwrap();
        let stream = streamer_with(
            only_time,
            vec![batch],
            Tail::End,
            ReadInfo::default(),
            handler.token(),
        )
        .await;
        let mut streamer = SequenceDataStreamer::new(vec![handler], vec![stream]).unwrap();
        let row = streamer.next().await.unwrap().unwrap();
        assert_eq!(row.timestamp_ns, 1);
        assert_eq!(row.view::<Value>().unwrap_err().kind(), ErrorKind::Schema);
    }

    #[tokio::test]
    async fn next_timestamp_peeks() {
        let (mut streamer, _scope) = open(
            vec![
                topic("/a", "Value", vec![&[3]]),
                topic("/b", "Value", vec![&[1]]),
            ],
            8,
        )
        .await;
        assert_eq!(streamer.next_timestamp().await.unwrap(), Some(1));
        assert_eq!(streamer.next_timestamp().await.unwrap(), Some(1));
        assert_eq!(streamer.next().await.unwrap().unwrap().timestamp_ns, 1);
        assert_eq!(streamer.next_timestamp().await.unwrap(), Some(3));
        assert_eq!(streamer.next().await.unwrap().unwrap().timestamp_ns, 3);
        assert_eq!(streamer.next_timestamp().await.unwrap(), None);
    }

    #[tokio::test]
    async fn info_combines_topics() {
        let mut a = topic("/a", "Value", vec![&[5]]);
        a.info = ReadInfo {
            msg_count: 3,
            timestamp_ns_min: Some(5),
            timestamp_ns_max: Some(9),
        };
        let mut b = topic("/b", "Value", vec![&[2]]);
        b.info = ReadInfo {
            msg_count: 4,
            timestamp_ns_min: Some(2),
            timestamp_ns_max: Some(6),
        };
        let empty = topic("/c", "Value", vec![]);
        let (streamer, _scope) = open(vec![a, b, empty], 8).await;
        assert_eq!(streamer.msg_count(), 7);
        assert_eq!(streamer.timestamp_ns_min(), Some(2));
        assert_eq!(streamer.timestamp_ns_max(), Some(9));
        assert_eq!(streamer.topics().len(), 3);
    }

    #[tokio::test]
    async fn close_then_closed() {
        let (mut streamer, _scope) = open(vec![topic("/a", "Value", vec![&[1, 2]])], 8).await;
        streamer.next().await.unwrap();
        streamer.close();
        let error = streamer.next().await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Closed);
    }

    #[tokio::test]
    async fn scope_close_cascades() {
        let (mut streamer, scope) = open(
            vec![
                topic("/a", "Value", vec![&[1, 2]]),
                topic("/b", "Value", vec![&[3]]),
            ],
            8,
        )
        .await;
        streamer.next().await.unwrap();
        scope.close();
        let error = streamer.next().await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Closed);
    }

    #[tokio::test]
    async fn topic_close_cascades() {
        let (mut streamer, _scope) = open(
            vec![
                topic("/a", "Value", vec![&[1, 2]]),
                topic("/b", "Value", vec![&[3]]),
            ],
            8,
        )
        .await;
        streamer.next().await.unwrap();
        streamer.topics()[1].clone().close();
        let error = streamer.next().await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::Closed);
    }

    #[tokio::test]
    async fn no_topics_empty() {
        let mut streamer = SequenceDataStreamer::new(Vec::new(), Vec::new()).unwrap();
        assert!(streamer.next().await.unwrap().is_none());
        assert_eq!(streamer.msg_count(), 0);
        assert_eq!(streamer.timestamp_ns_min(), None);
    }

    #[test]
    fn streamer_is_send() {
        fn send<T: Send>() {}
        send::<SequenceDataStreamer>();
    }
    #[tokio::test]
    async fn next_retry_after_cancel() {
        let connection = Connection::offline(1 << 20);
        let scope = Scope::root();
        let a = TopicHandler::offline(&connection, scope.child(), "seq", "/a", "Value");
        let b = TopicHandler::offline(&connection, scope.child(), "seq", "/b", "Value");
        let fast = streamer_with(
            schema(),
            vec![batch(&[1, 2])],
            Tail::End,
            ReadInfo::default(),
            a.token(),
        )
        .await;
        let slow = delayed(
            schema(),
            vec![batch(&[3])],
            std::time::Duration::from_millis(200),
        )
        .await;
        let mut streamer = SequenceDataStreamer::new(vec![a, b], vec![fast, slow]).unwrap();
        let cut = tokio::time::timeout(std::time::Duration::from_millis(50), streamer.next()).await;
        assert!(cut.is_err());
        let rows = collect(&mut streamer).await;
        let got: Vec<_> = rows.iter().map(|(n, t, _)| (n.as_str(), *t)).collect();
        assert_eq!(got, vec![("/a", 1), ("/a", 2), ("/b", 3)]);
    }
}
