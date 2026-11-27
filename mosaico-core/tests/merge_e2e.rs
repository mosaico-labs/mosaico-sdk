use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use mosaico_core::{
    ConnectOptions, Error, ErrorKind, Format, MosaicoClient, Ontology, Result,
    SequenceDataStreamer, SequenceHandler, SessionLevelErrorPolicy, Timestamp,
    TopicLevelErrorPolicy,
};

const T0: i64 = 1_700_000_000_000_000_000;

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

struct Spec {
    name: &'static str,
    tag: &'static str,
    rows: i64,
    period: i64,
    offset: i64,
}

const TOPICS: [Spec; 4] = [
    Spec {
        name: "/fast",
        tag: "Value",
        rows: 3000,
        period: 1,
        offset: 0,
    },
    Spec {
        name: "/slow",
        tag: "Value",
        rows: 300,
        period: 10,
        offset: 0,
    },
    Spec {
        name: "/other",
        tag: "Other",
        rows: 30,
        period: 100,
        offset: 5,
    },
    Spec {
        name: "/empty",
        tag: "Value",
        rows: 0,
        period: 1,
        offset: 0,
    },
];

async fn client() -> Option<MosaicoClient> {
    let port = std::env::var("MOSAICO_PORT").ok()?.parse().ok()?;
    Some(
        MosaicoClient::connect(ConnectOptions::new("localhost", port))
            .await
            .expect("mosaicod reachable on MOSAICO_PORT"),
    )
}

fn unique(prefix: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{nanos}_{n}")
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn timestamp(spec: &Spec, i: i64) -> i64 {
    T0 + spec.offset + i * spec.period
}

async fn write(client: &MosaicoClient, sequence: &str) {
    let mut writer = client
        .sequence_create(sequence, None, SessionLevelErrorPolicy::Report)
        .await
        .unwrap();
    for spec in &TOPICS {
        let topic = writer
            .topic_create(
                spec.name,
                spec.tag,
                Format::Default,
                schema(),
                None,
                TopicLevelErrorPolicy::Raise,
            )
            .await
            .unwrap();
        for start in (0..spec.rows).step_by(700) {
            let end = (start + 700).min(spec.rows);
            let timestamps: ArrayRef = Arc::new(Int64Array::from_iter_values(
                (start..end).map(|i| timestamp(spec, i)),
            ));
            let values: ArrayRef = Arc::new(Int64Array::from_iter_values(start..end));
            let batch = RecordBatch::try_new(schema(), vec![timestamps, values]).unwrap();
            topic.push_batch(batch).await.unwrap();
        }
    }
    writer.finish().await.unwrap();
}

fn expected(order: &[&str], start: Option<i64>, end: Option<i64>) -> Vec<(String, i64, i64)> {
    let mut rows = Vec::new();
    for (rank, name) in order.iter().enumerate() {
        let spec = TOPICS.iter().find(|s| s.name == *name).unwrap();
        for i in 0..spec.rows {
            let ts = timestamp(spec, i);
            if start.is_some_and(|s| ts < s) || end.is_some_and(|e| ts >= e) {
                continue;
            }
            rows.push((rank, ts, i, spec.name.to_owned()));
        }
    }
    rows.sort_by_key(|(rank, ts, i, _)| (*ts, *rank, *i));
    rows.into_iter().map(|(_, ts, i, n)| (n, ts, i)).collect()
}

async fn collect(stream: &mut SequenceDataStreamer) -> Result<Vec<(String, i64, i64)>> {
    let mut rows = Vec::new();
    while let Some(row) = stream.next().await? {
        let name = row.topic().name().to_owned();
        let value = match row.view::<Value>()? {
            Some(v) => v,
            None => {
                assert_eq!(row.topic().ontology_tag(), "Other");
                row.batch()
                    .column(1)
                    .as_primitive::<Int64Type>()
                    .value(row.row())
            }
        };
        rows.push((name, row.timestamp_ns.as_ns(), value));
    }
    Ok(rows)
}

async fn sequence() -> Option<(MosaicoClient, SequenceHandler, String)> {
    let client = client().await?;
    let name = unique("rust_e2e_merge");
    write(&client, &name).await;
    let seq = client.sequence_handler(&name).await.unwrap();
    Some((client, seq, name))
}

async fn cleanup(client: &MosaicoClient, name: &str) {
    client.sequence_delete(name).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_all_topics() {
    let Some((client, seq, name)) = sequence().await else {
        return;
    };
    let names: Vec<&str> = seq.topics().collect();
    assert_eq!(names, ["/empty", "/fast", "/other", "/slow"]);
    let mut stream = seq.get_data_streamer(&[], ..).await.unwrap();
    assert_eq!(stream.msg_count(), 3330);
    assert_eq!(stream.timestamp_ns_min(), Some(Timestamp::from_ns(T0)));
    assert_eq!(
        stream.timestamp_ns_max(),
        Some(Timestamp::from_ns(T0 + 2999))
    );
    assert_eq!(
        stream.next_timestamp().await.unwrap(),
        Some(Timestamp::from_ns(T0))
    );
    let rows = collect(&mut stream).await.unwrap();
    assert_eq!(rows, expected(&names, None, None));
    assert!(stream.next().await.unwrap().is_none());
    cleanup(&client, &name).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ties_follow_topics() {
    let Some((client, seq, name)) = sequence().await else {
        return;
    };
    let order = ["/slow", "/fast"];
    let mut stream = seq.get_data_streamer(&["slow", "/fast"], ..).await.unwrap();
    let rows = collect(&mut stream).await.unwrap();
    assert_eq!(rows[..3], expected(&order, None, None)[..3]);
    assert_eq!(rows[0].0, "/slow");
    assert_eq!(rows[1].0, "/fast");
    assert_eq!(rows, expected(&order, None, None));
    cleanup(&client, &name).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ranged_merge() {
    let Some((client, seq, name)) = sequence().await else {
        return;
    };
    let (start, end) = (T0 + 95, T0 + 405);
    let mut stream = seq.get_data_streamer(&[], start..end).await.unwrap();
    let topics = ["/empty", "/fast", "/other", "/slow"];
    let want = expected(&topics, Some(start), Some(end));
    assert_eq!(stream.msg_count(), want.len() as u64);
    assert_eq!(
        stream.timestamp_ns_min(),
        Some(Timestamp::from_ns(want[0].1))
    );
    assert_eq!(
        stream.timestamp_ns_max(),
        Some(Timestamp::from_ns(want[want.len() - 1].1))
    );
    let rows = collect(&mut stream).await.unwrap();
    assert_eq!(rows, want);
    cleanup(&client, &name).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn range_without_data() {
    let Some((client, seq, name)) = sequence().await else {
        return;
    };
    let mut stream = seq
        .get_data_streamer(&["/fast", "/empty"], T0 + 10_000..)
        .await
        .unwrap();
    assert_eq!(stream.msg_count(), 0);
    assert!(stream.next().await.unwrap().is_none());
    let mut empty = seq.get_data_streamer(&["/empty"], ..).await.unwrap();
    assert!(empty.next().await.unwrap().is_none());
    cleanup(&client, &name).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_arguments() {
    let Some((client, seq, name)) = sequence().await else {
        return;
    };
    let unknown = seq.get_data_streamer(&["/nope"], ..).await.err().unwrap();
    assert_eq!(unknown.kind(), ErrorKind::NotFound);
    let twice = seq
        .get_data_streamer(&["/fast", "fast"], ..)
        .await
        .err()
        .unwrap();
    assert_eq!(twice.kind(), ErrorKind::InvalidArgument);
    let inverted = seq
        .get_data_streamer(
            &["/fast", "/slow"],
            Timestamp::from_ns(T0 + 10)..Timestamp::from_ns(T0 + 10),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(inverted.kind(), ErrorKind::InvalidArgument);
    let other = crate::client().await.unwrap();
    let foreign = other.topic_handler(&name, "/fast").await.unwrap();
    let mixed = seq
        .get_window_streamer_for(vec![foreign], ..)
        .await
        .err()
        .unwrap();
    assert_eq!(mixed.kind(), ErrorKind::InvalidArgument);
    cleanup(&client, &name).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn close_stops_merge() {
    let Some((client, seq, name)) = sequence().await else {
        return;
    };
    let mut stream = seq.get_data_streamer(&[], ..).await.unwrap();
    stream.next().await.unwrap().unwrap();
    seq.close();
    let closed = stream.next().await.err().unwrap();
    assert_eq!(closed.kind(), ErrorKind::Closed);
    let mut again = seq.get_data_streamer(&[], ..).await.unwrap();
    assert!(again.next().await.unwrap().is_some());
    again.close();
    assert_eq!(again.next().await.err().unwrap().kind(), ErrorKind::Closed);
    cleanup(&client, &name).await;
}
