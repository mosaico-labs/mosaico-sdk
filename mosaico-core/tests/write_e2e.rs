use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use mosaico_core::{
    ConnectOptions, Format, MosaicoClient, SessionErrorPolicy, TopicErrorPolicy, TopicWriterStatus,
};

const ROW_BYTES: usize = 4 << 20;
const ROWS_PER_BATCH: usize = 8;
const BATCHES: usize = 12;

async fn client() -> Option<MosaicoClient> {
    let port = std::env::var("MOSAICO_PORT").ok()?.parse().ok()?;
    Some(
        MosaicoClient::connect(ConnectOptions::new("localhost", port))
            .await
            .expect("mosaicod reachable on MOSAICO_PORT"),
    )
}

fn unique(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}_{nanos}")
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("data", DataType::Binary, false),
    ]))
}

fn batch(index: usize, payload: &[u8]) -> RecordBatch {
    let first = (index * ROWS_PER_BATCH) as i64;
    let timestamps: ArrayRef = Arc::new(Int64Array::from_iter_values(
        first..first + ROWS_PER_BATCH as i64,
    ));
    let data: ArrayRef = Arc::new(BinaryArray::from_iter_values(std::iter::repeat_n(
        payload,
        ROWS_PER_BATCH,
    )));
    RecordBatch::try_new(schema(), vec![timestamps, data]).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_close_finishes_upload() {
    let Some(client) = client().await else {
        return;
    };
    let sequence = unique("rust_e2e_cancelled_close");
    let mut writer = client
        .sequence_create(&sequence, None, SessionErrorPolicy::Report)
        .await
        .unwrap();
    let topic = writer
        .topic_create(
            "/blob",
            "Blob",
            Format::Default,
            schema(),
            None,
            TopicErrorPolicy::Raise,
        )
        .await
        .unwrap();
    let payload = vec![7u8; ROW_BYTES];
    for index in 0..BATCHES {
        topic.push_batch(batch(index, &payload)).await.unwrap();
    }
    let cut = tokio::time::timeout(Duration::from_millis(5), topic.finalize(None)).await;
    let in_window = cut.is_err() && topic.status() == TopicWriterStatus::Finalized;
    let finished = writer.finish().await;
    let last = match &finished {
        Ok(()) => client
            .topic_handler(&sequence, "/blob")
            .await
            .ok()
            .and_then(|handler| handler.timestamp_ns_max()),
        Err(_) => None,
    };
    let _ = client.sequence_delete(&sequence).await;
    assert!(in_window, "the close was not cut while uploading");
    finished.unwrap();
    assert_eq!(last, Some((BATCHES * ROWS_PER_BATCH) as i64 - 1));
}
