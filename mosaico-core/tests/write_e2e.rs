use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use mosaico_core::{
    ConnectOptions, Error, Format, MosaicoClient, SequenceOrder, SessionLevelErrorPolicy,
    Timestamp, TopicLevelErrorPolicy, TopicWriterStatus,
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
        .sequence_create(&sequence, None, SessionLevelErrorPolicy::Report)
        .await
        .unwrap();
    let topic = writer
        .topic_create(
            "/blob",
            "Blob",
            Format::Default,
            schema(),
            None,
            TopicLevelErrorPolicy::Raise,
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
            .and_then(|handler| handler.timestamp_ns_max())
            .map(Timestamp::as_ns),
        Err(_) => None,
    };
    let _ = client.sequence_delete(&sequence).await;
    assert!(in_window, "the close was not cut while uploading");
    finished.unwrap();
    assert_eq!(last, Some((BATCHES * ROWS_PER_BATCH) as i64 - 1));
}

async fn write_blob(writer: &mut mosaico_core::SequenceWriter, topic: &str, batches: usize) {
    let topic = writer
        .topic_create(
            topic,
            "Blob",
            Format::Default,
            schema(),
            None,
            TopicLevelErrorPolicy::Raise,
        )
        .await
        .unwrap();
    for index in 0..batches {
        topic.push_batch(batch(index, b"x")).await.unwrap();
    }
}

async fn rows(handler: &mosaico_core::SequenceHandler, topics: &[&str]) -> usize {
    let mut streamer = handler.get_window_streamer(topics, ..).await.unwrap();
    let mut rows = 0;
    while let Some(window) = streamer.next_window().await.unwrap() {
        rows += window.order.len();
    }
    rows
}

#[tokio::test(flavor = "multi_thread")]
async fn sequence_reload() {
    let Some(client) = client().await else {
        return;
    };
    let sequence = unique("rust_e2e_reload");
    let mut writer = client
        .sequence_create(&sequence, None, SessionLevelErrorPolicy::Delete)
        .await
        .unwrap();
    write_blob(&mut writer, "/a", 1).await;
    writer.finish().await.unwrap();
    let mut handler = client.sequence_handler(&sequence).await.unwrap();
    let held = handler.topic_handler("/a").unwrap();
    let mut early = held.get_batch_streamer(..).await.unwrap();
    let mut update = client
        .sequence_update(&sequence, SessionLevelErrorPolicy::Delete)
        .await
        .unwrap();
    write_blob(&mut update, "/b", 2).await;
    update.finish().await.unwrap();
    let stale: Vec<_> = handler.topics().map(str::to_owned).collect();
    let missing = handler.topic_handler("/b").err().map(|e| e.kind());
    handler.reload().await.unwrap();
    let fresh: Vec<_> = handler.topics().map(str::to_owned).collect();
    let sessions = handler.sessions().len();
    let read_b = rows(&handler, &["/b"]).await;
    let read_all = rows(&handler, &[]).await;
    let read_held = held.to_arrow(..).await.unwrap().1.len();
    handler.close();
    let closed = early.next_batch().await.err().map(|e| e.kind());
    client.sequence_delete(&sequence).await.unwrap();
    let gone = handler.reload().await.err().map(|e| e.kind());
    assert_eq!(stale, ["/a"]);
    assert_eq!(missing, Some(mosaico_core::ErrorKind::NotFound));
    assert_eq!(fresh, ["/a", "/b"]);
    assert_eq!(sessions, 2);
    assert_eq!(read_b, 2 * ROWS_PER_BATCH);
    assert_eq!(read_all, 3 * ROWS_PER_BATCH);
    assert!(read_held > 0);
    assert_eq!(closed, Some(mosaico_core::ErrorKind::Closed));
    assert_eq!(gone, Some(mosaico_core::ErrorKind::NotFound));
    assert_eq!(handler.topics().count(), 2);
}

async fn create_three(client: &MosaicoClient, prefix: &str) -> Vec<String> {
    let base = unique(prefix);
    let mut names = Vec::new();
    for suffix in ["c", "a", "b"] {
        let name = format!("{base}_{suffix}");
        let mut writer = client
            .sequence_create(&name, None, SessionLevelErrorPolicy::Delete)
            .await
            .unwrap();
        let topic = writer
            .topic_create(
                "/blob",
                "Blob",
                Format::Default,
                schema(),
                None,
                TopicLevelErrorPolicy::Raise,
            )
            .await
            .unwrap();
        topic.push_batch(batch(0, b"x")).await.unwrap();
        writer.finish().await.unwrap();
        names.push(name);
    }
    names
}

async fn cleanup(client: &MosaicoClient, names: &[String]) {
    for name in names {
        let _ = client.sequence_delete(name).await;
    }
}

fn ours<'a>(listed: &'a [String], names: &[String]) -> Vec<&'a str> {
    listed
        .iter()
        .filter(|n| names.contains(n))
        .map(String::as_str)
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn paged_listing() {
    let Some(client) = client().await else {
        return;
    };
    let names = create_three(&client, "rust_e2e_paged_listing").await;
    let result = async {
        let mut listed = Vec::new();
        let mut pages = Vec::new();
        let mut page = client.list_sequences_page(2, SequenceOrder::Name).await?;
        loop {
            pages.push(page.names.len());
            listed.extend(page.names);
            let Some(cursor) = page.next_cursor else {
                break;
            };
            page = client.list_sequences_page(2, &cursor).await?;
        }
        Ok::<_, Error>((listed, pages))
    }
    .await;
    cleanup(&client, &names).await;
    let (listed, pages) = result.unwrap();
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(ours(&listed, &names), sorted);
    assert!(listed.windows(2).all(|w| w[0].as_bytes() < w[1].as_bytes()));
    assert!(pages[..pages.len() - 1].iter().all(|&n| n == 2));
    assert!(pages[pages.len() - 1] < 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn created_order_pages() {
    let Some(client) = client().await else {
        return;
    };
    let names = create_three(&client, "rust_e2e_created_pages").await;
    let result = async {
        let mut listed = Vec::new();
        let mut sizes = Vec::new();
        let mut page = client
            .list_sequences_page(1, SequenceOrder::CreatedAt)
            .await?;
        while let Some(cursor) = page.next_cursor.take() {
            sizes.push(page.names.len());
            listed.append(&mut page.names);
            page = client.list_sequences_page(1, cursor).await?;
        }
        listed.append(&mut page.names);
        let all = client.list_sequences().await?;
        Ok::<_, Error>((listed, sizes, all))
    }
    .await;
    cleanup(&client, &names).await;
    let (listed, sizes, all) = result.unwrap();
    assert!(sizes.iter().all(|&n| n == 1));
    assert_eq!(ours(&listed, &names), names);
    assert_eq!(ours(&all, &names), names);
}

#[tokio::test(flavor = "multi_thread")]
async fn iter_matches_list() {
    let Some(client) = client().await else {
        return;
    };
    let names = create_three(&client, "rust_e2e_iter_sequences").await;
    let result = async {
        let mut by_name = client.iter_sequences(2, SequenceOrder::Name);
        let mut listed = Vec::new();
        while let Some(name) = by_name.next().await? {
            listed.push(name);
        }
        let again = by_name.next().await?;
        let mut unlimited = client.iter_sequences(0, SequenceOrder::CreatedAt);
        let mut created = Vec::new();
        while let Some(name) = unlimited.next().await? {
            created.push(name);
        }
        let all = client.list_sequences().await?;
        Ok::<_, Error>((listed, again, created, all))
    }
    .await;
    cleanup(&client, &names).await;
    let (listed, again, created, all) = result.unwrap();
    assert!(again.is_none());
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(ours(&listed, &names), sorted);
    assert_eq!(ours(&created, &names), names);
    assert_eq!(ours(&all, &names), names);
}
