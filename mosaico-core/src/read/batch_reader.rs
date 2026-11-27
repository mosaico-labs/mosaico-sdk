use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_flight::Ticket;
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::error::FlightError;
use arrow_schema::SchemaRef;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::comm::connection::{Connection, Lease};
use crate::error::{Error, ErrorKind, Result};
use crate::scope::Token;

const PREFETCH: usize = 1;

pub(crate) struct BatchReader {
    schema: SchemaRef,
    batches: mpsc::Receiver<Result<RecordBatch>>,
    task: Option<JoinHandle<()>>,
}

impl BatchReader {
    pub(crate) async fn open(
        connection: &Connection,
        ticket: Ticket,
        token: Arc<Token>,
    ) -> Result<Self> {
        let data = connection.do_get(ticket).await?;
        let decoder = FlightDataDecoder::new(data.map(|item| item.map_err(FlightError::from)));
        Self::spawn(decoder, token, connection.lease()).await
    }

    #[cfg(test)]
    pub(crate) async fn start(decoder: FlightDataDecoder, token: Arc<Token>) -> Result<Self> {
        Self::spawn(decoder, token, None).await
    }

    async fn spawn(
        mut decoder: FlightDataDecoder,
        token: Arc<Token>,
        lease: Option<Arc<Lease>>,
    ) -> Result<Self> {
        let schema = read_schema(&mut decoder).await?;
        let (sender, batches) = mpsc::channel(PREFETCH);

        let task = tokio::spawn(async move {
            let _lease = lease;
            token
                .run_until_closed(forward_batches(decoder, sender))
                .await;
        });
        Ok(Self {
            schema,
            batches,
            task: Some(task),
        })
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(crate) async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let Some(task) = self.task.as_mut() else {
            return Ok(None);
        };
        if let Some(item) = self.batches.recv().await {
            return item.map(Some);
        }
        let ended = task.await;
        self.task = None;
        match ended {
            Ok(()) => Ok(None),
            Err(e) if e.is_panic() => {
                Err(Error::new(ErrorKind::Stream, "batch decoding failed").with_source(e))
            }
            Err(e) => Err(Error::new(ErrorKind::Stream, "batch reader cancelled").with_source(e)),
        }
    }
}

impl Drop for BatchReader {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn read_schema(decoder: &mut FlightDataDecoder) -> Result<SchemaRef> {
    while let Some(data) = decoder.next().await {
        match data?.payload {
            DecodedPayload::Schema(schema) => return Ok(schema),
            DecodedPayload::None => {}
            DecodedPayload::RecordBatch(_) => {
                return Err(Error::new(
                    ErrorKind::Protocol,
                    "record batch before the schema",
                ));
            }
        }
    }
    Err(Error::new(
        ErrorKind::Protocol,
        "stream ended before the schema",
    ))
}

async fn forward_batches(
    mut decoder: FlightDataDecoder,
    sender: mpsc::Sender<Result<RecordBatch>>,
) {
    loop {
        let Ok(permit) = sender.reserve().await else {
            return;
        };
        let Some(data) = decoder.next().await else {
            return;
        };
        let item = match data.map(|d| d.payload) {
            Ok(DecodedPayload::RecordBatch(batch)) if batch.num_rows() > 0 => Ok(batch),
            Ok(DecodedPayload::RecordBatch(_) | DecodedPayload::None) => continue,
            Ok(DecodedPayload::Schema(_)) => {
                Err(Error::new(ErrorKind::Protocol, "schema changed mid-stream"))
            }
            Err(e) => Err(Error::from(e)),
        };
        let failed = item.is_err();
        permit.send(item);
        if failed {
            return;
        }
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use arrow_array::RecordBatch;
    use arrow_flight::FlightData;
    use arrow_flight::decode::FlightDataDecoder;
    use arrow_flight::encode::FlightDataEncoderBuilder;
    use arrow_flight::error::FlightError;
    use arrow_schema::SchemaRef;
    use futures::stream::{self, BoxStream, StreamExt};

    use std::sync::Arc;

    use super::BatchReader;
    use crate::read::streamer::{ReadInfo, TopicBatchStreamer};
    use crate::scope::{Scope, Token};

    pub(crate) enum Tail {
        End,
        Fail,
        Hang,
    }

    pub(crate) async fn streamer(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        tail: Tail,
    ) -> TopicBatchStreamer {
        streamer_with(
            schema,
            batches,
            tail,
            ReadInfo::default(),
            Scope::root().token(),
        )
        .await
    }

    pub(crate) async fn delayed(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        delay: std::time::Duration,
    ) -> TopicBatchStreamer {
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::iter(batches.into_iter().map(Ok)))
            .enumerate()
            .then(move |(i, item)| async move {
                if i == 1 {
                    tokio::time::sleep(delay).await;
                }
                item
            });
        let token = Scope::root().token();
        let decoder = FlightDataDecoder::new(encoded);
        let reader = BatchReader::start(decoder, token.clone()).await.unwrap();
        TopicBatchStreamer::single(reader, ReadInfo::default(), token)
    }

    pub(crate) async fn streamer_with(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        tail: Tail,
        info: ReadInfo,
        token: Arc<Token>,
    ) -> TopicBatchStreamer {
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::iter(batches.into_iter().map(Ok)));
        let tail: BoxStream<'static, Result<FlightData, FlightError>> = match tail {
            Tail::End => stream::empty().boxed(),
            Tail::Fail => {
                stream::once(async { Err(FlightError::ProtocolError("cut".to_owned())) }).boxed()
            }
            Tail::Hang => stream::pending().boxed(),
        };
        let decoder = FlightDataDecoder::new(encoded.chain(tail));
        let reader = BatchReader::start(decoder, token.clone()).await.unwrap();
        TopicBatchStreamer::single(reader, info, token)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_flight::decode::FlightDataDecoder;
    use arrow_flight::encode::FlightDataEncoderBuilder;
    use arrow_schema::{DataType, Field, Schema};
    use futures::stream::{self, StreamExt};

    use super::BatchReader;
    use crate::scope::Scope;

    #[tokio::test]
    async fn skips_empty_batches() {
        let schema = Schema::new(vec![Field::new("timestamp_ns", DataType::Int64, false)]);
        let batches: Vec<_> = [vec![], vec![1, 2], vec![], vec![3]]
            .into_iter()
            .map(|values| {
                RecordBatch::try_new(
                    Arc::new(schema.clone()),
                    vec![Arc::new(Int64Array::from(values))],
                )
                .unwrap()
            })
            .collect();
        let data = arrow_flight::utils::batches_to_flight_data(&schema, batches).unwrap();
        assert_eq!(data.len(), 5);
        let decoder = FlightDataDecoder::new(stream::iter(data.into_iter().map(Ok)));
        let mut reader = BatchReader::start(decoder, Scope::root().token())
            .await
            .unwrap();
        let mut rows = Vec::new();
        while let Some(batch) = reader.next_batch().await.unwrap() {
            rows.push(batch.num_rows());
        }
        assert_eq!(rows, [2, 1]);
    }

    #[tokio::test]
    async fn close_frees_window() {
        let connection = crate::comm::connection::Connection::offline(1 << 20);
        let read = connection.reader().await;
        assert_eq!(connection.main_committed(), 16 << 20);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp_ns",
            DataType::Int64,
            false,
        )]));
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::empty())
            .chain(stream::pending());
        let token = Scope::root().token();
        let reader =
            BatchReader::spawn(FlightDataDecoder::new(encoded), token.clone(), read.lease())
                .await
                .unwrap();
        drop(read);
        assert_eq!(connection.main_committed(), 16 << 20);
        token.close();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(connection.main_committed(), 0);
        drop(reader);
    }

    #[tokio::test]
    async fn reads_one_ahead() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp_ns",
            DataType::Int64,
            false,
        )]));
        let batches: Vec<_> = (0..5)
            .map(|i| {
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![i]))])
                    .map_err(Into::into)
            })
            .collect();
        let pulled = Arc::new(AtomicUsize::new(0));
        let counter = pulled.clone();
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::iter(batches))
            .inspect(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
            });
        let mut reader = BatchReader::start(FlightDataDecoder::new(encoded), Scope::root().token())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(pulled.load(Ordering::SeqCst), 2);
        assert!(reader.next_batch().await.unwrap().is_some());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(pulled.load(Ordering::SeqCst), 3);
    }
}
