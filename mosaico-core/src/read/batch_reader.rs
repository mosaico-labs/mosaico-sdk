use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_flight::Ticket;
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::error::FlightError;
use arrow_schema::SchemaRef;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::comm::connection::Connection;
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
        Self::start(decoder, token).await
    }

    pub(crate) async fn start(mut decoder: FlightDataDecoder, token: Arc<Token>) -> Result<Self> {
        let schema = read_schema(&mut decoder).await?;
        let (sender, batches) = mpsc::channel(PREFETCH);
        let task = tokio::spawn(async move {
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

    pub(crate) fn close(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl Drop for BatchReader {
    fn drop(&mut self) {
        self.close();
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
    while let Some(data) = decoder.next().await {
        let item = match data.map(|d| d.payload) {
            Ok(DecodedPayload::RecordBatch(batch)) => Ok(batch),
            Ok(DecodedPayload::None) => continue,
            Ok(DecodedPayload::Schema(_)) => {
                Err(Error::new(ErrorKind::Protocol, "schema changed mid-stream"))
            }
            Err(e) => Err(Error::from(e)),
        };
        let failed = item.is_err();
        if sender.send(item).await.is_err() || failed {
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

    use super::BatchReader;
    use crate::read::streamer::BatchStreamer;
    use crate::scope::Scope;

    pub(crate) enum Tail {
        End,
        Fail,
        Hang,
    }

    pub(crate) async fn streamer(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        tail: Tail,
    ) -> BatchStreamer {
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
        let token = Scope::root().token();
        let decoder = FlightDataDecoder::new(encoded.chain(tail));
        let reader = BatchReader::start(decoder, token.clone()).await.unwrap();
        BatchStreamer::single(reader, token)
    }
}
