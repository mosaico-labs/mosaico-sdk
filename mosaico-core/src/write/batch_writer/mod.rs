mod budget;
mod encoder;
mod pending;
mod schema;

use std::pin::pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use arrow_array::{Array, RecordBatch, make_array};
use arrow_flight::{FlightData, FlightDescriptor, SchemaAsIpc};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{DataType, Schema, SchemaRef};
use futures::future::Either;
use prost::Message;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::comm::action::ActionKind;
use crate::comm::connection::{Connection, ServerInfo};
use crate::comm::proto;
use crate::error::{Error, ErrorKind, Result};
use crate::scope::Token;
use crate::write::session::{Interrupter, interrupted_error};
use crate::write::topic::{FlushPolicy, Status};

pub(crate) use self::budget::WriteBudget;
use self::encoder::{Encoder, ipc_compression, split};
use self::pending::Pending;
pub(crate) use self::schema::Conversion;
use self::schema::{plain_schema, relabel, shape, shape_of, unmasked_nulls};

const QUEUE_MESSAGES: usize = 2;

fn limits(info: &ServerInfo) -> (usize, usize) {
    let limit = usize::try_from(info.max_batch_size_bytes()).unwrap_or(usize::MAX);
    (limit, limit.clamp(1, u32::MAX as usize / QUEUE_MESSAGES))
}

enum Input {
    Batch(Queued),
    Flush(oneshot::Sender<()>),
    Policy(FlushPolicy),
    Finish,
}

struct Queued {
    batch: RecordBatch,
    bytes: usize,
    since: Option<Instant>,
    urgent: bool,
    permit: Permits,
}

struct Permits {
    _topic: OwnedSemaphorePermit,
    _budget: OwnedSemaphorePermit,
}

enum Frame {
    Data(FlightData),
    End,
}

type Failure = Arc<Mutex<Option<Error>>>;

pub(crate) struct BatchWriter {
    input: Option<mpsc::UnboundedSender<Input>>,
    schema: SchemaRef,
    shapes: Vec<DataType>,
    accepted: Option<(SchemaRef, Conversion)>,
    target_bytes: usize,
    record_limit: usize,
    queue: Queue,
    failure: Failure,
    status: Arc<Status>,
    interrupter: Interrupter,
    task: Option<JoinHandle<()>>,
}

impl BatchWriter {
    pub(crate) fn open(
        connection: &Connection,
        locator: String,
        topic_uuid: String,
        schema: SchemaRef,
        token: Arc<Token>,
        interrupter: Interrupter,
    ) -> Result<Self> {
        let (limit, target_bytes) = limits(connection.info());
        let options = IpcWriteOptions::default()
            .try_with_compression(ipc_compression(connection.compression()))
            .map_err(|e| {
                Error::new(ErrorKind::InvalidArgument, "invalid IPC compression").with_source(e)
            })?;
        let schema = plain_schema(&schema);
        let descriptor = FlightDescriptor::new_cmd(
            proto::flight::DoPutCmd {
                locator: locator.clone(),
                topic_uuid,
            }
            .encode_to_vec(),
        );
        let header =
            FlightData::from(SchemaAsIpc::new(&schema, &options)).with_descriptor(descriptor);
        let (input, receiver) = mpsc::unbounded_channel();
        let budget = connection.write_budget().clone();
        let pressure = budget.pressure();
        let queue = Queue::new(target_bytes, budget);
        let (frames, outgoing) = mpsc::channel(1);
        let failure = Failure::default();
        let status = Arc::new(Status::new());
        let encoding = Encoding {
            encoder: Arc::new(Encoder {
                schema: schema.clone(),
                options,
                limit,
            }),
            frames,
            notifier: Notifier {
                connection: connection.clone(),
                locator,
                status: status.clone(),
            },
            interrupter: interrupter.clone(),
        };
        let pending = Pending {
            target: target_bytes,
            policy: FlushPolicy::default(),
            queued: Vec::new(),
            bytes: 0,
            rows: 0,
            since: None,
        };
        let encode = encode_loop(receiver, header, pending, encoding, Some(pressure));
        let upload = upload(connection.dedicated(), outgoing);
        let slot = failure.clone();
        let closing = queue.semaphore.clone();
        let task = tokio::spawn(async move {
            let error = match token
                .run_until_closed(futures::future::try_join(upload, encode))
                .await
            {
                Some(Ok(_)) => None,
                Some(Err(error)) => Some(error),
                None => Some(Error::new(ErrorKind::Closed, "client closed")),
            };
            if let Some(error) = error {
                lock(&slot).get_or_insert(error);
            }
            closing.close();
        });
        let shapes = schema
            .fields()
            .iter()
            .map(|field| shape(field.data_type()))
            .collect();
        Ok(Self {
            input: Some(input),
            schema,
            shapes,
            accepted: None,
            target_bytes,
            record_limit: limit,
            queue,
            failure,
            status,
            interrupter,
            task: Some(task),
        })
    }

    pub(crate) fn target_bytes(&self) -> usize {
        self.target_bytes
    }

    pub(crate) fn record_limit(&self) -> usize {
        self.record_limit
    }

    pub(crate) fn status(&self) -> Arc<Status> {
        self.status.clone()
    }

    pub(crate) fn interrupted(&self) -> bool {
        self.interrupter.interrupted()
    }

    pub(crate) async fn enqueue(
        &mut self,
        batch: RecordBatch,
        since: Option<Instant>,
        urgent: bool,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if self.interrupter.interrupted() {
            return Err(interrupted_error());
        }
        let input = self.input()?.clone();
        for (batch, bytes) in split(batch, self.target_bytes) {
            let Some(permit) = self.queue.acquire(bytes).await else {
                return Err(self.stopped().await);
            };
            if self.interrupter.interrupted() {
                return Err(interrupted_error());
            }
            let queued = Queued {
                batch,
                bytes,
                since,
                urgent,
                permit,
            };
            if input.send(Input::Batch(queued)).is_err() {
                return Err(self.stopped().await);
            }
        }
        Ok(())
    }

    pub(crate) async fn flush(&mut self) -> Result<()> {
        let (done, flushed) = oneshot::channel();
        if self.input()?.send(Input::Flush(done)).is_err() {
            return Err(self.stopped().await);
        }
        match flushed.await {
            Ok(()) if self.interrupter.interrupted() => Err(interrupted_error()),
            Ok(()) => Ok(()),
            Err(_) => Err(self.stopped().await),
        }
    }

    pub(crate) fn set_policy(&self, policy: FlushPolicy) -> Result<()> {
        self.input()?
            .send(Input::Policy(policy))
            .map_err(|_| self.failure())
    }

    async fn stopped(&mut self) -> Error {
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        self.failure()
    }

    pub(crate) async fn finish(mut self) -> Result<()> {
        if let Some(input) = self.input.take() {
            let _ = input.send(Input::Finish);
        }
        if let Some(task) = self.task.take() {
            task.await.map_err(|e| {
                Error::new(ErrorKind::Stream, "topic writer task failed").with_source(e)
            })?;
        }
        match lock(&self.failure).take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(crate) async fn conform(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let schema = batch.schema();
        let conversion = match &self.accepted {
            Some((accepted, conversion)) if Arc::ptr_eq(accepted, &schema) => *conversion,
            _ => {
                let conversion = self.check_shape(&schema)?;
                self.accepted = Some((schema, conversion));
                conversion
            }
        };
        self.check_nulls(&batch)?;
        let schema = self.schema.clone();
        match conversion {
            Conversion::None => return Ok(batch),
            Conversion::Relabel => {
                let columns = batch
                    .columns()
                    .iter()
                    .zip(schema.fields())
                    .map(|(column, field)| make_array(relabel(column.to_data(), field.data_type())))
                    .collect();
                return RecordBatch::try_new(schema, columns)
                    .map_err(|e| Error::schema("cannot relabel columns").with_source(e));
            }
            Conversion::Cast => {}
        }
        tokio::task::spawn_blocking(move || {
            let columns = batch
                .columns()
                .iter()
                .zip(schema.fields())
                .map(|(column, field)| arrow_cast::cast(column, field.data_type()))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            RecordBatch::try_new(schema, columns)
        })
        .await
        .map_err(|e| Error::new(ErrorKind::Stream, "conversion task failed").with_source(e))?
        .map_err(|e| Error::schema("cannot convert view columns").with_source(e))
    }

    pub(crate) fn check_nulls(&self, batch: &RecordBatch) -> Result<()> {
        for (column, field) in batch.columns().iter().zip(self.schema.fields()) {
            if let Some(path) = unmasked_nulls(column.as_ref(), field, None) {
                return Err(Error::schema(format!(
                    "column '{path}' is not nullable but contains nulls"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn check_shape(&self, schema: &Schema) -> Result<Conversion> {
        let fields = self.schema.fields();
        if schema.fields().len() != fields.len() {
            return Err(Error::schema(format!(
                "batch has {} columns, topic schema has {}",
                schema.fields().len(),
                fields.len()
            )));
        }
        let mut conversion = Conversion::None;
        for ((actual, expected), expected_shape) in
            schema.fields().iter().zip(fields).zip(&self.shapes)
        {
            if actual.name() != expected.name() || shape(actual.data_type()) != *expected_shape {
                return Err(Error::schema(format!(
                    "column '{}' ({}) does not match topic column '{}' ({})",
                    actual.name(),
                    actual.data_type(),
                    expected.name(),
                    expected.data_type()
                )));
            }
            if actual.data_type() == expected.data_type() {
                continue;
            }
            if shape_of(actual.data_type(), false) == shape_of(expected.data_type(), false) {
                conversion = conversion.max(Conversion::Relabel);
            } else {
                conversion = Conversion::Cast;
            }
        }
        Ok(conversion)
    }

    fn input(&self) -> Result<&mpsc::UnboundedSender<Input>> {
        self.input
            .as_ref()
            .ok_or_else(|| Error::new(ErrorKind::Closed, "topic writer finished"))
    }

    fn failure(&self) -> Error {
        match lock(&self.failure).as_ref() {
            Some(error) => error.duplicate(),
            None => Error::new(ErrorKind::Closed, "topic writer closed"),
        }
    }
}

impl Drop for BatchWriter {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

struct Queue {
    semaphore: Arc<Semaphore>,
    message: usize,
    budget: Arc<WriteBudget>,
}

impl Queue {
    fn new(message: usize, budget: Arc<WriteBudget>) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(message * QUEUE_MESSAGES)),
            message,
            budget,
        }
    }

    async fn acquire(&self, bytes: usize) -> Option<Permits> {
        let permits = u32::try_from(bytes.clamp(1, self.message)).unwrap_or(u32::MAX);
        let topic = self
            .semaphore
            .clone()
            .acquire_many_owned(permits)
            .await
            .ok()?;
        let budget = self.budget.acquire(permits, &self.semaphore).await?;
        Some(Permits {
            _topic: topic,
            _budget: budget,
        })
    }
}

struct Notifier {
    connection: Connection,
    locator: String,
    status: Arc<Status>,
}

impl Notifier {
    async fn record_dropped(&self, count: u64) {
        let message = format!(
            "{count} record(s) dropped for topic '{}': a single record exceeds the message size limit",
            self.locator
        );
        self.status.record_dropped(count, message.clone());
        let _ = self
            .connection
            .notify_error(ActionKind::TopicNotificationCreate, &self.locator, message)
            .await;
    }
}

struct Encoding {
    encoder: Arc<Encoder>,
    frames: mpsc::Sender<Frame>,
    notifier: Notifier,
    interrupter: Interrupter,
}

impl Encoding {
    async fn send(&self, group: Vec<Queued>) -> Result<bool> {
        if self.interrupter.interrupted() {
            return Ok(true);
        }
        let (batches, permits): (Vec<_>, Vec<_>) = group
            .into_iter()
            .map(|queued| (queued.batch, queued.permit))
            .unzip();
        let worker = self.encoder.clone();
        let (flights, dropped) = tokio::task::spawn_blocking(move || worker.encode_all(batches))
            .await
            .map_err(|e| Error::new(ErrorKind::Stream, "encoding task failed").with_source(e))??;
        for flight in flights {
            if self.interrupter.interrupted() {
                break;
            }
            if self.frames.send(Frame::Data(flight)).await.is_err() {
                return Ok(false);
            }
        }
        drop(permits);
        if dropped > 0 {
            self.notifier.record_dropped(dropped).await;
        }
        Ok(true)
    }
}

enum Event {
    Input(Option<Input>),
    Pressure,
    PressureGone,
}

async fn next_event(
    input: &mut mpsc::UnboundedReceiver<Input>,
    pressure: Option<&mut watch::Receiver<u64>>,
) -> Event {
    let Some(pressure) = pressure else {
        return Event::Input(input.recv().await);
    };
    let changed = pin!(pressure.changed());
    let received = pin!(input.recv());
    match futures::future::select(changed, received).await {
        Either::Left((Ok(()), _)) => Event::Pressure,
        Either::Left((Err(_), _)) => Event::PressureGone,
        Either::Right((command, _)) => Event::Input(command),
    }
}

async fn encode_loop(
    mut input: mpsc::UnboundedReceiver<Input>,
    header: FlightData,
    mut pending: Pending,
    encoding: Encoding,
    mut pressure: Option<watch::Receiver<u64>>,
) -> Result<()> {
    if encoding.frames.send(Frame::Data(header)).await.is_err() {
        return Ok(());
    }
    loop {
        let command = match next_event(&mut input, pressure.as_mut()).await {
            Event::Input(Some(command)) => command,
            Event::Input(None) => break,
            Event::PressureGone => {
                pressure = None;
                continue;
            }
            Event::Pressure => {
                if let Some(group) = pending.take()
                    && !encoding.send(group).await?
                {
                    return Ok(());
                }
                continue;
            }
        };
        let (ready, done, finish) = match command {
            Input::Batch(item) => (pending.add(item), None, false),
            Input::Flush(done) => (pending.take().into_iter().collect(), Some(done), false),
            Input::Policy(policy) => (pending.set_policy(policy), None, false),
            Input::Finish => (pending.take().into_iter().collect(), None, true),
        };
        for group in ready {
            if !encoding.send(group).await? {
                return Ok(());
            }
        }
        if let Some(done) = done {
            let _ = done.send(());
        }
        if finish {
            let _ = encoding.frames.send(Frame::End).await;
            return Ok(());
        }
    }
    Err(Error::new(ErrorKind::Closed, "topic writer dropped"))
}

async fn upload(connection: Connection, outgoing: mpsc::Receiver<Frame>) -> Result<()> {
    let stream = futures::stream::unfold(outgoing, |mut outgoing| async move {
        match outgoing.recv().await {
            Some(Frame::Data(data)) => Some((data, outgoing)),
            Some(Frame::End) => None,
            None => futures::future::pending().await,
        }
    });
    let mut results = connection.do_put(stream).await?;
    while results.message().await?.is_some() {}
    Ok(())
}

fn lock(failure: &Failure) -> MutexGuard<'_, Option<Error>> {
    failure.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn an_item_never_waits_for_more_than_one_message() {
        let queue = Queue::new(100, Arc::new(WriteBudget::new(1000)));
        let _held = queue.acquire(99).await.unwrap();
        let large = tokio::time::timeout(Duration::from_secs(1), queue.acquire(10_000)).await;
        assert!(large.is_ok_and(|permit| permit.is_some()));
    }
}
