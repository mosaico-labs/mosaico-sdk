mod encoder;
mod pending;
mod schema;

use std::collections::VecDeque;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use arrow_array::{Array, RecordBatch, make_array};
use arrow_flight::{FlightData, FlightDescriptor, SchemaAsIpc};
use arrow_schema::{DataType, Schema, SchemaRef};
use futures::future::Either;
use prost::Message;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::comm::budget::{QUEUE_MESSAGES, WriteBudget, limits};
use crate::comm::connection::Connection;
use crate::comm::proto;
use crate::error::{Error, ErrorKind, Result};
use crate::scope::Token;
use crate::write::session::{Interrupter, interrupted_error};
use crate::write::topic::{FlushPolicy, Reporter};

pub(crate) use self::encoder::estimate;
use self::encoder::{Encoder, split};
use self::pending::Pending;
pub(crate) use self::schema::Conversion;
use self::schema::{plain_schema, relabel, shape, shape_of, unmasked_nulls};

const IN_FLIGHT: usize = 2;

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

struct Piece {
    batch: RecordBatch,
    bytes: usize,
    since: Option<Instant>,
    urgent: bool,
}

impl Piece {
    fn queued(self, permit: Permits) -> Queued {
        Queued {
            batch: self.batch,
            bytes: self.bytes,
            since: self.since,
            urgent: self.urgent,
            permit,
        }
    }
}

enum Frame {
    Data(FlightData, Vec<Permits>),
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
    unsent: VecDeque<Piece>,
    failure: Failure,
    ended: Arc<AtomicBool>,
    interrupter: Interrupter,
    task: Option<JoinHandle<()>>,
}

impl BatchWriter {
    pub(crate) fn open(
        connection: &Connection,
        reporter: Arc<Reporter>,
        topic_uuid: String,
        schema: SchemaRef,
        token: Arc<Token>,
        interrupter: Interrupter,
    ) -> Self {
        let (limit, target_bytes) = limits(connection.info());
        let options = connection.write_options().clone();
        let schema = plain_schema(&schema);
        let descriptor = FlightDescriptor::new_cmd(
            proto::flight::DoPutCmd {
                locator: reporter.locator().to_owned(),
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
        let encoding = Encoding {
            encoder: Arc::new(Encoder {
                schema: schema.clone(),
                options,
                limit,
            }),
            frames,
            reporter,
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
        let ended = Arc::new(AtomicBool::new(false));
        let upload = upload(
            connection.clone(),
            Outgoing {
                frames: outgoing,
                pressure: Some(connection.write_budget().pressure()),
                held: VecDeque::new(),
                ended: ended.clone(),
            },
        );
        let slot = failure.clone();
        let closing = queue.semaphore.clone();
        let task = tokio::spawn(async move {
            let error = match token
                .run_until_closed(futures::future::try_join(upload, encode))
                .await
            {
                Some(Ok(_)) => None,
                Some(Err(error)) => Some(error),
                None => Some(Error::new(
                    ErrorKind::Closed,
                    "the write session was closed",
                )),
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
        Self {
            input: Some(input),
            schema,
            shapes,
            accepted: None,
            target_bytes,
            record_limit: limit,
            queue,
            unsent: VecDeque::new(),
            failure,
            ended,
            interrupter,
            task: Some(task),
        }
    }

    pub(crate) fn target_bytes(&self) -> usize {
        self.target_bytes
    }

    pub(crate) fn record_limit(&self) -> usize {
        self.record_limit
    }

    pub(crate) fn interrupted(&self) -> bool {
        self.interrupter.interrupted()
    }

    pub(crate) fn end_sent(&self) -> Arc<AtomicBool> {
        self.ended.clone()
    }

    pub(crate) fn failed(&self) -> bool {
        lock(&self.failure).is_some()
    }

    pub(crate) async fn enqueue(
        &mut self,
        batch: RecordBatch,
        since: Option<Instant>,
        urgent: bool,
        committed: impl FnOnce(),
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            committed();
            return Ok(());
        }
        self.drain().await?;
        let mut pieces = split(batch, self.target_bytes)
            .into_iter()
            .map(|(batch, bytes)| Piece {
                batch,
                bytes,
                since,
                urgent,
            });
        if let Some(first) = pieces.next() {
            let permit = self.permit(first.bytes).await?;
            self.hand_over(first, permit).await?;
        }
        self.unsent.extend(pieces);
        committed();
        self.drain().await
    }

    pub(crate) async fn drain(&mut self) -> Result<()> {
        while let Some(bytes) = self.unsent.front().map(|piece| piece.bytes) {
            let permit = self.permit(bytes).await?;
            let Some(piece) = self.unsent.pop_front() else {
                break;
            };
            self.hand_over(piece, permit).await?;
        }
        Ok(())
    }

    async fn permit(&mut self, bytes: usize) -> Result<Permits> {
        if self.interrupter.interrupted() {
            return Err(interrupted_error());
        }
        let Some(permit) = self.queue.acquire(bytes).await else {
            return Err(self.stopped().await);
        };
        if self.interrupter.interrupted() {
            return Err(interrupted_error());
        }
        Ok(permit)
    }

    async fn hand_over(&mut self, piece: Piece, permit: Permits) -> Result<()> {
        if self
            .input()?
            .send(Input::Batch(piece.queued(permit)))
            .is_err()
        {
            return Err(self.stopped().await);
        }
        Ok(())
    }

    pub(crate) async fn flush(&mut self) -> Result<()> {
        self.drain().await?;
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
        if let Some(task) = self.task.as_mut() {
            let joined = task.await;
            self.task = None;
            joined.map_err(|e| {
                Error::new(ErrorKind::Stream, "topic writer task failed").with_source(e)
            })?;
        }
        match lock(&self.failure).take() {
            Some(error) => Err(error),
            None if self.interrupter.interrupted() => Err(interrupted_error()),
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

struct Encoding {
    encoder: Arc<Encoder>,
    frames: mpsc::Sender<Frame>,
    reporter: Arc<Reporter>,
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
        let last = flights.len().saturating_sub(1);
        let mut permits = Some(permits);
        for (index, flight) in flights.into_iter().enumerate() {
            if self.interrupter.interrupted() {
                break;
            }
            let carried = match index == last {
                true => permits.take().unwrap_or_default(),
                false => Vec::new(),
            };
            if self
                .frames
                .send(Frame::Data(flight, carried))
                .await
                .is_err()
            {
                return Ok(false);
            }
        }
        if dropped.count > 0 {
            self.reporter
                .record_dropped(dropped.count, dropped.largest, false)
                .await;
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
    if encoding
        .frames
        .send(Frame::Data(header, Vec::new()))
        .await
        .is_err()
    {
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

struct Outgoing {
    frames: mpsc::Receiver<Frame>,
    pressure: Option<watch::Receiver<u64>>,
    held: VecDeque<Vec<Permits>>,
    ended: Arc<AtomicBool>,
}

enum Wait {
    Frame(Option<Frame>),
    Pressure,
    PressureGone,
}

impl Outgoing {
    async fn next(&mut self) -> Option<FlightData> {
        let frame = match self.frames.try_recv() {
            Ok(frame) => Some(frame),
            Err(mpsc::error::TryRecvError::Disconnected) => None,
            Err(mpsc::error::TryRecvError::Empty) => self.wait().await,
        };
        match frame {
            Some(Frame::Data(data, permits)) => {
                self.held.push_back(permits);
                while self.held.len() > IN_FLIGHT {
                    self.held.pop_front();
                }
                Some(data)
            }
            Some(Frame::End) => {
                self.ended.store(true, Ordering::Relaxed);
                self.held.clear();
                None
            }
            None => futures::future::pending().await,
        }
    }

    async fn wait(&mut self) -> Option<Frame> {
        loop {
            let waited = match self.pressure.as_mut() {
                Some(pressure) if !self.held.is_empty() => {
                    pressure.borrow_and_update();
                    let changed = pin!(pressure.changed());
                    let received = pin!(self.frames.recv());
                    match futures::future::select(changed, received).await {
                        Either::Left((Ok(()), _)) => Wait::Pressure,
                        Either::Left((Err(_), _)) => Wait::PressureGone,
                        Either::Right((frame, _)) => Wait::Frame(frame),
                    }
                }
                _ => Wait::Frame(self.frames.recv().await),
            };
            match waited {
                Wait::Frame(frame) => return frame,
                Wait::Pressure => self.held.clear(),
                Wait::PressureGone => self.pressure = None,
            }
        }
    }
}

async fn upload(connection: Connection, outgoing: Outgoing) -> Result<()> {
    let stream = futures::stream::unfold(outgoing, |mut outgoing| async move {
        let data = outgoing.next().await?;
        Some((data, outgoing))
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

    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_array::{ArrayRef, Int64Array};
    use arrow_schema::Field;

    use super::*;

    fn idle_writer(target: usize) -> (BatchWriter, mpsc::UnboundedReceiver<Input>) {
        let (input, received) = mpsc::unbounded_channel();
        let writer = BatchWriter {
            input: Some(input),
            schema: Arc::new(Schema::new(vec![Field::new("t", DataType::Int64, false)])),
            shapes: vec![shape(&DataType::Int64)],
            accepted: None,
            target_bytes: target,
            record_limit: target,
            queue: Queue::new(target, Arc::new(WriteBudget::new(1 << 20))),
            unsent: VecDeque::new(),
            failure: Failure::default(),
            ended: Arc::new(AtomicBool::new(false)),
            interrupter: Interrupter::new(),
            task: None,
        };
        (writer, received)
    }

    fn rows(values: std::ops::Range<i64>) -> RecordBatch {
        let column: ArrayRef = Arc::new(Int64Array::from_iter_values(values));
        RecordBatch::try_from_iter([("t", column)]).unwrap()
    }

    fn values(input: Input) -> Vec<i64> {
        match input {
            Input::Batch(queued) => queued
                .batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec(),
            _ => Vec::new(),
        }
    }

    fn queued(received: &mut mpsc::UnboundedReceiver<Input>) -> Vec<i64> {
        let mut sent = Vec::new();
        while let Ok(input) = received.try_recv() {
            sent.extend(values(input));
        }
        sent
    }

    #[tokio::test]
    async fn cancelled_enqueue_sends_rest() {
        let (mut writer, mut received) = idle_writer(80);
        let full = (QUEUE_MESSAGES * 10) as i64;
        let mut committed = false;
        let enqueue = writer.enqueue(rows(0..100), None, false, || committed = true);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), enqueue)
                .await
                .is_err()
        );
        assert!(committed);
        let mut sent = queued(&mut received);
        assert_eq!(sent, (0..full).collect::<Vec<_>>());
        let rest = tokio::spawn(async move {
            let mut rest = Vec::new();
            while let Some(input) = received.recv().await {
                rest.extend(values(input));
            }
            rest
        });
        writer.drain().await.unwrap();
        drop(writer);
        sent.extend(rest.await.unwrap());
        assert_eq!(sent, (0..100).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn cancelled_enqueue_leaves_nothing() {
        let (mut writer, mut received) = idle_writer(80);
        let full = (QUEUE_MESSAGES * 10) as i64;
        writer
            .enqueue(rows(0..full), None, false, || {})
            .await
            .unwrap();
        let mut committed = false;
        let enqueue = writer.enqueue(rows(full..full + 10), None, false, || committed = true);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), enqueue)
                .await
                .is_err()
        );
        assert!(!committed);
        assert!(writer.unsent.is_empty());
        assert_eq!(queued(&mut received), (0..full).collect::<Vec<_>>());
    }

    fn outgoing(
        received: mpsc::Receiver<Frame>,
        pressure: Option<watch::Receiver<u64>>,
    ) -> Outgoing {
        Outgoing {
            frames: received,
            pressure,
            held: VecDeque::new(),
            ended: Arc::new(AtomicBool::new(false)),
        }
    }

    #[tokio::test]
    async fn permits_held_two_frames() {
        let queue = Queue::new(10, Arc::new(WriteBudget::new(1000)));
        let free = || queue.semaphore.available_permits();
        let (frames, received) = mpsc::channel(8);
        let mut outgoing = outgoing(received, None);
        for _ in 0..3 {
            let permits = vec![queue.acquire(10).await.unwrap()];
            frames
                .send(Frame::Data(FlightData::default(), permits))
                .await
                .unwrap();
        }
        let full = 10 * QUEUE_MESSAGES;
        assert_eq!(free(), full - 30);
        outgoing.next().await.unwrap();
        outgoing.next().await.unwrap();
        assert_eq!(free(), full - 30);
        outgoing.next().await.unwrap();
        assert_eq!(free(), full - 20);
        frames.send(Frame::End).await.unwrap();
        assert!(outgoing.next().await.is_none());
        assert_eq!(free(), full);
        assert!(outgoing.ended.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn idle_topic_frees_permits() {
        let budget = Arc::new(WriteBudget::new(10));
        let queue = Queue::new(10, budget.clone());
        let (frames, received) = mpsc::channel(8);
        let mut outgoing = outgoing(received, Some(budget.pressure()));
        let permits = vec![queue.acquire(10).await.unwrap()];
        frames
            .send(Frame::Data(FlightData::default(), permits))
            .await
            .unwrap();
        outgoing.next().await.unwrap();
        let idle = tokio::spawn(async move { outgoing.next().await.is_none() });
        let other = Semaphore::new(100);
        let waiting = tokio::time::timeout(Duration::from_secs(1), budget.acquire(10, &other));
        assert!(waiting.await.is_ok_and(|permit| permit.is_some()));
        frames.send(Frame::End).await.unwrap();
        assert!(idle.await.unwrap());
    }

    #[tokio::test]
    async fn permits_kept_without_pressure() {
        let budget = Arc::new(WriteBudget::new(10));
        let queue = Queue::new(10, budget.clone());
        let (frames, received) = mpsc::channel(8);
        let mut outgoing = outgoing(received, None);
        let permits = vec![queue.acquire(10).await.unwrap()];
        frames
            .send(Frame::Data(FlightData::default(), permits))
            .await
            .unwrap();
        outgoing.next().await.unwrap();
        let idle = tokio::spawn(async move { outgoing.next().await.is_none() });
        let other = Semaphore::new(100);
        let waiting = tokio::time::timeout(Duration::from_millis(100), budget.acquire(10, &other));
        assert!(waiting.await.is_err());
        frames.send(Frame::End).await.unwrap();
        assert!(idle.await.unwrap());
    }

    #[tokio::test]
    async fn item_waits_one_message() {
        let queue = Queue::new(100, Arc::new(WriteBudget::new(1000)));
        let _held = queue.acquire(99).await.unwrap();
        let large = tokio::time::timeout(Duration::from_secs(1), queue.acquire(10_000)).await;
        assert!(large.is_ok_and(|permit| permit.is_some()));
    }
}
