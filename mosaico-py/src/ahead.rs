use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, GenericListArray, Int64Array, OffsetSizeTrait, RecordBatch};
use mosaico_core::{Error, ErrorKind, MergeWindow, Result};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinHandle;

const AHEAD_BYTES: usize = 8 << 20;
const CONTENDED_CALLS: u32 = 3;
const SETTLE: Duration = Duration::from_secs(1);
const IDLE: u8 = 0;
const READING: u8 = 1;
const FAILED: u8 = 2;

pub(crate) trait Source: Send + 'static {
    type Item: Joinable;

    fn next(&mut self) -> impl Future<Output = Result<Option<Self::Item>>> + Send;
    fn close(&mut self);
}

pub(crate) trait Joinable: Send + Sized + 'static {
    type State: Default + Send;

    fn bytes(&self) -> usize;
    fn join(state: &mut Self::State, items: Vec<Self>) -> Result<Self>;
}

impl Source for mosaico_core::TopicBatchStreamer {
    type Item = RecordBatch;

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        self.next_batch().await
    }

    fn close(&mut self) {
        mosaico_core::TopicBatchStreamer::close(self);
    }
}

impl Source for mosaico_core::SequenceWindowStreamer {
    type Item = Merged;

    async fn next(&mut self) -> Result<Option<Merged>> {
        Ok(self.next_window().await?.map(Merged::from))
    }

    fn close(&mut self) {
        mosaico_core::SequenceWindowStreamer::close(self);
    }
}

fn slice_bytes(batch: &RecordBatch) -> usize {
    batch.columns().iter().map(array_bytes).sum()
}

fn view_bytes(views: &[u128]) -> usize {
    views
        .iter()
        .map(|view| match *view as u32 {
            len if len > 12 => 16 + len as usize,
            _ => 16,
        })
        .sum()
}

fn list_bytes<O: OffsetSizeTrait>(list: &GenericListArray<O>) -> usize {
    let offsets = list.value_offsets();
    let first = offsets[0].as_usize();
    let last = offsets[list.len()].as_usize();
    offsets.len() * O::get_byte_width() + array_bytes(&list.values().slice(first, last - first))
}

fn array_bytes(array: &ArrayRef) -> usize {
    let nulls = array.nulls().map_or(0, |n| n.len().div_ceil(8));
    let values = if let Some(a) = array.as_string_view_opt() {
        view_bytes(a.views())
    } else if let Some(a) = array.as_binary_view_opt() {
        view_bytes(a.views())
    } else if let Some(a) = array.as_list_opt::<i32>() {
        list_bytes(a)
    } else if let Some(a) = array.as_list_opt::<i64>() {
        list_bytes(a)
    } else if let Some(a) = array.as_struct_opt() {
        a.columns().iter().map(array_bytes).sum()
    } else {
        return array
            .to_data()
            .get_slice_memory_size()
            .unwrap_or_else(|_| array.get_array_memory_size());
    };
    nulls + values
}

fn joined(error: arrow_schema::ArrowError) -> Error {
    Error::schema(format!("cannot join batches: {error}"))
}

fn concat_batches(mut batches: Vec<RecordBatch>) -> Result<RecordBatch> {
    if batches.len() == 1 {
        return Ok(batches.remove(0));
    }
    arrow_select::concat::concat_batches(&batches[0].schema(), &batches).map_err(joined)
}

fn concat_timestamps(mut arrays: Vec<Int64Array>) -> Result<Int64Array> {
    if arrays.len() == 1 {
        return Ok(arrays.remove(0));
    }
    let refs: Vec<&dyn Array> = arrays.iter().map(|a| a as &dyn Array).collect();
    arrow_select::concat::concat(&refs)
        .map_err(joined)?
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| Error::schema("timestamps are not Int64"))
}

impl Joinable for RecordBatch {
    type State = ();

    fn bytes(&self) -> usize {
        slice_bytes(self)
    }

    fn join(_: &mut (), items: Vec<Self>) -> Result<Self> {
        concat_batches(items)
    }
}

#[derive(Clone)]
pub(crate) struct Part {
    pub(crate) topic: u16,
    pub(crate) batch: RecordBatch,
    pub(crate) timestamps: Int64Array,
}

#[derive(Clone)]
pub(crate) struct Merged {
    pub(crate) parts: Vec<Part>,
    pub(crate) order: Vec<u16>,
}

impl From<MergeWindow> for Merged {
    fn from(window: MergeWindow) -> Self {
        let parts = window
            .batches
            .into_iter()
            .map(|b| Part {
                topic: b.topic,
                batch: b.batch,
                timestamps: b.timestamps,
            })
            .collect();
        Self {
            parts,
            order: window.order,
        }
    }
}

struct Current {
    part: Part,
    read: usize,
}

#[derive(Default)]
pub(crate) struct Shown {
    topics: Vec<Option<Current>>,
}

impl Shown {
    fn tail(&self, topic: u16) -> Option<Part> {
        let current = self.topics.get(usize::from(topic))?.as_ref()?;
        let left = current.part.batch.num_rows() - current.read;
        (left > 0).then(|| Part {
            topic,
            batch: current.part.batch.slice(current.read, left),
            timestamps: current.part.timestamps.slice(current.read, left),
        })
    }

    fn show(&mut self, window: &Merged) {
        for part in &window.parts {
            let index = usize::from(part.topic);
            if self.topics.len() <= index {
                self.topics.resize_with(index + 1, || None);
            }
            self.topics[index] = Some(Current {
                part: part.clone(),
                read: 0,
            });
        }
        for topic in &window.order {
            if let Some(Some(current)) = self.topics.get_mut(usize::from(*topic)) {
                current.read += 1;
            }
        }
    }
}

impl Joinable for Merged {
    type State = Shown;

    fn bytes(&self) -> usize {
        self.parts.iter().map(|part| slice_bytes(&part.batch)).sum()
    }

    fn join(shown: &mut Shown, items: Vec<Self>) -> Result<Self> {
        let mut topics: Vec<(u16, Vec<RecordBatch>, Vec<Int64Array>)> = Vec::new();
        let mut order = Vec::new();
        for window in items {
            order.extend(window.order);
            for part in window.parts {
                match topics.iter_mut().find(|(topic, ..)| *topic == part.topic) {
                    Some((_, batches, timestamps)) => {
                        batches.push(part.batch);
                        timestamps.push(part.timestamps);
                    }
                    None => {
                        let mut batches = Vec::new();
                        let mut timestamps = Vec::new();
                        if let Some(tail) = shown.tail(part.topic) {
                            batches.push(tail.batch);
                            timestamps.push(tail.timestamps);
                        }
                        batches.push(part.batch);
                        timestamps.push(part.timestamps);
                        topics.push((part.topic, batches, timestamps));
                    }
                }
            }
        }
        let parts = topics
            .into_iter()
            .map(|(topic, batches, timestamps)| {
                Ok(Part {
                    topic,
                    batch: concat_batches(batches)?,
                    timestamps: concat_timestamps(timestamps)?,
                })
            })
            .collect::<Result<_>>()?;
        let window = Self { parts, order };
        shown.show(&window);
        Ok(window)
    }
}

type Held<T> = (Result<T>, usize, Option<OwnedSemaphorePermit>);

#[derive(Clone, Default)]
pub(crate) struct Busy(Arc<AtomicU8>);

impl Busy {
    fn set(&self, state: u8) {
        self.0.store(state, Ordering::Release);
    }

    pub(crate) fn holds_open(&self) -> bool {
        let started = Instant::now();
        loop {
            match self.0.load(Ordering::Acquire) {
                READING if started.elapsed() < SETTLE => {
                    std::thread::sleep(Duration::from_micros(50));
                }
                FAILED => return true,
                _ => return false,
            }
        }
    }
}

struct Ahead<T> {
    items: mpsc::UnboundedReceiver<Held<T>>,
    peeked: Option<Held<T>>,
    failed: Option<Error>,
    task: JoinHandle<()>,
}

impl<T: Joinable> Ahead<T> {
    fn spawn<S: Source<Item = T>>(
        runtime: &tokio::runtime::Runtime,
        mut source: S,
        busy: Busy,
    ) -> Self {
        let (sender, items) = mpsc::unbounded_channel();
        let budget = Arc::new(Semaphore::new(AHEAD_BYTES));
        let task = runtime.spawn(async move {
            loop {
                busy.set(READING);
                let item = match source.next().await {
                    Ok(Some(item)) => item,
                    Ok(None) => {
                        busy.set(IDLE);
                        return;
                    }
                    Err(e) => {
                        let closed = e.kind() == ErrorKind::Closed;
                        busy.set(if closed { IDLE } else { FAILED });
                        if sender.send((Err(e), 0, None)).is_err() {
                            busy.set(IDLE);
                        }
                        return;
                    }
                };
                busy.set(IDLE);
                let bytes = item.bytes();
                let cost = u32::try_from(bytes.clamp(1, AHEAD_BYTES)).unwrap_or(u32::MAX);
                let Ok(permit) = budget.clone().acquire_many_owned(cost).await else {
                    return;
                };
                if sender.send((Ok(item), bytes, Some(permit))).is_err() {
                    return;
                }
            }
        });
        Self {
            items,
            peeked: None,
            failed: None,
            task,
        }
    }

    async fn take(&mut self, state: &mut T::State) -> Result<Option<T>> {
        if let Some(error) = self.failed.take() {
            return Err(error);
        }
        let first = match self.peeked.take() {
            Some(held) => held,
            None => match self.items.recv().await {
                Some(held) => held,
                None => return Ok(None),
            },
        };
        let (item, mut size, _permit) = first;
        let mut ready = vec![item?];
        let mut permits = Vec::new();
        while let Ok(held) = self.items.try_recv() {
            match held {
                (Ok(item), bytes, permit) if size + bytes <= AHEAD_BYTES => {
                    size += bytes;
                    ready.push(item);
                    permits.extend(permit);
                }
                (Ok(item), bytes, permit) => {
                    self.peeked = Some((Ok(item), bytes, permit));
                    break;
                }
                (Err(error), ..) => {
                    self.failed = Some(error);
                    break;
                }
            }
        }
        T::join(state, ready).map(Some)
    }

    fn close(&mut self) {
        self.task.abort();
        self.items.close();
        self.peeked = None;
        self.failed = None;
    }
}

impl<T> Drop for Ahead<T> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

enum Mode<S: Source> {
    Direct(S),
    Ahead(Ahead<S::Item>),
    Switching,
}

pub(crate) struct Feed<S: Source> {
    mode: Mode<S>,
    state: <S::Item as Joinable>::State,
    join: bool,
    busy: Busy,
    contended: u32,
}

impl<S: Source> Feed<S> {
    pub(crate) fn new(source: S, join: bool) -> Self {
        Self {
            mode: Mode::Direct(source),
            state: Default::default(),
            join,
            busy: Busy::default(),
            contended: 0,
        }
    }

    pub(crate) fn busy(&self) -> Busy {
        self.busy.clone()
    }

    pub(crate) fn into_direct(self) -> Option<S> {
        match self.mode {
            Mode::Direct(source) => Some(source),
            Mode::Ahead(_) | Mode::Switching => None,
        }
    }

    pub(crate) async fn next(&mut self, contended: bool) -> Result<Option<S::Item>> {
        self.watch(contended);
        let next = match &mut self.mode {
            Mode::Direct(source) => match source.next().await {
                Ok(Some(item)) => {
                    <S::Item as Joinable>::join(&mut self.state, vec![item]).map(Some)
                }
                other => other,
            },
            Mode::Ahead(ahead) => ahead.take(&mut self.state).await,
            Mode::Switching => Ok(None),
        };
        if next.is_err() {
            self.busy.set(IDLE);
        }
        next
    }

    fn watch(&mut self, contended: bool) {
        if !self.join || !matches!(self.mode, Mode::Direct(_)) {
            return;
        }
        self.contended = if contended { self.contended + 1 } else { 0 };
        if self.contended < CONTENDED_CALLS {
            return;
        }
        let Ok(runtime) = crate::runtime::runtime() else {
            return;
        };
        if let Mode::Direct(source) = std::mem::replace(&mut self.mode, Mode::Switching) {
            self.mode = Mode::Ahead(Ahead::spawn(runtime, source, self.busy.clone()));
        }
    }

    pub(crate) fn close(&mut self) {
        match &mut self.mode {
            Mode::Direct(source) => source.close(),
            Mode::Ahead(ahead) => ahead.close(),
            Mode::Switching => {}
        }
        self.busy.set(IDLE);
    }
}
