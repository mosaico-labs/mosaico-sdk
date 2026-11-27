use std::cell::RefCell;
use std::ffi::{CStr, c_char, c_int, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::pin;
use std::ptr;
use std::sync::{Arc, OnceLock};

use arrow_array::ffi::{FFI_ArrowArray, from_ffi_and_data_type};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{Array, RecordBatch, RecordBatchReader, StructArray, make_array};
use arrow_data::ArrayData;
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{ArrowError, DataType, Schema, SchemaRef, UnionMode};
use pyo3::exceptions::PyKeyboardInterrupt;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyCapsuleMethods, PyTuple};

use crate::errors::invalid;
use crate::runtime::{ProcessBound, SIGNAL_CHECK_INTERVAL, block_on, on_main_thread, runtime};

#[pyclass(frozen, module = "mosaico_rs._core")]
pub(crate) struct SchemaHolder(pub(crate) SchemaRef);

#[pymethods]
impl SchemaHolder {
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        schema_capsule(py, &self.0)
    }
}

#[pyclass(frozen, module = "mosaico_rs._core")]
pub(crate) struct RawBatch {
    batch: RecordBatch,
    exported: OnceLock<Exported>,
}

struct Exported(*mut FFI_ArrowArray);

unsafe impl Send for Exported {}
unsafe impl Sync for Exported {}

impl Drop for Exported {
    fn drop(&mut self) {
        drop(unsafe { Box::from_raw(self.0) });
    }
}

impl RawBatch {
    pub(crate) fn new(batch: RecordBatch) -> Self {
        Self {
            batch,
            exported: OnceLock::new(),
        }
    }
}

#[pymethods]
impl RawBatch {
    fn array_address(&self) -> usize {
        let exported = self.exported.get_or_init(|| {
            let data = StructArray::from(self.batch.clone()).into_data();
            Exported(Box::into_raw(Box::new(FFI_ArrowArray::new(&data))))
        });
        exported.0 as usize
    }

    #[pyo3(signature = (requested_schema = None))]
    fn __arrow_c_array__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let _ = requested_schema;
        let schema = schema_capsule(py, &self.batch.schema())?;
        let data = StructArray::from(self.batch.clone()).into_data();
        let array = PyCapsule::new_with_value(py, FFI_ArrowArray::new(&data), c"arrow_array")?;
        PyTuple::new(py, [schema.into_any(), array.into_any()])
    }
}

pub(crate) fn schema_capsule<'py>(
    py: Python<'py>,
    schema: &Schema,
) -> PyResult<Bound<'py, PyCapsule>> {
    let ffi = FFI_ArrowSchema::try_from(schema).map_err(arrow_error)?;
    PyCapsule::new_with_value(py, ffi, c"arrow_schema")
}

pub(crate) fn stream_capsule<'py>(
    py: Python<'py>,
    reader: Box<dyn RecordBatchReader + Send>,
) -> PyResult<Bound<'py, PyCapsule>> {
    PyCapsule::new_with_value(py, FFI_ArrowArrayStream::new(reader), c"arrow_array_stream")
}

pub(crate) fn import_schema(object: &Bound<'_, PyAny>) -> PyResult<SchemaRef> {
    let capsule = object.call_method0("__arrow_c_schema__")?;
    let capsule = capsule.cast::<PyCapsule>()?;
    let pointer = capsule.pointer_checked(Some(c"arrow_schema"))?;
    let ffi = unsafe { pointer.cast::<FFI_ArrowSchema>().as_ref() };
    let schema = catch_unwind(AssertUnwindSafe(|| Schema::try_from(ffi)))
        .map_err(|_| invalid("cannot import the schema: inconsistent Arrow data"))?
        .map_err(arrow_error)?;
    Ok(Arc::new(schema))
}

pub(crate) struct KnownSchema {
    description: Vec<u8>,
    data_type: DataType,
    schema: SchemaRef,
}

impl KnownSchema {
    fn parse(ffi: &FFI_ArrowSchema, description: Vec<u8>) -> Result<Self, String> {
        let data_type = DataType::try_from(ffi).map_err(|e| e.to_string())?;
        let DataType::Struct(fields) = &data_type else {
            return Err(format!(
                "expected a record batch, got an array of {data_type}"
            ));
        };
        let metadata = ffi.metadata().map_err(|e| e.to_string())?;
        let schema = Arc::new(Schema::new_with_metadata(fields.clone(), metadata));
        Ok(Self {
            description,
            data_type,
            schema,
        })
    }
}

pub(crate) struct Imported {
    known: Arc<KnownSchema>,
    data: ArrayData,
}

impl Imported {
    pub(crate) fn known(&self) -> &Arc<KnownSchema> {
        &self.known
    }

    pub(crate) fn batch(self) -> Result<RecordBatch, String> {
        RecordBatch::from(StructArray::from(self.data))
            .with_schema(self.known.schema.clone())
            .map_err(|e| e.to_string())
    }

    pub(crate) fn owned(self) -> Result<RecordBatch, String> {
        catch_unwind(AssertUnwindSafe(move || {
            let rows = self.data.len();
            let columns = self
                .data
                .child_data()
                .iter()
                .map(|child| make_array(crate::arrow::copy::copy(child)))
                .collect();
            unsafe { RecordBatch::new_unchecked(self.known.schema.clone(), columns, rows) }
        }))
        .map_err(|_| "cannot copy the record batch: inconsistent Arrow data".to_owned())
    }
}

pub(crate) fn import_batch(
    object: &Bound<'_, PyAny>,
    known: Option<Arc<KnownSchema>>,
) -> Result<Imported, String> {
    let pair = object.call_method0("__arrow_c_array__").map_err(|e| {
        let kind = object.get_type().name().map(|n| n.to_string());
        format!(
            "expected an Arrow record batch, got {}: {e}",
            kind.unwrap_or_default()
        )
    })?;
    let (schema, array) = pair
        .extract::<(Bound<'_, PyAny>, Bound<'_, PyAny>)>()
        .map_err(|e: PyErr| e.to_string())?;
    let schema = schema.cast::<PyCapsule>().map_err(|e| e.to_string())?;
    let array = array.cast::<PyCapsule>().map_err(|e| e.to_string())?;
    let schema = schema
        .pointer_checked(Some(c"arrow_schema"))
        .map_err(|e| e.to_string())?;
    let array = array
        .pointer_checked(Some(c"arrow_array"))
        .map_err(|e| e.to_string())?;
    let schema = unsafe { schema.cast::<FFI_ArrowSchema>().as_ref() };
    let array = unsafe {
        ptr::replace(
            array.cast::<FFI_ArrowArray>().as_ptr(),
            FFI_ArrowArray::empty(),
        )
    };
    if array.is_released() {
        return Err("the Arrow array was already consumed".to_owned());
    }
    catch_unwind(AssertUnwindSafe(|| import_struct(array, schema, known))).unwrap_or_else(|_| {
        Err("cannot import the record batch: inconsistent Arrow data".to_owned())
    })
}

fn import_struct(
    array: FFI_ArrowArray,
    schema: &FFI_ArrowSchema,
    known: Option<Arc<KnownSchema>>,
) -> Result<Imported, String> {
    let description = describe(schema);
    let known = match known {
        Some(known) if known.description == description => known,
        _ => Arc::new(KnownSchema::parse(schema, description)?),
    };
    let data = unsafe { from_ffi_and_data_type(array, known.data_type.clone()) }
        .map_err(|e| e.to_string())?;
    let data = flatten_offsets(&data, 0, data.len());
    data.validate().map_err(|e| e.to_string())?;
    if data.null_count() > 0 {
        return Err("a record batch cannot have null rows".to_owned());
    }
    Ok(Imported { known, data })
}

#[repr(C)]
struct CSchema {
    format: *const c_char,
    name: *const c_char,
    metadata: *const c_char,
    flags: i64,
    n_children: i64,
    children: *const *const CSchema,
    dictionary: *const CSchema,
    release: *const c_void,
    private_data: *const c_void,
}

const ABSENT: u32 = u32::MAX;

fn describe(schema: &FFI_ArrowSchema) -> Vec<u8> {
    let mut out = Vec::with_capacity(1024);
    unsafe { describe_node(ptr::from_ref(schema).cast::<CSchema>(), &mut out) };
    out
}

unsafe fn describe_node(node: *const CSchema, out: &mut Vec<u8>) {
    let node = unsafe { &*node };
    push_bytes(out, unsafe { c_string(node.format) });
    push_bytes(out, unsafe { c_string(node.name) });
    push_bytes(out, unsafe { metadata_bytes(node.metadata) });
    out.extend_from_slice(&node.flags.to_le_bytes());
    out.extend_from_slice(&node.n_children.to_le_bytes());
    for i in 0..usize::try_from(node.n_children).unwrap_or(0) {
        unsafe { describe_node(*node.children.add(i), out) };
    }
    match node.dictionary.is_null() {
        true => out.push(0),
        false => {
            out.push(1);
            unsafe { describe_node(node.dictionary, out) };
        }
    }
}

unsafe fn c_string<'a>(value: *const c_char) -> Option<&'a [u8]> {
    (!value.is_null()).then(|| unsafe { CStr::from_ptr(value) }.to_bytes())
}

unsafe fn metadata_bytes<'a>(metadata: *const c_char) -> Option<&'a [u8]> {
    if metadata.is_null() {
        return None;
    }
    let base = metadata.cast::<u8>();
    let read = |at: usize| unsafe { base.add(at).cast::<i32>().read_unaligned() };
    let pairs = usize::try_from(read(0)).unwrap_or(0);
    let mut at = 4;
    for _ in 0..pairs * 2 {
        at += 4 + usize::try_from(read(at)).unwrap_or(0);
    }
    Some(unsafe { std::slice::from_raw_parts(base, at) })
}

fn push_bytes(out: &mut Vec<u8>, bytes: Option<&[u8]>) {
    match bytes {
        Some(bytes) => {
            out.extend_from_slice(&u32::try_from(bytes.len()).unwrap_or(ABSENT).to_le_bytes());
            out.extend_from_slice(bytes);
        }
        None => out.extend_from_slice(&ABSENT.to_le_bytes()),
    }
}

fn flatten_offsets(data: &ArrayData, start: usize, len: usize) -> ArrayData {
    let first = data.offset() + start;
    let (children, buffers) = match data.data_type() {
        DataType::Struct(_) => (flatten_children(data, first, len), Vec::new()),
        DataType::Union(_, UnionMode::Sparse) => (
            flatten_children(data, first, len),
            vec![data.buffers()[0].slice_with_length(first, len)],
        ),
        DataType::FixedSizeList(_, size) => {
            let size = usize::try_from(*size).unwrap_or_default();
            let values = flatten_offsets(&data.child_data()[0], first * size, len * size);
            (vec![values], Vec::new())
        }
        _ => {
            let children: Vec<ArrayData> = data
                .child_data()
                .iter()
                .map(|child| flatten_offsets(child, 0, child.len()))
                .collect();
            let rebuilt = if children.is_empty() {
                data.clone()
            } else {
                unsafe {
                    data.clone()
                        .into_builder()
                        .child_data(children)
                        .build_unchecked()
                }
            };
            if start == 0 && len == rebuilt.len() {
                return rebuilt;
            }
            return rebuilt.slice(start, len);
        }
    };
    let nulls = data.nulls().map(|nulls| nulls.slice(start, len));
    let builder = ArrayData::builder(data.data_type().clone())
        .len(len)
        .nulls(nulls)
        .buffers(buffers)
        .child_data(children);
    unsafe { builder.build_unchecked() }
}

fn flatten_children(data: &ArrayData, first: usize, len: usize) -> Vec<ArrayData> {
    data.child_data()
        .iter()
        .map(|child| flatten_offsets(child, first, len))
        .collect()
}

pub(crate) fn arrow_error(error: ArrowError) -> PyErr {
    invalid(error.to_string())
}

pub(crate) struct BlockingReader {
    schema: SchemaRef,
    streamer: ProcessBound<mosaico_core::TopicBatchStreamer>,
    pending: Option<RecordBatch>,
}

impl BlockingReader {
    pub(crate) fn new(
        schema: SchemaRef,
        streamer: mosaico_core::TopicBatchStreamer,
        pending: Option<RecordBatch>,
    ) -> Self {
        Self {
            schema,
            streamer: ProcessBound::new(streamer),
            pending,
        }
    }
}

impl Iterator for BlockingReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(batch) = self.pending.take() {
            return Some(Ok(batch));
        }
        let streamer = match self.streamer.get_mut() {
            Ok(streamer) => streamer,
            Err(message) => return Some(Err(external(message))),
        };
        catch_unwind(AssertUnwindSafe(|| next_blocking(streamer)))
            .unwrap_or_else(|_| Some(Err(external("internal error while reading the stream"))))
    }
}

impl RecordBatchReader for BlockingReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn next_blocking(
    streamer: &mut mosaico_core::TopicBatchStreamer,
) -> Option<Result<RecordBatch, ArrowError>> {
    let runtime = match runtime() {
        Ok(runtime) => runtime,
        Err(message) => return Some(Err(external(message))),
    };
    let mut next = pin!(streamer.next_batch());
    loop {
        let polled = block_on(runtime, async {
            tokio::time::timeout(SIGNAL_CHECK_INTERVAL, next.as_mut()).await
        });
        if let Ok(result) = polled {
            return result
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))
                .transpose();
        }
        if let Some(message) = interrupted() {
            return Some(Err(external(message)));
        }
    }
}

fn interrupted() -> Option<String> {
    if !on_main_thread() {
        return None;
    }
    Python::attach(|py| {
        let error = py.check_signals().err()?;
        let name = error
            .get_type(py)
            .name()
            .map(|name| name.to_string())
            .unwrap_or_default();
        if error.is_instance_of::<PyKeyboardInterrupt>(py) {
            unsafe { pyo3::ffi::PyErr_SetInterrupt() };
        } else {
            raise_later(error);
        }
        Some(format!("interrupted by {name}"))
    })
}

thread_local! {
    static RAISED: RefCell<Option<PyErr>> = const { RefCell::new(None) };
}

fn raise_later(error: PyErr) {
    let first = RAISED.with_borrow_mut(|saved| saved.is_none() && saved.replace(error).is_none());
    if first && unsafe { pyo3::ffi::Py_AddPendingCall(Some(raise_saved), ptr::null_mut()) } != 0 {
        RAISED.with_borrow_mut(Option::take);
    }
}

extern "C" fn raise_saved(_: *mut c_void) -> c_int {
    match RAISED.with_borrow_mut(Option::take) {
        Some(error) => {
            Python::attach(|py| {
                if let Some(handled) = handled(py) {
                    error.set_context(py, Some(handled));
                }
                error.restore(py);
            });
            -1
        }
        None => 0,
    }
}

fn handled(py: Python<'_>) -> Option<PyErr> {
    let info = py.import("sys").ok()?.call_method0("exc_info").ok()?;
    let value = info.get_item(1).ok()?;
    (!value.is_none()).then(|| PyErr::from_value(value))
}

fn external(message: impl Into<String>) -> ArrowError {
    ArrowError::ExternalError(message.into().into())
}
