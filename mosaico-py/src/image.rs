use mosaico_ontology::Pixels;
use pyo3::buffer::PyBuffer;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::arrow::ffi::{RawBatch, import_batch};
use crate::errors::{invalid, to_py};

fn contiguous(data: &PyBuffer<u8>) -> PyResult<&[u8]> {
    if !data.is_c_contiguous() {
        return Err(invalid("image data must be a contiguous buffer"));
    }
    Ok(unsafe { std::slice::from_raw_parts(data.buf_ptr().cast::<u8>(), data.len_bytes()) })
}

#[pyfunction]
pub(crate) fn encode_png<'py>(
    py: Python<'py>,
    data: PyBuffer<u8>,
    height: u32,
    stride: u32,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = contiguous(&data)?;
    let encoded = py
        .detach(|| {
            mosaico_ontology::encode_png(Pixels {
                data: bytes,
                height,
                stride,
            })
        })
        .map_err(to_py)?;
    Ok(PyBytes::new(py, &encoded))
}

#[pyfunction]
pub(crate) fn decode_png<'py>(
    py: Python<'py>,
    data: PyBuffer<u8>,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = contiguous(&data)?;
    let decoded = py
        .detach(|| mosaico_ontology::decode_png(bytes))
        .map_err(to_py)?;
    Ok(PyBytes::new(py, &decoded))
}

fn transform_batch(
    py: Python<'_>,
    batch: &Bound<'_, PyAny>,
    transform: fn(&arrow_array::RecordBatch) -> mosaico_core::Result<arrow_array::RecordBatch>,
) -> PyResult<RawBatch> {
    let batch = import_batch(batch, None)
        .and_then(|batch| batch.batch())
        .map_err(invalid)?;
    let out = py.detach(|| transform(&batch)).map_err(to_py)?;
    Ok(RawBatch::new(out))
}

#[pyfunction]
pub(crate) fn encode_png_batch(py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<RawBatch> {
    transform_batch(py, batch, mosaico_ontology::encode_png_batch)
}

#[pyfunction]
pub(crate) fn decode_png_batch(py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<RawBatch> {
    transform_batch(py, batch, mosaico_ontology::decode_png_batch)
}

#[pyfunction]
pub(crate) fn decode_png_batches(
    py: Python<'_>,
    batches: Vec<Bound<'_, PyAny>>,
) -> PyResult<Vec<RawBatch>> {
    let batches = batches
        .iter()
        .map(|batch| import_batch(batch, None).and_then(|batch| batch.batch()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(invalid)?;
    let decoded = py
        .detach(|| mosaico_ontology::decode_png_batches(&batches))
        .map_err(to_py)?;
    Ok(decoded.into_iter().map(RawBatch::new).collect())
}
