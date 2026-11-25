mod arrow;
mod client;
mod errors;
mod image;
mod read;
mod record;
mod runtime;
mod write;

use pyo3::prelude::*;

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<client::Client>()?;
    m.add_class::<read::SequenceHandler>()?;
    m.add_class::<read::TopicHandler>()?;
    m.add_class::<read::BatchStream>()?;
    m.add_class::<arrow::ffi::SchemaHolder>()?;
    m.add_class::<arrow::ffi::RawBatch>()?;
    m.add_class::<write::SequenceWriter>()?;
    m.add_class::<write::TopicWriter>()?;
    m.add_function(wrap_pyfunction!(write::topic_name, m)?)?;
    m.add_function(wrap_pyfunction!(image::encode_png, m)?)?;
    m.add_function(wrap_pyfunction!(image::encode_png_batch, m)?)?;
    m.add_function(wrap_pyfunction!(image::decode_png, m)?)?;
    m.add_function(wrap_pyfunction!(image::decode_png_batch, m)?)?;
    m.add_function(wrap_pyfunction!(image::decode_png_batches, m)?)?;
    Ok(())
}
