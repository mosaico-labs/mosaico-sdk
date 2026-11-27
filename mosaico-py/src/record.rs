use std::str::FromStr;

use mosaico_ontology::{
    CompressedImage, Header, Image, ImageFormat, Imu, Quaternion, Time, Vector3d,
};
use pyo3::buffer::PyBuffer;
use pyo3::conversion::FromPyObjectOwned;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

use crate::errors::invalid;

fn optional<'py>(
    object: &Bound<'py, PyAny>,
    name: &Bound<'py, PyString>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    let value = object.getattr(name)?;
    Ok((!value.is_none()).then_some(value))
}

fn field<'py, T: FromPyObjectOwned<'py>>(
    object: &Bound<'py, PyAny>,
    name: &Bound<'py, PyString>,
) -> PyResult<T> {
    object
        .getattr(name)
        .and_then(|value| value.extract::<T>().map_err(Into::into))
        .map_err(|e| at(name, e))
}

fn at(name: &Bound<'_, PyString>, error: PyErr) -> PyErr {
    let py = name.py();
    let message = error
        .value(py)
        .str()
        .map_or_else(|_| error.to_string(), |text| text.to_string());
    match message.strip_prefix("field '") {
        Some(rest) => invalid(format!("field '{name}.{rest}")),
        None => invalid(format!("field '{name}': {message}")),
    }
}

fn nested<'py, T>(
    object: &Bound<'py, PyAny>,
    name: &Bound<'py, PyString>,
    convert: impl FnOnce(&Bound<'py, PyAny>) -> PyResult<T>,
) -> PyResult<Option<T>> {
    optional(object, name)
        .and_then(|value| value.map(|v| convert(&v)).transpose())
        .map_err(|e| at(name, e))
}

fn time(object: &Bound<'_, PyAny>) -> PyResult<Time> {
    let py = object.py();
    Ok(Time {
        seconds: field(object, intern!(py, "seconds"))?,
        nanoseconds: field(object, intern!(py, "nanoseconds"))?,
    })
}

fn header(object: &Bound<'_, PyAny>) -> PyResult<Option<Header>> {
    let py = object.py();
    nested(object, intern!(py, "header"), |header| {
        Ok(Header {
            timestamp: nested(header, intern!(py, "timestamp"), time)?,
            frame_id: field(header, intern!(py, "frame_id"))?,
            sample_counter: field(header, intern!(py, "sample_counter"))?,
        })
    })
}

fn covariance(object: &Bound<'_, PyAny>) -> PyResult<(Option<Vec<f64>>, Option<i16>)> {
    let py = object.py();
    Ok((
        field(object, intern!(py, "covariance"))?,
        field(object, intern!(py, "covariance_type"))?,
    ))
}

fn vector3d(object: &Bound<'_, PyAny>) -> PyResult<Vector3d> {
    let py = object.py();
    let (covariance, covariance_type) = covariance(object)?;
    Ok(Vector3d {
        x: field(object, intern!(py, "x"))?,
        y: field(object, intern!(py, "y"))?,
        z: field(object, intern!(py, "z"))?,
        covariance,
        covariance_type,
        header: header(object)?,
    })
}

fn quaternion(object: &Bound<'_, PyAny>) -> PyResult<Quaternion> {
    let py = object.py();
    let (covariance, covariance_type) = covariance(object)?;
    Ok(Quaternion {
        x: field(object, intern!(py, "x"))?,
        y: field(object, intern!(py, "y"))?,
        z: field(object, intern!(py, "z"))?,
        w: field(object, intern!(py, "w"))?,
        covariance,
        covariance_type,
        header: header(object)?,
    })
}

fn required<'py, T>(
    object: &Bound<'py, PyAny>,
    name: &Bound<'py, PyString>,
    convert: impl FnOnce(&Bound<'py, PyAny>) -> PyResult<T>,
) -> PyResult<T> {
    nested(object, name, convert)?.ok_or_else(|| invalid(format!("field '{name}' is required")))
}

pub(crate) fn imu(object: &Bound<'_, PyAny>) -> PyResult<Imu> {
    let py = object.py();
    Ok(Imu {
        header: header(object)?,
        acceleration: required(object, intern!(py, "acceleration"), vector3d)?,
        angular_velocity: required(object, intern!(py, "angular_velocity"), vector3d)?,
        orientation: nested(object, intern!(py, "orientation"), quaternion)?,
    })
}

fn bytes(object: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    let py = object.py();
    let data = object.getattr(intern!(py, "data"))?;
    if let Ok(bytes) = data.cast::<PyBytes>() {
        return Ok(bytes.as_bytes().to_vec());
    }
    match PyBuffer::<u8>::get(&data) {
        Ok(buffer) => buffer.to_vec(py),
        Err(_) => Err(invalid(format!(
            "field 'data' must be bytes, not {}",
            data.get_type().name()?
        ))),
    }
}

fn format(object: &Bound<'_, PyAny>) -> PyResult<ImageFormat> {
    let py = object.py();
    let name = intern!(py, "format");
    let format: String = field(object, name)?;
    ImageFormat::from_str(&format).map_err(|e| at(name, invalid(e.to_string())))
}

pub(crate) fn image(object: &Bound<'_, PyAny>) -> PyResult<Image> {
    let py = object.py();
    Ok(Image {
        header: header(object)?,
        data: bytes(object)?,
        format: format(object)?,
        width: field(object, intern!(py, "width"))?,
        height: field(object, intern!(py, "height"))?,
        stride: field(object, intern!(py, "stride"))?,
        encoding: field(object, intern!(py, "encoding"))?,
        is_bigendian: field(object, intern!(py, "is_bigendian"))?,
    })
}

pub(crate) fn compressed_image(object: &Bound<'_, PyAny>) -> PyResult<CompressedImage> {
    Ok(CompressedImage {
        header: header(object)?,
        data: bytes(object)?,
        format: format(object)?,
    })
}
