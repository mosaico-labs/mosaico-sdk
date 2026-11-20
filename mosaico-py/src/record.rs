use std::str::FromStr;

use mosaico_ontology::{
    CompressedImage, Header, Image, ImageFormat, Imu, Quaternion, Time, Vector3d,
};
use pyo3::buffer::PyBuffer;
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

fn time(object: &Bound<'_, PyAny>) -> PyResult<Time> {
    let py = object.py();
    Ok(Time {
        seconds: object.getattr(intern!(py, "seconds"))?.extract()?,
        nanoseconds: object.getattr(intern!(py, "nanoseconds"))?.extract()?,
    })
}

fn header(object: &Bound<'_, PyAny>) -> PyResult<Option<Header>> {
    let py = object.py();
    let Some(header) = optional(object, intern!(py, "header"))? else {
        return Ok(None);
    };
    Ok(Some(Header {
        timestamp: optional(&header, intern!(py, "timestamp"))?
            .map(|t| time(&t))
            .transpose()?,
        frame_id: header.getattr(intern!(py, "frame_id"))?.extract()?,
        sample_counter: header.getattr(intern!(py, "sample_counter"))?.extract()?,
    }))
}

fn covariance(object: &Bound<'_, PyAny>) -> PyResult<(Option<Vec<f64>>, Option<i16>)> {
    let py = object.py();
    Ok((
        object.getattr(intern!(py, "covariance"))?.extract()?,
        object.getattr(intern!(py, "covariance_type"))?.extract()?,
    ))
}

fn vector3d(object: &Bound<'_, PyAny>) -> PyResult<Vector3d> {
    let py = object.py();
    let (covariance, covariance_type) = covariance(object)?;
    Ok(Vector3d {
        x: object.getattr(intern!(py, "x"))?.extract()?,
        y: object.getattr(intern!(py, "y"))?.extract()?,
        z: object.getattr(intern!(py, "z"))?.extract()?,
        covariance,
        covariance_type,
        header: header(object)?,
    })
}

fn quaternion(object: &Bound<'_, PyAny>) -> PyResult<Quaternion> {
    let py = object.py();
    let (covariance, covariance_type) = covariance(object)?;
    Ok(Quaternion {
        x: object.getattr(intern!(py, "x"))?.extract()?,
        y: object.getattr(intern!(py, "y"))?.extract()?,
        z: object.getattr(intern!(py, "z"))?.extract()?,
        w: object.getattr(intern!(py, "w"))?.extract()?,
        covariance,
        covariance_type,
        header: header(object)?,
    })
}

fn required<'py>(
    object: &Bound<'py, PyAny>,
    name: &Bound<'py, PyString>,
) -> PyResult<Bound<'py, PyAny>> {
    optional(object, name)?.ok_or_else(|| invalid(format!("field '{name}' is required")))
}

pub(crate) fn imu(object: &Bound<'_, PyAny>) -> PyResult<Imu> {
    let py = object.py();
    Ok(Imu {
        header: header(object)?,
        acceleration: vector3d(&required(object, intern!(py, "acceleration"))?)?,
        angular_velocity: vector3d(&required(object, intern!(py, "angular_velocity"))?)?,
        orientation: optional(object, intern!(py, "orientation"))?
            .map(|o| quaternion(&o))
            .transpose()?,
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
    let format: String = object.getattr(intern!(py, "format"))?.extract()?;
    ImageFormat::from_str(&format).map_err(|e| invalid(e.to_string()))
}

pub(crate) fn image(object: &Bound<'_, PyAny>) -> PyResult<Image> {
    let py = object.py();
    Ok(Image {
        header: header(object)?,
        data: bytes(object)?,
        format: format(object)?,
        width: object.getattr(intern!(py, "width"))?.extract()?,
        height: object.getattr(intern!(py, "height"))?.extract()?,
        stride: object.getattr(intern!(py, "stride"))?.extract()?,
        encoding: object.getattr(intern!(py, "encoding"))?.extract()?,
        is_bigendian: object.getattr(intern!(py, "is_bigendian"))?.extract()?,
    })
}

pub(crate) fn compressed_image(object: &Bound<'_, PyAny>) -> PyResult<CompressedImage> {
    Ok(CompressedImage {
        header: header(object)?,
        data: bytes(object)?,
        format: format(object)?,
    })
}
