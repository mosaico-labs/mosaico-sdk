use arrow_array::StructArray;
use arrow_array::types::{Float64Type, Int16Type};
use mosaico_core::Result;

use crate::columns::{Optional, OptionalF64List, OptionalStruct, Required};
use crate::header::{Header, HeaderColumns, HeaderView};

#[derive(Debug, Clone, PartialEq)]
pub struct Vector3d {
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub covariance: Option<Vec<f64>>,
    pub covariance_type: Option<i16>,
    pub header: Option<Header>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Quaternion {
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub w: f64,
    pub covariance: Option<Vec<f64>>,
    pub covariance_type: Option<i16>,
    pub header: Option<Header>,
}

struct Extras {
    covariance: OptionalF64List,
    covariance_type: Optional<Int16Type>,
    header: OptionalStruct<HeaderColumns>,
}

impl Extras {
    fn new(array: &StructArray, path: &str) -> Result<Self> {
        Ok(Self {
            covariance: OptionalF64List::new(array, path, "covariance")?,
            covariance_type: Optional::new(array, path, "covariance_type")?,
            header: OptionalStruct::new(array, path, "header", HeaderColumns::new)?,
        })
    }
}

pub(crate) struct Vector3dColumns {
    x: Required<Float64Type>,
    y: Required<Float64Type>,
    z: Required<Float64Type>,
    extras: Extras,
}

impl Vector3dColumns {
    pub(crate) fn new(array: &StructArray, path: &str) -> Result<Self> {
        Ok(Self {
            x: Required::new(array, path, "x")?,
            y: Required::new(array, path, "y")?,
            z: Required::new(array, path, "z")?,
            extras: Extras::new(array, path)?,
        })
    }
}

pub(crate) struct QuaternionColumns {
    x: Required<Float64Type>,
    y: Required<Float64Type>,
    z: Required<Float64Type>,
    w: Required<Float64Type>,
    extras: Extras,
}

impl QuaternionColumns {
    pub(crate) fn new(array: &StructArray, path: &str) -> Result<Self> {
        Ok(Self {
            x: Required::new(array, path, "x")?,
            y: Required::new(array, path, "y")?,
            z: Required::new(array, path, "z")?,
            w: Required::new(array, path, "w")?,
            extras: Extras::new(array, path)?,
        })
    }
}

#[derive(Clone, Copy)]
pub struct Vector3dView<'a> {
    columns: &'a Vector3dColumns,
    row: usize,
}

impl<'a> Vector3dView<'a> {
    pub(crate) fn new(columns: &'a Vector3dColumns, row: usize) -> Self {
        Self { columns, row }
    }

    pub fn x(&self) -> Result<f64> {
        self.columns.x.get(self.row)
    }

    pub fn y(&self) -> Result<f64> {
        self.columns.y.get(self.row)
    }

    pub fn z(&self) -> Result<f64> {
        self.columns.z.get(self.row)
    }

    pub fn covariance(&self) -> Result<Option<&'a [f64]>> {
        self.columns.extras.covariance.get(self.row)
    }

    pub fn covariance_type(&self) -> Option<i16> {
        self.columns.extras.covariance_type.get(self.row)
    }

    pub fn header(&self) -> Option<HeaderView<'a>> {
        let columns = self.columns.extras.header.get(self.row)?;
        Some(HeaderView::new(columns, self.row))
    }

    pub fn to_model(&self) -> Result<Vector3d> {
        Ok(Vector3d {
            x: self.x()?,
            y: self.y()?,
            z: self.z()?,
            covariance: self.covariance()?.map(<[f64]>::to_vec),
            covariance_type: self.covariance_type(),
            header: self.header().map(|h| h.to_model()).transpose()?,
        })
    }
}

#[derive(Clone, Copy)]
pub struct QuaternionView<'a> {
    columns: &'a QuaternionColumns,
    row: usize,
}

impl<'a> QuaternionView<'a> {
    pub(crate) fn new(columns: &'a QuaternionColumns, row: usize) -> Self {
        Self { columns, row }
    }

    pub fn x(&self) -> Result<f64> {
        self.columns.x.get(self.row)
    }

    pub fn y(&self) -> Result<f64> {
        self.columns.y.get(self.row)
    }

    pub fn z(&self) -> Result<f64> {
        self.columns.z.get(self.row)
    }

    pub fn w(&self) -> Result<f64> {
        self.columns.w.get(self.row)
    }

    pub fn covariance(&self) -> Result<Option<&'a [f64]>> {
        self.columns.extras.covariance.get(self.row)
    }

    pub fn covariance_type(&self) -> Option<i16> {
        self.columns.extras.covariance_type.get(self.row)
    }

    pub fn header(&self) -> Option<HeaderView<'a>> {
        let columns = self.columns.extras.header.get(self.row)?;
        Some(HeaderView::new(columns, self.row))
    }

    pub fn to_model(&self) -> Result<Quaternion> {
        Ok(Quaternion {
            x: self.x()?,
            y: self.y()?,
            z: self.z()?,
            w: self.w()?,
            covariance: self.covariance()?.map(<[f64]>::to_vec),
            covariance_type: self.covariance_type(),
            header: self.header().map(|h| h.to_model()).transpose()?,
        })
    }
}
