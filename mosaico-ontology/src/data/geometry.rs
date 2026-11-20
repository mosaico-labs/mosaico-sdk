use std::sync::Arc;

use arrow_array::builder::{Float64Builder, Int16Builder, ListBuilder};
use arrow_array::types::{Float64Type, Int16Type};
use arrow_array::{ArrayRef, StructArray};
use arrow_schema::{DataType, Field, Fields};
use mosaico_core::Result;

use crate::arrow::build::StructBuilder;
use crate::arrow::columns::{Optional, OptionalF64List, OptionalStruct, Required};
use crate::data::header::{
    Header, HeaderColumns, HeaderView, OptionalHeader, header_field, header_size,
};

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

const COMPONENT_BYTES: usize = 8;

fn extras_size(covariance: &Option<Vec<f64>>, header: &Option<Header>) -> usize {
    2 + covariance
        .as_ref()
        .map_or(0, |c| c.len() * COMPONENT_BYTES + 4)
        + header_size(header)
}

impl Vector3d {
    pub(crate) fn size(&self) -> usize {
        3 * COMPONENT_BYTES + extras_size(&self.covariance, &self.header)
    }
}

impl Quaternion {
    pub(crate) fn size(&self) -> usize {
        4 * COMPONENT_BYTES + extras_size(&self.covariance, &self.header)
    }
}

struct ExtrasBuilder {
    header: OptionalHeader,
    covariance: ListBuilder<Float64Builder>,
    covariance_type: Int16Builder,
}

impl Default for ExtrasBuilder {
    fn default() -> Self {
        Self {
            header: OptionalHeader::default(),
            covariance: ListBuilder::new(Float64Builder::new()),
            covariance_type: Int16Builder::new(),
        }
    }
}

impl ExtrasBuilder {
    fn fields() -> Vec<Field> {
        vec![
            header_field(),
            Field::new(
                "covariance",
                DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true))),
                true,
            ),
            Field::new("covariance_type", DataType::Int16, true),
        ]
    }

    fn append(
        &mut self,
        header: Option<&Header>,
        covariance: Option<&[f64]>,
        covariance_type: Option<i16>,
    ) {
        self.header.append(header);
        match covariance {
            Some(values) => {
                self.covariance.values().append_slice(values);
                self.covariance.append(true);
            }
            None => self.covariance.append(false),
        }
        self.covariance_type.append_option(covariance_type);
    }

    fn finish(&mut self) -> Vec<ArrayRef> {
        vec![
            self.header.finish(),
            Arc::new(self.covariance.finish()),
            Arc::new(self.covariance_type.finish()),
        ]
    }
}

fn component_fields(names: &[&str]) -> impl Iterator<Item = Field> {
    names
        .iter()
        .map(|name| Field::new(*name, DataType::Float64, true))
}

#[derive(Default)]
pub(crate) struct Vector3dBuilder {
    extras: ExtrasBuilder,
    x: Float64Builder,
    y: Float64Builder,
    z: Float64Builder,
}

impl StructBuilder<Vector3d> for Vector3dBuilder {
    fn fields() -> Fields {
        let mut fields = ExtrasBuilder::fields();
        fields.extend(component_fields(&["x", "y", "z"]));
        Fields::from(fields)
    }

    fn append(&mut self, value: Option<&Vector3d>) {
        self.extras.append(
            value.and_then(|v| v.header.as_ref()),
            value.and_then(|v| v.covariance.as_deref()),
            value.and_then(|v| v.covariance_type),
        );
        self.x.append_value(value.map_or(0.0, |v| v.x));
        self.y.append_value(value.map_or(0.0, |v| v.y));
        self.z.append_value(value.map_or(0.0, |v| v.z));
    }

    fn finish(&mut self) -> Vec<ArrayRef> {
        let mut columns = self.extras.finish();
        columns.push(Arc::new(self.x.finish()));
        columns.push(Arc::new(self.y.finish()));
        columns.push(Arc::new(self.z.finish()));
        columns
    }
}

#[derive(Default)]
pub(crate) struct QuaternionBuilder {
    extras: ExtrasBuilder,
    x: Float64Builder,
    y: Float64Builder,
    z: Float64Builder,
    w: Float64Builder,
}

impl StructBuilder<Quaternion> for QuaternionBuilder {
    fn fields() -> Fields {
        let mut fields = ExtrasBuilder::fields();
        fields.extend(component_fields(&["x", "y", "z", "w"]));
        Fields::from(fields)
    }

    fn append(&mut self, value: Option<&Quaternion>) {
        self.extras.append(
            value.and_then(|q| q.header.as_ref()),
            value.and_then(|q| q.covariance.as_deref()),
            value.and_then(|q| q.covariance_type),
        );
        self.x.append_value(value.map_or(0.0, |q| q.x));
        self.y.append_value(value.map_or(0.0, |q| q.y));
        self.z.append_value(value.map_or(0.0, |q| q.z));
        self.w.append_value(value.map_or(0.0, |q| q.w));
    }

    fn finish(&mut self) -> Vec<ArrayRef> {
        let mut columns = self.extras.finish();
        columns.push(Arc::new(self.x.finish()));
        columns.push(Arc::new(self.y.finish()));
        columns.push(Arc::new(self.z.finish()));
        columns.push(Arc::new(self.w.finish()));
        columns
    }
}
