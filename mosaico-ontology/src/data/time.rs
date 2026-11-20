use std::sync::Arc;

use arrow_array::builder::{Int32Builder, UInt32Builder};
use arrow_array::types::{Int32Type, UInt32Type};
use arrow_array::{ArrayRef, StructArray};
use arrow_schema::{DataType, Field, Fields};
use mosaico_core::Result;

use crate::arrow::build::StructBuilder;
use crate::arrow::columns::Required;

#[derive(Debug, Clone, PartialEq)]
pub struct Time {
    pub seconds: i32,
    pub nanoseconds: u32,
}

pub(crate) struct TimeColumns {
    seconds: Required<Int32Type>,
    nanoseconds: Required<UInt32Type>,
}

impl TimeColumns {
    pub(crate) fn new(array: &StructArray, path: &str) -> Result<Self> {
        Ok(Self {
            seconds: Required::new(array, path, "seconds")?,
            nanoseconds: Required::new(array, path, "nanoseconds")?,
        })
    }
}

#[derive(Clone, Copy)]
pub struct TimeView<'a> {
    columns: &'a TimeColumns,
    row: usize,
}

impl<'a> TimeView<'a> {
    pub(crate) fn new(columns: &'a TimeColumns, row: usize) -> Self {
        Self { columns, row }
    }

    pub fn seconds(&self) -> Result<i32> {
        self.columns.seconds.get(self.row)
    }

    pub fn nanoseconds(&self) -> Result<u32> {
        self.columns.nanoseconds.get(self.row)
    }

    pub fn to_model(&self) -> Result<Time> {
        Ok(Time {
            seconds: self.seconds()?,
            nanoseconds: self.nanoseconds()?,
        })
    }
}

#[derive(Default)]
pub(crate) struct TimeBuilder {
    seconds: Int32Builder,
    nanoseconds: UInt32Builder,
}

impl StructBuilder<Time> for TimeBuilder {
    fn fields() -> Fields {
        Fields::from(vec![
            Field::new("seconds", DataType::Int32, false),
            Field::new("nanoseconds", DataType::UInt32, false),
        ])
    }

    fn append(&mut self, value: Option<&Time>) {
        self.seconds
            .append_value(value.map_or(0, |time| time.seconds));
        self.nanoseconds
            .append_value(value.map_or(0, |time| time.nanoseconds));
    }

    fn finish(&mut self) -> Vec<ArrayRef> {
        vec![
            Arc::new(self.seconds.finish()),
            Arc::new(self.nanoseconds.finish()),
        ]
    }
}
