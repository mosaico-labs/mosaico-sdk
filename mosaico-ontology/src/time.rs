use arrow_array::StructArray;
use arrow_array::types::{Int32Type, UInt32Type};
use mosaico_core::Result;

use crate::columns::Required;

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
