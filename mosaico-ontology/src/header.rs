use arrow_array::StructArray;
use arrow_array::types::UInt64Type;
use mosaico_core::Result;

use crate::columns::{Optional, OptionalString, OptionalStruct};
use crate::time::{Time, TimeColumns, TimeView};

#[derive(Debug, Clone, PartialEq)]
pub struct Header {
    pub timestamp: Option<Time>,
    pub frame_id: Option<String>,
    pub sample_counter: Option<u64>,
}

pub(crate) struct HeaderColumns {
    timestamp: OptionalStruct<TimeColumns>,
    frame_id: OptionalString,
    sample_counter: Optional<UInt64Type>,
}

impl HeaderColumns {
    pub(crate) fn new(array: &StructArray, path: &str) -> Result<Self> {
        Ok(Self {
            timestamp: OptionalStruct::new(array, path, "timestamp", TimeColumns::new)?,
            frame_id: OptionalString::new(array, path, "frame_id")?,
            sample_counter: Optional::new(array, path, "sample_counter")?,
        })
    }
}

#[derive(Clone, Copy)]
pub struct HeaderView<'a> {
    columns: &'a HeaderColumns,
    row: usize,
}

impl<'a> HeaderView<'a> {
    pub(crate) fn new(columns: &'a HeaderColumns, row: usize) -> Self {
        Self { columns, row }
    }

    pub fn timestamp(&self) -> Option<TimeView<'a>> {
        let columns = self.columns.timestamp.get(self.row)?;
        Some(TimeView::new(columns, self.row))
    }

    pub fn frame_id(&self) -> Option<&'a str> {
        self.columns.frame_id.get(self.row)
    }

    pub fn sample_counter(&self) -> Option<u64> {
        self.columns.sample_counter.get(self.row)
    }

    pub fn to_model(&self) -> Result<Header> {
        Ok(Header {
            timestamp: self.timestamp().map(|t| t.to_model()).transpose()?,
            frame_id: self.frame_id().map(str::to_owned),
            sample_counter: self.sample_counter(),
        })
    }
}
