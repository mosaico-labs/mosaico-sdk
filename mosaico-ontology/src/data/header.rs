use std::sync::Arc;

use arrow_array::builder::{StringBuilder, UInt64Builder};
use arrow_array::types::UInt64Type;
use arrow_array::{ArrayRef, StructArray};
use arrow_schema::{DataType, Field, Fields};
use mosaico_core::Result;

use crate::arrow::build::{Nested, StructBuilder, struct_field};
use crate::arrow::columns::{Optional, OptionalString, OptionalStruct};
use crate::data::time::{Time, TimeBuilder, TimeColumns, TimeView};

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

impl Header {
    pub(crate) fn size(&self) -> usize {
        16 + self.frame_id.as_ref().map_or(0, String::len)
    }
}

#[derive(Default)]
pub(crate) struct HeaderBuilder {
    timestamp: Nested<Time, TimeBuilder>,
    frame_id: StringBuilder,
    sample_counter: UInt64Builder,
}

impl StructBuilder<Header> for HeaderBuilder {
    fn fields() -> Fields {
        Fields::from(vec![
            struct_field::<Time, TimeBuilder>("timestamp", true),
            Field::new("frame_id", DataType::Utf8, true),
            Field::new("sample_counter", DataType::UInt64, true),
        ])
    }

    fn append(&mut self, value: Option<&Header>) {
        self.timestamp
            .append(value.and_then(|header| header.timestamp.as_ref()));
        self.frame_id
            .append_option(value.and_then(|header| header.frame_id.as_deref()));
        self.sample_counter
            .append_option(value.and_then(|header| header.sample_counter));
    }

    fn finish(&mut self) -> Vec<ArrayRef> {
        vec![
            self.timestamp.finish(),
            Arc::new(self.frame_id.finish()),
            Arc::new(self.sample_counter.finish()),
        ]
    }
}

pub(crate) type OptionalHeader = Nested<Header, HeaderBuilder>;

pub(crate) fn header_field() -> Field {
    struct_field::<Header, HeaderBuilder>("header", true)
}

pub(crate) fn header_size(header: &Option<Header>) -> usize {
    header.as_ref().map_or(0, Header::size)
}
