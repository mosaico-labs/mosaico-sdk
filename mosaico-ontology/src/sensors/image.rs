use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, BooleanBuilder, Int32Builder, StringBuilder};
use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Fields};
use mosaico_core::{Encode, Error, Format, Ontology, RecordBuilder, Result};

use crate::arrow::columns::{
    OptionalBool, OptionalStruct, Required, RequiredBinary, RequiredString,
};
use crate::data::header::{
    Header, HeaderColumns, HeaderView, OptionalHeader, header_field, header_size,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ImageFormat {
    Raw,
    Png,
    Jpeg,
    Jpg,
    Tiff,
    H264,
    Hevc,
}

impl ImageFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Png => "png",
            Self::Jpeg => "jpeg",
            Self::Jpg => "jpg",
            Self::Tiff => "tiff",
            Self::H264 => "h264",
            Self::Hevc => "hevc",
        }
    }

    pub fn is_stateful(self) -> bool {
        matches!(self, Self::H264 | Self::Hevc)
    }

    fn parse(value: &str) -> Result<Self> {
        Ok(match value {
            "raw" => Self::Raw,
            "png" => Self::Png,
            "jpeg" => Self::Jpeg,
            "jpg" => Self::Jpg,
            "tiff" => Self::Tiff,
            "h264" => Self::H264,
            "hevc" => Self::Hevc,
            _ => return Err(Error::schema(format!("unknown image format '{value}'"))),
        })
    }
}

impl FromStr for ImageFormat {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        Self::parse(value)
    }
}

impl fmt::Display for ImageFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Image {
    pub header: Option<Header>,
    pub data: Vec<u8>,
    pub format: ImageFormat,
    pub width: i32,
    pub height: i32,
    pub stride: i32,
    pub encoding: String,
    pub is_bigendian: Option<bool>,
}

pub struct ImageColumns {
    header: OptionalStruct<HeaderColumns>,
    data: RequiredBinary,
    format: RequiredString,
    width: Required<Int32Type>,
    height: Required<Int32Type>,
    stride: Required<Int32Type>,
    encoding: RequiredString,
    is_bigendian: OptionalBool,
}

#[derive(Clone, Copy)]
pub struct ImageView<'a> {
    columns: &'a ImageColumns,
    row: usize,
}

impl<'a> ImageView<'a> {
    pub fn header(&self) -> Option<HeaderView<'a>> {
        let columns = self.columns.header.get(self.row)?;
        Some(HeaderView::new(columns, self.row))
    }

    pub fn data(&self) -> Result<&'a [u8]> {
        self.columns.data.get(self.row)
    }

    pub fn format(&self) -> Result<ImageFormat> {
        ImageFormat::parse(self.columns.format.get(self.row)?)
    }

    pub fn width(&self) -> Result<i32> {
        self.columns.width.get(self.row)
    }

    pub fn height(&self) -> Result<i32> {
        self.columns.height.get(self.row)
    }

    pub fn stride(&self) -> Result<i32> {
        self.columns.stride.get(self.row)
    }

    pub fn encoding(&self) -> Result<&'a str> {
        self.columns.encoding.get(self.row)
    }

    pub fn is_bigendian(&self) -> Option<bool> {
        self.columns.is_bigendian.get(self.row)
    }

    pub fn to_model(&self) -> Result<Image> {
        Ok(Image {
            header: self.header().map(|h| h.to_model()).transpose()?,
            data: self.data()?.to_vec(),
            format: self.format()?,
            width: self.width()?,
            height: self.height()?,
            stride: self.stride()?,
            encoding: self.encoding()?.to_owned(),
            is_bigendian: self.is_bigendian(),
        })
    }
}

impl Ontology for Image {
    const TAG: &'static str = "Image";
    type Columns = ImageColumns;
    type View<'a> = ImageView<'a>;

    fn columns(batch: &RecordBatch) -> Result<ImageColumns> {
        let array = StructArray::from(batch.clone());
        Ok(ImageColumns {
            header: OptionalStruct::new(&array, "", "header", HeaderColumns::new)?,
            data: RequiredBinary::new(&array, "", "data")?,
            format: RequiredString::new(&array, "", "format")?,
            width: Required::new(&array, "", "width")?,
            height: Required::new(&array, "", "height")?,
            stride: Required::new(&array, "", "stride")?,
            encoding: RequiredString::new(&array, "", "encoding")?,
            is_bigendian: OptionalBool::new(&array, "", "is_bigendian")?,
        })
    }

    fn view(columns: &ImageColumns, row: usize) -> ImageView<'_> {
        ImageView { columns, row }
    }

    fn from_view(view: &ImageView<'_>) -> Result<Image> {
        view.to_model()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompressedImage {
    pub header: Option<Header>,
    pub data: Vec<u8>,
    pub format: ImageFormat,
}

pub struct CompressedImageColumns {
    header: OptionalStruct<HeaderColumns>,
    data: RequiredBinary,
    format: RequiredString,
}

#[derive(Clone, Copy)]
pub struct CompressedImageView<'a> {
    columns: &'a CompressedImageColumns,
    row: usize,
}

impl<'a> CompressedImageView<'a> {
    pub fn header(&self) -> Option<HeaderView<'a>> {
        let columns = self.columns.header.get(self.row)?;
        Some(HeaderView::new(columns, self.row))
    }

    pub fn data(&self) -> Result<&'a [u8]> {
        self.columns.data.get(self.row)
    }

    pub fn format(&self) -> Result<ImageFormat> {
        ImageFormat::parse(self.columns.format.get(self.row)?)
    }

    pub fn to_model(&self) -> Result<CompressedImage> {
        Ok(CompressedImage {
            header: self.header().map(|h| h.to_model()).transpose()?,
            data: self.data()?.to_vec(),
            format: self.format()?,
        })
    }
}

impl Ontology for CompressedImage {
    const TAG: &'static str = "CompressedImage";
    type Columns = CompressedImageColumns;
    type View<'a> = CompressedImageView<'a>;

    fn columns(batch: &RecordBatch) -> Result<CompressedImageColumns> {
        let array = StructArray::from(batch.clone());
        Ok(CompressedImageColumns {
            header: OptionalStruct::new(&array, "", "header", HeaderColumns::new)?,
            data: RequiredBinary::new(&array, "", "data")?,
            format: RequiredString::new(&array, "", "format")?,
        })
    }

    fn view(columns: &CompressedImageColumns, row: usize) -> CompressedImageView<'_> {
        CompressedImageView { columns, row }
    }

    fn from_view(view: &CompressedImageView<'_>) -> Result<CompressedImage> {
        view.to_model()
    }
}

const IMAGE_FIXED_BYTES: usize = 16;

#[derive(Default)]
pub struct ImageBuilder {
    header: OptionalHeader,
    data: BinaryBuilder,
    format: StringBuilder,
    width: Int32Builder,
    height: Int32Builder,
    stride: Int32Builder,
    encoding: StringBuilder,
    is_bigendian: BooleanBuilder,
}

impl RecordBuilder<Image> for ImageBuilder {
    fn append(&mut self, value: &Image) {
        self.header.append(value.header.as_ref());
        self.data.append_value(&value.data);
        self.format.append_value(value.format.as_str());
        self.width.append_value(value.width);
        self.height.append_value(value.height);
        self.stride.append_value(value.stride);
        self.encoding.append_value(&value.encoding);
        self.is_bigendian.append_option(value.is_bigendian);
    }

    fn finish(&mut self) -> Vec<ArrayRef> {
        vec![
            self.header.finish(),
            Arc::new(self.data.finish()),
            Arc::new(self.format.finish()),
            Arc::new(self.width.finish()),
            Arc::new(self.height.finish()),
            Arc::new(self.stride.finish()),
            Arc::new(self.encoding.finish()),
            Arc::new(self.is_bigendian.finish()),
        ]
    }
}

impl Encode for Image {
    const FORMAT: Format = Format::Image;
    type Builder = ImageBuilder;

    fn fields() -> Fields {
        Fields::from(vec![
            header_field(),
            Field::new("data", DataType::Binary, false),
            Field::new("format", DataType::Utf8, false),
            Field::new("width", DataType::Int32, false),
            Field::new("height", DataType::Int32, false),
            Field::new("stride", DataType::Int32, false),
            Field::new("encoding", DataType::Utf8, false),
            Field::new("is_bigendian", DataType::Boolean, true),
        ])
    }

    fn size(&self) -> usize {
        header_size(&self.header)
            + self.data.len()
            + self.format.as_str().len()
            + self.encoding.len()
            + IMAGE_FIXED_BYTES
    }
}

#[derive(Default)]
pub struct CompressedImageBuilder {
    header: OptionalHeader,
    data: BinaryBuilder,
    format: StringBuilder,
}

impl RecordBuilder<CompressedImage> for CompressedImageBuilder {
    fn append(&mut self, value: &CompressedImage) {
        self.header.append(value.header.as_ref());
        self.data.append_value(&value.data);
        self.format.append_value(value.format.as_str());
    }

    fn finish(&mut self) -> Vec<ArrayRef> {
        vec![
            self.header.finish(),
            Arc::new(self.data.finish()),
            Arc::new(self.format.finish()),
        ]
    }
}

impl Encode for CompressedImage {
    const FORMAT: Format = Format::Image;
    type Builder = CompressedImageBuilder;

    fn fields() -> Fields {
        Fields::from(vec![
            header_field(),
            Field::new("data", DataType::Binary, false),
            Field::new("format", DataType::Utf8, false),
        ])
    }

    fn size(&self) -> usize {
        header_size(&self.header) + self.data.len() + self.format.as_str().len() + 8
    }
}
