use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::Fields;

use crate::comm::proto;
use crate::error::Result;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Format {
    #[default]
    Default,
    Ragged,
    Image,
}

impl Format {
    pub(crate) fn to_proto(self) -> i32 {
        let format = match self {
            Self::Default => proto::core::Format::Default,
            Self::Ragged => proto::core::Format::Ragged,
            Self::Image => proto::core::Format::Image,
        };
        format as i32
    }
}

pub trait Ontology: Sized + Send + Sync + 'static {
    const TAG: &'static str;
    type Columns: Send + Sync;
    type View<'a>
    where
        Self: 'a;

    fn columns(batch: &RecordBatch) -> Result<Self::Columns>;
    fn view(columns: &Self::Columns, row: usize) -> Self::View<'_>;
    fn from_view(view: &Self::View<'_>) -> Result<Self>;
}

pub trait Encode: Ontology {
    const FORMAT: Format;
    type Builder: RecordBuilder<Self>;

    fn fields() -> Fields;
    fn size(&self) -> usize;
}

pub trait RecordBuilder<T>: Default + Send + 'static {
    fn append(&mut self, value: &T);
    fn finish(&mut self) -> Vec<ArrayRef>;
}

#[non_exhaustive]
pub struct Message<'a, T: Ontology + 'a> {
    pub timestamp_ns: i64,
    pub view: T::View<'a>,
}

impl<'a, T: Ontology + 'a> Message<'a, T> {
    pub fn to_model(&self) -> Result<T> {
        T::from_view(&self.view)
    }
}
