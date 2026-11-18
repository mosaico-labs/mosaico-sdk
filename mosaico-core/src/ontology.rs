use arrow_array::RecordBatch;

use crate::error::Result;

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

pub struct Message<'a, T: Ontology + 'a> {
    pub timestamp_ns: i64,
    pub data: T::View<'a>,
}

impl<'a, T: Ontology + 'a> Message<'a, T> {
    pub fn get_data(&self) -> Result<T> {
        T::from_view(&self.data)
    }
}
