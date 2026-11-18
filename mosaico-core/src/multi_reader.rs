use arrow_array::RecordBatch;

use crate::batch_reader::BatchReader;
use crate::error::Result;

#[derive(Debug, Clone, Copy)]
pub(crate) struct MergeLimits {
    pub(crate) max_rows: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct Window {
    pub(crate) slices: Vec<RecordBatch>,
    pub(crate) order: Vec<u16>,
}

enum Mode {
    Merge(MergeLimits),
    Concat,
}

pub(crate) struct MultiReader {
    readers: Vec<BatchReader>,
    mode: Mode,
}

impl MultiReader {
    pub(crate) fn merge(readers: Vec<BatchReader>, limits: MergeLimits) -> Self {
        Self {
            readers,
            mode: Mode::Merge(limits),
        }
    }

    pub(crate) fn concat(readers: Vec<BatchReader>) -> Self {
        Self {
            readers,
            mode: Mode::Concat,
        }
    }

    pub(crate) async fn next_window(&mut self) -> Result<Option<Window>> {
        todo!()
    }

    pub(crate) async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        todo!()
    }
}
