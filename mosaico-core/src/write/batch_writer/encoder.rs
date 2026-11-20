use std::collections::VecDeque;

use arrow_array::cast::AsArray;
use arrow_array::{Array, OffsetSizeTrait, RecordBatch};
use arrow_flight::FlightData;
use arrow_ipc::CompressionType;
use arrow_ipc::writer::{CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::{DataType, SchemaRef};

use crate::comm::connection::Compression;
use crate::error::{Error, ErrorKind, Result};

pub(super) struct Encoder {
    pub(super) schema: SchemaRef,
    pub(super) options: IpcWriteOptions,
    pub(super) limit: usize,
}

impl Encoder {
    pub(super) fn encode_all(&self, batches: Vec<RecordBatch>) -> Result<(Vec<FlightData>, u64)> {
        let batch = match <[RecordBatch; 1]>::try_from(batches) {
            Ok([batch]) => batch,
            Err(batches) => arrow_select::concat::concat_batches(&self.schema, &batches)
                .map_err(|e| Error::new(ErrorKind::Stream, "cannot join batches").with_source(e))?,
        };
        self.encode(batch)
    }

    fn encode(&self, batch: RecordBatch) -> Result<(Vec<FlightData>, u64)> {
        let generator = IpcDataGenerator::default();
        let mut compression = CompressionContext::default();
        let mut pending = VecDeque::from([batch]);
        let mut out = Vec::new();
        let mut dropped = 0;
        while let Some(batch) = pending.pop_front() {
            let mut tracker = DictionaryTracker::new(false);
            let (dictionaries, data) = generator
                .encode(&batch, &mut tracker, &self.options, &mut compression)
                .map_err(|e| Error::new(ErrorKind::Stream, "cannot encode batch").with_source(e))?;
            let size = std::iter::once(&data)
                .chain(&dictionaries)
                .map(|d| d.ipc_message.len() + d.arrow_data.len())
                .max()
                .unwrap_or(0);
            if size <= self.limit {
                out.extend(dictionaries.into_iter().map(FlightData::from));
                out.push(FlightData::from(data));
            } else if batch.num_rows() <= 1 {
                dropped += 1;
            } else {
                let half = batch.num_rows() / 2;
                pending.push_front(batch.slice(half, batch.num_rows() - half));
                pending.push_front(batch.slice(0, half));
            }
        }
        Ok((out, dropped))
    }
}

pub(super) fn split(batch: RecordBatch, target: usize) -> Vec<(RecordBatch, usize)> {
    let rows = batch.num_rows();
    let bytes = estimate(&batch);
    if bytes <= target || rows <= 1 {
        return vec![(batch, bytes)];
    }
    let step = (rows * target / bytes).max(1);
    (0..rows)
        .step_by(step)
        .map(|start| {
            let slice = batch.slice(start, step.min(rows - start));
            let bytes = estimate(&slice);
            (slice, bytes)
        })
        .collect()
}

fn estimate(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|column| slice_bytes(column.as_ref()))
        .sum()
}

fn slice_bytes(array: &dyn Array) -> usize {
    let nulls = array.nulls().map_or(0, |nulls| nulls.len().div_ceil(8));
    match array.data_type() {
        DataType::Struct(_) => {
            nulls
                + array
                    .as_struct()
                    .columns()
                    .iter()
                    .map(|column| slice_bytes(column.as_ref()))
                    .sum::<usize>()
        }
        DataType::List(_) => nulls + list_bytes(array.as_list::<i32>()),
        DataType::LargeList(_) => nulls + list_bytes(array.as_list::<i64>()),
        DataType::Map(_, _) => {
            let map = array.as_map();
            let offsets = map.value_offsets();
            let first = offsets[0] as usize;
            let last = offsets[offsets.len() - 1] as usize;
            nulls
                + std::mem::size_of_val(offsets)
                + slice_bytes(&map.entries().slice(first, last - first))
        }
        DataType::FixedSizeList(_, _) => {
            nulls + slice_bytes(array.as_fixed_size_list().values().as_ref())
        }
        _ => array
            .to_data()
            .get_slice_memory_size()
            .unwrap_or_else(|_| array.get_array_memory_size()),
    }
}

fn list_bytes<O: OffsetSizeTrait>(list: &arrow_array::GenericListArray<O>) -> usize {
    let offsets = list.value_offsets();
    let first = offsets[0].as_usize();
    let last = offsets[offsets.len() - 1].as_usize();
    std::mem::size_of_val(offsets) + slice_bytes(list.values().slice(first, last - first).as_ref())
}

pub(super) fn ipc_compression(compression: Compression) -> Option<CompressionType> {
    match compression {
        Compression::None | Compression::Gzip => None,
        Compression::Lz4 => Some(CompressionType::LZ4_FRAME),
        Compression::Zstd => Some(CompressionType::ZSTD),
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use arrow_array::{ArrayRef, Float64Array, ListArray, StructArray};
    use arrow_buffer::OffsetBuffer;

    use arrow_schema::{DataType, Field};

    use super::*;

    #[test]
    fn estimate_counts_only_the_rows_of_a_slice() {
        let rows = 1000;
        let values = Float64Array::from_iter_values((0..rows * 9).map(|v| v as f64));
        let item = Arc::new(Field::new_list_field(DataType::Float64, true));
        let offsets = OffsetBuffer::from_lengths(vec![9; rows]);
        let list = ListArray::new(item, offsets, Arc::new(values), None);
        let field = Arc::new(Field::new("covariance", list.data_type().clone(), true));
        let inner = StructArray::from(vec![(field, Arc::new(list) as ArrayRef)]);
        let batch = RecordBatch::try_from_iter([("pose", Arc::new(inner) as ArrayRef)]).unwrap();
        let whole = estimate(&batch);
        let slice = estimate(&batch.slice(500, 10));
        assert!(slice * 50 < whole, "slice {slice}, whole {whole}");
        assert!(slice >= 10 * 9 * 8, "slice {slice}");
    }
}
