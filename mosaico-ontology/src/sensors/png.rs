use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};

use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{Array, ArrayRef, BinaryArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use mosaico_core::{Error, Result};
use png::{BitDepth, ColorType, Compression, DecodingError, Encoder, EncodingError};
use rayon::ThreadPool;
use rayon::prelude::*;

const MAX_COLUMN_BYTES: usize = i32::MAX as usize;

#[derive(Debug, Clone, Copy)]
pub struct Pixels<'a> {
    pub data: &'a [u8],
    pub height: u32,
    pub stride: u32,
}

pub fn encode_png(pixels: Pixels<'_>) -> Result<Vec<u8>> {
    let expected = pixels.height as usize * pixels.stride as usize;
    if expected == 0 || pixels.data.len() != expected {
        return Err(Error::invalid_argument(format!(
            "image data is {} bytes, expected height {} x stride {}",
            pixels.data.len(),
            pixels.height,
            pixels.stride
        )));
    }
    let mut out = Vec::with_capacity(expected / 2);
    let mut encoder = Encoder::new(&mut out, pixels.stride, pixels.height);
    encoder.set_color(ColorType::Grayscale);
    encoder.set_depth(BitDepth::Eight);
    encoder.set_compression(Compression::Fast);
    let mut writer = encoder.write_header().map_err(failed)?;
    writer.write_image_data(pixels.data).map_err(failed)?;
    writer.finish().map_err(failed)?;
    Ok(out)
}

pub fn encode_png_column(images: &[Pixels<'_>]) -> Result<BinaryArray> {
    let rows: Vec<_> = images.iter().copied().enumerate().collect();
    let encoded = encode_rows(&rows)?;
    binary_column(encoded.iter().map(|image| Some(image.as_slice())))
}

fn encode_rows(rows: &[(usize, Pixels<'_>)]) -> Result<Vec<Vec<u8>>> {
    pool()?.install(|| {
        rows.par_iter()
            .map(|(row, pixels)| {
                encode_png(*pixels).map_err(|e| Error::invalid_argument(format!("row {row}: {e}")))
            })
            .collect()
    })
}

fn binary_column<'a>(
    values: impl Iterator<Item = Option<&'a [u8]>> + Clone,
) -> Result<BinaryArray> {
    let total: usize = values.clone().flatten().map(<[u8]>::len).sum();
    if total > MAX_COLUMN_BYTES {
        return Err(Error::invalid_argument(format!(
            "encoded images are {total} bytes, more than a binary column can hold: push fewer rows at a time"
        )));
    }
    Ok(BinaryArray::from_iter(values))
}

pub const THREADS_VARIABLE: &str = "MOSAICO_PNG_THREADS";

fn threads() -> Result<usize> {
    let Ok(value) = std::env::var(THREADS_VARIABLE) else {
        return Ok(0);
    };
    match value.trim().parse::<usize>() {
        Ok(threads) if threads > 0 => Ok(threads),
        _ => Err(Error::invalid_argument(format!(
            "{THREADS_VARIABLE} must be a positive number of threads, not '{value}'"
        ))),
    }
}

struct ProcessPool {
    pid: u32,
    pool: ThreadPool,
}

fn pool() -> Result<&'static ThreadPool> {
    static CURRENT: AtomicPtr<ProcessPool> = AtomicPtr::new(ptr::null_mut());

    let pid = std::process::id();
    let mut current = CURRENT.load(Ordering::Acquire);
    loop {
        if let Some(installed) = unsafe { current.as_ref() }
            && installed.pid == pid
        {
            return Ok(&installed.pool);
        }
        let pool = rayon::ThreadPoolBuilder::new()
            .thread_name(|i| format!("mosaico-png-{i}"))
            .num_threads(threads()?)
            .build()
            .map_err(|e| {
                Error::invalid_argument(format!("cannot start the encoding threads: {e}"))
            })?;
        let new = Box::into_raw(Box::new(ProcessPool { pid, pool }));
        match CURRENT.compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(unsafe { &(*new).pool }),
            Err(installed) => {
                drop(unsafe { Box::from_raw(new) });
                current = installed;
            }
        }
    }
}

pub fn encode_png_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let rows = batch.num_rows();
    let data = binary_values(column(batch, "data")?)?;
    let formats = string_values(column(batch, "format")?)?;
    let heights = int32_values(column(batch, "height")?)?;
    let strides = int32_values(column(batch, "stride")?)?;
    let raw: Vec<usize> = (0..rows)
        .filter(|&row| formats[row] == Some("raw"))
        .collect();
    if raw.is_empty() {
        return Ok(batch.clone());
    }
    let mut pixels = Vec::with_capacity(raw.len());
    for &row in &raw {
        let (Some(data), Some(height), Some(stride)) = (data[row], heights[row], strides[row])
        else {
            return Err(Error::invalid_argument(format!(
                "row {row}: 'data', 'height' and 'stride' must not be null"
            )));
        };
        let size = |value: i32, name: &str| {
            u32::try_from(value).map_err(|_| {
                Error::invalid_argument(format!("row {row}: '{name}' must not be negative"))
            })
        };
        let image = Pixels {
            data,
            height: size(height, "height")?,
            stride: size(stride, "stride")?,
        };
        pixels.push((row, image));
    }
    let encoded = encode_rows(&pixels)?;
    replace_rows(batch, &data, &formats, &raw, &encoded, "png")
}

pub fn decode_png(data: &[u8]) -> Result<Vec<u8>> {
    let limits = png::Limits {
        bytes: MAX_COLUMN_BYTES,
    };
    let mut reader = png::Decoder::new_with_limits(std::io::Cursor::new(data), limits)
        .read_info()
        .map_err(decode_failed)?;
    let size = reader
        .output_buffer_size()
        .ok_or_else(|| Error::invalid_argument("png image is too large to decode".to_owned()))?;
    let mut out = vec![0; size];
    let info = reader.next_frame(&mut out).map_err(decode_failed)?;
    out.truncate(info.buffer_size());
    Ok(as_pillow(out, &info))
}

fn as_pillow(data: Vec<u8>, info: &png::OutputInfo) -> Vec<u8> {
    let depth = info.bit_depth as usize;
    match (info.color_type, info.bit_depth) {
        (ColorType::Grayscale, BitDepth::One | BitDepth::Two | BitDepth::Four) => {
            unpack(&data, info, 255 / ((1 << depth) - 1))
        }
        (ColorType::Indexed, BitDepth::One | BitDepth::Two | BitDepth::Four) => {
            unpack(&data, info, 1)
        }
        (ColorType::Grayscale, BitDepth::Sixteen) => {
            let mut data = data;
            if cfg!(target_endian = "little") {
                for sample in data.as_chunks_mut::<2>().0 {
                    sample.swap(0, 1);
                }
            }
            data
        }
        (ColorType::GrayscaleAlpha, BitDepth::Sixteen) => data
            .as_chunks::<4>()
            .0
            .iter()
            .flat_map(|[gray, _, alpha, _]| [*gray, *gray, *gray, *alpha])
            .collect(),
        (ColorType::Rgb | ColorType::Rgba, BitDepth::Sixteen) => data
            .as_chunks::<2>()
            .0
            .iter()
            .map(|[high, _]| *high)
            .collect(),
        _ => data,
    }
}

fn unpack(data: &[u8], info: &png::OutputInfo, scale: usize) -> Vec<u8> {
    let depth = info.bit_depth as usize;
    let mask = (1usize << depth) - 1;
    let width = info.width as usize;
    let mut out = Vec::with_capacity(width * info.height as usize);
    for row in data.chunks(info.line_size).take(info.height as usize) {
        for x in 0..width {
            let bit = x * depth;
            let value = (row[bit / 8] as usize >> (8 - depth - bit % 8)) & mask;
            out.push((value * scale) as u8);
        }
    }
    out
}

pub fn decode_png_column(images: &[&[u8]]) -> Result<BinaryArray> {
    let rows: Vec<_> = images.iter().copied().enumerate().collect();
    let decoded = decode_rows(&rows)?;
    binary_column(decoded.iter().map(|image| Some(image.as_slice())))
}

fn decode_rows(rows: &[(usize, &[u8])]) -> Result<Vec<Vec<u8>>> {
    pool()?.install(|| {
        rows.par_iter()
            .map(|(row, data)| {
                decode_png(data).map_err(|e| Error::invalid_argument(format!("row {row}: {e}")))
            })
            .collect()
    })
}

pub fn decode_png_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let mut decoded = decode_png_batches(std::slice::from_ref(batch))?;
    Ok(decoded.remove(0))
}

pub fn decode_png_batches(batches: &[RecordBatch]) -> Result<Vec<RecordBatch>> {
    let mut columns = Vec::with_capacity(batches.len());
    let mut rows = Vec::new();
    let mut offset = 0;
    for batch in batches {
        let data = binary_values(column(batch, "data")?)?;
        let formats = string_values(column(batch, "format")?)?;
        for (row, format) in formats.iter().enumerate() {
            if *format != Some("png") {
                continue;
            }
            let Some(value) = data[row] else {
                return Err(Error::invalid_argument(format!(
                    "row {}: 'data' must not be null",
                    offset + row
                )));
            };
            rows.push((offset + row, value));
        }
        offset += batch.num_rows();
        columns.push((data, formats));
    }
    let decoded = decode_rows(&rows)?;
    let mut changed = rows.iter().map(|(row, _)| *row).zip(decoded).peekable();
    let mut offset = 0;
    let mut out = Vec::with_capacity(batches.len());
    for (batch, (data, formats)) in batches.iter().zip(&columns) {
        let end = offset + batch.num_rows();
        let mut local = Vec::new();
        let mut values = Vec::new();
        while let Some((row, value)) = changed.next_if(|(row, _)| *row < end) {
            local.push(row - offset);
            values.push(value);
        }
        let plain = column(batch, "data")?.data_type() == &DataType::Binary
            && column(batch, "format")?.data_type() == &DataType::Utf8;
        out.push(match local.is_empty() && plain {
            true => batch.clone(),
            false => replace_rows(batch, data, formats, &local, &values, "raw")?,
        });
        offset = end;
    }
    Ok(out)
}

fn replace_rows(
    batch: &RecordBatch,
    data: &[Option<&[u8]>],
    formats: &[Option<&str>],
    changed: &[usize],
    values: &[Vec<u8>],
    format: &str,
) -> Result<RecordBatch> {
    let rows = batch.num_rows();
    let mut changed = changed.iter().copied().zip(values).peekable();
    let mut new_data = Vec::with_capacity(rows);
    let mut new_formats = Vec::with_capacity(rows);
    for row in 0..rows {
        match changed.next_if(|&(changed_row, _)| changed_row == row) {
            Some((_, value)) => {
                new_data.push(Some(value.as_slice()));
                new_formats.push(Some(format));
            }
            None => {
                new_data.push(data[row]);
                new_formats.push(formats[row]);
            }
        }
    }
    let new_data: ArrayRef = Arc::new(binary_column(new_data.iter().copied())?);
    let new_formats: ArrayRef = Arc::new(StringArray::from(new_formats));
    let schema = batch.schema();
    let mut fields = Vec::with_capacity(schema.fields().len());
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        let (data_type, column) = match field.name().as_str() {
            "data" => (DataType::Binary, new_data.clone()),
            "format" => (DataType::Utf8, new_formats.clone()),
            _ => (field.data_type().clone(), column.clone()),
        };
        fields.push(Field::clone(field).with_data_type(data_type));
        columns.push(column);
    }
    let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    RecordBatch::try_new(schema, columns)
        .map_err(|e| Error::invalid_argument(format!("cannot build the image batch: {e}")))
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    batch
        .column_by_name(name)
        .ok_or_else(|| Error::invalid_argument(format!("batch has no '{name}' column")))
}

fn wrong_type(column: &ArrayRef, expected: &str) -> Error {
    Error::invalid_argument(format!(
        "column of type {} where {expected} was expected",
        column.data_type()
    ))
}

fn binary_values(column: &ArrayRef) -> Result<Vec<Option<&[u8]>>> {
    Ok(match column.data_type() {
        DataType::Binary => column.as_binary::<i32>().iter().collect(),
        DataType::LargeBinary => column.as_binary::<i64>().iter().collect(),
        DataType::BinaryView => column.as_binary_view().iter().collect(),
        _ => return Err(wrong_type(column, "binary")),
    })
}

fn string_values(column: &ArrayRef) -> Result<Vec<Option<&str>>> {
    Ok(match column.data_type() {
        DataType::Utf8 => column.as_string::<i32>().iter().collect(),
        DataType::LargeUtf8 => column.as_string::<i64>().iter().collect(),
        DataType::Utf8View => column.as_string_view().iter().collect(),
        _ => return Err(wrong_type(column, "string")),
    })
}

fn int32_values(column: &ArrayRef) -> Result<Vec<Option<i32>>> {
    match column.as_primitive_opt::<Int32Type>() {
        Some(values) => Ok(values.iter().collect()),
        None => Err(wrong_type(column, "int32")),
    }
}

fn failed(e: EncodingError) -> Error {
    Error::invalid_argument(format!("png encoding failed: {e}"))
}

fn decode_failed(e: DecodingError) -> Error {
    Error::invalid_argument(format!("png decoding failed: {e}"))
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;
    use mosaico_core::ErrorKind;

    use super::*;

    fn decode(bytes: &[u8]) -> (u32, u32, Vec<u8>) {
        let mut reader = png::Decoder::new(std::io::Cursor::new(bytes))
            .read_info()
            .unwrap();
        let mut out = vec![0; reader.output_buffer_size().unwrap()];
        let info = reader.next_frame(&mut out).unwrap();
        assert_eq!(info.color_type, ColorType::Grayscale);
        assert_eq!(info.bit_depth, BitDepth::Eight);
        out.truncate(info.buffer_size());
        (info.height, info.width, out)
    }

    fn image(height: u32, stride: u32, seed: u32) -> Vec<u8> {
        (0..height * stride)
            .map(|i| ((i % stride) / 3 + (i / stride) / 2 + seed * 7) as u8)
            .collect()
    }

    #[test]
    fn decodes_to_the_same_bytes_as_wide_grayscale() {
        let data = image(48, 64 * 3, 1);
        let encoded = encode_png(Pixels {
            data: &data,
            height: 48,
            stride: 192,
        })
        .unwrap();
        assert!(encoded.len() < data.len());
        assert_eq!(decode(&encoded), (48, 192, data));
    }

    #[test]
    fn column_keeps_the_input_order() {
        let images: Vec<Vec<u8>> = (0..17).map(|seed| image(20, 30, seed)).collect();
        let pixels: Vec<Pixels> = images
            .iter()
            .map(|data| Pixels {
                data,
                height: 20,
                stride: 30,
            })
            .collect();
        let column = encode_png_column(&pixels).unwrap();
        assert_eq!(column.len(), images.len());
        for (row, data) in images.iter().enumerate() {
            assert_eq!(decode(column.value(row)).2, *data);
        }
    }

    #[test]
    fn batch_encodes_only_raw_rows() {
        let images: Vec<Vec<u8>> = (0..3).map(|seed| image(4, 6, seed)).collect();
        let data: ArrayRef = Arc::new(arrow_array::BinaryViewArray::from_iter_values(
            images.iter().map(Vec::as_slice),
        ));
        let format: ArrayRef = Arc::new(StringArray::from(vec!["raw", "jpeg", "raw"]));
        let size: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![4, 1, 4]));
        let stride: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![6, 1, 6]));
        let batch = RecordBatch::try_from_iter([
            ("data", data),
            ("format", format),
            ("height", size),
            ("stride", stride),
        ])
        .unwrap();
        let encoded = encode_png_batch(&batch).unwrap();
        let data = encoded.column(0).as_binary::<i32>();
        let format = encoded.column(1).as_string::<i32>();
        assert_eq!(
            format.iter().collect::<Vec<_>>(),
            [Some("png"), Some("jpeg"), Some("png")]
        );
        assert_eq!(decode(data.value(0)).2, images[0]);
        assert_eq!(data.value(1), images[1].as_slice());
        assert_eq!(decode(data.value(2)).2, images[2]);
        assert_eq!(encoded.schema().field(0).data_type(), &DataType::Binary);
    }

    #[test]
    fn decoding_returns_the_encoded_pixels() {
        let data = image(48, 192, 3);
        let encoded = encode_png(Pixels {
            data: &data,
            height: 48,
            stride: 192,
        })
        .unwrap();
        assert_eq!(decode_png(&encoded).unwrap(), data);
        let column = decode_png_column(&[encoded.as_slice(), encoded.as_slice()]).unwrap();
        assert_eq!(column.value(1), data.as_slice());
        assert_eq!(
            decode_png(b"not a png").unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
    }

    #[test]
    fn batch_decodes_only_png_rows() {
        let images: Vec<Vec<u8>> = (0..3).map(|seed| image(4, 6, seed)).collect();
        let data: ArrayRef = Arc::new(BinaryArray::from_iter_values(
            images.iter().map(Vec::as_slice),
        ));
        let format: ArrayRef = Arc::new(StringArray::from(vec!["raw", "jpeg", "raw"]));
        let size: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![4, 1, 4]));
        let stride: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![6, 1, 6]));
        let batch = RecordBatch::try_from_iter([
            ("data", data),
            ("format", format),
            ("height", size),
            ("stride", stride),
        ])
        .unwrap();
        let decoded = decode_png_batch(&encode_png_batch(&batch).unwrap()).unwrap();
        assert_eq!(
            decoded.column(0).as_binary::<i32>(),
            batch.column(0).as_binary::<i32>()
        );
        assert_eq!(
            decoded
                .column(1)
                .as_string::<i32>()
                .iter()
                .collect::<Vec<_>>(),
            [Some("raw"), Some("jpeg"), Some("raw")]
        );
    }

    fn encoded(color: ColorType, depth: BitDepth, width: u32, data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut encoder = Encoder::new(&mut out, width, 1);
        encoder.set_color(color);
        encoder.set_depth(depth);
        if color == ColorType::Indexed {
            encoder.set_palette(vec![0; 16 * 3]);
        }
        let mut writer = encoder.write_header().unwrap();
        writer.write_image_data(data).unwrap();
        writer.finish().unwrap();
        out
    }

    #[test]
    fn decoding_follows_pillow_for_other_depths() {
        let gray2 = encoded(ColorType::Grayscale, BitDepth::Two, 4, &[0b00_01_10_11]);
        assert_eq!(decode_png(&gray2).unwrap(), [0, 85, 170, 255]);
        let indexed4 = encoded(ColorType::Indexed, BitDepth::Four, 3, &[0x1f, 0x70]);
        assert_eq!(decode_png(&indexed4).unwrap(), [1, 15, 7]);
        let rgb16 = encoded(ColorType::Rgb, BitDepth::Sixteen, 1, &[1, 2, 3, 4, 5, 6]);
        assert_eq!(decode_png(&rgb16).unwrap(), [1, 3, 5]);
        let gray_alpha16 = encoded(
            ColorType::GrayscaleAlpha,
            BitDepth::Sixteen,
            1,
            &[9, 8, 7, 6],
        );
        assert_eq!(decode_png(&gray_alpha16).unwrap(), [9, 9, 9, 7]);
        let gray16 = encoded(ColorType::Grayscale, BitDepth::Sixteen, 1, &[1, 2]);
        assert_eq!(decode_png(&gray16).unwrap(), 0x0102u16.to_ne_bytes());
    }

    #[test]
    fn rejects_data_that_does_not_match_the_size() {
        let data = image(4, 10, 0);
        for (height, stride) in [(4, 11), (0, 10), (4, 0)] {
            let e = encode_png(Pixels {
                data: &data,
                height,
                stride,
            })
            .unwrap_err();
            assert_eq!(e.kind(), ErrorKind::InvalidArgument);
        }
        let pixels = [
            Pixels {
                data: &data,
                height: 4,
                stride: 10,
            },
            Pixels {
                data: &data,
                height: 5,
                stride: 10,
            },
        ];
        assert!(encode_png_column(&pixels).is_err());
    }
}
