use arrow_buffer::{ArrowNativeType, BooleanBuffer, Buffer, NullBuffer};
use arrow_data::ArrayData;
use arrow_data::transform::MutableArrayData;
use arrow_schema::DataType;

const VIEW_WIDTH: usize = 16;

pub(crate) fn copy(data: &ArrayData) -> ArrayData {
    let (offset, len) = (data.offset(), data.len());
    if len == 0 {
        return ArrayData::new_empty(data.data_type());
    }
    let (buffers, children) = match data.data_type() {
        DataType::Null => return ArrayData::new_null(&DataType::Null, len),
        DataType::Boolean => (vec![copy_bits(&data.buffers()[0], offset, len)], Vec::new()),
        DataType::Utf8 | DataType::Binary => (copy_values::<i32>(data), Vec::new()),
        DataType::LargeUtf8 | DataType::LargeBinary => (copy_values::<i64>(data), Vec::new()),
        DataType::List(_) | DataType::Map(_, _) => copy_list::<i32>(data),
        DataType::LargeList(_) => copy_list::<i64>(data),
        DataType::FixedSizeList(_, size) => {
            let size = size.as_usize();
            let child = data.child_data()[0].slice(offset * size, len * size);
            (Vec::new(), vec![copy(&child)])
        }
        DataType::FixedSizeBinary(width) => {
            let width = width.as_usize();
            let values = copy_bytes(&data.buffers()[0], offset * width, len * width);
            (vec![values], Vec::new())
        }
        DataType::Struct(_) => {
            let children = data
                .child_data()
                .iter()
                .map(|child| match child.len() == len {
                    true => copy(child),
                    false => copy(&child.slice(offset, len)),
                })
                .collect();
            (Vec::new(), children)
        }
        DataType::Dictionary(key, _) => match key.primitive_width() {
            Some(width) => {
                let keys = copy_bytes(&data.buffers()[0], offset * width, len * width);
                (vec![keys], vec![copy(&data.child_data()[0])])
            }
            None => return fallback(data),
        },
        DataType::Utf8View | DataType::BinaryView => {
            let views = copy_bytes(&data.buffers()[0], offset * VIEW_WIDTH, len * VIEW_WIDTH);
            let buffers = std::iter::once(views)
                .chain(
                    data.buffers()[1..]
                        .iter()
                        .map(|b| Buffer::from_slice_ref(b.as_slice())),
                )
                .collect();
            (buffers, Vec::new())
        }
        other => match other.primitive_width() {
            Some(width) => {
                let values = copy_bytes(&data.buffers()[0], offset * width, len * width);
                (vec![values], Vec::new())
            }
            None => return fallback(data),
        },
    };
    let builder = ArrayData::builder(data.data_type().clone())
        .len(len)
        .nulls(data.nulls().map(copy_nulls))
        .buffers(buffers)
        .child_data(children);
    unsafe { builder.build_unchecked() }
}

fn copy_bytes(buffer: &Buffer, start: usize, len: usize) -> Buffer {
    Buffer::from_slice_ref(&buffer.as_slice()[start..start + len])
}

fn copy_bits(buffer: &Buffer, offset: usize, len: usize) -> Buffer {
    let bits = BooleanBuffer::new(buffer.clone(), offset, len).sliced();
    Buffer::from_slice_ref(bits.as_slice())
}

fn copy_nulls(nulls: &NullBuffer) -> NullBuffer {
    let bits = copy_bits(nulls.buffer(), nulls.offset(), nulls.len());
    let bits = BooleanBuffer::new(bits, 0, nulls.len());
    unsafe { NullBuffer::new_unchecked(bits, nulls.null_count()) }
}

fn rebased<O: ArrowNativeType>(data: &ArrayData) -> (Buffer, usize, usize) {
    let offsets = &data.buffer::<O>(0)[..=data.len()];
    let first = offsets[0].as_usize();
    let last = offsets[data.len()].as_usize();
    let rebased: Vec<O> = offsets
        .iter()
        .map(|o| O::usize_as(o.as_usize() - first))
        .collect();
    (Buffer::from_vec(rebased), first, last)
}

fn copy_values<O: ArrowNativeType>(data: &ArrayData) -> Vec<Buffer> {
    let (offsets, first, last) = rebased::<O>(data);
    vec![offsets, copy_bytes(&data.buffers()[1], first, last - first)]
}

fn copy_list<O: ArrowNativeType>(data: &ArrayData) -> (Vec<Buffer>, Vec<ArrayData>) {
    let (offsets, first, last) = rebased::<O>(data);
    let child = data.child_data()[0].slice(first, last - first);
    (vec![offsets], vec![copy(&child)])
}

fn fallback(data: &ArrayData) -> ArrayData {
    let mut copy = MutableArrayData::new(vec![data], false, data.len());
    copy.extend(0, 0, data.len());
    let copy = copy.freeze();
    match shares_memory(copy.data_type()) {
        true => copy_shared(copy),
        false => copy,
    }
}

fn shares_memory(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, _) | DataType::Utf8View | DataType::BinaryView => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => shares_memory(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|field| shares_memory(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| shares_memory(field.data_type())),
        DataType::RunEndEncoded(_, values) => shares_memory(values.data_type()),
        _ => false,
    }
}

fn copy_shared(data: ArrayData) -> ArrayData {
    let dictionary = matches!(data.data_type(), DataType::Dictionary(_, _));
    let children = data
        .child_data()
        .iter()
        .map(|child| match dictionary {
            true => fallback(child),
            false if shares_memory(child.data_type()) => copy_shared(child.clone()),
            false => child.clone(),
        })
        .collect();
    let buffers = match data.data_type() {
        DataType::Utf8View | DataType::BinaryView => data
            .buffers()
            .iter()
            .map(|buffer| Buffer::from_slice_ref(buffer.as_slice()))
            .collect(),
        _ => data.buffers().to_vec(),
    };
    unsafe {
        data.into_builder()
            .buffers(buffers)
            .child_data(children)
            .build_unchecked()
    }
}
