use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, OffsetSizeTrait};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer, OffsetBuffer};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};

pub(super) fn unmasked_nulls(
    array: &dyn Array,
    field: &Field,
    mask: Option<&NullBuffer>,
) -> Option<String> {
    let path = |child: String| format!("{}.{child}", field.name());
    let nulls = array.logical_nulls();
    if !field.is_nullable()
        && let Some(nulls) = &nulls
    {
        let masked = mask.map_or(0, NullBuffer::null_count);
        let combined = NullBuffer::union(mask, Some(nulls)).map_or(0, |n| n.null_count());
        if combined > masked {
            return Some(field.name().clone());
        }
    }
    if !has_required(field.data_type()) {
        return None;
    }
    let mask = NullBuffer::union(mask, nulls.as_ref());
    let mask = mask.as_ref();
    match field.data_type() {
        DataType::Struct(fields) => fields
            .iter()
            .zip(array.as_struct().columns())
            .find_map(|(field, column)| unmasked_nulls(column.as_ref(), field, mask))
            .map(path),
        DataType::List(item) => {
            let list = array.as_list::<i32>();
            let values = list.values();
            let mask = spread(list.offsets(), mask, values.len());
            unmasked_nulls(values.as_ref(), item, Some(&mask)).map(path)
        }
        DataType::LargeList(item) => {
            let list = array.as_list::<i64>();
            let values = list.values();
            let mask = spread(list.offsets(), mask, values.len());
            unmasked_nulls(values.as_ref(), item, Some(&mask)).map(path)
        }
        DataType::Map(entries, _) => {
            let map = array.as_map();
            let values = map.entries();
            let mask = spread(map.offsets(), mask, values.len());
            unmasked_nulls(values, entries, Some(&mask)).map(path)
        }
        DataType::FixedSizeList(item, size) => {
            let list = array.as_fixed_size_list();
            let values = list.values();
            let size = usize::try_from(*size).unwrap_or(0);
            let mut valid = BooleanBufferBuilder::new(values.len());
            for row in 0..list.len() {
                let start = usize::try_from(list.value_offset(row)).unwrap_or(0);
                valid.append_n(start.saturating_sub(valid.len()), false);
                valid.append_n(size, mask.is_none_or(|mask| mask.is_valid(row)));
            }
            valid.append_n(values.len().saturating_sub(valid.len()), false);
            let mask = NullBuffer::new(valid.finish());
            unmasked_nulls(values.as_ref(), item, Some(&mask)).map(path)
        }
        _ => None,
    }
}

fn spread<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    mask: Option<&NullBuffer>,
    len: usize,
) -> NullBuffer {
    let mut valid = BooleanBufferBuilder::new(len);
    valid.append_n(offsets[0].as_usize(), false);
    for (row, range) in offsets.windows(2).enumerate() {
        let valid_row = mask.is_none_or(|mask| mask.is_valid(row));
        valid.append_n(range[1].as_usize() - range[0].as_usize(), valid_row);
    }
    valid.append_n(len.saturating_sub(valid.len()), false);
    NullBuffer::new(valid.finish())
}

fn has_required(data_type: &DataType) -> bool {
    let required = |field: &FieldRef| !field.is_nullable() || has_required(field.data_type());
    match data_type {
        DataType::Struct(fields) => fields.iter().any(required),
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::FixedSizeList(item, _)
        | DataType::Map(item, _) => required(item),
        _ => false,
    }
}

pub(super) fn plain_schema(schema: &Schema) -> SchemaRef {
    let fields: Vec<FieldRef> = schema.fields().iter().map(plain_field).collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn plain_field(field: &FieldRef) -> FieldRef {
    Arc::new(Field::clone(field).with_data_type(plain_type(field.data_type())))
}

fn plain_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        DataType::List(field) => DataType::List(plain_field(field)),
        DataType::LargeList(field) => DataType::LargeList(plain_field(field)),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(plain_field(field), *size),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(plain_field).collect()),
        DataType::Map(field, sorted) => DataType::Map(plain_field(field), *sorted),
        DataType::Dictionary(key, value) => {
            DataType::Dictionary(key.clone(), Box::new(plain_type(value)))
        }
        other => other.clone(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Conversion {
    None,
    Relabel,
    Cast,
}

pub(super) fn relabel(data: ArrayData, expected: &DataType) -> ArrayData {
    let children = match expected {
        DataType::Struct(fields) => data
            .child_data()
            .iter()
            .zip(fields)
            .map(|(child, field)| relabel(child.clone(), field.data_type()))
            .collect(),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => data
            .child_data()
            .iter()
            .map(|child| relabel(child.clone(), field.data_type()))
            .collect(),
        DataType::Dictionary(_, value) => data
            .child_data()
            .iter()
            .map(|child| relabel(child.clone(), value))
            .collect(),
        _ => data.child_data().to_vec(),
    };
    unsafe {
        data.into_builder()
            .data_type(expected.clone())
            .child_data(children)
            .build_unchecked()
    }
}

pub(super) fn shape(data_type: &DataType) -> DataType {
    shape_of(data_type, true)
}

pub(super) fn shape_of(data_type: &DataType, plain_views: bool) -> DataType {
    let item = |field: &FieldRef| {
        Arc::new(Field::new(
            "item",
            shape_of(field.data_type(), plain_views),
            field.is_nullable(),
        ))
    };
    let named = |field: &FieldRef| {
        Arc::new(Field::new(
            field.name(),
            shape_of(field.data_type(), plain_views),
            field.is_nullable(),
        ))
    };
    match data_type {
        DataType::Utf8View if plain_views => DataType::Utf8,
        DataType::BinaryView if plain_views => DataType::Binary,
        DataType::List(field) => DataType::List(item(field)),
        DataType::LargeList(field) => DataType::LargeList(item(field)),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(item(field), *size),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(named).collect()),
        DataType::Map(field, sorted) => DataType::Map(named(field), *sorted),
        DataType::Dictionary(key, value) => {
            DataType::Dictionary(key.clone(), Box::new(shape_of(value, plain_views)))
        }
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {

    use arrow_array::{ArrayRef, Float64Array, Int64Array, StructArray, make_array};
    use arrow_buffer::Buffer;
    use arrow_data::ArrayData;
    use arrow_schema::{DataType, Field, Fields};

    use super::*;

    #[test]
    fn metadata_differences_are_relabelled_without_copies() {
        let plain = Field::new("x", DataType::Float64, true);
        let described = plain.clone().with_metadata(
            [("description".to_owned(), "x".to_owned())]
                .into_iter()
                .collect(),
        );
        let actual = DataType::Struct(Fields::from(vec![plain]));
        let expected = DataType::Struct(Fields::from(vec![described]));
        assert_ne!(actual, expected);
        assert_eq!(shape_of(&actual, false), shape_of(&expected, false));
        let values = Float64Array::from(vec![1.0, 2.0]);
        let data = StructArray::new(
            match &actual {
                DataType::Struct(fields) => fields.clone(),
                _ => unreachable!(),
            },
            vec![Arc::new(values) as ArrayRef],
            None,
        )
        .into_data();
        let pointer = data.child_data()[0].buffers()[0].as_ptr();
        let relabelled = relabel(data, &expected);
        assert_eq!(relabelled.data_type(), &expected);
        assert_eq!(relabelled.child_data()[0].buffers()[0].as_ptr(), pointer);
        let views = DataType::Utf8View;
        assert_ne!(shape_of(&views, false), shape_of(&DataType::Utf8, false));
    }

    fn pose(x: Vec<Option<f64>>, valid: Option<Vec<bool>>) -> (StructArray, Field) {
        let fields = Fields::from(vec![Field::new("x", DataType::Float64, false)]);
        let x: ArrayRef = Arc::new(Float64Array::from(x));
        let nulls = valid.map(NullBuffer::from);
        let array = unsafe { StructArray::new_unchecked(fields.clone(), vec![x], nulls) };
        (array, Field::new("pose", DataType::Struct(fields), true))
    }

    #[test]
    fn a_null_in_a_required_struct_child_is_found() {
        let (array, field) = pose(vec![Some(1.0), None], None);
        assert_eq!(
            unmasked_nulls(&array, &field, None),
            Some("pose.x".to_owned())
        );
    }

    #[test]
    fn a_null_under_a_null_parent_is_allowed() {
        let (array, field) = pose(vec![Some(1.0), None], Some(vec![true, false]));
        assert_eq!(unmasked_nulls(&array, &field, None), None);
    }

    fn items(valid: Option<Vec<bool>>) -> (ArrayRef, Field) {
        let item = Arc::new(Field::new("item", DataType::Int64, false));
        let values = Int64Array::from(vec![Some(1), Some(2), None]);
        let mut data = ArrayData::builder(DataType::List(item.clone()))
            .len(2)
            .add_buffer(Buffer::from_slice_ref([0i32, 2, 3]))
            .add_child_data(values.into_data());
        if let Some(valid) = valid {
            data = data.nulls(Some(NullBuffer::from(valid)));
        }
        let array = make_array(unsafe { data.build_unchecked() });
        (array, Field::new("list", DataType::List(item), true))
    }

    #[test]
    fn a_null_item_in_a_valid_list_is_found() {
        let (array, field) = items(None);
        assert_eq!(
            unmasked_nulls(array.as_ref(), &field, None),
            Some("list.item".to_owned())
        );
        assert_eq!(
            unmasked_nulls(array.slice(0, 1).as_ref(), &field, None),
            None
        );
    }

    #[test]
    fn a_null_item_in_a_null_list_is_allowed() {
        let (array, field) = items(Some(vec![true, false]));
        assert_eq!(unmasked_nulls(array.as_ref(), &field, None), None);
    }

    #[test]
    fn a_required_top_level_column_with_nulls_is_found() {
        let array = Int64Array::from(vec![Some(1), None]);
        let field = Field::new("v", DataType::Int64, false);
        assert_eq!(unmasked_nulls(&array, &field, None), Some("v".to_owned()));
        let field = Field::new("v", DataType::Int64, true);
        assert_eq!(unmasked_nulls(&array, &field, None), None);
    }
}
