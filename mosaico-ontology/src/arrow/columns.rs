use arrow_array::cast::AsArray;
use arrow_array::types::{ArrowPrimitiveType, Float64Type};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Float64Array, GenericListArray,
    PrimitiveArray, StringArray, StringViewArray, StructArray,
};
use arrow_buffer::NullBuffer;
use mosaico_core::{Error, Result};

fn child_path(path: &str, name: &str) -> String {
    if path.is_empty() {
        name.to_owned()
    } else {
        format!("{path}.{name}")
    }
}

fn required_column<'a>(parent: &'a StructArray, name: &str, path: &str) -> Result<&'a ArrayRef> {
    parent
        .column_by_name(name)
        .ok_or_else(|| Error::schema(format!("missing field {path}")))
}

fn wrong_type(path: &str) -> Error {
    Error::schema(format!("field {path} has an unexpected type"))
}

fn null_in(path: &str) -> Error {
    Error::schema(format!("null in required field {path}"))
}

pub(crate) struct Required<T: ArrowPrimitiveType> {
    array: PrimitiveArray<T>,
    path: String,
}

impl<T: ArrowPrimitiveType> Required<T> {
    pub(crate) fn new(parent: &StructArray, path: &str, name: &str) -> Result<Self> {
        let path = child_path(path, name);
        let array = required_column(parent, name, &path)?
            .as_primitive_opt::<T>()
            .ok_or_else(|| wrong_type(&path))?
            .clone();
        Ok(Self { array, path })
    }

    pub(crate) fn get(&self, row: usize) -> Result<T::Native> {
        if self.array.is_null(row) {
            return Err(null_in(&self.path));
        }
        Ok(self.array.value(row))
    }
}

pub(crate) struct Optional<T: ArrowPrimitiveType> {
    array: Option<PrimitiveArray<T>>,
}

impl<T: ArrowPrimitiveType> Optional<T> {
    pub(crate) fn new(parent: &StructArray, path: &str, name: &str) -> Result<Self> {
        let array = match parent.column_by_name(name) {
            None => None,
            Some(column) => Some(
                column
                    .as_primitive_opt::<T>()
                    .ok_or_else(|| wrong_type(&child_path(path, name)))?
                    .clone(),
            ),
        };
        Ok(Self { array })
    }

    pub(crate) fn get(&self, row: usize) -> Option<T::Native> {
        let array = self.array.as_ref()?;
        array.is_valid(row).then(|| array.value(row))
    }
}

enum Strings {
    Utf8(StringArray),
    Utf8View(StringViewArray),
}

impl Strings {
    fn new(column: &ArrayRef, path: &str) -> Result<Self> {
        if let Some(array) = column.as_string_opt::<i32>() {
            Ok(Self::Utf8(array.clone()))
        } else if let Some(array) = column.as_string_view_opt() {
            Ok(Self::Utf8View(array.clone()))
        } else {
            Err(wrong_type(path))
        }
    }

    fn get(&self, row: usize) -> Option<&str> {
        match self {
            Self::Utf8(array) => array.is_valid(row).then(|| array.value(row)),
            Self::Utf8View(array) => array.is_valid(row).then(|| array.value(row)),
        }
    }
}

pub(crate) struct RequiredString {
    array: Strings,
    path: String,
}

impl RequiredString {
    pub(crate) fn new(parent: &StructArray, path: &str, name: &str) -> Result<Self> {
        let path = child_path(path, name);
        let array = Strings::new(required_column(parent, name, &path)?, &path)?;
        Ok(Self { array, path })
    }

    pub(crate) fn get(&self, row: usize) -> Result<&str> {
        self.array.get(row).ok_or_else(|| null_in(&self.path))
    }
}

pub(crate) struct OptionalString {
    array: Option<Strings>,
}

impl OptionalString {
    pub(crate) fn new(parent: &StructArray, path: &str, name: &str) -> Result<Self> {
        let array = match parent.column_by_name(name) {
            None => None,
            Some(column) => Some(Strings::new(column, &child_path(path, name))?),
        };
        Ok(Self { array })
    }

    pub(crate) fn get(&self, row: usize) -> Option<&str> {
        self.array.as_ref()?.get(row)
    }
}

enum Binaries {
    Binary(BinaryArray),
    BinaryView(BinaryViewArray),
}

pub(crate) struct RequiredBinary {
    array: Binaries,
    path: String,
}

impl RequiredBinary {
    pub(crate) fn new(parent: &StructArray, path: &str, name: &str) -> Result<Self> {
        let path = child_path(path, name);
        let column = required_column(parent, name, &path)?;
        let array = if let Some(array) = column.as_binary_opt::<i32>() {
            Binaries::Binary(array.clone())
        } else if let Some(array) = column.as_binary_view_opt() {
            Binaries::BinaryView(array.clone())
        } else {
            return Err(wrong_type(&path));
        };
        Ok(Self { array, path })
    }

    pub(crate) fn get(&self, row: usize) -> Result<&[u8]> {
        let value = match &self.array {
            Binaries::Binary(array) => array.is_valid(row).then(|| array.value(row)),
            Binaries::BinaryView(array) => array.is_valid(row).then(|| array.value(row)),
        };
        value.ok_or_else(|| null_in(&self.path))
    }
}

pub(crate) struct OptionalBool {
    array: Option<BooleanArray>,
}

impl OptionalBool {
    pub(crate) fn new(parent: &StructArray, path: &str, name: &str) -> Result<Self> {
        let array = match parent.column_by_name(name) {
            None => None,
            Some(column) => Some(
                column
                    .as_boolean_opt()
                    .ok_or_else(|| wrong_type(&child_path(path, name)))?
                    .clone(),
            ),
        };
        Ok(Self { array })
    }

    pub(crate) fn get(&self, row: usize) -> Option<bool> {
        let array = self.array.as_ref()?;
        array.is_valid(row).then(|| array.value(row))
    }
}

pub(crate) struct OptionalF64List {
    list: Option<(GenericListArray<i32>, Float64Array)>,
    path: String,
}

impl OptionalF64List {
    pub(crate) fn new(parent: &StructArray, path: &str, name: &str) -> Result<Self> {
        let path = child_path(path, name);
        let list = match parent.column_by_name(name) {
            None => None,
            Some(column) => {
                let list = column
                    .as_list_opt::<i32>()
                    .ok_or_else(|| wrong_type(&path))?;
                let values = list
                    .values()
                    .as_primitive_opt::<Float64Type>()
                    .ok_or_else(|| wrong_type(&path))?;
                Some((list.clone(), values.clone()))
            }
        };
        Ok(Self { list, path })
    }

    pub(crate) fn get(&self, row: usize) -> Result<Option<&[f64]>> {
        let Some((list, values)) = self.list.as_ref() else {
            return Ok(None);
        };
        if list.is_null(row) {
            return Ok(None);
        }
        let offsets = list.value_offsets();
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        if values
            .nulls()
            .is_some_and(|n| n.slice(start, end - start).null_count() > 0)
        {
            return Err(Error::schema(format!("null item in field {}", self.path)));
        }
        Ok(Some(&values.values()[start..end]))
    }
}

pub(crate) struct RequiredStruct<C> {
    nulls: Option<NullBuffer>,
    columns: C,
    path: String,
}

impl<C> RequiredStruct<C> {
    pub(crate) fn new(
        parent: &StructArray,
        path: &str,
        name: &str,
        columns: impl FnOnce(&StructArray, &str) -> Result<C>,
    ) -> Result<Self> {
        let path = child_path(path, name);
        let array = required_column(parent, name, &path)?
            .as_struct_opt()
            .ok_or_else(|| wrong_type(&path))?;
        Ok(Self {
            nulls: array.nulls().cloned(),
            columns: columns(array, &path)?,
            path,
        })
    }

    pub(crate) fn get(&self, row: usize) -> Result<&C> {
        if self.nulls.as_ref().is_some_and(|n| n.is_null(row)) {
            return Err(null_in(&self.path));
        }
        Ok(&self.columns)
    }
}

pub(crate) struct OptionalStruct<C> {
    inner: Option<(Option<NullBuffer>, C)>,
}

impl<C> OptionalStruct<C> {
    pub(crate) fn new(
        parent: &StructArray,
        path: &str,
        name: &str,
        columns: impl FnOnce(&StructArray, &str) -> Result<C>,
    ) -> Result<Self> {
        let inner = match parent.column_by_name(name) {
            None => None,
            Some(column) => {
                let path = child_path(path, name);
                let array = column.as_struct_opt().ok_or_else(|| wrong_type(&path))?;
                Some((array.nulls().cloned(), columns(array, &path)?))
            }
        };
        Ok(Self { inner })
    }

    pub(crate) fn get(&self, row: usize) -> Option<&C> {
        let (nulls, columns) = self.inner.as_ref()?;
        if nulls.as_ref().is_some_and(|n| n.is_null(row)) {
            return None;
        }
        Some(columns)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, ListArray};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use arrow_schema::{DataType, Field};
    use mosaico_core::ErrorKind;

    use super::*;

    fn parent(columns: Vec<(&str, ArrayRef)>) -> StructArray {
        let (fields, arrays): (Vec<_>, Vec<_>) = columns
            .into_iter()
            .map(|(name, array)| {
                (
                    Arc::new(Field::new(name, array.data_type().clone(), true)),
                    array,
                )
            })
            .unzip();
        StructArray::new(fields.into(), arrays, None)
    }

    fn schema_error<T>(result: Result<T>, text: &str) {
        let Err(error) = result else {
            panic!("expected an error with {text}");
        };
        assert_eq!(error.kind(), ErrorKind::Schema);
        assert!(error.to_string().contains(text), "{error}");
    }

    #[test]
    fn required_null_and_missing() {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.0), None]));
        let a = parent(vec![("x", values)]);
        let x = Required::<Float64Type>::new(&a, "acceleration", "x").unwrap();
        assert_eq!(x.get(0).unwrap(), 1.0);
        schema_error(x.get(1), "null in required field acceleration.x");
        schema_error(
            Required::<Float64Type>::new(&a, "acceleration", "y"),
            "missing field acceleration.y",
        );
    }

    #[test]
    fn wrong_column_type() {
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let a = parent(vec![("x", ints.clone()), ("s", ints)]);
        schema_error(
            Required::<Float64Type>::new(&a, "a", "x"),
            "field a.x has an unexpected type",
        );
        schema_error(Optional::<Float64Type>::new(&a, "a", "x"), "a.x");
        schema_error(RequiredString::new(&a, "", "s"), "field s");
        schema_error(OptionalBool::new(&a, "", "x"), "field x");
    }

    #[test]
    fn strings_and_views() {
        let utf8: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
        let view: ArrayRef = Arc::new(StringViewArray::from(vec![Some("b"), None]));
        let a = parent(vec![("u", utf8), ("v", view)]);
        for (name, first) in [("u", "a"), ("v", "b")] {
            let required = RequiredString::new(&a, "h", name).unwrap();
            assert_eq!(required.get(0).unwrap(), first);
            schema_error(required.get(1), "null in required field h.");
            let optional = OptionalString::new(&a, "h", name).unwrap();
            assert_eq!(optional.get(1), None);
        }
        assert_eq!(OptionalString::new(&a, "h", "w").unwrap().get(0), None);
    }

    #[test]
    fn list_null_item() {
        let values = Float64Array::from(vec![Some(1.0), Some(2.0), None, Some(4.0)]);
        let field = Arc::new(Field::new("item", DataType::Float64, true));
        let list = ListArray::new(
            field,
            OffsetBuffer::new(vec![0, 2, 4, 4].into()),
            Arc::new(values),
            Some(NullBuffer::from(vec![true, true, false])),
        );
        let a = parent(vec![("covariance", Arc::new(list.clone()))]);
        let covariance = OptionalF64List::new(&a, "acceleration", "covariance").unwrap();
        assert_eq!(covariance.get(0).unwrap(), Some(&[1.0, 2.0][..]));
        schema_error(
            covariance.get(1),
            "null item in field acceleration.covariance",
        );
        assert_eq!(covariance.get(2).unwrap(), None);
        let sliced = parent(vec![("covariance", Arc::new(list.slice(1, 2)))]);
        let covariance = OptionalF64List::new(&sliced, "", "covariance").unwrap();
        schema_error(covariance.get(0), "null item");
        assert_eq!(covariance.get(1).unwrap(), None);
    }

    #[test]
    fn struct_nulls() {
        let inner = StructArray::new(
            vec![Arc::new(Field::new("x", DataType::Float64, true))].into(),
            vec![Arc::new(Float64Array::from(vec![1.0, 2.0]))],
            Some(NullBuffer::from(vec![true, false])),
        );
        let a = parent(vec![("orientation", Arc::new(inner))]);
        let required = RequiredStruct::new(&a, "", "orientation", |_, _| Ok(())).unwrap();
        assert!(required.get(0).is_ok());
        schema_error(required.get(1), "null in required field orientation");
        let optional = OptionalStruct::new(&a, "", "orientation", |_, _| Ok(())).unwrap();
        assert!(optional.get(0).is_some());
        assert!(optional.get(1).is_none());
        let missing = OptionalStruct::new(&a, "", "header", |_, _| Ok(())).unwrap();
        assert!(missing.get(0).is_none());
        schema_error(
            RequiredStruct::new(&a, "", "acceleration", |_, _| Ok(())),
            "missing field acceleration",
        );
    }
}
