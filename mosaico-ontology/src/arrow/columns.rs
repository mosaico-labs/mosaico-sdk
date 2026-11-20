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
