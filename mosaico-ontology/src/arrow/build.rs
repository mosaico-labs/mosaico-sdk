use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::{ArrayRef, StructArray};
use arrow_buffer::NullBufferBuilder;
use arrow_schema::{DataType, Field, Fields};

pub(crate) trait StructBuilder<T>: Default + Send + 'static {
    fn fields() -> Fields;
    fn append(&mut self, value: Option<&T>);
    fn finish(&mut self) -> Vec<ArrayRef>;
}

pub(crate) fn struct_field<T, B: StructBuilder<T>>(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Struct(B::fields()), nullable)
}

pub(crate) struct Nested<T, B> {
    builder: B,
    valid: NullBufferBuilder,
    value: PhantomData<fn(&T)>,
}

impl<T, B: StructBuilder<T>> Default for Nested<T, B> {
    fn default() -> Self {
        Self {
            builder: B::default(),
            valid: NullBufferBuilder::new(0),
            value: PhantomData,
        }
    }
}

impl<T, B: StructBuilder<T>> Nested<T, B> {
    pub(crate) fn append(&mut self, value: Option<&T>) {
        self.valid.append(value.is_some());
        self.builder.append(value);
    }

    pub(crate) fn finish(&mut self) -> ArrayRef {
        let columns = self.builder.finish();
        let nulls = self.valid.finish();
        Arc::new(StructArray::new(B::fields(), columns, nulls))
    }
}
