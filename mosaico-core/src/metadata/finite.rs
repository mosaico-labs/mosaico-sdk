use std::fmt::{self, Display, Write};

use serde::ser::{self, Serialize};
use serde_json::Value;

pub(super) fn non_finite<T: Serialize + ?Sized>(value: &T, path: &str) -> Option<String> {
    let mut checker = Checker {
        path: path.to_owned(),
    };
    match value.serialize(&mut checker) {
        Err(Found::NonFinite(path)) => Some(path),
        _ => None,
    }
}

#[derive(Debug)]
enum Found {
    NonFinite(String),
    Other,
}

impl Display for Found {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Found::NonFinite(path) => write!(f, "non-finite float at '{path}'"),
            Found::Other => f.write_str("not serializable"),
        }
    }
}

impl std::error::Error for Found {}

impl ser::Error for Found {
    fn custom<T: Display>(_: T) -> Self {
        Found::Other
    }
}

struct Checker {
    path: String,
}

impl Checker {
    fn float(&self, finite: bool) -> Result<(), Found> {
        if finite {
            Ok(())
        } else {
            Err(Found::NonFinite(self.path.clone()))
        }
    }

    fn push_key(&mut self, key: &str) {
        if !self.path.is_empty() {
            self.path.push('.');
        }
        self.path.push_str(key);
    }

    fn compound(&mut self, variant: Option<&str>) -> Compound<'_> {
        let end = self.path.len();
        if let Some(variant) = variant {
            self.push_key(variant);
        }
        Compound {
            base: self.path.len(),
            end,
            next: 0,
            checker: self,
        }
    }
}

struct Compound<'a> {
    checker: &'a mut Checker,
    base: usize,
    end: usize,
    next: usize,
}

impl Compound<'_> {
    fn element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Found> {
        self.checker.path.truncate(self.base);
        let _ = write!(self.checker.path, "[{}]", self.next);
        self.next += 1;
        value.serialize(&mut *self.checker)
    }

    fn field<T: Serialize + ?Sized>(&mut self, key: &str, value: &T) -> Result<(), Found> {
        self.checker.path.truncate(self.base);
        self.checker.push_key(key);
        value.serialize(&mut *self.checker)
    }

    fn finish(self) -> Result<(), Found> {
        self.checker.path.truncate(self.end);
        Ok(())
    }
}

impl<'a> ser::Serializer for &'a mut Checker {
    type Ok = ();
    type Error = Found;
    type SerializeSeq = Compound<'a>;
    type SerializeTuple = Compound<'a>;
    type SerializeTupleStruct = Compound<'a>;
    type SerializeTupleVariant = Compound<'a>;
    type SerializeMap = Compound<'a>;
    type SerializeStruct = Compound<'a>;
    type SerializeStructVariant = Compound<'a>;

    fn serialize_bool(self, _: bool) -> Result<(), Found> {
        Ok(())
    }

    fn serialize_i8(self, _: i8) -> Result<(), Found> {
        Ok(())
    }

    fn serialize_i16(self, _: i16) -> Result<(), Found> {
        Ok(())
    }

    fn serialize_i32(self, _: i32) -> Result<(), Found> {
        Ok(())
    }

    fn serialize_i64(self, _: i64) -> Result<(), Found> {
        Ok(())
    }

    fn serialize_i128(self, _: i128) -> Result<(), Found> {
        Ok(())
    }

    fn serialize_u8(self, _: u8) -> Result<(), Found> {
        Ok(())
    }

    fn serialize_u16(self, _: u16) -> Result<(), Found> {
        Ok(())
    }

    fn serialize_u32(self, _: u32) -> Result<(), Found> {
        Ok(())
    }

    fn serialize_u64(self, _: u64) -> Result<(), Found> {
        Ok(())
    }

    fn serialize_u128(self, _: u128) -> Result<(), Found> {
        Ok(())
    }

    fn serialize_f32(self, value: f32) -> Result<(), Found> {
        self.float(value.is_finite())
    }

    fn serialize_f64(self, value: f64) -> Result<(), Found> {
        self.float(value.is_finite())
    }

    fn serialize_char(self, _: char) -> Result<(), Found> {
        Ok(())
    }

    fn serialize_str(self, _: &str) -> Result<(), Found> {
        Ok(())
    }

    fn serialize_bytes(self, _: &[u8]) -> Result<(), Found> {
        Ok(())
    }

    fn serialize_none(self) -> Result<(), Found> {
        Ok(())
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<(), Found> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), Found> {
        Ok(())
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<(), Found> {
        Ok(())
    }

    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<(), Found> {
        Ok(())
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<(), Found> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<(), Found> {
        let mut compound = self.compound(None);
        compound.field(variant, value)?;
        compound.finish()
    }

    fn serialize_seq(self, _: Option<usize>) -> Result<Compound<'a>, Found> {
        Ok(self.compound(None))
    }

    fn serialize_tuple(self, _: usize) -> Result<Compound<'a>, Found> {
        Ok(self.compound(None))
    }

    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Compound<'a>, Found> {
        Ok(self.compound(None))
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        _: usize,
    ) -> Result<Compound<'a>, Found> {
        Ok(self.compound(Some(variant)))
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Compound<'a>, Found> {
        Ok(self.compound(None))
    }

    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Compound<'a>, Found> {
        Ok(self.compound(None))
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        _: usize,
    ) -> Result<Compound<'a>, Found> {
        Ok(self.compound(Some(variant)))
    }
}

impl ser::SerializeSeq for Compound<'_> {
    type Ok = ();
    type Error = Found;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Found> {
        self.element(value)
    }

    fn end(self) -> Result<(), Found> {
        self.finish()
    }
}

impl ser::SerializeTuple for Compound<'_> {
    type Ok = ();
    type Error = Found;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Found> {
        self.element(value)
    }

    fn end(self) -> Result<(), Found> {
        self.finish()
    }
}

impl ser::SerializeTupleStruct for Compound<'_> {
    type Ok = ();
    type Error = Found;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Found> {
        self.element(value)
    }

    fn end(self) -> Result<(), Found> {
        self.finish()
    }
}

impl ser::SerializeTupleVariant for Compound<'_> {
    type Ok = ();
    type Error = Found;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Found> {
        self.element(value)
    }

    fn end(self) -> Result<(), Found> {
        self.finish()
    }
}

impl ser::SerializeMap for Compound<'_> {
    type Ok = ();
    type Error = Found;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Found> {
        let key = match serde_json::to_value(key) {
            Ok(Value::String(key)) => key,
            Ok(other) => other.to_string(),
            Err(_) => return Err(Found::Other),
        };
        self.checker.path.truncate(self.base);
        self.checker.push_key(&key);
        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Found> {
        value.serialize(&mut *self.checker)
    }

    fn end(self) -> Result<(), Found> {
        self.finish()
    }
}

impl ser::SerializeStruct for Compound<'_> {
    type Ok = ();
    type Error = Found;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Found> {
        self.field(key, value)
    }

    fn end(self) -> Result<(), Found> {
        self.finish()
    }
}

impl ser::SerializeStructVariant for Compound<'_> {
    type Ok = ();
    type Error = Found;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Found> {
        self.field(key, value)
    }

    fn end(self) -> Result<(), Found> {
        self.finish()
    }
}
