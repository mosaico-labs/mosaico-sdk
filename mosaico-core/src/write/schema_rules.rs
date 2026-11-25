use std::collections::HashSet;

use arrow_schema::{DataType, Fields, Schema};

use crate::error::{Error, Result};
use crate::write::topic::TIMESTAMP;

pub(crate) fn check_schema(schema: &Schema) -> Result<()> {
    match schema
        .field_with_name(TIMESTAMP)
        .map(|field| field.data_type())
    {
        Ok(DataType::Int64) => {}
        _ => {
            return Err(Error::schema(format!(
                "schema must have a '{TIMESTAMP}' column of type Int64"
            )));
        }
    }
    unique_names(schema.fields(), "")?;
    match schema
        .flattened_fields()
        .into_iter()
        .find(|field| !valid_name(field.name()))
    {
        Some(field) => Err(Error::schema(format!(
            "invalid field name '{}': mosaicod accepts only letters, digits and '_'",
            field.name()
        ))),
        None => Ok(()),
    }
}

fn unique_names(fields: &Fields, parent: &str) -> Result<()> {
    let mut seen = HashSet::with_capacity(fields.len());
    for field in fields {
        let path = if parent.is_empty() {
            field.name().clone()
        } else {
            format!("{parent}.{}", field.name())
        };
        if !seen.insert(field.name()) {
            return Err(Error::schema(format!("duplicate field '{path}'")));
        }
        let mut data_type = field.data_type();
        while let DataType::List(inner)
        | DataType::LargeList(inner)
        | DataType::ListView(inner)
        | DataType::LargeListView(inner)
        | DataType::FixedSizeList(inner, _) = data_type
        {
            data_type = inner.data_type();
        }
        if let DataType::Struct(children) = data_type {
            unique_names(children, &path)?;
        }
    }
    Ok(())
}

fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;

    use super::*;
    use crate::error::ErrorKind;

    fn schema(fields: Vec<Field>) -> Schema {
        let mut all = vec![Field::new(TIMESTAMP, DataType::Int64, false)];
        all.extend(fields);
        Schema::new(all)
    }

    fn rejected(schema: &Schema) -> String {
        let error = check_schema(schema).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Schema);
        error.to_string()
    }

    fn float(name: &str) -> Field {
        Field::new(name, DataType::Float64, false)
    }

    fn structure(name: &str, fields: Vec<Field>) -> Field {
        Field::new(name, DataType::Struct(fields.into()), false)
    }

    #[test]
    fn accepts_nested_schema() {
        let point = structure("position", vec![float("x"), float("y"), float("z_2")]);
        let list = Field::new_list(
            "points",
            Field::new_list_field(point.data_type().clone(), false),
            false,
        );
        check_schema(&schema(vec![point, list, float("Accel_X")])).unwrap();
    }

    #[test]
    fn timestamp_must_be_int64() {
        let missing = Schema::new(vec![float("x")]);
        assert!(rejected(&missing).contains("'timestamp_ns' column of type Int64"));
        let wrong = Schema::new(vec![Field::new(TIMESTAMP, DataType::Int32, false)]);
        assert!(rejected(&wrong).contains("'timestamp_ns' column of type Int64"));
    }

    #[test]
    fn rejects_duplicate_names() {
        let top = schema(vec![Field::new(TIMESTAMP, DataType::Int64, false)]);
        assert_eq!(rejected(&top), "duplicate field 'timestamp_ns'");
        let nested = schema(vec![structure("pose", vec![float("x"), float("x")])]);
        assert_eq!(rejected(&nested), "duplicate field 'pose.x'");
        let item = structure("item", vec![float("a"), float("a")]);
        let listed = schema(vec![Field::new_list("points", Arc::new(item), false)]);
        assert_eq!(rejected(&listed), "duplicate field 'points.a'");
    }

    #[test]
    fn rejects_invalid_field_names() {
        for name in ["linear-velocity", "accel.x", "Accel X", "velocità", ""] {
            let message = rejected(&schema(vec![float(name)]));
            assert!(
                message.starts_with(&format!("invalid field name '{name}'")),
                "{message}"
            );
        }
        let nested = schema(vec![structure("imu", vec![float("angular-velocity")])]);
        assert!(rejected(&nested).starts_with("invalid field name 'angular-velocity'"));
    }
}
