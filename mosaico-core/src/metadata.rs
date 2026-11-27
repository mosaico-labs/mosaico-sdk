use std::string::FromUtf8Error;
use std::sync::OnceLock;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::error::{Error, ErrorKind, Result};

mod finite;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Map<String, Value>")]
pub struct UserMetadata(Map<String, Value>);

impl UserMetadata {
    pub fn new() -> Self {
        Self(Map::new())
    }

    pub fn from_serialize<T: Serialize + ?Sized>(value: &T) -> Result<Self> {
        let json = serde_json::to_value(value).map_err(|e| {
            Error::invalid_argument(format!("user metadata cannot be converted to JSON: {e}"))
        })?;
        let metadata = Self::try_from(json)?;
        check_finite(value, "")?;
        Ok(metadata)
    }

    pub fn from_json(text: &str) -> Result<Self> {
        let value: Value = serde_json::from_str(text).map_err(|e| {
            Error::invalid_argument(format!("user metadata is not valid JSON: {e}"))
        })?;
        Self::try_from(value)
    }

    pub fn insert(
        &mut self,
        key: impl Into<String>,
        value: impl Serialize,
    ) -> Result<Option<Value>> {
        let key = key.into();
        let json = serde_json::to_value(&value).map_err(|e| {
            Error::invalid_argument(format!(
                "user metadata '{key}' cannot be converted to JSON: {e}"
            ))
        })?;
        if !valid_key(&key) {
            return Err(invalid_key(&key));
        }
        check_value(&json, &key)?;
        check_finite(&value, &key)?;
        Ok(self.0.insert(key, json))
    }

    pub fn remove(&mut self, key: &str) -> Option<Value> {
        self.0.remove(key)
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_map(&self) -> &Map<String, Value> {
        &self.0
    }

    pub fn into_map(self) -> Map<String, Value> {
        self.0
    }

    pub fn deserialize_as<T: DeserializeOwned>(&self) -> Result<T> {
        T::deserialize(&self.0)
            .map_err(|e| Error::schema(format!("user metadata does not match the type: {e}")))
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(&self.0).unwrap_or_default()
    }

    pub(crate) fn to_wire(&self) -> Vec<u8> {
        serde_json::to_vec(&self.0).unwrap_or_default()
    }
}

impl TryFrom<Map<String, Value>> for UserMetadata {
    type Error = Error;

    fn try_from(map: Map<String, Value>) -> Result<Self> {
        check_keys(&map, "")?;
        Ok(Self(map))
    }
}

impl TryFrom<Value> for UserMetadata {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Object(map) => Self::try_from(map),
            other => Err(Error::invalid_argument(format!(
                "user metadata must be a JSON object, not {}",
                kind(&other)
            ))),
        }
    }
}

impl std::str::FromStr for UserMetadata {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self> {
        Self::from_json(text)
    }
}

impl From<UserMetadata> for Map<String, Value> {
    fn from(metadata: UserMetadata) -> Self {
        metadata.0
    }
}

impl From<UserMetadata> for Value {
    fn from(metadata: UserMetadata) -> Self {
        Value::Object(metadata.0)
    }
}

fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && !key.contains("--")
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b" _-".contains(&b))
}

fn check_keys(map: &Map<String, Value>, path: &str) -> Result<()> {
    if let Some(key) = map.keys().find(|k| !valid_key(k)) {
        return Err(invalid_key(&join(path, key)));
    }
    map.iter()
        .try_for_each(|(key, value)| check_value(value, &join(path, key)))
}

fn check_value(value: &Value, path: &str) -> Result<()> {
    match value {
        Value::Object(map) => check_keys(map, path),
        Value::Array(items) => items
            .iter()
            .enumerate()
            .try_for_each(|(i, item)| check_value(item, &format!("{path}[{i}]"))),
        _ => Ok(()),
    }
}

fn check_finite<T: Serialize + ?Sized>(value: &T, path: &str) -> Result<()> {
    match finite::non_finite(value, path) {
        Some(path) => Err(Error::invalid_argument(format!(
            "user metadata '{path}' is NaN or infinite, which JSON cannot hold"
        ))),
        None => Ok(()),
    }
}

fn join(path: &str, key: &str) -> String {
    if path.is_empty() {
        key.to_owned()
    } else {
        format!("{path}.{key}")
    }
}

fn invalid_key(path: &str) -> Error {
    Error::invalid_argument(format!(
        "invalid user metadata key '{path}': a key must be non-empty, have no '--' and use only ASCII letters, digits, spaces, '_' and '-'"
    ))
}

fn kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

type Parsed = std::result::Result<Option<UserMetadata>, String>;

#[derive(Debug, Default)]
pub(crate) struct StoredMetadata {
    json: Option<String>,
    parsed: OnceLock<Parsed>,
}

impl StoredMetadata {
    pub(crate) fn from_wire(bytes: Vec<u8>) -> std::result::Result<Self, FromUtf8Error> {
        let json = if bytes.is_empty() {
            None
        } else {
            Some(String::from_utf8(bytes)?)
        };
        Ok(Self {
            json,
            parsed: OnceLock::new(),
        })
    }

    pub(crate) fn json(&self) -> Option<&str> {
        self.json.as_deref()
    }

    pub(crate) fn get(&self, owner: impl FnOnce() -> String) -> Result<Option<&UserMetadata>> {
        self.parsed
            .get_or_init(|| self.json.as_deref().map_or(Ok(None), parse_stored))
            .as_ref()
            .map(Option::as_ref)
            .map_err(|msg| {
                Error::new(
                    ErrorKind::Protocol,
                    format!("{}: user metadata {msg}", owner()),
                )
            })
    }
}

fn parse_stored(text: &str) -> Parsed {
    match serde_json::from_str::<Value>(text) {
        Ok(Value::Object(map)) => Ok(Some(UserMetadata(map))),
        Ok(Value::Null) => Ok(None),
        Ok(other) => Err(format!(
            "is {}, not a JSON object; user_metadata_json() returns it as stored",
            kind(&other)
        )),
        Err(e) => Err(format!("is not valid JSON: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Rig {
        rate: u32,
        name: String,
        gains: Vec<f64>,
    }

    fn refused(text: &str) -> Error {
        UserMetadata::from_json(text).unwrap_err()
    }

    #[test]
    fn valid_keys_accepted() {
        for key in ["valid-key 123", " valid-key ", "a_b", "-", "A9"] {
            assert!(valid_key(key), "{key}");
        }
        let metadata = UserMetadata::from_json(r#"{"valid-key 123": {" valid-key ": 1}}"#);
        assert!(metadata.is_ok());
    }

    #[test]
    fn invalid_keys_refused() {
        for key in ["", "a--b", "a.b", "é", "a*", "a/b"] {
            assert!(!valid_key(key), "{key}");
            let error = UserMetadata::from_serialize(&json!({ key: 1 })).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidArgument);
            assert!(error.to_string().contains(&format!("'{key}'")), "{error}");
        }
    }

    #[test]
    fn nested_key_refused() {
        let error = refused(r#"{"a": {"b": {"c.d": 1}}}"#);
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        assert!(error.to_string().contains("'a.b.c.d'"), "{error}");
    }

    #[test]
    fn array_key_refused() {
        let error = refused(r#"{"x": [{"ok": 1}, {"a--b": 1}]}"#);
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        assert!(error.to_string().contains("'x[1].a--b'"), "{error}");
        let error = refused(r#"{"x": [[{"a--b": 1}]]}"#);
        assert!(error.to_string().contains("'x[0][0].a--b'"), "{error}");
    }

    #[test]
    fn non_object_refused() {
        for text in ["[1, 2]", "1", "\"a\"", "null", "true"] {
            let error = refused(text);
            assert_eq!(error.kind(), ErrorKind::InvalidArgument);
            assert!(error.to_string().contains("JSON object"), "{error}");
        }
    }

    #[test]
    fn bad_json_refused() {
        let error = refused("{\"a\": ");
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        assert!(error.to_string().contains("not valid JSON"), "{error}");
    }

    #[test]
    fn insert_checks_key() {
        let mut metadata = UserMetadata::new();
        assert_eq!(metadata.insert("rate", 100).unwrap(), None);
        assert_eq!(metadata.insert("rate", 200).unwrap(), Some(json!(100)));
        let error = metadata.insert("a.b", 1).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        let error = metadata.insert("nested", json!({"x--y": 1})).unwrap_err();
        assert!(error.to_string().contains("'nested.x--y'"), "{error}");
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata.get("rate"), Some(&json!(200)));
        assert_eq!(metadata.remove("rate"), Some(json!(200)));
        assert!(metadata.is_empty());
    }

    #[test]
    fn serde_checks_keys() {
        let ok: UserMetadata = serde_json::from_str(r#"{"a": {"b": 1}}"#).unwrap();
        assert_eq!(serde_json::to_string(&ok).unwrap(), r#"{"a":{"b":1}}"#);
        let bad = serde_json::from_str::<UserMetadata>(r#"{"a": {"b.c": 1}}"#).unwrap_err();
        assert!(bad.to_string().contains("'a.b.c'"), "{bad}");
    }

    #[test]
    fn typed_roundtrip() {
        let rig = Rig {
            rate: 1000,
            name: "imu".to_owned(),
            gains: vec![0.1, 2.5],
        };
        let metadata = UserMetadata::from_serialize(&rig).unwrap();
        assert_eq!(metadata.get("rate"), Some(&json!(1000)));
        assert_eq!(metadata.deserialize_as::<Rig>().unwrap(), rig);
        let reread = UserMetadata::from_json(&metadata.to_json()).unwrap();
        assert_eq!(reread, metadata);
    }

    #[derive(Serialize)]
    enum Mode {
        Gain(f64),
        Pair(u8, f32),
        Named { scale: f64 },
    }

    #[test]
    fn nan_refused() {
        let rig = Rig {
            rate: 1,
            name: "imu".to_owned(),
            gains: vec![0.5, f64::NAN],
        };
        let error = UserMetadata::from_serialize(&rig).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        assert!(error.to_string().contains("'gains[1]'"), "{error}");
        let nested = json!({"a": {"b": [1, {"c": 2}]}});
        assert!(UserMetadata::from_serialize(&nested).is_ok());
        let map = std::collections::BTreeMap::from([("k", [f32::NAN])]);
        let error = UserMetadata::from_serialize(&map).unwrap_err();
        assert!(error.to_string().contains("'k[0]'"), "{error}");
    }

    #[test]
    fn infinity_refused() {
        let mut metadata = UserMetadata::new();
        let error = metadata.insert("gain", f64::INFINITY).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        assert!(error.to_string().contains("'gain'"), "{error}");
        let error = metadata
            .insert("v", vec![Some(1.0), Some(f64::NEG_INFINITY)])
            .unwrap_err();
        assert!(error.to_string().contains("'v[1]'"), "{error}");
        assert_eq!(metadata.insert("none", None::<f64>).unwrap(), None);
        assert_eq!(metadata.get("none"), Some(&Value::Null));
        assert_eq!(metadata.len(), 1);
    }

    #[test]
    fn variant_paths() {
        let cases = [
            (Mode::Gain(f64::NAN), "'m.Gain'"),
            (Mode::Pair(1, f32::INFINITY), "'m.Pair[1]'"),
            (Mode::Named { scale: f64::NAN }, "'m.Named.scale'"),
        ];
        for (mode, path) in cases {
            let error = UserMetadata::new().insert("m", mode).unwrap_err();
            assert!(error.to_string().contains(path), "{error}");
        }
    }

    #[test]
    fn typed_mismatch_schema() {
        let metadata = UserMetadata::from_json(r#"{"rate": "fast"}"#).unwrap();
        let error = metadata.deserialize_as::<Rig>().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Schema);
    }

    #[test]
    fn empty_sends_braces() {
        assert_eq!(UserMetadata::new().to_wire(), b"{}");
        assert_eq!(UserMetadata::default().to_json(), "{}");
    }

    #[test]
    fn stored_empty_none() {
        let stored = StoredMetadata::from_wire(Vec::new()).unwrap();
        assert_eq!(stored.json(), None);
        assert_eq!(stored.get(|| "seq".to_owned()).unwrap(), None);
    }

    #[test]
    fn stored_null_none() {
        let stored = StoredMetadata::from_wire(b"null".to_vec()).unwrap();
        assert_eq!(stored.json(), Some("null"));
        assert_eq!(stored.get(|| "seq".to_owned()).unwrap(), None);
    }

    #[test]
    fn stored_object_parsed() {
        let stored = StoredMetadata::from_wire(br#"{"a.b": [1]}"#.to_vec()).unwrap();
        let metadata = stored.get(|| "seq".to_owned()).unwrap().unwrap();
        assert_eq!(metadata.get("a.b"), Some(&json!([1])));
    }

    #[test]
    fn stored_array_kept() {
        let stored = StoredMetadata::from_wire(b"[1,2]".to_vec()).unwrap();
        let error = stored.get(|| "topic 'seq/a'".to_owned()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Protocol);
        assert!(error.to_string().starts_with("topic 'seq/a': "), "{error}");
        assert!(error.to_string().contains("an array"), "{error}");
        assert_eq!(
            stored.get(|| "x".to_owned()).unwrap_err().kind(),
            ErrorKind::Protocol
        );
        assert_eq!(stored.json(), Some("[1,2]"));
    }

    #[test]
    fn stored_bad_json() {
        let stored = StoredMetadata::from_wire(b"{".to_vec()).unwrap();
        let error = stored.get(|| "seq".to_owned()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Protocol);
        assert!(error.to_string().contains("not valid JSON"), "{error}");
    }

    #[test]
    fn stored_bad_utf8() {
        assert!(StoredMetadata::from_wire(vec![b'"', 0xff, b'"']).is_err());
    }
}
