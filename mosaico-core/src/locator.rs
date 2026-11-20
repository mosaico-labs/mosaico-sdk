use crate::error::{Error, ErrorKind, Result};

pub(crate) fn topic_name(raw: &str) -> String {
    format!("/{}", raw.trim_matches('/'))
}

pub(crate) fn sequence_name(raw: &str) -> Result<&str> {
    let name = raw.trim_matches('/');
    if name.contains('/') {
        return Err(Error::new(
            ErrorKind::InvalidArgument,
            format!("invalid sequence name '{raw}': '/' is not allowed"),
        ));
    }
    Ok(name)
}
