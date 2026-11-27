use crate::error::{Error, ErrorKind, Result};

pub fn topic_name(raw: &str) -> String {
    let parts: Vec<&str> = raw
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .collect();
    format!("/{}", parts.join("/"))
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

#[doc(hidden)]
pub fn check_sequence_name(raw: &str) -> Result<()> {
    new_sequence_name(raw).map(drop)
}

pub(crate) fn new_sequence_name(raw: &str) -> Result<&str> {
    let name = sequence_name(raw)?;
    check_new_name("sequence", raw, name, false)?;
    Ok(name)
}

pub(crate) fn new_topic_name(raw: &str) -> Result<String> {
    let name = topic_name(raw);
    check_new_name("topic", raw, &name[1..], true)?;
    Ok(name)
}

fn check_new_name(kind: &str, raw: &str, name: &str, nested: bool) -> Result<()> {
    let invalid = |reason: String| Error::new(ErrorKind::InvalidArgument, reason);
    if raw.is_empty() {
        return Err(invalid(format!("empty {kind} name")));
    }
    if name.is_empty() {
        return Err(invalid(format!(
            "invalid {kind} name '{raw}': empty after '/' removal"
        )));
    }
    if !name.starts_with(|c: char| c.is_ascii_alphanumeric()) {
        return Err(invalid(format!(
            "invalid {kind} name '{raw}': it must begin with a letter or a number"
        )));
    }
    let unsupported: Vec<char> = name
        .chars()
        .filter(|&c| !(c.is_ascii_alphanumeric() || c == '-' || c == '_' || (nested && c == '/')))
        .collect();
    if !unsupported.is_empty() {
        return Err(invalid(format!(
            "invalid {kind} name '{raw}': unsupported characters {unsupported:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_name_normalization() {
        assert_eq!(topic_name("imu"), "/imu");
        assert_eq!(topic_name("/sensors//imu/"), "/sensors/imu");
        assert_eq!(topic_name("./sensors/./imu"), "/sensors/imu");
        assert_eq!(topic_name(""), "/");
    }

    #[test]
    fn topic_name_rules() {
        assert_eq!(
            new_topic_name("/sensors//imu_1-a/").unwrap(),
            "/sensors/imu_1-a"
        );
        for bad in [
            "", "/", "//", "_imu", "-imu", "imu cam", "imu.raw", "a/../b", "càm", "a:b",
        ] {
            let error = new_topic_name(bad).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidArgument, "{bad}");
        }
    }

    #[test]
    fn sequence_name_rules() {
        assert_eq!(new_sequence_name("/run_01-a/").unwrap(), "run_01-a");
        for bad in ["", "/", "a/b", "_run", "run 1", "run.1", "rün"] {
            let error = new_sequence_name(bad).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidArgument, "{bad}");
        }
    }
}
