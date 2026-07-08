pub(crate) const REDACTED: &str = "[redacted]";

pub(crate) fn redacted() -> String {
    REDACTED.to_owned()
}

pub(crate) fn redact_exact(value: impl AsRef<str>, secret: Option<&str>) -> String {
    let value = value.as_ref();
    match secret {
        Some(secret) if !secret.is_empty() => value.replace(secret, REDACTED),
        _ => value.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_redaction_removes_secret_value() {
        assert_eq!(
            redact_exact("postgres://user:secret-value@host/db", Some("secret-value")),
            "postgres://user:[redacted]@host/db"
        );
    }
}
