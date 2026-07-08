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

pub(crate) fn redact_uri_userinfo(value: impl AsRef<str>) -> String {
    let value = value.as_ref();
    let Some(scheme_end) = value.find("://") else {
        return value.to_owned();
    };
    let authority_start = scheme_end + 3;
    let authority_end = value[authority_start..]
        .find(['/', '?', '#'])
        .map(|offset| authority_start + offset)
        .unwrap_or(value.len());
    let Some(at_offset) = value[authority_start..authority_end].find('@') else {
        return value.to_owned();
    };
    let at = authority_start + at_offset;
    format!("{}{}{}", &value[..authority_start], REDACTED, &value[at..])
}

pub(crate) fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.contains("secret")
        || key.contains("token")
        || key.contains("password")
        || key.contains("credential")
        || key.contains("authorization")
        || key.contains("api_key")
        || key.contains("apikey")
        || key.contains("connection_string")
        || key.contains("dsn")
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

    #[test]
    fn uri_userinfo_redaction_removes_secret_like_destination_credentials() {
        assert_eq!(
            redact_uri_userinfo("postgres://user:secret-value@host/db"),
            "postgres://[redacted]@host/db"
        );
        assert_eq!(
            redact_uri_userinfo("duckdb://.cdf/dev.duckdb"),
            "duckdb://.cdf/dev.duckdb"
        );
    }
}
