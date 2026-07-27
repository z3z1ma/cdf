pub const REDACTED: &str = "[redacted]";

pub fn redacted() -> String {
    REDACTED.to_owned()
}

pub fn redact_exact(value: impl AsRef<str>, secret: Option<&str>) -> String {
    let value = value.as_ref();
    match secret {
        Some(secret) if !secret.is_empty() => value.replace(secret, REDACTED),
        _ => value.to_owned(),
    }
}

pub fn redact_uri_userinfo(value: impl AsRef<str>) -> String {
    let value = value.as_ref();
    let mut redacted_value = String::with_capacity(value.len());
    let mut cursor = 0;
    while let Some(relative_scheme_end) = value[cursor..].find("://") {
        let scheme_end = cursor + relative_scheme_end;
        let authority_start = scheme_end + 3;
        let authority_end = value[authority_start..]
            .find(['/', '?', '#', ' ', '\t', '\n', '\r', '"', '\'', '<', '>'])
            .map(|offset| authority_start + offset)
            .unwrap_or(value.len());
        if let Some(at_offset) = value[authority_start..authority_end].find('@') {
            let at = authority_start + at_offset;
            redacted_value.push_str(&value[cursor..authority_start]);
            redacted_value.push_str(REDACTED);
            redacted_value.push_str(&value[at..=at]);
            cursor = at + 1;
        } else {
            redacted_value.push_str(&value[cursor..authority_end]);
            cursor = authority_end;
        }
        if cursor == value.len() {
            break;
        }
    }
    redacted_value.push_str(&value[cursor..]);
    redacted_value
}

pub fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.contains("secret")
        || key.contains("token")
        || key.contains("password")
        || key.contains("credential")
        || key.contains("authorization")
        || key.contains("api_key")
        || key.contains("apikey")
        || key.contains("private_key")
        || key.contains("private-key")
        || key.contains("private key")
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
        assert_eq!(
            redact_uri_userinfo("from postgres://alice:first@one/db to mysql://bob:second@two/db"),
            "from postgres://[redacted]@one/db to mysql://[redacted]@two/db"
        );
    }

    #[test]
    fn private_key_labels_are_sensitive() {
        for key in ["private_key", "private-key", "private key"] {
            assert!(is_sensitive_key(key));
        }
    }
}
