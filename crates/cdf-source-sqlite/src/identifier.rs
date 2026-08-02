use cdf_kernel::{CdfError, Result};
use serde::{Deserialize, Serialize};

/// A validated SQLite identifier. Values are always quoted before entering SQL.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct SqliteIdentifier(String);

impl SqliteIdentifier {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() || value.len() > 255 || value.chars().any(char::is_control) {
            return Err(CdfError::contract(
                "SQLite identifiers must be nonempty, at most 255 bytes, and control-free",
            ));
        }
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn quoted(&self) -> String {
        format!("\"{}\"", self.0.replace('"', "\"\""))
    }
}

impl std::fmt::Display for SqliteIdentifier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_and_quotes_identifiers_without_interpreting_sql() {
        let identifier = SqliteIdentifier::new("order\"detail").unwrap();
        assert_eq!(identifier.quoted(), "\"order\"\"detail\"");
        assert!(SqliteIdentifier::new("").is_err());
        assert!(SqliteIdentifier::new("bad\nname").is_err());
    }
}
