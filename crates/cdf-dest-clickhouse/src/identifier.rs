use std::fmt;

use cdf_kernel::{CdfError, IdentifierRules, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub(crate) struct ClickHouseIdentifier(String);

impl ClickHouseIdentifier {
    pub(crate) fn user(value: impl Into<String>) -> Result<Self> {
        let identifier = Self::new(value)?;
        if identifier.as_str().starts_with("_cdf_") {
            return Err(CdfError::contract(
                "ClickHouse user identifiers cannot use the reserved _cdf_ prefix",
            ));
        }
        Ok(identifier)
    }

    pub(crate) fn framework(value: impl Into<String>) -> Result<Self> {
        let identifier = Self::new(value)?;
        if !identifier.as_str().starts_with("_cdf_") {
            return Err(CdfError::internal(
                "ClickHouse framework identifiers must use the reserved _cdf_ prefix",
            ));
        }
        Ok(identifier)
    }

    pub(crate) fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() || value.len() > 255 || value.chars().any(char::is_control) {
            return Err(CdfError::contract(
                "ClickHouse identifiers must contain 1..=255 control-free UTF-8 bytes",
            ));
        }
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn quoted(&self) -> String {
        let escaped = self.0.replace('\\', "\\\\").replace('`', "\\`");
        format!("`{escaped}`")
    }
}

impl TryFrom<String> for ClickHouseIdentifier {
    type Error = CdfError;

    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}

impl From<ClickHouseIdentifier> for String {
    fn from(value: ClickHouseIdentifier) -> Self {
        value.0
    }
}

impl fmt::Display for ClickHouseIdentifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

pub(crate) fn identifier_rules() -> IdentifierRules {
    IdentifierRules {
        normalizer: "namecase-v1".to_owned(),
        max_length: Some(255),
        allowed_pattern: Some("^[a-z_][a-z0-9_]*$".to_owned()),
    }
}

pub(crate) fn qualified(database: &ClickHouseIdentifier, table: &ClickHouseIdentifier) -> String {
    format!("{}.{}", database.quoted(), table.quoted())
}

pub(crate) fn string_literal(value: &str) -> Result<String> {
    if value.chars().any(char::is_control) {
        return Err(CdfError::contract(
            "ClickHouse SQL literals must not contain control characters",
        ));
    }
    Ok(format!(
        "'{}'",
        value.replace('\\', "\\\\").replace('\'', "\\'")
    ))
}
