use std::fmt;

use cdf_kernel::{CdfError, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct MongoDbIdentifier(String);

impl MongoDbIdentifier {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() || value.len() > 255 {
            return Err(CdfError::contract(
                "MongoDB database and collection names must contain 1..=255 bytes",
            ));
        }
        if value.as_bytes().contains(&0)
            || value.contains('/')
            || value.contains('\\')
            || value.contains('"')
            || value.starts_with("system.")
        {
            return Err(CdfError::contract(
                "MongoDB database and collection names must not contain NUL, slash, backslash, quote, or the reserved `system.` prefix",
            ));
        }
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MongoDbIdentifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

pub(crate) fn validate_field_path(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 1_024
        || value.starts_with('$')
        || value.ends_with('.')
        || value.split('.').any(|part| {
            part.is_empty()
                || part.starts_with('$')
                || part.as_bytes().contains(&0)
                || part.contains('/')
                || part.contains('\\')
        })
    {
        return Err(CdfError::contract(format!(
            "MongoDB field path `{value}` is not a canonical dotted field path"
        )));
    }
    Ok(())
}
