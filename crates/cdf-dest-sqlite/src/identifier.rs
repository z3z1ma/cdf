use cdf_dest_sql::ValidatedSqlIdentifier;
use cdf_kernel::{IdentifierRules, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct SqliteIdentifier(String);

impl SqliteIdentifier {
    pub(crate) fn user(value: &str) -> Result<Self> {
        let validated = ValidatedSqlIdentifier::user(&sqlite_identifier_rules(), value)?;
        Ok(Self(validated.as_str().to_owned()))
    }

    pub(crate) fn system(value: &str) -> Result<Self> {
        let validated = ValidatedSqlIdentifier::system(&sqlite_identifier_rules(), value)?;
        Ok(Self(validated.as_str().to_owned()))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn quoted(&self) -> String {
        quote_identifier(&self.0)
    }
}

impl std::fmt::Display for SqliteIdentifier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub(crate) fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(crate) fn sqlite_identifier_rules() -> IdentifierRules {
    IdentifierRules {
        normalizer: "namecase-v1/sqlite-quoted-v1".to_owned(),
        max_length: Some(255),
        allowed_pattern: Some(
            "quoted UTF-8 identifier without NUL; cdf reserves _cdf_*".to_owned(),
        ),
    }
}
