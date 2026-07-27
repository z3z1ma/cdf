use std::{fmt, ops::Deref};

use cdf_kernel::{CdfError, IdentifierRules, Result};

const NAMECASE_V1: &str = "namecase-v1";
const POSTGRES_QUOTED_V1: &str = "namecase-v1/postgres-quoted-v1";
const DUCKDB_ALLOWED_PATTERN: &str = "^[a-z_][a-z0-9_]*$";
const POSTGRES_ALLOWED_PATTERN: &str = "quoted UTF-8 identifier without NUL; cdf reserves _cdf_*";

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ValidatedSqlIdentifier(String);

impl ValidatedSqlIdentifier {
    pub fn user(rules: &IdentifierRules, value: &str) -> Result<Self> {
        validate_common(rules, value)?;
        match rules.normalizer.as_str() {
            NAMECASE_V1 => validate_namecase(rules, value)?,
            POSTGRES_QUOTED_V1 => validate_postgres_quoted(rules, value, false)?,
            other => {
                return Err(CdfError::contract(format!(
                    "SQL destination identifier normalizer {other:?} is not supported"
                )));
            }
        }
        Ok(Self(value.to_owned()))
    }

    pub fn system(rules: &IdentifierRules, value: &str) -> Result<Self> {
        validate_common(rules, value)?;
        match rules.normalizer.as_str() {
            NAMECASE_V1 => validate_namecase(rules, value)?,
            POSTGRES_QUOTED_V1 => validate_postgres_quoted(rules, value, true)?,
            other => {
                return Err(CdfError::contract(format!(
                    "SQL destination identifier normalizer {other:?} is not supported"
                )));
            }
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for ValidatedSqlIdentifier {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for ValidatedSqlIdentifier {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl fmt::Display for ValidatedSqlIdentifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

fn validate_common(rules: &IdentifierRules, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(CdfError::contract(
            "SQL destination identifier cannot be empty",
        ));
    }
    if value.contains('\0') {
        return Err(CdfError::contract(format!(
            "SQL destination identifier {value:?} contains NUL"
        )));
    }
    if let Some(max_length) = rules.max_length
        && value.len() > usize::from(max_length)
    {
        return Err(CdfError::contract(format!(
            "SQL destination identifier {value:?} exceeds {max_length} bytes"
        )));
    }
    Ok(())
}

fn validate_namecase(rules: &IdentifierRules, value: &str) -> Result<()> {
    if rules.allowed_pattern.as_deref() != Some(DUCKDB_ALLOWED_PATTERN) {
        return Err(CdfError::contract(format!(
            "namecase-v1 SQL destination rules must declare {DUCKDB_ALLOWED_PATTERN:?}"
        )));
    }
    let mut characters = value.chars();
    let first = characters
        .next()
        .ok_or_else(|| CdfError::contract("namecase-v1 identifier cannot be empty"))?;
    if !(first == '_' || first.is_ascii_lowercase())
        || !characters.all(|character| {
            character == '_' || character.is_ascii_lowercase() || character.is_ascii_digit()
        })
    {
        return Err(CdfError::contract(format!(
            "SQL destination identifier {value:?} does not satisfy {DUCKDB_ALLOWED_PATTERN}"
        )));
    }
    Ok(())
}

fn validate_postgres_quoted(
    rules: &IdentifierRules,
    value: &str,
    allow_system_prefix: bool,
) -> Result<()> {
    if !matches!(
        rules.allowed_pattern.as_deref(),
        None | Some(POSTGRES_ALLOWED_PATTERN)
    ) {
        return Err(CdfError::contract(format!(
            "postgres-quoted-v1 SQL destination rules declare unsupported allowed pattern {:?}",
            rules.allowed_pattern
        )));
    }
    if !allow_system_prefix && value.starts_with("_cdf_") {
        return Err(CdfError::contract(format!(
            "SQL destination identifier {value:?} uses reserved _cdf_ prefix"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn duckdb_rules() -> IdentifierRules {
        IdentifierRules {
            normalizer: NAMECASE_V1.to_owned(),
            max_length: None,
            allowed_pattern: Some(DUCKDB_ALLOWED_PATTERN.to_owned()),
        }
    }

    fn postgres_rules() -> IdentifierRules {
        IdentifierRules {
            normalizer: POSTGRES_QUOTED_V1.to_owned(),
            max_length: Some(63),
            allowed_pattern: Some(POSTGRES_ALLOWED_PATTERN.to_owned()),
        }
    }

    #[test]
    fn namecase_rules_reject_unquoted_interpolation_inputs() {
        assert!(ValidatedSqlIdentifier::user(&duckdb_rules(), "orders_2026").is_ok());
        assert!(ValidatedSqlIdentifier::user(&duckdb_rules(), "Orders").is_err());
        assert!(ValidatedSqlIdentifier::user(&duckdb_rules(), "orders; drop table").is_err());
    }

    #[test]
    fn quoted_rules_preserve_utf8_but_reserve_framework_names() {
        let identifier =
            ValidatedSqlIdentifier::user(&postgres_rules(), "Résumé \"2026\"").unwrap();
        assert_eq!(identifier.as_str(), "Résumé \"2026\"");
        assert!(ValidatedSqlIdentifier::user(&postgres_rules(), "_cdf_loads").is_err());
        assert!(ValidatedSqlIdentifier::system(&postgres_rules(), "_cdf_loads").is_ok());
        assert!(ValidatedSqlIdentifier::user(&postgres_rules(), "x\0y").is_err());
    }
}
