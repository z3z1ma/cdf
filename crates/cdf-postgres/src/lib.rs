#![doc = "Shared Postgres protocol identifiers for cdf adapters."]

use std::fmt;

use cdf_kernel::{CdfError, Result, TargetName};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PostgresIdentifier(String);

impl PostgresIdentifier {
    pub fn user(value: impl Into<String>) -> Result<Self> {
        Self::new(value, ReservedNamePolicy::RejectCdfPrefix)
    }

    pub fn system(value: impl Into<String>) -> Result<Self> {
        Self::new(value, ReservedNamePolicy::AllowCdfPrefix)
    }

    fn new(value: impl Into<String>, reserved: ReservedNamePolicy) -> Result<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(CdfError::contract("Postgres identifier cannot be empty"));
        }
        if value.len() > 63 {
            return Err(CdfError::contract(format!(
                "Postgres identifier {value:?} exceeds 63 bytes"
            )));
        }
        if value.contains('\0') {
            return Err(CdfError::contract(format!(
                "Postgres identifier {value:?} contains NUL"
            )));
        }
        if reserved == ReservedNamePolicy::RejectCdfPrefix && value.starts_with("_cdf_") {
            return Err(CdfError::contract(format!(
                "Postgres identifier {value:?} uses reserved _cdf_ prefix"
            )));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn quoted(&self) -> String {
        format!("\"{}\"", self.0.replace('"', "\"\""))
    }
}

impl fmt::Display for PostgresIdentifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReservedNamePolicy {
    AllowCdfPrefix,
    RejectCdfPrefix,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresTarget {
    pub schema: Option<PostgresIdentifier>,
    pub table: PostgresIdentifier,
}

impl PostgresTarget {
    pub fn new(schema: Option<&str>, table: &str) -> Result<Self> {
        Ok(Self {
            schema: schema.map(PostgresIdentifier::user).transpose()?,
            table: PostgresIdentifier::user(table)?,
        })
    }

    pub fn parse(value: &str) -> Result<Self> {
        let parts = value.split('.').collect::<Vec<_>>();
        match parts.as_slice() {
            [table] => Self::new(None, table),
            [schema, table] => Self::new(Some(schema), table),
            _ => Err(CdfError::contract(format!(
                "Postgres target {value:?} must be table or schema.table"
            ))),
        }
    }

    pub fn sql(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", schema.quoted(), self.table.quoted()),
            None => self.table.quoted(),
        }
    }

    pub fn display_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", schema.as_str(), self.table.as_str()),
            None => self.table.as_str().to_owned(),
        }
    }

    pub fn target_name(&self) -> Result<TargetName> {
        TargetName::new(self.display_name())
    }
}

pub fn quote_identifier(value: &str) -> Result<String> {
    Ok(PostgresIdentifier::user(value)?.quoted())
}
