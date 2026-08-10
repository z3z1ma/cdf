use cdf_kernel::{CdfError, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MySqlIdentifier(String);

impl MySqlIdentifier {
    pub fn user(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() || value.len() > 64 {
            return Err(CdfError::contract(
                "MySQL identifier must contain between 1 and 64 bytes",
            ));
        }
        if value
            .bytes()
            .any(|byte| byte == 0 || byte.is_ascii_control())
        {
            return Err(CdfError::contract(
                "MySQL identifier cannot contain NUL or control characters",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn quoted(&self) -> String {
        format!("`{}`", self.0.replace('`', "``"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlTarget {
    pub database: Option<MySqlIdentifier>,
    pub table: MySqlIdentifier,
}

impl MySqlTarget {
    pub fn parse(value: &str) -> Result<Self> {
        let parts = value.split('.').collect::<Vec<_>>();
        match parts.as_slice() {
            [table] => Ok(Self {
                database: None,
                table: MySqlIdentifier::user(*table)?,
            }),
            [database, table] => Ok(Self {
                database: Some(MySqlIdentifier::user(*database)?),
                table: MySqlIdentifier::user(*table)?,
            }),
            _ => Err(CdfError::contract(
                "MySQL table must be `table` or `database.table`",
            )),
        }
    }

    pub fn sql(&self) -> String {
        match &self.database {
            Some(database) => format!("{}.{}", database.quoted(), self.table.quoted()),
            None => self.table.quoted(),
        }
    }

    pub fn display_name(&self) -> String {
        match &self.database {
            Some(database) => format!("{}.{}", database.as_str(), self.table.as_str()),
            None => self.table.as_str().to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_mysql_identifiers_without_changing_identity() {
        let target = MySqlTarget::parse("warehouse.order`facts").unwrap();
        assert_eq!(target.sql(), "`warehouse`.`order``facts`");
        assert_eq!(target.display_name(), "warehouse.order`facts");
        assert!(MySqlTarget::parse("a.b.c").is_err());
    }
}
