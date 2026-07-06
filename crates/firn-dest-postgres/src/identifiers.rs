use crate::validate::*;
use crate::*;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PostgresIdentifier(String);

impl PostgresIdentifier {
    pub fn user(value: impl Into<String>) -> Result<Self> {
        Self::new(value, ReservedNamePolicy::RejectFirnPrefix)
    }

    pub fn system(value: impl Into<String>) -> Result<Self> {
        Self::new(value, ReservedNamePolicy::AllowFirnPrefix)
    }

    fn new(value: impl Into<String>, reserved: ReservedNamePolicy) -> Result<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(FirnError::contract("Postgres identifier cannot be empty"));
        }
        if value.len() > 63 {
            return Err(FirnError::contract(format!(
                "Postgres identifier {value:?} exceeds 63 bytes"
            )));
        }
        if value.contains('\0') {
            return Err(FirnError::contract(format!(
                "Postgres identifier {value:?} contains NUL"
            )));
        }
        if reserved == ReservedNamePolicy::RejectFirnPrefix && value.starts_with("_firn_") {
            return Err(FirnError::contract(format!(
                "Postgres identifier {value:?} uses reserved _firn_ prefix"
            )));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn quoted(&self) -> String {
        quote_identifier_unchecked(&self.0)
    }
}

impl fmt::Display for PostgresIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReservedNamePolicy {
    AllowFirnPrefix,
    RejectFirnPrefix,
}

pub fn quote_identifier(value: &str) -> Result<String> {
    Ok(PostgresIdentifier::user(value)?.quoted())
}

pub(crate) fn quote_identifier_unchecked(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
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
            _ => Err(FirnError::contract(format!(
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

    pub(crate) fn target_name(&self) -> Result<TargetName> {
        TargetName::new(self.display_name())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresColumn {
    pub name: PostgresIdentifier,
    pub data_type: String,
    pub nullable: bool,
}

impl PostgresColumn {
    pub fn new(name: &str, data_type: &str, nullable: bool) -> Result<Self> {
        validate_type_fragment(data_type)?;
        Ok(Self {
            name: PostgresIdentifier::user(name)?,
            data_type: data_type.to_owned(),
            nullable,
        })
    }

    pub(crate) fn definition_sql(&self) -> String {
        self.definition_sql_with_nullability(self.nullable)
    }

    pub(crate) fn definition_sql_with_nullability(&self, nullable: bool) -> String {
        let nullability = if nullable { "" } else { " NOT NULL" };
        format!("{} {}{}", self.name.quoted(), self.data_type, nullability)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresExistingColumn {
    pub name: PostgresIdentifier,
    pub data_type: String,
    pub nullable: bool,
}

impl PostgresExistingColumn {
    pub fn new(name: &str, data_type: &str, nullable: bool) -> Result<Self> {
        validate_type_fragment(data_type)?;
        Ok(Self {
            name: PostgresIdentifier::user(name)?,
            data_type: data_type.to_owned(),
            nullable,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresExistingTable {
    pub columns: BTreeMap<String, PostgresExistingColumn>,
    pub primary_key: Vec<PostgresIdentifier>,
}

impl PostgresExistingTable {
    pub fn new(
        columns: Vec<PostgresExistingColumn>,
        primary_key: Vec<&str>,
    ) -> Result<PostgresExistingTable> {
        let mut by_name = BTreeMap::new();
        for column in columns {
            if by_name
                .insert(column.name.as_str().to_owned(), column)
                .is_some()
            {
                return Err(FirnError::contract(
                    "Postgres existing table has duplicate column names",
                ));
            }
        }
        Ok(Self {
            columns: by_name,
            primary_key: primary_key
                .into_iter()
                .map(PostgresIdentifier::user)
                .collect::<Result<Vec<_>>>()?,
        })
    }
}
