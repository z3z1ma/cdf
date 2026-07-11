use crate::validate::validate_type_fragment;
use crate::*;

pub(crate) fn quote_identifier_unchecked(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresColumn {
    pub name: PostgresIdentifier,
    pub data_type: String,
    pub nullable: bool,
}

impl PostgresColumn {
    pub fn new(name: &str, data_type: &str, nullable: bool) -> Result<Self> {
        Self::with_identifier(PostgresIdentifier::user(name)?, data_type, nullable)
    }

    pub(crate) fn system(name: &str, data_type: &str, nullable: bool) -> Result<Self> {
        Self::with_identifier(PostgresIdentifier::system(name)?, data_type, nullable)
    }

    fn with_identifier(name: PostgresIdentifier, data_type: &str, nullable: bool) -> Result<Self> {
        validate_type_fragment(data_type)?;
        Ok(Self {
            name,
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
        Self::with_identifier(PostgresIdentifier::user(name)?, data_type, nullable)
    }

    fn with_identifier(name: PostgresIdentifier, data_type: &str, nullable: bool) -> Result<Self> {
        validate_type_fragment(data_type)?;
        Ok(Self {
            name,
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
                return Err(CdfError::contract(
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
