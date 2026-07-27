use crate::validate::validate_type_fragment;
use crate::*;

pub(crate) fn postgres_identifier_rules() -> IdentifierRules {
    IdentifierRules {
        normalizer: "namecase-v1/postgres-quoted-v1".to_owned(),
        max_length: Some(63),
        allowed_pattern: Some(
            "quoted UTF-8 identifier without NUL; cdf reserves _cdf_*".to_owned(),
        ),
    }
}

pub(crate) fn validate_user_identifier(
    value: &PostgresIdentifier,
) -> Result<cdf_dest_sql::ValidatedSqlIdentifier> {
    cdf_dest_sql::ValidatedSqlIdentifier::user(&postgres_identifier_rules(), value.as_str())
}

pub(crate) fn validate_system_identifier(
    value: &PostgresIdentifier,
) -> Result<cdf_dest_sql::ValidatedSqlIdentifier> {
    cdf_dest_sql::ValidatedSqlIdentifier::system(&postgres_identifier_rules(), value.as_str())
}

pub(crate) fn quote_validated_identifier(value: &cdf_dest_sql::ValidatedSqlIdentifier) -> String {
    format!("\"{}\"", value.as_str().replace('"', "\"\""))
}

pub(crate) fn quote_user_identifier(value: &PostgresIdentifier) -> Result<String> {
    let validated = validate_user_identifier(value)?;
    Ok(quote_validated_identifier(&validated))
}

pub(crate) fn quote_system_identifier(value: &PostgresIdentifier) -> Result<String> {
    let validated = validate_system_identifier(value)?;
    Ok(quote_validated_identifier(&validated))
}

fn is_framework_column(value: &PostgresIdentifier) -> bool {
    matches!(
        value.as_str(),
        CDF_ROW_KEY_COLUMN | CDF_LOADED_AT_COLUMN | cdf_contract::VARIANT_COLUMN_NAME
    )
}

pub(crate) fn quote_column_identifier(value: &PostgresIdentifier) -> Result<String> {
    if is_framework_column(value) {
        quote_system_identifier(value)
    } else {
        quote_user_identifier(value)
    }
}

pub(crate) fn validated_target_sql(target: &PostgresTarget) -> Result<String> {
    let table = quote_user_identifier(&target.table)?;
    target.schema.as_ref().map_or(Ok(table.clone()), |schema| {
        Ok(format!("{}.{}", quote_user_identifier(schema)?, table))
    })
}

pub(crate) fn validated_user_column_definition(column: &PostgresColumn) -> Result<String> {
    validated_column_definition(column, column.nullable, is_framework_column(&column.name))
}

pub(crate) fn validated_system_column_definition(column: &PostgresColumn) -> Result<String> {
    validated_column_definition(column, column.nullable, true)
}

pub(crate) fn validated_column_definition(
    column: &PostgresColumn,
    nullable: bool,
    system: bool,
) -> Result<String> {
    let name = if system {
        quote_system_identifier(&column.name)?
    } else {
        quote_user_identifier(&column.name)?
    };
    let nullability = if nullable { "" } else { " NOT NULL" };
    Ok(format!("{name} {}{nullability}", column.data_type))
}

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
