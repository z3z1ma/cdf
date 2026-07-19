use std::{
    fs::File,
    io::{BufReader, Read},
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

use crate::sql::{duckdb_error, quote_ident};
use crate::*;

const NANOARROW_EXTENSION_PATH_ENV: &str = "CDF_DUCKDB_NANOARROW_EXTENSION_PATH";
const NANOARROW_EXTENSION_SHA256_ENV: &str = "CDF_DUCKDB_NANOARROW_EXTENSION_SHA256";
const NANOARROW_STATIC_LINK_ENV: &str = "CDF_DUCKDB_NANOARROW_STATIC_LINK";
const REQUIRED_NANOARROW_VERSION: &str = "0.8.0";

#[derive(Clone, Debug)]
pub(crate) enum DuckDbNanoarrowExtension {
    StaticallyLinked,
    Loadable { path: PathBuf, sha256: String },
}

impl DuckDbNanoarrowExtension {
    pub(crate) fn from_env() -> Result<Option<Self>> {
        let path = optional_utf8_env(NANOARROW_EXTENSION_PATH_ENV)?;
        let sha256 = optional_utf8_env(NANOARROW_EXTENSION_SHA256_ENV)?;
        match (path, sha256) {
            (None, None) => match option_env!("CDF_DUCKDB_NANOARROW_STATIC_LINK") {
                None => Ok(None),
                Some("1") => Ok(Some(Self::StaticallyLinked)),
                Some(value) => Err(CdfError::internal(format!(
                    "compile-time {NANOARROW_STATIC_LINK_ENV} must be `1` when set, observed {value:?}"
                ))),
            },
            (Some(_), None) => Err(CdfError::contract(format!(
                "{NANOARROW_EXTENSION_PATH_ENV} requires {NANOARROW_EXTENSION_SHA256_ENV}; CDF will not load an unpinned DuckDB extension"
            ))),
            (None, Some(_)) => Err(CdfError::contract(format!(
                "{NANOARROW_EXTENSION_SHA256_ENV} requires {NANOARROW_EXTENSION_PATH_ENV}"
            ))),
            (Some(path), Some(sha256)) => Self::new(path, sha256).map(Some),
        }
    }

    pub(crate) fn new(path: impl Into<PathBuf>, sha256: impl Into<String>) -> Result<Self> {
        let path = path.into();
        if !path.is_absolute() || !path.is_file() {
            return Err(CdfError::contract(format!(
                "DuckDB nanoarrow extension must be an existing absolute file: {}",
                path.display()
            )));
        }
        let sha256 = normalize_sha256(&sha256.into())?;
        let observed = sha256_file(&path)?;
        if observed != sha256 {
            return Err(CdfError::contract(format!(
                "DuckDB nanoarrow extension {} has SHA-256 {observed}, expected {sha256}",
                path.display()
            )));
        }
        Ok(Self::Loadable { path, sha256 })
    }

    pub(crate) fn configure(&self, config: Config) -> Result<Config> {
        match self {
            Self::StaticallyLinked => Ok(config),
            Self::Loadable { .. } => config
                .allow_unsigned_extensions()
                .map_err(|error| duckdb_error("enable explicitly pinned DuckDB extension", error)),
        }
    }

    pub(crate) fn load(&self, connection: &Connection) -> Result<()> {
        if let Self::Loadable { path, sha256 } = self {
            let observed = sha256_file(path)?;
            if &observed != sha256 {
                return Err(CdfError::contract(format!(
                    "DuckDB nanoarrow extension {} changed after selection: observed SHA-256 {observed}, expected {sha256}",
                    path.display()
                )));
            }
            connection
                .execute_batch(&format!("LOAD {};", duckdb_string_literal(path)?))
                .map_err(|error| {
                    duckdb_error(
                        format!(
                            "load digest-pinned DuckDB nanoarrow extension {}",
                            path.display()
                        ),
                        error,
                    )
                })?;
        }
        let version = connection
            .query_row("SELECT nanoarrow_version()", [], |row| {
                row.get::<_, String>(0)
            })
            .map_err(|error| duckdb_error("query loaded nanoarrow version", error))?;
        if version != REQUIRED_NANOARROW_VERSION {
            return Err(CdfError::contract(format!(
                "DuckDB nanoarrow extension reports version {version}, required {REQUIRED_NANOARROW_VERSION}"
            )));
        }
        Ok(())
    }

    pub(crate) fn sha256(&self) -> Option<&str> {
        match self {
            Self::StaticallyLinked => None,
            Self::Loadable { sha256, .. } => Some(sha256),
        }
    }

    pub(crate) const fn linkage(&self) -> &'static str {
        match self {
            Self::StaticallyLinked => "statically_linked",
            Self::Loadable { .. } => "digest_pinned_loadable",
        }
    }
}

pub(crate) fn ingest_canonical_files(
    writer: &mut DuckDbArrowWriter,
    paths: &[PathBuf],
    expected_rows: u64,
    merge: bool,
) -> Result<()> {
    if paths.is_empty() || expected_rows == 0 {
        return Err(CdfError::internal(
            "DuckDB nanoarrow ingress requires nonempty canonical files",
        ));
    }
    let package_row_key_start = writer
        .first_row_key
        .ok_or_else(|| CdfError::internal("DuckDB row-key allocator is not initialized"))?;
    let user_columns = writer.persisted_fields[..writer.user_field_count]
        .iter()
        .map(|field| quote_ident(&field.name))
        .collect::<Vec<_>>();
    let mut insert_columns = writer
        .persisted_fields
        .iter()
        .map(|field| quote_ident(&field.name))
        .collect::<Vec<_>>();
    let mut select_columns = user_columns;
    select_columns.push(format!(
        "CAST({package_row_key_start} + {} AS UBIGINT)",
        quote_ident(cdf_package_contract::CDF_PACKAGE_ROW_ORD_FIELD)
    ));
    if merge {
        insert_columns.push(quote_ident(CDF_STAGE_ORDER_COLUMN));
        select_columns.push(quote_ident(cdf_package_contract::CDF_PACKAGE_ROW_ORD_FIELD));
    }
    let source = paths
        .iter()
        .map(|path| duckdb_string_literal(path))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let sql = format!(
        "INSERT INTO {} ({}) SELECT {} FROM read_arrow([{}])",
        writer.write_target.sql_name(),
        insert_columns.join(", "),
        select_columns.join(", "),
        source,
    );
    let rows = writer.conn.execute(&sql, []).map_err(|error| {
        duckdb_error("ingest canonical Arrow IPC segments with nanoarrow", error)
    })?;
    let rows = u64::try_from(rows)
        .map_err(|_| CdfError::data("DuckDB nanoarrow row count exceeds u64"))?;
    if rows != expected_rows {
        return Err(CdfError::data(format!(
            "DuckDB nanoarrow accepted {rows} rows but canonical segment identities require {expected_rows}"
        )));
    }
    writer.rows_received = rows;
    Ok(())
}

fn optional_utf8_env(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) if value.trim().is_empty() => Err(CdfError::contract(format!(
            "{name} cannot be empty when set"
        ))),
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(CdfError::contract(format!(
            "{name} must be valid UTF-8 when set"
        ))),
    }
}

fn normalize_sha256(value: &str) -> Result<String> {
    let value = value.strip_prefix("sha256:").unwrap_or(value);
    if value.len() != 64
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
        || value.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return Err(CdfError::contract(format!(
            "{NANOARROW_EXTENSION_SHA256_ENV} must be 64 lowercase hexadecimal characters, optionally prefixed by sha256:"
        )));
    }
    Ok(value.to_owned())
}

fn sha256_file(path: &Path) -> Result<String> {
    let file = File::open(path).map_err(|error| {
        CdfError::contract(format!(
            "open DuckDB nanoarrow extension {}: {error}",
            path.display()
        ))
    })?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = reader.read(&mut buffer).map_err(|error| {
            CdfError::contract(format!(
                "hash DuckDB nanoarrow extension {}: {error}",
                path.display()
            ))
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn duckdb_string_literal(path: &Path) -> Result<String> {
    let value = path.to_str().ok_or_else(|| {
        CdfError::contract(format!(
            "DuckDB nanoarrow requires a UTF-8 local path: {}",
            path.display()
        ))
    })?;
    Ok(format!("'{}'", value.replace('\'', "''")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn extension_pin_requires_exact_digest_and_absolute_file() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"nanoarrow-extension").unwrap();
        let digest = sha256_file(file.path()).unwrap();
        let extension = DuckDbNanoarrowExtension::new(file.path(), digest.clone()).unwrap();
        assert_eq!(extension.sha256(), Some(digest.as_str()));
        assert_eq!(extension.linkage(), "digest_pinned_loadable");
        assert!(
            DuckDbNanoarrowExtension::new(file.path(), "0".repeat(64))
                .unwrap_err()
                .to_string()
                .contains("has SHA-256")
        );
        assert!(DuckDbNanoarrowExtension::new("relative.extension", digest).is_err());
    }

    #[test]
    fn path_literal_escapes_quotes() {
        assert_eq!(
            duckdb_string_literal(Path::new("/tmp/cdf's.arrow")).unwrap(),
            "'/tmp/cdf''s.arrow'"
        );
    }

    #[test]
    fn statically_linked_extension_needs_no_loadable_artifact() {
        let extension = DuckDbNanoarrowExtension::StaticallyLinked;
        assert_eq!(extension.linkage(), "statically_linked");
        assert_eq!(extension.sha256(), None);
        extension.configure(Config::default()).unwrap();
    }
}
