use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use duckdb::Connection;

use crate::{CdfError, Result, sql::duckdb_error};

pub(crate) struct DuckDbProfileCapture {
    output_path: PathBuf,
    scratch_path: PathBuf,
}

impl DuckDbProfileCapture {
    pub(crate) fn start(
        connection: &Connection,
        directory: Option<&Path>,
        attempt: usize,
        scan_threads: usize,
    ) -> Result<Option<Self>> {
        let Some(directory) = directory else {
            return Ok(None);
        };
        fs::create_dir_all(directory).map_err(|error| {
            CdfError::destination(format!(
                "create DuckDB profile directory {}: {error}",
                directory.display()
            ))
        })?;
        let started_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let name = format!(
            "duckdb-materialization-p{}-{started_ns}-attempt-{attempt}-scan-{scan_threads}.json",
            std::process::id()
        );
        let output_path = directory.join(&name);
        let scratch_path = directory.join(format!(".{name}.capture.json"));
        let scratch = sql_string_literal(&scratch_path)?;
        connection
            .execute_batch(&format!(
                "CALL enable_profiling(format := 'json', save_location := {scratch}, coverage := 'all', mode := 'detailed')"
            ))
            .map_err(|error| duckdb_error("enable DuckDB materialization profiling", error))?;
        Ok(Some(Self {
            output_path,
            scratch_path,
        }))
    }

    pub(crate) fn finish(self, connection: &Connection) -> Result<PathBuf> {
        let capture = fs::read(&self.scratch_path)
            .map_err(|error| {
                CdfError::destination(format!(
                    "read DuckDB materialization profile {}: {error}",
                    self.scratch_path.display()
                ))
            })
            .and_then(|bytes| {
                serde_json::from_slice::<serde_json::Value>(&bytes)
                    .map_err(|error| {
                        CdfError::destination(format!(
                            "DuckDB materialization profile {} is not valid JSON: {error}",
                            self.scratch_path.display()
                        ))
                    })
                    .map(|_| bytes)
            });
        let disable = connection
            .execute_batch("CALL disable_profiling()")
            .map_err(|error| duckdb_error("disable DuckDB materialization profiling", error));
        let _ = fs::remove_file(&self.scratch_path);
        let bytes = match (capture, disable) {
            (Ok(bytes), Ok(())) => bytes,
            (Err(error), Ok(())) | (Ok(_), Err(error)) => return Err(error),
            (Err(error), Err(disable_error)) => {
                return Err(CdfError::destination(format!(
                    "{error}; DuckDB materialization profiler cleanup also failed: {disable_error}"
                )));
            }
        };
        let publish_path = self.output_path.with_extension("json.publish");
        fs::write(&publish_path, bytes).map_err(|error| {
            CdfError::destination(format!(
                "write DuckDB materialization profile {}: {error}",
                publish_path.display()
            ))
        })?;
        fs::rename(&publish_path, &self.output_path).map_err(|error| {
            CdfError::destination(format!(
                "publish DuckDB materialization profile {}: {error}",
                self.output_path.display()
            ))
        })?;
        Ok(self.output_path)
    }
}

fn sql_string_literal(path: &Path) -> Result<String> {
    let value = path.to_str().ok_or_else(|| {
        CdfError::contract("DuckDB profile directory must be valid UTF-8 when profiling is enabled")
    })?;
    Ok(format!("'{}'", value.replace('\'', "''")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captures_only_the_explicit_materialization_query() {
        let temp = tempfile::tempdir().unwrap();
        let connection = Connection::open_in_memory().unwrap();
        let capture = DuckDbProfileCapture::start(&connection, Some(temp.path()), 1, 2)
            .unwrap()
            .unwrap();
        connection
            .execute_batch("CREATE TABLE profiled AS SELECT * FROM range(4096)")
            .unwrap();
        let profile = capture.finish(&connection).unwrap();
        let profile: serde_json::Value =
            serde_json::from_slice(&fs::read(profile).unwrap()).unwrap();
        assert!(
            profile
                .get("query_name")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|query| query.contains("CREATE TABLE profiled"))
        );
        assert!(profile.get("system_peak_buffer_memory").is_some());
        assert!(profile.get("system_peak_temp_dir_size").is_some());
    }

    #[test]
    fn failed_capture_disables_profiling_before_returning() {
        let temp = tempfile::tempdir().unwrap();
        let connection = Connection::open_in_memory().unwrap();
        let capture = DuckDbProfileCapture::start(&connection, Some(temp.path()), 1, 2)
            .unwrap()
            .unwrap();
        connection.execute_batch("SELECT * FROM range(32)").unwrap();
        let scratch_path = capture.scratch_path.clone();
        fs::remove_file(&scratch_path).unwrap();

        assert!(capture.finish(&connection).is_err());
        connection.execute_batch("SELECT * FROM range(16)").unwrap();
        assert!(
            !scratch_path.exists(),
            "a query after failed capture must not recreate the profile output"
        );
    }
}
