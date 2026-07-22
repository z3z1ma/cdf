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
    runtime_settings: serde_json::Value,
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
        let runtime_settings = capture_runtime_settings(connection)?;
        let scratch = sql_string_literal(&scratch_path)?;
        connection
            .execute_batch(&format!(
                "CALL enable_profiling(format := 'json', save_location := {scratch}, coverage := 'all', mode := 'detailed')"
            ))
            .map_err(|error| duckdb_error("enable DuckDB materialization profiling", error))?;
        Ok(Some(Self {
            output_path,
            scratch_path,
            runtime_settings,
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
                let mut profile =
                    serde_json::from_slice::<serde_json::Value>(&bytes).map_err(|error| {
                        CdfError::destination(format!(
                            "DuckDB materialization profile {} is not valid JSON: {error}",
                            self.scratch_path.display()
                        ))
                    })?;
                let profile = profile.as_object_mut().ok_or_else(|| {
                    CdfError::destination(format!(
                        "DuckDB materialization profile {} must be a JSON object",
                        self.scratch_path.display()
                    ))
                })?;
                profile.insert(
                    "cdf_duckdb_runtime_settings".to_owned(),
                    self.runtime_settings.clone(),
                );
                serde_json::to_vec(&profile).map_err(|error| {
                    CdfError::destination(format!(
                        "serialize DuckDB materialization profile {}: {error}",
                        self.scratch_path.display()
                    ))
                })
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

fn capture_runtime_settings(connection: &Connection) -> Result<serde_json::Value> {
    let settings: (String, i64, String, bool) = connection
        .query_row(
            "SELECT current_setting('memory_limit'), current_setting('threads'), current_setting('max_temp_directory_size'), current_setting('preserve_insertion_order')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .map_err(|error| duckdb_error("capture DuckDB materialization runtime settings", error))?;
    Ok(serde_json::json!({
        "memory_limit": settings.0,
        "threads": settings.1,
        "max_temp_directory_size": settings.2,
        "preserve_insertion_order": settings.3,
    }))
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
        assert_eq!(
            profile["cdf_duckdb_runtime_settings"]["preserve_insertion_order"],
            true
        );
        assert!(
            profile["cdf_duckdb_runtime_settings"]["memory_limit"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
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
