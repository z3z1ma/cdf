use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{BenchResult, PhaseMetric, WorkerMeasurement, bench_error};

const MAX_CDF_STDOUT_BYTES: usize = 2 * 1024 * 1024;
const MAX_CDF_STDERR_BYTES: usize = 256 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CdfWorkspaceMode {
    FreshCopy,
    InPlace,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdfCommandWorkload {
    pub cdf_executable: PathBuf,
    pub workspace_template: PathBuf,
    pub workspace_parent: PathBuf,
    pub workspace_mode: CdfWorkspaceMode,
    pub args: Vec<String>,
    #[serde(default)]
    pub expected_rows: Option<u64>,
    #[serde(default)]
    pub derived_logical_bytes: Option<u64>,
    #[serde(default)]
    pub expected_physical_bytes: Option<u64>,
    #[serde(default)]
    pub expected_package_hash: Option<String>,
    #[serde(default)]
    pub expected_schema_hash: Option<String>,
    #[serde(default)]
    pub spill_bytes: Option<u64>,
    #[serde(default)]
    pub preserve_state: bool,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
}

pub fn run_cdf_command_workload(workload: &CdfCommandWorkload) -> BenchResult<WorkerMeasurement> {
    if workload.cdf_executable.as_os_str().is_empty() {
        return Err(bench_error("CDF command workload requires cdf_executable"));
    }
    if !workload.workspace_template.is_dir() {
        return Err(bench_error(format!(
            "CDF command workspace template `{}` is not a directory",
            workload.workspace_template.display()
        )));
    }
    fs::create_dir_all(&workload.workspace_parent)?;

    let mut retained_workspace = None;
    let workspace = match workload.workspace_mode {
        CdfWorkspaceMode::FreshCopy => {
            let temp = tempfile::tempdir_in(&workload.workspace_parent)?;
            copy_workspace(
                &workload.workspace_template,
                temp.path(),
                workload.preserve_state,
            )?;
            let path = temp.path().to_path_buf();
            retained_workspace = Some(temp);
            path
        }
        CdfWorkspaceMode::InPlace => workload.workspace_template.clone(),
    };

    let started = Instant::now();
    let output = run_cdf_child(
        &workload.cdf_executable,
        &workload.args,
        &workspace,
        &workload.environment,
        workload.timeout_ms.map(Duration::from_millis),
    )?;
    let timed_wall_time_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    drop(retained_workspace);

    if !output.status.success() {
        return Err(bench_error(format!(
            "CDF command exited with code {}; stderr: {}",
            output
                .status
                .code()
                .map(|code| code.to_string())
                .unwrap_or_else(|| "unknown".to_owned()),
            sanitized_tail(&output.stderr)
        )));
    }

    let stdout_json = serde_json::from_slice::<Value>(&output.stdout).map_err(|error| {
        bench_error(format!(
            "successful CDF command emitted invalid JSON output: {error}"
        ))
    })?;
    let observed_rows = extract_row_count(&stdout_json).unwrap_or(0);
    let phases = extract_phase_metrics(&stdout_json);
    let observed_physical_bytes = extract_physical_bytes(&stdout_json)
        .or_else(|| phase_bytes(&phases, "source_read"))
        .unwrap_or(0);
    let observed_logical_bytes = workload
        .derived_logical_bytes
        .or_else(|| phase_bytes(&phases, "validation_normalization"))
        .or_else(|| extract_physical_bytes(&stdout_json))
        .unwrap_or(0);
    validate_expected_u64("row count", workload.expected_rows, observed_rows)?;
    validate_expected_u64(
        "physical byte count",
        workload.expected_physical_bytes,
        observed_physical_bytes,
    )?;
    validate_expected_text(
        "package hash",
        workload.expected_package_hash.as_deref(),
        extract_package_hash(&stdout_json),
    )?;
    validate_expected_text(
        "schema hash",
        workload.expected_schema_hash.as_deref(),
        extract_schema_hash(&stdout_json),
    )?;
    if workload.derived_logical_bytes.is_some()
        && (workload.expected_package_hash.is_none() || workload.expected_schema_hash.is_none())
    {
        return Err(bench_error(
            "derived logical bytes require expected package and schema hashes",
        ));
    }

    Ok(WorkerMeasurement {
        timed_wall_time_ns: Some(timed_wall_time_ns),
        rows: observed_rows,
        logical_bytes: observed_logical_bytes,
        physical_bytes: observed_physical_bytes,
        spill_bytes: workload.spill_bytes.unwrap_or(0),
        phases,
    })
}

fn phase_bytes(phases: &[PhaseMetric], expected_phase: &str) -> Option<u64> {
    phases
        .iter()
        .find(|phase| phase.phase == expected_phase)
        .map(|phase| phase.bytes)
}

struct CdfChildOutput {
    status: std::process::ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

fn run_cdf_child(
    executable: &Path,
    args: &[String],
    workspace: &Path,
    environment: &BTreeMap<String, String>,
    timeout: Option<Duration>,
) -> BenchResult<CdfChildOutput> {
    let mut command = Command::new(executable);
    command
        .args(args)
        .envs(environment)
        .current_dir(workspace)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    {
        command.process_group(0);
    }
    let mut child = command.spawn()?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| bench_error("CDF command stdout pipe was not created"))?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or_else(|| bench_error("CDF command stderr pipe was not created"))?;
    let stdout_reader =
        thread::spawn(move || read_limited(&mut stdout, MAX_CDF_STDOUT_BYTES, "stdout"));
    let stderr_reader =
        thread::spawn(move || read_limited(&mut stderr, MAX_CDF_STDERR_BYTES, "stderr"));
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if timeout.is_some_and(|limit| started.elapsed() >= limit) {
            terminate_child_tree(&mut child);
            let _ = stdout_reader.join();
            let _ = stderr_reader.join();
            return Err(bench_error(format!(
                "CDF command exceeded worker timeout of {}ms",
                timeout
                    .map(|limit| limit.as_millis().to_string())
                    .unwrap_or_else(|| "unknown".to_owned())
            )));
        }
        thread::sleep(Duration::from_millis(10));
    };
    let stdout = stdout_reader
        .join()
        .map_err(|_| bench_error("CDF command stdout reader panicked"))??;
    let stderr = stderr_reader
        .join()
        .map_err(|_| bench_error("CDF command stderr reader panicked"))??;
    Ok(CdfChildOutput {
        status,
        stdout,
        stderr,
    })
}

fn read_limited(reader: &mut impl Read, limit: usize, stream: &str) -> BenchResult<Vec<u8>> {
    let mut output = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let available = limit.saturating_sub(output.len());
        let retained = available.min(read);
        output.extend_from_slice(&buffer[..retained]);
        if retained != read {
            return Err(bench_error(format!(
                "CDF command {stream} exceeded the {limit} byte capture limit"
            )));
        }
    }
    Ok(output)
}

fn terminate_child_tree(child: &mut Child) {
    #[cfg(unix)]
    {
        let group = format!("-{}", child.id());
        let _ = Command::new("kill").args(["-TERM", &group]).status();
        thread::sleep(Duration::from_millis(250));
        if child.try_wait().ok().flatten().is_none() {
            let _ = Command::new("kill").args(["-KILL", &group]).status();
        }
    }
    #[cfg(not(unix))]
    {
        let _ = child.kill();
    }
    let _ = child.wait();
}

fn copy_workspace(source: &Path, destination: &Path, preserve_state: bool) -> BenchResult<()> {
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let relative = source_path
            .strip_prefix(source)
            .map_err(|error| bench_error(format!("workspace path escape: {error}")))?;
        copy_workspace_entry(source, relative, destination, preserve_state)?;
    }
    Ok(())
}

fn copy_workspace_entry(
    source_root: &Path,
    relative: &Path,
    destination_root: &Path,
    preserve_state: bool,
) -> BenchResult<()> {
    if should_skip(relative, preserve_state) {
        return Ok(());
    }
    let source = source_root.join(relative);
    let destination = destination_root.join(relative);
    let metadata = fs::symlink_metadata(&source)?;
    if metadata.is_dir() {
        fs::create_dir_all(&destination)?;
        for entry in fs::read_dir(&source)? {
            let entry = entry?;
            let child_relative = relative.join(entry.file_name());
            copy_workspace_entry(
                source_root,
                &child_relative,
                destination_root,
                preserve_state,
            )?;
        }
    } else if metadata.is_file() {
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(&source, &destination)?;
    } else if metadata.file_type().is_symlink() {
        return Err(bench_error(format!(
            "benchmark workspace copy refuses symlink `{}`; materialize the workspace before measuring",
            relative.display()
        )));
    }
    Ok(())
}

fn should_skip(relative: &Path, preserve_state: bool) -> bool {
    let components = relative
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => value.to_str(),
            _ => None,
        })
        .collect::<Vec<_>>();
    if components.is_empty() {
        return false;
    }
    if components.iter().any(|component| {
        matches!(
            *component,
            ".git" | "target" | "secrets" | ".aws" | ".codex"
        )
    }) {
        return true;
    }
    if components.len() >= 2
        && components[0] == ".cdf"
        && matches!(components[1], "packages" | "tmp" | "spool" | "secrets")
    {
        return true;
    }
    if !preserve_state && components.as_slice() == [".cdf", "state.db"] {
        return true;
    }
    let Some(name) = components.last() else {
        return false;
    };
    name == &".env"
        || name.starts_with(".env.")
        || name.ends_with(".duckdb")
        || name.ends_with(".duckdb.wal")
}

fn sanitized_tail(bytes: &[u8]) -> String {
    let retained = if bytes.len() > 4096 {
        &bytes[bytes.len() - 4096..]
    } else {
        bytes
    };
    let mut text = String::from_utf8_lossy(retained)
        .chars()
        .map(|character| match character {
            '/' | '\\' | '@' => '-',
            character if character.is_control() => ' ',
            character => character,
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if bytes.len() > retained.len() {
        text.insert_str(0, "[stderr truncated] ");
    }
    text
}

fn extract_row_count(value: &Value) -> Option<u64> {
    value
        .pointer("/result/row_count")
        .and_then(Value::as_u64)
        .or_else(|| {
            value
                .pointer("/result/receipt/counts/rows_written")
                .and_then(Value::as_u64)
        })
        .or_else(|| value.pointer("/row_count").and_then(Value::as_u64))
}

fn extract_physical_bytes(value: &Value) -> Option<u64> {
    value.pointer("/result/byte_count").and_then(Value::as_u64)
}

fn extract_package_hash(value: &Value) -> Option<&str> {
    value
        .pointer("/result/package_hash")
        .and_then(Value::as_str)
}

fn extract_schema_hash(value: &Value) -> Option<&str> {
    value.pointer("/result/schema_hash").and_then(Value::as_str)
}

fn validate_expected_u64(label: &str, expected: Option<u64>, observed: u64) -> BenchResult<()> {
    if let Some(expected) = expected
        && observed != expected
    {
        return Err(bench_error(format!(
            "CDF command {label} mismatch: expected {expected}, observed {observed}"
        )));
    }
    Ok(())
}

fn validate_expected_text(
    label: &str,
    expected: Option<&str>,
    observed: Option<&str>,
) -> BenchResult<()> {
    if let Some(expected) = expected
        && observed != Some(expected)
    {
        return Err(bench_error(format!(
            "CDF command {label} mismatch: expected {expected}, observed {}",
            observed.unwrap_or("<missing>")
        )));
    }
    Ok(())
}

fn extract_phase_metrics(value: &Value) -> Vec<PhaseMetric> {
    let mut phases = Vec::new();
    if let Some(metrics) = value.pointer("/result/phases").and_then(Value::as_array) {
        for metric in metrics {
            if let Some(metric) = phase_metric_from_value(metric) {
                phases.push(metric);
            }
        }
        if !phases.is_empty() {
            return phases;
        }
    }
    let Some(events) = value
        .pointer("/result/ledger_events/events")
        .and_then(Value::as_array)
    else {
        return phases;
    };
    for event in events {
        if event.get("kind").and_then(Value::as_str) != Some("phase_measured") {
            continue;
        }
        let Some(metric) = event
            .pointer("/details/attributes/metric/value")
            .and_then(Value::as_object)
        else {
            continue;
        };
        if let Some(metric) = phase_metric_from_value(&Value::Object(metric.clone())) {
            phases.push(metric);
        }
    }
    phases
}

fn phase_metric_from_value(metric: &Value) -> Option<PhaseMetric> {
    let phase = metric.get("phase").and_then(Value::as_str)?;
    let duration_ns = metric.get("duration_ns").and_then(Value::as_u64)?;
    let bytes = metric
        .get("output_bytes")
        .and_then(Value::as_u64)
        .filter(|bytes| *bytes > 0)
        .or_else(|| metric.get("input_bytes").and_then(Value::as_u64))
        .unwrap_or(0);
    Some(PhaseMetric {
        phase: phase.to_owned(),
        duration_ns,
        bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_replay_phase_metrics_are_preferred_over_ledger_fallback() {
        let value = serde_json::json!({
            "result": {
                "phases": [
                    {
                        "phase": "destination_write_receipt",
                        "duration_ns": 17,
                        "input_bytes": 19,
                        "output_bytes": 23
                    },
                    {
                        "phase": "checkpoint_gate",
                        "duration_ns": 29,
                        "input_bytes": 0,
                        "output_bytes": 0
                    }
                ],
                "ledger_events": {
                    "events": [{
                        "kind": "phase_measured",
                        "details": {
                            "attributes": {
                                "metric": {
                                    "value": {
                                        "phase": "wrong_fallback",
                                        "duration_ns": 31,
                                        "output_bytes": 37
                                    }
                                }
                            }
                        }
                    }]
                }
            }
        });

        let phases = extract_phase_metrics(&value);
        assert_eq!(phases.len(), 2);
        assert_eq!(phases[0].phase, "destination_write_receipt");
        assert_eq!(phases[0].duration_ns, 17);
        assert_eq!(phases[0].bytes, 23);
        assert_eq!(phases[1].phase, "checkpoint_gate");
        assert_eq!(phases[1].duration_ns, 29);
        assert_eq!(phases[1].bytes, 0);
    }

    #[test]
    fn cdf_command_worker_uses_fresh_workspace_and_extracts_json_metrics() {
        let temp = tempfile::tempdir().unwrap();
        let template = temp.path().join("template");
        fs::create_dir(&template).unwrap();
        fs::write(template.join("cdf.toml"), "").unwrap();
        let marker = template.join("marker");

        let workload = CdfCommandWorkload {
            cdf_executable: PathBuf::from("/bin/sh"),
            workspace_template: template.clone(),
            workspace_parent: temp.path().join("workspaces"),
            workspace_mode: CdfWorkspaceMode::FreshCopy,
            args: vec![
                "-c".to_owned(),
                "touch marker; printf '%s' '{\"result\":{\"row_count\":7,\"ledger_events\":{\"events\":[{\"kind\":\"phase_measured\",\"details\":{\"attributes\":{\"metric\":{\"value\":{\"phase\":\"source_read\",\"duration_ns\":11,\"output_bytes\":13}}}}},{\"kind\":\"phase_measured\",\"details\":{\"attributes\":{\"metric\":{\"value\":{\"phase\":\"validation_normalization\",\"duration_ns\":17,\"output_bytes\":101}}}}}]}}}'".to_owned(),
            ],
            expected_rows: Some(7),
            derived_logical_bytes: None,
            expected_physical_bytes: Some(13),
            expected_package_hash: None,
            expected_schema_hash: None,
            spill_bytes: None,
            preserve_state: false,
            timeout_ms: None,
            environment: BTreeMap::new(),
        };

        let measurement = run_cdf_command_workload(&workload).unwrap();
        assert_eq!(measurement.rows, 7);
        assert_eq!(measurement.logical_bytes, 101);
        assert_eq!(measurement.physical_bytes, 13);
        assert_eq!(measurement.phases.len(), 2);
        assert_eq!(measurement.phases[0].phase, "source_read");
        assert!(
            !marker.exists(),
            "fresh sample polluted the workspace template"
        );
    }

    #[test]
    fn physical_bytes_never_fall_back_to_a_logical_phase_counter() {
        let phases = vec![
            PhaseMetric {
                phase: "source_read".to_owned(),
                duration_ns: 1,
                bytes: 7,
            },
            PhaseMetric {
                phase: "validation_normalization".to_owned(),
                duration_ns: 2,
                bytes: 101,
            },
        ];

        assert_eq!(phase_bytes(&phases, "source_read"), Some(7));
        assert_eq!(phase_bytes(&phases, "validation_normalization"), Some(101));
        assert_eq!(phase_bytes(&phases, "missing"), None);
    }

    #[test]
    fn fresh_workspace_drops_runtime_state_by_default() {
        let temp = tempfile::tempdir().unwrap();
        let template = temp.path().join("template");
        fs::create_dir(&template).unwrap();
        fs::create_dir(template.join(".cdf")).unwrap();
        fs::write(template.join(".cdf").join("state.db"), "runtime-state").unwrap();

        let destination = temp.path().join("copy");
        copy_workspace(&template, &destination, false).unwrap();
        assert!(!destination.join(".cdf").join("state.db").exists());

        let preserved = temp.path().join("preserved");
        copy_workspace(&template, &preserved, true).unwrap();
        assert!(preserved.join(".cdf").join("state.db").exists());
    }

    #[test]
    fn cdf_command_timeout_kills_the_child() {
        let temp = tempfile::tempdir().unwrap();
        let template = temp.path().join("template");
        fs::create_dir(&template).unwrap();
        let workload = CdfCommandWorkload {
            cdf_executable: PathBuf::from("/bin/sh"),
            workspace_template: template,
            workspace_parent: temp.path().join("workspaces"),
            workspace_mode: CdfWorkspaceMode::FreshCopy,
            args: vec!["-c".to_owned(), "sleep 5".to_owned()],
            expected_rows: None,
            derived_logical_bytes: None,
            expected_physical_bytes: None,
            expected_package_hash: None,
            expected_schema_hash: None,
            spill_bytes: None,
            preserve_state: false,
            timeout_ms: Some(50),
            environment: BTreeMap::new(),
        };
        let error =
            run_cdf_command_workload(&workload).expect_err("worker should enforce its own timeout");
        assert!(
            error
                .to_string()
                .contains("CDF command exceeded worker timeout")
        );
    }

    #[test]
    fn cdf_command_worker_passes_explicit_environment_to_child() {
        let temp = tempfile::tempdir().unwrap();
        let template = temp.path().join("template");
        fs::create_dir(&template).unwrap();
        let mut environment = BTreeMap::new();
        environment.insert("CDF_BENCH_TEST_FLAG".to_owned(), "seen".to_owned());
        let workload = CdfCommandWorkload {
            cdf_executable: PathBuf::from("/bin/sh"),
            workspace_template: template,
            workspace_parent: temp.path().join("workspaces"),
            workspace_mode: CdfWorkspaceMode::FreshCopy,
            args: vec![
                "-c".to_owned(),
                "test \"$CDF_BENCH_TEST_FLAG\" = seen; printf '%s' '{\"result\":{\"package_hash\":\"sha256:package\",\"schema_hash\":\"sha256:schema\",\"receipt\":{\"counts\":{\"rows_written\":1}},\"byte_count\":3}}'".to_owned(),
            ],
            expected_rows: Some(1),
            derived_logical_bytes: Some(2),
            expected_physical_bytes: Some(3),
            expected_package_hash: Some("sha256:package".to_owned()),
            expected_schema_hash: Some("sha256:schema".to_owned()),
            spill_bytes: None,
            preserve_state: false,
            timeout_ms: None,
            environment,
        };
        let measurement = run_cdf_command_workload(&workload).unwrap();
        assert_eq!(measurement.rows, 1);
        assert_eq!(measurement.logical_bytes, 2);
        assert_eq!(measurement.physical_bytes, 3);
    }

    #[test]
    fn cdf_command_worker_rejects_requested_values_that_child_did_not_observe() {
        let temp = tempfile::tempdir().unwrap();
        let template = temp.path().join("template");
        fs::create_dir(&template).unwrap();
        let workload = CdfCommandWorkload {
            cdf_executable: PathBuf::from("/bin/sh"),
            workspace_template: template,
            workspace_parent: temp.path().join("workspaces"),
            workspace_mode: CdfWorkspaceMode::FreshCopy,
            args: vec![
                "-c".to_owned(),
                "printf '%s' '{\"result\":{\"package_hash\":\"sha256:other\",\"schema_hash\":\"sha256:schema\",\"row_count\":1,\"byte_count\":3}}'".to_owned(),
            ],
            expected_rows: Some(1),
            derived_logical_bytes: Some(2),
            expected_physical_bytes: Some(3),
            expected_package_hash: Some("sha256:package".to_owned()),
            expected_schema_hash: Some("sha256:schema".to_owned()),
            spill_bytes: None,
            preserve_state: false,
            timeout_ms: None,
            environment: BTreeMap::new(),
        };

        let error = run_cdf_command_workload(&workload).unwrap_err();
        assert!(error.to_string().contains("package hash mismatch"));
    }
}
