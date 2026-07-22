use std::{collections::BTreeMap, fs, path::Path};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{BenchResult, bench_error};

pub const DUCKDB_PROFILE_SUMMARY_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DuckDbProfileSummary {
    pub schema_version: u16,
    pub query_name: String,
    pub latency_ns: Option<u64>,
    pub cpu_time_ns: Option<u64>,
    pub rows_returned: Option<u64>,
    pub result_set_size_bytes: Option<u64>,
    pub cumulative_cardinality: Option<u64>,
    pub cumulative_rows_scanned: Option<u64>,
    pub system_peak_buffer_memory_bytes: Option<u64>,
    pub system_peak_temp_directory_bytes: Option<u64>,
    pub total_bytes_read: Option<u64>,
    pub total_bytes_written: Option<u64>,
    pub total_memory_allocated_bytes: Option<u64>,
    pub operators: Vec<DuckDbOperatorProfile>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DuckDbOperatorProfile {
    pub depth: usize,
    pub name: String,
    pub operator_type: String,
    pub timing_ns: Option<u64>,
    pub cardinality: Option<u64>,
    pub rows_scanned: Option<u64>,
    pub result_set_size_bytes: Option<u64>,
    pub total_bytes_read: Option<u64>,
    pub total_bytes_written: Option<u64>,
    pub extra_info: BTreeMap<String, Value>,
}

pub fn read_duckdb_profile(path: impl AsRef<Path>) -> BenchResult<DuckDbProfileSummary> {
    let value: Value = serde_json::from_slice(&fs::read(path)?)?;
    summarize_duckdb_profile(&value)
}

pub fn summarize_duckdb_profile(value: &Value) -> BenchResult<DuckDbProfileSummary> {
    let root = value
        .as_object()
        .ok_or_else(|| bench_error("DuckDB profile root must be a JSON object"))?;
    let query_name = root
        .get("query_name")
        .and_then(Value::as_str)
        .ok_or_else(|| bench_error("DuckDB profile omitted query_name"))?
        .to_owned();
    let mut operators = Vec::new();
    if let Some(children) = root.get("children").and_then(Value::as_array) {
        for child in children {
            collect_operator(child, 0, &mut operators)?;
        }
    }
    Ok(DuckDbProfileSummary {
        schema_version: DUCKDB_PROFILE_SUMMARY_VERSION,
        query_name,
        latency_ns: seconds_to_ns(root.get("latency"))?,
        cpu_time_ns: seconds_to_ns(root.get("cpu_time"))?,
        rows_returned: u64_metric(root.get("rows_returned"), "rows_returned")?,
        result_set_size_bytes: u64_metric(root.get("result_set_size"), "result_set_size")?,
        cumulative_cardinality: u64_metric(
            root.get("cumulative_cardinality"),
            "cumulative_cardinality",
        )?,
        cumulative_rows_scanned: u64_metric(
            root.get("cumulative_rows_scanned"),
            "cumulative_rows_scanned",
        )?,
        system_peak_buffer_memory_bytes: u64_metric(
            root.get("system_peak_buffer_memory"),
            "system_peak_buffer_memory",
        )?,
        system_peak_temp_directory_bytes: u64_metric(
            root.get("system_peak_temp_dir_size"),
            "system_peak_temp_dir_size",
        )?,
        total_bytes_read: u64_metric(root.get("total_bytes_read"), "total_bytes_read")?,
        total_bytes_written: u64_metric(root.get("total_bytes_written"), "total_bytes_written")?,
        total_memory_allocated_bytes: u64_metric(
            root.get("total_memory_allocated"),
            "total_memory_allocated",
        )?,
        operators,
    })
}

fn collect_operator(
    value: &Value,
    depth: usize,
    operators: &mut Vec<DuckDbOperatorProfile>,
) -> BenchResult<()> {
    let object = value
        .as_object()
        .ok_or_else(|| bench_error("DuckDB profile operator must be a JSON object"))?;
    let name = object
        .get("operator_name")
        .and_then(Value::as_str)
        .ok_or_else(|| bench_error("DuckDB profile operator omitted operator_name"))?
        .trim()
        .to_owned();
    let operator_type = object
        .get("operator_type")
        .and_then(Value::as_str)
        .ok_or_else(|| bench_error("DuckDB profile operator omitted operator_type"))?
        .to_owned();
    let extra_info = object
        .get("extra_info")
        .and_then(Value::as_object)
        .map(|values| {
            values
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        })
        .unwrap_or_default();
    operators.push(DuckDbOperatorProfile {
        depth,
        name,
        operator_type,
        timing_ns: seconds_to_ns(object.get("operator_timing"))?,
        cardinality: u64_metric(object.get("operator_cardinality"), "operator_cardinality")?,
        rows_scanned: u64_metric(object.get("operator_rows_scanned"), "operator_rows_scanned")?,
        result_set_size_bytes: u64_metric(
            object.get("result_set_size"),
            "operator result_set_size",
        )?,
        total_bytes_read: u64_metric(object.get("total_bytes_read"), "operator total_bytes_read")?,
        total_bytes_written: u64_metric(
            object.get("total_bytes_written"),
            "operator total_bytes_written",
        )?,
        extra_info,
    });
    if let Some(children) = object.get("children").and_then(Value::as_array) {
        for child in children {
            collect_operator(child, depth.saturating_add(1), operators)?;
        }
    }
    Ok(())
}

fn seconds_to_ns(value: Option<&Value>) -> BenchResult<Option<u64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let seconds = value
        .as_f64()
        .ok_or_else(|| bench_error("DuckDB profile duration metric must be numeric"))?;
    if !seconds.is_finite() || seconds.is_sign_negative() {
        return Err(bench_error(
            "DuckDB profile duration metric must be finite and nonnegative",
        ));
    }
    let nanoseconds = seconds * 1_000_000_000.0;
    if nanoseconds > u64::MAX as f64 {
        return Err(bench_error(
            "DuckDB profile duration exceeds u64 nanoseconds",
        ));
    }
    Ok(Some(nanoseconds.round() as u64))
}

fn u64_metric(value: Option<&Value>, name: &str) -> BenchResult<Option<u64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    value.as_u64().map(Some).ok_or_else(|| {
        bench_error(format!(
            "DuckDB profile {name} must be a nonnegative integer"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_duckdb_profile_metrics_and_operator_tree() {
        let profile = serde_json::json!({
            "query_name": "CREATE TABLE target AS SELECT * FROM scan()",
            "latency": 1.25,
            "cpu_time": 3.5,
            "rows_returned": 1,
            "cumulative_cardinality": 21,
            "cumulative_rows_scanned": 10,
            "system_peak_buffer_memory": 4096,
            "system_peak_temp_dir_size": 2048,
            "total_bytes_read": 100,
            "total_bytes_written": 200,
            "total_memory_allocated": 8192,
            "children": [{
                "operator_name": "CREATE_TABLE_AS ",
                "operator_type": "CREATE_TABLE_AS",
                "operator_timing": 1.0,
                "operator_cardinality": 1,
                "operator_rows_scanned": 0,
                "result_set_size": 8,
                "extra_info": {"Table": "target"},
                "children": [{
                    "operator_name": "CDF_SCAN ",
                    "operator_type": "TABLE_SCAN",
                    "operator_timing": 0.75,
                    "operator_cardinality": 10,
                    "operator_rows_scanned": 10,
                    "result_set_size": 80,
                    "extra_info": {},
                    "children": []
                }]
            }]
        });
        let summary = summarize_duckdb_profile(&profile).unwrap();
        assert_eq!(summary.latency_ns, Some(1_250_000_000));
        assert_eq!(summary.cpu_time_ns, Some(3_500_000_000));
        assert_eq!(summary.system_peak_buffer_memory_bytes, Some(4096));
        assert_eq!(summary.system_peak_temp_directory_bytes, Some(2048));
        assert_eq!(summary.operators.len(), 2);
        assert_eq!(summary.operators[0].name, "CREATE_TABLE_AS");
        assert_eq!(summary.operators[1].depth, 1);
        assert_eq!(summary.operators[1].timing_ns, Some(750_000_000));
    }
}
