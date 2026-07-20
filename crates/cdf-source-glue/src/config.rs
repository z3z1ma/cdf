use cdf_http::SecretUri;
use cdf_kernel::{CdfError, Result};
use serde::{Deserialize, Serialize};

pub const DEFAULT_MAXIMUM_RESPONSE_BYTES: u64 = 16 * 1024 * 1024;
pub const DEFAULT_MAXIMUM_PARTITIONS: usize = 1_000_000;
pub const DEFAULT_MAXIMUM_OBJECTS: usize = 10_000_000;
pub const DEFAULT_MAXIMUM_TASK_BYTES: u64 = 256 * 1024;
pub const DEFAULT_MAXIMUM_TASK_AUTHORITY_BYTES: u64 = 16 * 1024 * 1024;
pub const DEFAULT_TASK_WRITER_BUFFER_BYTES: usize = 1024 * 1024;
pub const DEFAULT_BATCH_ROWS: usize = 64 * 1024;
pub const DEFAULT_MAXIMUM_BATCH_BYTES: u64 = 32 * 1024 * 1024;
pub const DEFAULT_MAXIMUM_CONCURRENCY: u16 = u16::MAX;
pub const DEFAULT_STREAM_BUFFER_BATCHES: u16 = 2;
pub const DEFAULT_PLANNING_SPILL_GROWTH_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlueSourceOptions {
    pub region: String,
    #[serde(default)]
    pub catalog_id: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub credentials: Option<String>,
    #[serde(default)]
    pub object_credentials: Option<String>,
    #[serde(default)]
    pub egress_allowlist: Vec<String>,
    #[serde(default = "default_maximum_response_bytes")]
    pub maximum_response_bytes: u64,
    #[serde(default = "default_maximum_partitions")]
    pub maximum_partitions: usize,
    #[serde(default = "default_maximum_objects")]
    pub maximum_objects: usize,
    #[serde(default = "default_maximum_task_bytes")]
    pub maximum_task_bytes: u64,
    #[serde(default = "default_maximum_task_authority_bytes")]
    pub maximum_task_authority_bytes: u64,
    #[serde(default = "default_task_writer_buffer_bytes")]
    pub task_writer_buffer_bytes: usize,
    #[serde(default = "default_batch_rows")]
    pub batch_rows: usize,
    #[serde(default = "default_maximum_batch_bytes")]
    pub maximum_batch_bytes: u64,
    #[serde(default = "default_maximum_concurrency")]
    pub maximum_concurrency: u16,
    #[serde(default = "default_stream_buffer_batches")]
    pub stream_buffer_batches: u16,
    #[serde(default = "default_planning_spill_growth_bytes")]
    pub planning_spill_growth_bytes: u64,
}

impl GlueSourceOptions {
    pub fn validate(&self) -> Result<()> {
        require_text("Glue region", &self.region)?;
        if let Some(catalog_id) = &self.catalog_id {
            require_text("Glue catalog id", catalog_id)?;
        }
        if let Some(endpoint) = &self.endpoint {
            let parsed = url::Url::parse(endpoint)
                .map_err(|_| CdfError::contract("Glue endpoint must be an absolute HTTP URL"))?;
            if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
                return Err(CdfError::contract(
                    "Glue endpoint must be an absolute HTTP URL with a host",
                ));
            }
        }
        for reference in [&self.credentials, &self.object_credentials]
            .into_iter()
            .flatten()
        {
            SecretUri::new(reference.clone())?;
        }
        let mut hosts = self.egress_allowlist.clone();
        hosts.sort();
        if hosts.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(CdfError::contract(
                "Glue egress allowlist hosts must be unique",
            ));
        }
        for host in hosts {
            require_text("Glue egress allowlist host", &host)?;
        }
        if self.maximum_response_bytes == 0
            || self.maximum_partitions == 0
            || self.maximum_objects == 0
            || self.maximum_task_bytes == 0
            || self.maximum_task_authority_bytes == 0
            || self.task_writer_buffer_bytes == 0
            || self.batch_rows == 0
            || self.maximum_batch_bytes < 8 * 1024
            || self.maximum_concurrency == 0
            || self.stream_buffer_batches == 0
            || self.planning_spill_growth_bytes < 8192
        {
            return Err(CdfError::contract(
                "Glue response, inventory, task, batch, concurrency, and stream bounds must be nonzero and maximum_batch_bytes must be at least 8192",
            ));
        }
        self.execution_working_set_bytes()?;
        Ok(())
    }

    pub fn execution_working_set_bytes(&self) -> Result<u64> {
        self.maximum_batch_bytes
            .checked_mul(u64::from(self.stream_buffer_batches) + 1)
            .ok_or_else(|| CdfError::contract("Glue execution working-set knobs overflow u64"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlueResourceOptions {
    pub database: String,
    pub table: String,
    #[serde(default)]
    pub partition_expression: Option<String>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default = "default_format_options")]
    pub format_options: serde_json::Value,
}

impl GlueResourceOptions {
    pub fn validate(&self) -> Result<()> {
        require_text("Glue database", &self.database)?;
        require_text("Glue table", &self.table)?;
        if let Some(expression) = &self.partition_expression {
            require_text("Glue partition expression", expression)?;
        }
        if let Some(format) = &self.format {
            require_text("Glue format override", format)?;
        }
        if !self.format_options.is_object() {
            return Err(CdfError::contract(
                "Glue format_options must be a JSON object",
            ));
        }
        Ok(())
    }

    pub fn display_name(&self) -> String {
        format!("{}.{}", self.database, self.table)
    }
}

fn require_text(label: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(CdfError::contract(format!(
            "{label} must be nonempty and control-free"
        )));
    }
    Ok(())
}

const fn default_maximum_response_bytes() -> u64 {
    DEFAULT_MAXIMUM_RESPONSE_BYTES
}
const fn default_maximum_partitions() -> usize {
    DEFAULT_MAXIMUM_PARTITIONS
}
const fn default_maximum_objects() -> usize {
    DEFAULT_MAXIMUM_OBJECTS
}
const fn default_maximum_task_bytes() -> u64 {
    DEFAULT_MAXIMUM_TASK_BYTES
}
const fn default_maximum_task_authority_bytes() -> u64 {
    DEFAULT_MAXIMUM_TASK_AUTHORITY_BYTES
}
const fn default_task_writer_buffer_bytes() -> usize {
    DEFAULT_TASK_WRITER_BUFFER_BYTES
}
const fn default_batch_rows() -> usize {
    DEFAULT_BATCH_ROWS
}
const fn default_maximum_batch_bytes() -> u64 {
    DEFAULT_MAXIMUM_BATCH_BYTES
}
const fn default_maximum_concurrency() -> u16 {
    DEFAULT_MAXIMUM_CONCURRENCY
}
const fn default_stream_buffer_batches() -> u16 {
    DEFAULT_STREAM_BUFFER_BATCHES
}
const fn default_planning_spill_growth_bytes() -> u64 {
    DEFAULT_PLANNING_SPILL_GROWTH_BYTES
}

fn default_format_options() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omitted_format_options_compile_to_an_empty_object() {
        let options: GlueResourceOptions = serde_json::from_value(serde_json::json!({
            "database": "analytics",
            "table": "events"
        }))
        .unwrap();
        options.validate().unwrap();
        assert_eq!(options.format_options, serde_json::json!({}));
    }
}
