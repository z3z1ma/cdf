use cdf_http::SecretUri;
use cdf_kernel::{CdfError, Result, TableSnapshotSelector};
use serde::{Deserialize, Serialize};

pub const DEFAULT_MAXIMUM_METADATA_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_METADATA_PARSE_AMPLIFICATION_BPS: u32 = 40_000;
pub const DEFAULT_MAXIMUM_METADATA_FILES: usize = 1_000_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergSourceOptions {
    pub catalog: IcebergCatalogOptions,
    #[serde(default)]
    pub object_credentials: Option<String>,
    #[serde(default)]
    pub egress_allowlist: Vec<String>,
    #[serde(default = "default_maximum_metadata_bytes")]
    pub maximum_metadata_bytes: u64,
    #[serde(default = "default_metadata_parse_amplification_bps")]
    pub metadata_parse_amplification_bps: u32,
    #[serde(default = "default_maximum_metadata_files")]
    pub maximum_metadata_files: usize,
}

impl IcebergSourceOptions {
    pub fn validate(&self) -> Result<()> {
        self.catalog.validate()?;
        if let Some(reference) = &self.object_credentials {
            SecretUri::new(reference.clone())?;
        }
        validate_hosts(&self.egress_allowlist)?;
        if self.maximum_metadata_bytes == 0 {
            return Err(CdfError::contract(
                "Iceberg maximum_metadata_bytes must be greater than zero",
            ));
        }
        if self.metadata_parse_amplification_bps < 10_000 {
            return Err(CdfError::contract(
                "Iceberg metadata_parse_amplification_bps must be at least 10000 (1x)",
            ));
        }
        if self.maximum_metadata_files == 0 {
            return Err(CdfError::contract(
                "Iceberg maximum_metadata_files must be greater than zero",
            ));
        }
        Ok(())
    }

    pub fn catalog_identity(&self) -> String {
        self.catalog.identity()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum IcebergCatalogOptions {
    Filesystem {
        warehouse: String,
    },
    Rest {
        uri: String,
        #[serde(default)]
        warehouse: Option<String>,
        #[serde(default)]
        credentials: Option<String>,
    },
    Glue {
        region: String,
        #[serde(default)]
        catalog_id: Option<String>,
        #[serde(default)]
        warehouse: Option<String>,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        credentials: Option<String>,
    },
}

impl IcebergCatalogOptions {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Filesystem { warehouse } => validate_location("Iceberg warehouse", warehouse),
            Self::Rest {
                uri,
                warehouse,
                credentials,
            } => {
                validate_http_url("Iceberg REST catalog URI", uri)?;
                if let Some(warehouse) = warehouse {
                    validate_text("Iceberg REST warehouse", warehouse)?;
                }
                if let Some(reference) = credentials {
                    SecretUri::new(reference.clone())?;
                }
                Ok(())
            }
            Self::Glue {
                region,
                catalog_id,
                warehouse,
                endpoint,
                credentials,
            } => {
                validate_token("AWS Glue region", region)?;
                if let Some(catalog_id) = catalog_id {
                    validate_token("AWS Glue catalog id", catalog_id)?;
                }
                if let Some(warehouse) = warehouse {
                    validate_location("AWS Glue warehouse", warehouse)?;
                }
                if let Some(endpoint) = endpoint {
                    validate_http_url("AWS Glue endpoint", endpoint)?;
                }
                if let Some(reference) = credentials {
                    SecretUri::new(reference.clone())?;
                }
                Ok(())
            }
        }
    }

    pub fn identity(&self) -> String {
        match self {
            Self::Filesystem { warehouse } => format!("filesystem:{warehouse}"),
            Self::Rest { uri, warehouse, .. } => warehouse.as_ref().map_or_else(
                || format!("rest:{uri}"),
                |value| format!("rest:{uri}:{value}"),
            ),
            Self::Glue {
                region,
                catalog_id,
                endpoint,
                ..
            } => format!(
                "glue:{}:{}:{}",
                region,
                catalog_id.as_deref().unwrap_or("default"),
                endpoint.as_deref().unwrap_or("aws")
            ),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergResourceOptions {
    pub namespace: Vec<String>,
    pub table: String,
    #[serde(default)]
    pub selector: IcebergSnapshotSelector,
}

impl IcebergResourceOptions {
    pub fn validate(&self) -> Result<()> {
        if self.namespace.is_empty() {
            return Err(CdfError::contract(
                "Iceberg table namespace requires at least one component",
            ));
        }
        for component in &self.namespace {
            validate_text("Iceberg namespace component", component)?;
        }
        validate_text("Iceberg table name", &self.table)?;
        self.selector.validate()
    }

    pub fn display_name(&self) -> String {
        format!("{}.{}", self.namespace.join("."), self.table)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum IcebergSnapshotSelector {
    #[default]
    Current,
    Branch {
        name: String,
    },
    Tag {
        name: String,
    },
    Snapshot {
        snapshot_id: i64,
    },
    Timestamp {
        timestamp_ms: i64,
    },
}

impl IcebergSnapshotSelector {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Current => Ok(()),
            Self::Branch { name } | Self::Tag { name } => validate_text("Iceberg ref name", name),
            Self::Snapshot { snapshot_id } if *snapshot_id > 0 => Ok(()),
            Self::Timestamp { timestamp_ms } if *timestamp_ms >= 0 => Ok(()),
            Self::Snapshot { .. } => Err(CdfError::contract(
                "Iceberg snapshot selector requires a positive snapshot_id",
            )),
            Self::Timestamp { .. } => Err(CdfError::contract(
                "Iceberg timestamp selector requires a nonnegative timestamp_ms",
            )),
        }
    }

    pub fn position_selector(&self) -> TableSnapshotSelector {
        match self {
            Self::Current => TableSnapshotSelector::Current,
            Self::Branch { name } => TableSnapshotSelector::Branch { name: name.clone() },
            Self::Tag { name } => TableSnapshotSelector::Tag { name: name.clone() },
            Self::Snapshot { snapshot_id } => TableSnapshotSelector::Snapshot {
                snapshot_id: *snapshot_id,
            },
            Self::Timestamp { timestamp_ms } => TableSnapshotSelector::Timestamp {
                timestamp_ms: *timestamp_ms,
            },
        }
    }
}

const fn default_maximum_metadata_bytes() -> u64 {
    DEFAULT_MAXIMUM_METADATA_BYTES
}

const fn default_metadata_parse_amplification_bps() -> u32 {
    DEFAULT_METADATA_PARSE_AMPLIFICATION_BPS
}

const fn default_maximum_metadata_files() -> usize {
    DEFAULT_MAXIMUM_METADATA_FILES
}

fn validate_hosts(hosts: &[String]) -> Result<()> {
    let mut sorted = hosts.to_vec();
    sorted.sort();
    if sorted.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(CdfError::contract(
            "Iceberg egress_allowlist entries must be unique",
        ));
    }
    for host in hosts {
        validate_text("Iceberg egress host", host)?;
        if host.contains("//") || host.contains(['/', '?', '#', '@']) {
            return Err(CdfError::contract(
                "Iceberg egress_allowlist entries must be host names, not URLs",
            ));
        }
    }
    Ok(())
}

fn validate_location(label: &str, value: &str) -> Result<()> {
    validate_text(label, value)?;
    if value.contains(['?', '#']) {
        return Err(CdfError::contract(format!(
            "{label} cannot contain query parameters or fragments"
        )));
    }
    Ok(())
}

fn validate_http_url(label: &str, value: &str) -> Result<()> {
    let parsed = url::Url::parse(value)
        .map_err(|error| CdfError::contract(format!("{label} is invalid: {error}")))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(CdfError::contract(format!(
            "{label} requires an HTTP(S) URL without userinfo, query, or fragment"
        )));
    }
    Ok(())
}

fn validate_token(label: &str, value: &str) -> Result<()> {
    validate_text(label, value)?;
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(CdfError::contract(format!(
            "{label} must be a safe ASCII token"
        )));
    }
    Ok(())
}

fn validate_text(label: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(CdfError::contract(format!(
            "{label} must be nonempty text without control characters"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selectors_are_exclusive_and_portable() {
        let resource: IcebergResourceOptions = serde_json::from_value(serde_json::json!({
            "namespace": ["analytics"],
            "table": "events",
            "selector": {"kind": "branch", "name": "audit"}
        }))
        .unwrap();
        resource.validate().unwrap();
        assert_eq!(
            resource.selector.position_selector(),
            TableSnapshotSelector::Branch {
                name: "audit".to_owned()
            }
        );
        assert!(
            serde_json::from_value::<IcebergResourceOptions>(serde_json::json!({
                "namespace": ["analytics"],
                "table": "events",
                "selector": {"kind": "snapshot", "snapshot_id": 7, "name": "ambiguous"}
            }))
            .is_err()
        );
    }

    #[test]
    fn all_capacity_limits_are_explicit_knobs() {
        let source: IcebergSourceOptions = serde_json::from_value(serde_json::json!({
            "catalog": {"kind": "filesystem", "warehouse": ".warehouse"}
        }))
        .unwrap();
        source.validate().unwrap();
        assert_eq!(source.maximum_metadata_bytes, 64 * 1024 * 1024);
        assert_eq!(source.metadata_parse_amplification_bps, 40_000);
        assert_eq!(source.maximum_metadata_files, 1_000_000);
    }
}
