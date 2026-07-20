use std::io::Write;

use cdf_kernel::{CdfError, CompiledScanIntent, FilePosition, Result};
use cdf_runtime::artifact_hash;
use serde::{Deserialize, Serialize};

use crate::model::GlueFormatMapping;

pub const GLUE_TASK_SET_TYPE: &str = "glue-object-v1";
pub const GLUE_TASK_VERSION: u16 = 1;
pub const GLUE_TASK_AUTHORITY_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlueTaskAuthority {
    pub version: u16,
    pub region: String,
    pub catalog_id: Option<String>,
    pub database: String,
    pub table: String,
    pub table_generation: String,
    pub partition_expression: Option<String>,
    pub scan_intent: CompiledScanIntent,
}

impl GlueTaskAuthority {
    pub fn validate(&self) -> Result<()> {
        if self.version != GLUE_TASK_AUTHORITY_VERSION {
            return Err(CdfError::contract(format!(
                "Glue task authority version {} is unsupported; expected {GLUE_TASK_AUTHORITY_VERSION}",
                self.version
            )));
        }
        for (label, value) in [
            ("region", self.region.as_str()),
            ("database", self.database.as_str()),
            ("table", self.table.as_str()),
            ("table generation", self.table_generation.as_str()),
        ] {
            if value.is_empty() || value.chars().any(char::is_control) {
                return Err(CdfError::contract(format!(
                    "Glue task authority {label} must be nonempty and control-free"
                )));
            }
        }
        self.scan_intent.validate()
    }

    pub fn content_sha256(&self) -> Result<String> {
        self.validate()?;
        artifact_hash(self)
    }

    pub fn encode_to(&self, output: &mut dyn Write) -> Result<()> {
        self.validate()?;
        serde_json::to_writer(output, self)
            .map_err(|error| CdfError::data(format!("encode Glue task authority: {error}")))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlueObjectTask {
    pub version: u16,
    pub canonical_ordinal: u64,
    pub file: FilePosition,
    pub format: GlueFormatMapping,
    pub data_columns: Vec<String>,
    pub partition_values: Vec<Option<String>>,
}

impl GlueObjectTask {
    pub fn validate_against(&self, authority: &GlueTaskAuthority) -> Result<()> {
        authority.validate()?;
        if self.version != GLUE_TASK_VERSION {
            return Err(CdfError::contract(format!(
                "Glue object task version {} is unsupported; expected {GLUE_TASK_VERSION}",
                self.version
            )));
        }
        cdf_kernel::SourcePosition::FileManifest(cdf_kernel::FileManifest {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            files: vec![self.file.clone()],
        })
        .validate()?;
        if self.format.format_id.is_empty() || !self.format.options.is_object() {
            return Err(CdfError::contract(
                "Glue task requires a format id and object-valued format options",
            ));
        }
        if self.data_columns.iter().any(String::is_empty) {
            return Err(CdfError::contract(
                "Glue task data-column names must be nonempty",
            ));
        }
        Ok(())
    }

    pub fn content_sha256(&self) -> Result<String> {
        artifact_hash(self)
    }

    pub fn encode_to(&self, output: &mut dyn Write) -> Result<()> {
        serde_json::to_writer(output, self)
            .map_err(|error| CdfError::data(format!("encode Glue object task: {error}")))
    }
}
