use std::{collections::BTreeMap, path::PathBuf, process::ExitStatus, time::Duration};

use cdf_kernel::{
    Batch, ResourceDescriptor, SchemaSnapshotReference, SchemaSource, ScopeKey, TrustLevel,
    WriteDisposition,
};
use cdf_runtime::BoundedFormatRead;
use serde::{Deserialize, Serialize};

pub const DEFAULT_STDERR_LINE_LIMIT: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandSpec {
    pub program: String,
    pub args: Vec<String>,
    pub current_dir: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
}

impl CommandSpec {
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            current_dir: None,
            env: BTreeMap::new(),
        }
    }

    pub fn with_args(mut self, args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.args = args.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_current_dir(mut self, current_dir: impl Into<PathBuf>) -> Self {
        self.current_dir = Some(current_dir.into());
        self
    }

    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StdoutFormat {
    ArrowIpc,
    Ndjson,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SupervisionOptions {
    pub timeout: Option<Duration>,
    pub stderr_line_limit: usize,
}

impl Default for SupervisionOptions {
    fn default() -> Self {
        Self {
            timeout: None,
            stderr_line_limit: DEFAULT_STDERR_LINE_LIMIT,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StderrTrace {
    pub lines: Vec<String>,
    pub truncated: bool,
}

#[derive(Clone, Debug)]
pub struct SubprocessOutput {
    pub read: SubprocessRead,
    pub stderr: StderrTrace,
    pub exit_status: ExitStatus,
}

#[derive(Clone, Debug)]
pub struct SubprocessRead {
    pub descriptor: ResourceDescriptor,
    pub batches: Vec<Batch>,
}

impl SubprocessRead {
    pub(crate) fn from_bounded(
        read: BoundedFormatRead,
        scope: ScopeKey,
    ) -> cdf_kernel::Result<Self> {
        let schema_hash = cdf_kernel::canonical_arrow_schema_hash(read.schema.as_ref())?;
        let resource_id = read
            .batches
            .first()
            .map(|batch| batch.header.resource_id.clone())
            .ok_or_else(|| {
                cdf_kernel::CdfError::internal("bounded subprocess read emitted no batch")
            })?;
        Ok(Self {
            descriptor: ResourceDescriptor {
                resource_id: resource_id.clone(),
                schema_source: SchemaSource::Discovered {
                    snapshot: SchemaSnapshotReference {
                        schema_hash: schema_hash.clone(),
                        path: format!(".cdf/schemas/{resource_id}@{schema_hash}.json"),
                        metadata: BTreeMap::from([(
                            "probe".to_owned(),
                            "subprocess-format-driver".to_owned(),
                        )]),
                    },
                },
                primary_key: Vec::new(),
                merge_key: Vec::new(),
                cursor: None,
                write_disposition: WriteDisposition::Append,
                deduplication: None,
                contract: None,
                state_scope: scope,
                freshness: None,
                trust_level: TrustLevel::Experimental,
            },
            batches: read.batches,
        })
    }
}

impl StderrTrace {
    pub fn from_bytes(bytes: &[u8], line_limit: usize) -> Self {
        let text = String::from_utf8_lossy(bytes);
        let mut lines = Vec::new();
        let mut truncated = false;
        for line in text.lines() {
            if lines.len() == line_limit {
                truncated = true;
                break;
            }
            lines.push(line.to_owned());
        }
        Self { lines, truncated }
    }

    pub fn summary(&self) -> String {
        if self.lines.is_empty() {
            return "<empty>".to_owned();
        }
        let mut summary = self.lines.join(" | ");
        if self.truncated {
            summary.push_str(" | <truncated>");
        }
        summary
    }
}
