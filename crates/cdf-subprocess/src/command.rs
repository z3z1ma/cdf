use std::{
    collections::BTreeMap,
    path::PathBuf,
    process::ExitStatus,
    sync::{Arc, Mutex},
    time::Duration,
};

use cdf_kernel::{
    Batch, ResourceDescriptor, ResourceId, SchemaHash, SchemaSnapshotReference, SchemaSource,
    ScopeKey, TrustLevel, WriteDisposition,
};
use cdf_memory::{AccountedBytes, MemoryLease};
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
    pub maximum_stdout_bytes: u64,
    pub maximum_stderr_bytes: u64,
}

impl Default for SupervisionOptions {
    fn default() -> Self {
        Self {
            timeout: None,
            stderr_line_limit: DEFAULT_STDERR_LINE_LIMIT,
            maximum_stdout_bytes: 64 * 1024 * 1024,
            maximum_stderr_bytes: 256 * 1024,
        }
    }
}

#[derive(Debug)]
pub struct BoundedCommandOutput {
    pub stdout: BoundedCommandBytes,
    pub stderr: StderrTrace,
    pub exit_status: ExitStatus,
}

#[derive(Debug)]
pub struct BoundedCommandBytes {
    bytes: Vec<u8>,
    lease: Option<MemoryLease>,
}

impl BoundedCommandBytes {
    pub(crate) fn new(bytes: Vec<u8>, lease: MemoryLease) -> cdf_kernel::Result<Self> {
        let lease = if bytes.is_empty() {
            None
        } else {
            lease.reconcile(u64::try_from(bytes.len()).map_err(|_| {
                cdf_kernel::CdfError::data("subprocess output length exceeds u64")
            })?)?;
            Some(lease)
        };
        Ok(Self { bytes, lease })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_accounted(self) -> cdf_kernel::Result<AccountedBytes> {
        let lease = self.lease.ok_or_else(|| {
            cdf_kernel::CdfError::data("subprocess stdout did not contain any bytes")
        })?;
        AccountedBytes::new(bytes::Bytes::from(self.bytes), lease)
    }
}

impl AsRef<[u8]> for BoundedCommandBytes {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

#[derive(Debug)]
pub struct StderrTrace {
    bytes: BoundedCommandBytes,
    line_limit: usize,
}

#[derive(Debug)]
pub struct SubprocessOutput {
    pub read: SubprocessRead,
    pub stderr: StderrTrace,
    pub exit_status: ExitStatus,
}

pub struct SubprocessStreamOutput {
    pub descriptor: ResourceDescriptor,
    pub batches: cdf_runtime::FormatBatchStream,
    pub completion: SubprocessCompletionHandle,
}

#[derive(Debug)]
pub struct SubprocessCompletion {
    pub stderr: StderrTrace,
    pub exit_status: ExitStatus,
}

#[derive(Clone, Debug, Default)]
pub struct SubprocessCompletionHandle {
    inner: Arc<Mutex<Option<SubprocessCompletion>>>,
}

impl SubprocessCompletionHandle {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn complete(&self, completion: SubprocessCompletion) -> cdf_kernel::Result<()> {
        let mut guard = self.inner.lock().unwrap();
        if guard.is_some() {
            return Err(cdf_kernel::CdfError::internal(
                "subprocess stream completion was recorded twice",
            ));
        }
        *guard = Some(completion);
        Ok(())
    }

    pub fn take(&self) -> cdf_kernel::Result<SubprocessCompletion> {
        self.inner
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| cdf_kernel::CdfError::internal("subprocess stream has not completed"))
    }
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
            descriptor: descriptor_for_schema_hash(
                resource_id,
                schema_hash,
                scope,
                "subprocess-format-driver",
            ),
            batches: read.batches,
        })
    }
}

pub(crate) fn descriptor_for_schema_hash(
    resource_id: ResourceId,
    schema_hash: SchemaHash,
    scope: ScopeKey,
    probe: &'static str,
) -> ResourceDescriptor {
    ResourceDescriptor {
        resource_id: resource_id.clone(),
        schema_source: SchemaSource::Discovered {
            snapshot: SchemaSnapshotReference {
                schema_hash: schema_hash.clone(),
                path: format!(".cdf/schemas/{resource_id}@{schema_hash}.json"),
                metadata: BTreeMap::from([("probe".to_owned(), probe.to_owned())]),
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
    }
}

impl StderrTrace {
    pub(crate) fn new(bytes: BoundedCommandBytes, line_limit: usize) -> Self {
        Self { bytes, line_limit }
    }

    pub fn lines(&self) -> Vec<String> {
        let text = String::from_utf8_lossy(self.bytes.as_bytes());
        let mut lines = Vec::new();
        for line in text.lines() {
            if lines.len() == self.line_limit {
                break;
            }
            lines.push(line.to_owned());
        }
        lines
    }

    pub fn is_truncated(&self) -> bool {
        String::from_utf8_lossy(self.bytes.as_bytes())
            .lines()
            .nth(self.line_limit)
            .is_some()
    }

    pub fn summary(&self) -> String {
        let lines = self.lines();
        if lines.is_empty() {
            return "<empty>".to_owned();
        }
        let mut summary = lines.join(" | ");
        if self.is_truncated() {
            summary.push_str(" | <truncated>");
        }
        summary
    }
}
