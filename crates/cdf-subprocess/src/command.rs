use std::{collections::BTreeMap, path::PathBuf, process::ExitStatus, time::Duration};

use cdf_memory::{AccountedBytes, MemoryLease};
use serde::{Deserialize, Serialize};

use crate::StreamIdentity;

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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubprocessProtocol {
    ArrowIpc,
    Ndjson,
    Singer { stream: StreamIdentity },
    Airbyte { stream: StreamIdentity },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SupervisionOptions {
    pub timeout: Option<Duration>,
    /// Grace period between cooperative process-group termination and forced termination.
    pub termination_grace: Duration,
    pub stderr_line_limit: usize,
    /// Maximum stdout retained by [`crate::run_bounded_command`]. This is not a streaming limit.
    pub maximum_stdout_bytes: u64,
    /// Optional total-transfer policy for streaming stdout. `None` permits an arbitrarily long
    /// stream while in-flight chunks remain memory-ledger bounded.
    pub maximum_streamed_stdout_bytes: Option<u64>,
    /// Maximum in-flight stdout chunk requested from the child pipe.
    pub maximum_stream_chunk_bytes: u64,
    /// Maximum bytes in one Singer/Airbyte NDJSON message, excluding an optional line ending.
    pub maximum_protocol_line_bytes: u64,
    /// Accounted transient DOM/serialization scratch for one Singer/Airbyte message. Protocol
    /// admission requires this to be at least 32 times the configured line payload boundary.
    pub protocol_parser_scratch_bytes: u64,
    /// Maximum record bytes accumulated before one protocol row-compat decode window is flushed.
    pub protocol_row_window_bytes: u64,
    /// Optional per-process virtual address-space fence inherited by child descendants.
    pub maximum_child_address_space_bytes: Option<u64>,
    pub maximum_stderr_bytes: u64,
}

impl Default for SupervisionOptions {
    fn default() -> Self {
        Self {
            timeout: None,
            termination_grace: Duration::from_millis(250),
            stderr_line_limit: DEFAULT_STDERR_LINE_LIMIT,
            maximum_stdout_bytes: 64 * 1024 * 1024,
            maximum_streamed_stdout_bytes: None,
            maximum_stream_chunk_bytes: 16 * 1024 * 1024,
            maximum_protocol_line_bytes: 1024 * 1024,
            protocol_parser_scratch_bytes: 32 * 1024 * 1024,
            protocol_row_window_bytes: 16 * 1024 * 1024,
            maximum_child_address_space_bytes: None,
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
    discarded_bytes: u64,
}

impl StderrTrace {
    pub(crate) fn new(bytes: BoundedCommandBytes, line_limit: usize, discarded_bytes: u64) -> Self {
        Self {
            bytes,
            line_limit,
            discarded_bytes,
        }
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
        self.discarded_bytes > 0
            || String::from_utf8_lossy(self.bytes.as_bytes())
                .lines()
                .nth(self.line_limit)
                .is_some()
    }

    pub fn discarded_bytes(&self) -> u64 {
        self.discarded_bytes
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
