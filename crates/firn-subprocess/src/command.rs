use std::{collections::BTreeMap, path::PathBuf, process::ExitStatus, time::Duration};

use firn_formats::FormatRead;
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
    pub read: FormatRead,
    pub stderr: StderrTrace,
    pub exit_status: ExitStatus,
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
