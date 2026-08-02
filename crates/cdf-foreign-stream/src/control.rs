use cdf_kernel::{CdfError, Result, SourcePosition, WatermarkClaim};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignControlEvent {
    pub sequence: u64,
    pub kind: ForeignControlKind,
}

impl ForeignControlEvent {
    pub fn new(sequence: u64, kind: ForeignControlKind) -> Result<Self> {
        if sequence == 0 {
            return Err(CdfError::contract(
                "foreign control event sequence must be greater than zero",
            ));
        }
        Ok(Self { sequence, kind })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ForeignControlKind {
    SourcePosition {
        position: SourcePosition,
    },
    Watermarks {
        watermarks: Vec<WatermarkClaim>,
    },
    ForeignState {
        position: SourcePosition,
    },
    /// Ordered protocol metadata whose payload is already represented by compiled schema/catalog
    /// authority. The canonical hash preserves the observed fact without retaining unbounded or
    /// potentially secret-bearing protocol JSON in the control queue.
    ProtocolMetadata {
        protocol: String,
        message_type: String,
        payload_sha256: String,
    },
    Progress {
        rows: u64,
        bytes: u64,
    },
    Diagnostic {
        severity: ForeignDiagnosticSeverity,
        message: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForeignDiagnosticSeverity {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ForeignTerminalStatus {
    Succeeded {
        final_position: Option<SourcePosition>,
    },
    Failed {
        retryable: bool,
        message: String,
    },
    Cancelled,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ForeignStreamSummary {
    pub outcome_count: u64,
    pub control_count: u64,
    pub terminal: Option<ForeignTerminalStatus>,
}
