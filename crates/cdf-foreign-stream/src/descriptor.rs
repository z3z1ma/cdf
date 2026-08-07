use std::fmt;

use cdf_kernel::{CdfError, Result};
use serde::{Deserialize, Serialize};

pub use cdf_kernel::{
    SourceCopyClassification as ForeignCopyClassification,
    SourceExecutionLane as ForeignExecutionLane, SourceTransferMode as ForeignTransferMode,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignProducerDescriptor {
    pub producer_id: ForeignProducerId,
    pub protocol_version: ForeignProtocolVersion,
    pub transfer_modes: Vec<ForeignTransferMode>,
    pub schema_acquisition: ForeignSchemaAcquisition,
    pub startup: ForeignStartupModel,
    pub lanes: ForeignLaneCapabilities,
    pub memory: ForeignMemoryContract,
    pub cancellation: ForeignCancellationContract,
    pub state: ForeignStateContract,
    pub security: ForeignSecurityContract,
}

/// How a foreign producer supplies the fixed schema required before execution begins.
///
/// A declared handshake is metadata-only. A stream bootstrap starts the real producer and
/// requires the compiler to retain that same invocation across the schema-authority barrier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForeignSchemaAcquisition {
    DeclaredHandshake,
    StreamBootstrap,
}

impl ForeignProducerDescriptor {
    pub fn validate(&self) -> Result<()> {
        self.producer_id.validate()?;
        self.protocol_version.validate()?;
        if self.transfer_modes.is_empty() {
            return Err(CdfError::contract(
                "foreign producer descriptor requires at least one transfer mode",
            ));
        }
        if self.lanes.maximum_internal_parallelism == 0 {
            return Err(CdfError::contract(
                "foreign producer lane parallelism must be greater than zero",
            ));
        }
        self.memory.validate()?;
        self.security.validate()
    }

    pub fn supports_transfer_mode(&self, mode: ForeignTransferMode) -> bool {
        self.transfer_modes.contains(&mode)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ForeignProducerId(String);

impl ForeignProducerId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let id = Self(value.into());
        id.validate()?;
        Ok(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<()> {
        validate_token("foreign producer id", &self.0, 128)
    }
}

impl fmt::Display for ForeignProducerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ForeignProtocolVersion(String);

impl ForeignProtocolVersion {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let version = Self(value.into());
        version.validate()?;
        Ok(version)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<()> {
        validate_token("foreign protocol version", &self.0, 64)
    }
}

impl fmt::Display for ForeignProtocolVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForeignStartupModel {
    InProcessAttached,
    ChildProcess,
    Sandbox,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignLaneCapabilities {
    pub execution_lane: ForeignExecutionLane,
    pub maximum_internal_parallelism: u16,
    pub backpressure: ForeignBackpressure,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForeignBackpressure {
    Pull,
    Pipe,
    HostWindow,
    UnsupportedBounded,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignMemoryContract {
    pub payload_window_bytes: Option<u64>,
    pub control_queue_bytes: Option<u64>,
    pub diagnostic_queue_bytes: Option<u64>,
    pub native_scratch_bytes: Option<u64>,
    pub child_process_bytes: Option<u64>,
}

impl ForeignMemoryContract {
    pub fn validate(&self) -> Result<()> {
        validate_optional_positive("payload window bytes", self.payload_window_bytes)?;
        validate_optional_positive("control queue bytes", self.control_queue_bytes)?;
        validate_optional_positive("diagnostic queue bytes", self.diagnostic_queue_bytes)?;
        validate_optional_positive("native scratch bytes", self.native_scratch_bytes)?;
        validate_optional_positive("child process bytes", self.child_process_bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignCancellationContract {
    pub cooperative_stop: bool,
    pub interrupt_safe: bool,
    pub force_termination_authorized: bool,
    pub drains_on_cancel: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignStateContract {
    pub emits_positions: bool,
    pub emits_watermarks: bool,
    pub emits_foreign_state: bool,
    pub terminal_state_required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignSecurityContract {
    pub ambient_network: bool,
    pub ambient_filesystem: bool,
    pub secret_names: Vec<String>,
}

impl ForeignSecurityContract {
    pub fn validate(&self) -> Result<()> {
        for secret_name in &self.secret_names {
            validate_token("foreign secret name", secret_name, 256)?;
        }
        Ok(())
    }
}

fn validate_optional_positive(label: &str, value: Option<u64>) -> Result<()> {
    if value == Some(0) {
        return Err(CdfError::contract(format!(
            "foreign producer {label} must be greater than zero when configured"
        )));
    }
    Ok(())
}

fn validate_token(label: &str, value: &str, max_len: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > max_len
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(CdfError::contract(format!(
            "{label} must contain 1..={max_len} non-whitespace, control-free characters",
        )));
    }
    Ok(())
}
