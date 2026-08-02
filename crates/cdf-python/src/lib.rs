#![doc = "Optional Python authoring and interchange boundary for cdf."]

#[cfg(test)]
use cdf_http::{HttpRequest, Redactor, SecretProvider, SecretUri, TraceEvent};
#[cfg(test)]
use cdf_kernel::{
    Batch, CdfError, PartitionId, ResourceId, Result, ScopeKey, SourcePosition, TrustLevel,
    WriteDisposition,
};
#[cfg(test)]
use pyo3::{Bound, PyAny, Python, prelude::*, types::PyModule};
#[cfg(test)]
use sha2::{Digest, Sha256};

#[allow(
    unsafe_code,
    reason = "Arrow/Python FFI exception governed by .10x/decisions/compiler-enforced-rust-safety-walls.md"
)]
mod arrow_capsule;
mod bridge;
mod bridge_types;
mod context;
mod dlt;
mod driver;
mod internal;
mod interpreter;
mod resource;
#[cfg(test)]
mod tests;

pub use bridge::{PythonResourceBridge, arrow_boundary_for};
pub use bridge_types::{
    ARROW_C_ARRAY_METHOD, ARROW_C_STREAM_METHOD, ArrowCapsuleBoundary, DEFAULT_DICT_BATCH_ROWS,
    DEFAULT_MAX_BOUNDARY_BYTES, PythonBridgeOptions, PythonFirstObservation, PythonStreamSummary,
    PythonYieldKind,
};
pub use context::{ContextLogEvent, PythonContext};
pub use dlt::{
    DLT_METADATA_ATTR, DltBridgeMappingEntry, DltBridgeMappingStatus, DltBridgeMappingTable,
    DltBridgeMetadata, DltBridgeObjectKind, DltBridgeSummary, DltCurrentStateView,
    DltIncrementalHint, DltSchemaContractHint, DltWriteDisposition, DltWriteDispositionHint,
    composite_dlt_state, dlt_current_state_view, extract_dlt_metadata, fixture_dlt_foreign_state,
    fixture_state_delta_position,
};
pub use driver::PythonSourceDriver;
pub use interpreter::{
    InterpreterReport, InterpreterRequirement, PythonConcurrencyMode, PythonExecutionSemantics,
    attached_interpreter_report, execution_semantics, inspect_interpreter,
    python_execution_lane_spec, validate_attached_interpreter,
};
pub use resource::PythonResource;
