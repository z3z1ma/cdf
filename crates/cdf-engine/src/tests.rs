use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    task::Poll,
};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray, TimestampMillisecondArray,
    builder::{Int32Builder, MapBuilder, StringBuilder, StringDictionaryBuilder},
    types::Int32Type,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use cdf_contract::{
    CDF_VARIANT_SEMANTIC, ContractPolicy, DeclarativeExpression, DedupKeep, FieldCoercionDecision,
    FieldDisposition, NestedDataPolicy, ObservedSchema, RESIDUAL_ENCODING_METADATA_KEY,
    RESIDUAL_ENCODING_NAME, RedactionDecision, RowRule, RowViolationDisposition,
    VARIANT_COLUMN_NAME, compile_resource_validation_program, compile_validation_program,
    reconcile_schema,
};
use cdf_kernel::{
    BackpressureSupport, Batch, BatchHeader, BatchId, BatchStream, CapabilitySupport,
    ContentObjectKey, ContentProviderGeneration, ContentStoreNamespace, ContractRef,
    CursorPosition, CursorValue, DeduplicationSpec, DeliveryGuarantee,
    DiscoveryExecutorBudgetEvidence, DiscoveryManifestHash, DiscoveryManifestReference,
    DrainTermination, EXECUTION_EXTENT_VERSION, EffectiveSchemaCatalogEntry,
    EffectiveSchemaEvidence, EffectiveSchemaObservationEvidence, EffectiveSchemaRuntime,
    EpochClosureTrigger, EstimateSupport, EventTimeDomain, ExecutionExtent, FileManifest,
    FilePosition, FilterCapabilities, FreshnessSpec, IncrementalShape, LateDataAction,
    PLAN_PHYSICAL_SCHEMA_HASH_KEY, PLAN_SCHEMA_OBSERVATION_BINDING_KEY,
    PLAN_SCHEMA_OBSERVATION_ID_KEY, PLANNED_TASK_SET_REFERENCE_VERSION, PartitionAttestation,
    PartitionAuthority, PartitionId, PartitionPlan, PartitioningCapabilities,
    PlannedTaskSetReference, PreContractObservedValue, PreContractQuarantineFact,
    PreContractResidualCandidate, PredicateId, PushdownFidelity, QueryableResource,
    ResourceCapabilities, ResourceDescriptor, ResourceId, ResourceStream, Result, RunId, RunPhase,
    RunPhaseStatus, STRATIFIED_HASH_SELECTOR_V1, STREAM_EPOCH_POLICY_VERSION, SafeFrontierPolicy,
    ScanPlan, ScanPredicate, ScanRequest, SchemaBaselineReference, SchemaHash,
    SchemaObservationFieldQuarantine, SchemaSnapshotReference, SchemaSource, ScopeKey,
    SourcePosition, StreamEpochPolicy, TerminalSchemaObservationQuarantine, TrustLevel,
    WATERMARK_CLAIM_VERSION, WatermarkAuthority, WatermarkClaim, WatermarkObservationContext,
    WatermarkPolicy, WatermarkValue, WriteDisposition, source_name, with_semantic,
};
use cdf_package_contract::{
    DEDUP_SUMMARY_FILE, LATE_DATA_PAYLOAD_CATALOG_FILE, LateDataPayloadCatalog,
    LateDataPayloadLocation, PackageStatus, QuarantineObservedValue, SegmentEntry,
};
use datafusion::{
    catalog::TableProvider, physical_plan::common::collect as collect_stream, prelude::*,
};
use futures_executor::block_on;
use futures_util::{StreamExt, stream};
use proptest::{
    prelude::*,
    test_runner::{Config as ProptestConfig, RngAlgorithm, RngSeed, TestCaseError, TestRunner},
};
use tempfile::TempDir;
use tracing::{
    Event, Id, Metadata, Subscriber,
    field::{Field as TracingField, Visit},
    span::{Attributes, Record},
};

use super::*;

fn semantic_field(field: Field, reference: &str) -> Field {
    with_semantic(field, &reference.parse().unwrap())
}

mod determinism;
mod execution;
mod expression_ir;
mod package_evidence;
mod planning;
mod retry_drain;
mod schema_admission;
mod segmentation;
mod sql_analysis;
mod support;
mod workers;
