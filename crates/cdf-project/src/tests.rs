use super::*;
use crate::discovery_manifest::{DiscoverySelectorCandidate, plan_discovery_selection};
use crate::internal::*;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    fs,
    path::Path,
    sync::{Arc, Mutex, mpsc},
    thread,
    time::Duration,
};

use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::FileWriter;
use arrow_schema::{
    DataType, Field, Fields, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
};
use bytes::Bytes;
use cdf_contract::{ContractPolicy, ObservedSchema, compile_validation_program};
use cdf_declarative::SourceDeclaration;
use cdf_engine::{EnginePlan, EnginePlanInput, Planner};
use cdf_http::{
    HttpMethod, HttpRequest, HttpResponse, HttpTransport, SecretProvider, SecretUri, SecretValue,
};
use cdf_kernel::ExecutionExtent;
use cdf_kernel::{
    BoxFuture, CapabilitySupport, CdfError, CheckpointId, ConcurrencyLimit, DestinationId,
    DestinationSheet, DiscoveryManifestHash, DiscoveryManifestReference, IdempotencySupport,
    IdentifierRules, PipelineId, QueryableResource, ResourceId, RunId, ScanRequest, SchemaHash,
    SchemaSource, SourcePosition, TargetName, TransactionSupport, TypeMapping, TypeMappingFidelity,
    WriteDisposition, source_name,
};
use cdf_memory::{
    AccountedBytes, ConsumerKey, MemoryClass, MemoryCoordinator, ReservationRequest, reserve,
};
use cdf_object_access::{
    FileIdentityMetadata, FileTransportFacade, FileTransportLocation, FileTransportResource,
    HttpFileRequest, HttpFileResponse, HttpFileTransport,
};
use cdf_runtime::{
    AccountedByteStream, ByteExtent, ByteSource, ByteSourceCapabilities, ContentIdentity,
    GenerationStrength, RunCancellation, SequentialReadRequest,
};
use cdf_source_files::{FileRuntimeDependencies, FileSourceDriver};
use flate2::{Compression, write::GzEncoder};
use futures_util::stream;
use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path as ObjectPath};
use sha2::{Digest, Sha256};
mod discovery_schema;
mod project_files;
mod project_inputs;
mod query_compiler;
mod resource_sql;
pub(crate) mod support;
