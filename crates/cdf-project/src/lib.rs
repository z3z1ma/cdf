#![doc = "Project configuration and orchestration boundary for cdf."]

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fmt, fs,
    path::{Path, PathBuf},
};

use cdf_contract::NORMALIZER_NAMECASE_V1;
use cdf_declarative::{
    CompiledResource, DeclarativeDocument, compile_document, compile_document_with_project_root,
    parse_toml as parse_declarative_toml, parse_yaml as parse_declarative_yaml,
};
use cdf_http::{SecretProvider, SecretUri, SecretValue};
use cdf_kernel::{
    CdfError, DestinationSheet, ResourceCapabilities, ResourceDescriptor, Result, SchemaSource,
};
use cdf_runtime::SourceRegistry;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use sha2::{Digest, Sha256};

pub const PROJECT_FILE_NAME: &str = "cdf.toml";
pub const LOCK_FILE_NAME: &str = "cdf.lock";
pub const LOCKFILE_VERSION: u16 = 1;

mod backfill;
mod discovery_manifest;
mod internal;
mod lock_cas;
mod lockfile;
mod models;
mod observation_cache;
mod project_files;
mod promotion;
mod runtime;
#[cfg(test)]
mod runtime_tests;
mod scaffold;
mod schema_discovery;
mod schema_snapshot;
mod secrets;
mod sources;
#[cfg(test)]
mod tests;

pub use backfill::*;
pub use discovery_manifest::*;
pub use lock_cas::*;
pub use lockfile::*;
pub use models::*;
pub use observation_cache::*;
pub use project_files::*;
pub use promotion::*;
pub use runtime::*;
pub use scaffold::*;
pub use schema_discovery::*;
pub use schema_snapshot::*;
pub use secrets::*;
pub use sources::*;
