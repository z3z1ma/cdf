#![doc = "Project configuration and orchestration boundary for firn."]

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fmt, fs,
    path::{Path, PathBuf},
};

use firn_contract::NORMALIZER_NAMECASE_V1;
use firn_declarative::{
    CompiledResource, CompiledResourcePlan, DeclarativeDocument, compile_document,
    parse_toml as parse_declarative_toml, parse_yaml as parse_declarative_yaml,
};
use firn_http::{AuthScheme, SecretProvider, SecretUri, SecretValue};
use firn_kernel::{
    DestinationSheet, FirnError, ResourceCapabilities, ResourceDescriptor, Result, SchemaSource,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use sha2::{Digest, Sha256};

pub const PROJECT_FILE_NAME: &str = "firn.toml";
pub const LOCK_FILE_NAME: &str = "firn.lock";
pub const LOCKFILE_VERSION: u16 = 1;

mod internal;
mod lockfile;
mod models;
mod runtime;
#[cfg(test)]
mod runtime_tests;
mod secrets;
mod sources;
#[cfg(test)]
mod tests;

pub use lockfile::*;
pub use models::*;
pub use runtime::*;
pub use secrets::*;
pub use sources::*;
