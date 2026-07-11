#![doc = "File and object-store source adapter for cdf."]

use cdf_http::{AuthScheme, EgressAllowlist, SecretUri};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

mod driver;
mod runtime;
mod transport;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileFormatDeclaration {
    Csv,
    Json,
    Ndjson,
    Parquet,
    ArrowIpc,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileCompressionDeclaration {
    Auto,
    None,
    Gzip,
    Zstd,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileResourcePlan {
    pub source: String,
    pub root: String,
    pub glob: String,
    pub format: FileFormatDeclaration,
    pub format_declared: bool,
    pub compression: FileCompressionDeclaration,
    pub auth: Option<AuthScheme>,
    pub credentials: Option<SecretUri>,
    pub allowlist: EgressAllowlist,
}

pub use driver::FileSourceDriver;
pub use runtime::*;
pub use transport::*;

#[cfg(test)]
pub(crate) fn test_execution_services() -> cdf_runtime::ExecutionServices {
    static SERVICES: std::sync::OnceLock<cdf_runtime::ExecutionServices> =
        std::sync::OnceLock::new();
    SERVICES
        .get_or_init(|| {
            cdf_engine::StandaloneExecutionHost::default_services(128 * 1024 * 1024)
                .expect("file source test execution host")
                .1
        })
        .clone()
}
