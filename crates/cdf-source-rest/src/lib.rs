#![doc = "REST source adapter for cdf."]

use std::collections::BTreeMap;

use cdf_http::{AuthScheme, EgressAllowlist, PaginationConfig, RateLimitPolicy, SecretUri};
use cdf_kernel::PushdownFidelity;

mod driver;
mod runtime;

pub(crate) const REST_MAXIMUM_BATCH_BYTES: u64 = 32 * 1024 * 1024;
// The JSON driver's compiled decode working set. Source admission separately accounts the retained
// response page, for a truthful 128 MiB maximum per active REST partition without double-counting.
pub(crate) const REST_MAXIMUM_DECODE_BYTES: u64 = 96 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestParameterValue {
    Literal(String),
    Secret(SecretUri),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestResourcePlan {
    pub source: String,
    pub base_url: String,
    pub path: String,
    pub params: BTreeMap<String, RestParameterValue>,
    pub record_selector: String,
    pub pagination: Option<PaginationConfig>,
    pub auth: Option<AuthScheme>,
    pub rate_limit: RateLimitPolicy,
    pub quota_authority: String,
    pub respect_headers: Vec<String>,
    pub allowlist: EgressAllowlist,
    pub cursor_param: Option<String>,
    pub cursor_filter_fidelity: PushdownFidelity,
    pub records_transform: Option<String>,
}

pub use driver::RestSourceDriver;
pub use runtime::*;
