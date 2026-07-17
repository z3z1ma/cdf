#![doc = "REST source adapter for cdf."]

use std::collections::BTreeMap;

use cdf_http::{AuthScheme, EgressAllowlist, PaginationConfig, RateLimitPolicy, SecretUri};
use cdf_kernel::PushdownFidelity;

mod driver;
mod runtime;

pub(crate) const REST_MAXIMUM_RESPONSE_BYTES: u64 = 32 * 1024 * 1024;
// Paginated execution double-buffers bounded response pages so network I/O for N+1 can overlap
// Arrow decode for N. The source scheduler admits both buffers before opening the partition.
pub(crate) const REST_MAXIMUM_POLL_BYTES: u64 = REST_MAXIMUM_RESPONSE_BYTES * 2;
// The JSON driver's compiled decode working set. Together with the two admitted response pages,
// this is a truthful 160 MiB maximum per active REST partition without double-counting.
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
