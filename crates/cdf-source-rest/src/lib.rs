#![doc = "REST source adapter for cdf."]

use std::collections::BTreeMap;

use cdf_http::{AuthScheme, EgressAllowlist, PaginationConfig, RateLimitPolicy};
use cdf_kernel::PushdownFidelity;

mod driver;
mod runtime;

pub(crate) const REST_MAXIMUM_BATCH_BYTES: u64 = 32 * 1024 * 1024;
// serde_json's owned DOM is retained only for one bounded REST page. The reservation covers the
// worst structural case (small scalar/container tokens, geometric Vec capacity, map nodes,
// decoded strings), the temporary NDJSON projection, and allocator slack. B5 replaces this
// conservative execution path with the Arrow tape decoder; until then scheduler truth is more
// important than optimistic concurrency.
pub(crate) const REST_JSON_SCRATCH_MULTIPLIER: u64 = 96;
pub(crate) const REST_MAXIMUM_DECODE_BYTES: u64 =
    REST_MAXIMUM_BATCH_BYTES * (REST_JSON_SCRATCH_MULTIPLIER + 2);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestResourcePlan {
    pub source: String,
    pub base_url: String,
    pub path: String,
    pub params: BTreeMap<String, String>,
    pub record_selector: String,
    pub pagination: Option<PaginationConfig>,
    pub auth: Option<AuthScheme>,
    pub rate_limit: RateLimitPolicy,
    pub respect_headers: Vec<String>,
    pub allowlist: EgressAllowlist,
    pub cursor_param: Option<String>,
    pub cursor_filter_fidelity: PushdownFidelity,
    pub records_transform: Option<String>,
}

pub use driver::RestSourceDriver;
pub use runtime::*;
