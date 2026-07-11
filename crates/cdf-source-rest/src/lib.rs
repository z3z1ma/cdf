#![doc = "REST source adapter for cdf."]

use std::collections::BTreeMap;

use cdf_http::{AuthScheme, EgressAllowlist, PaginationConfig, RateLimitPolicy};
use cdf_kernel::PushdownFidelity;

mod driver;
mod runtime;

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
