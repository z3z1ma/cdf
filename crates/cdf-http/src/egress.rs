use std::collections::BTreeSet;

use cdf_kernel::{CdfError, Result};

use crate::{
    message::{HttpRequest, HttpResponse},
    support::{host_from_url, host_matches, normalize_host},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EgressAllowlist {
    AllowAny,
    AllowHosts(BTreeSet<String>),
}

impl EgressAllowlist {
    pub fn allow_any() -> Self {
        Self::AllowAny
    }

    pub fn from_hosts(hosts: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::AllowHosts(hosts.into_iter().map(normalize_host).collect())
    }

    pub fn check(&self, request: &HttpRequest) -> Result<()> {
        let host = host_from_url(&request.url)?;
        match self {
            Self::AllowAny => Ok(()),
            Self::AllowHosts(hosts) if hosts.iter().any(|allowed| host_matches(&host, allowed)) => {
                Ok(())
            }
            Self::AllowHosts(_) => Err(CdfError::auth(format!(
                "egress to host `{host}` is denied by allowlist"
            ))),
        }
    }
}

pub trait HttpTransport: Send + Sync {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse>;
}

pub fn send_with_policy(
    transport: &dyn HttpTransport,
    allowlist: &EgressAllowlist,
    request: HttpRequest,
) -> Result<HttpResponse> {
    allowlist.check(&request)?;
    transport.send(request)
}
