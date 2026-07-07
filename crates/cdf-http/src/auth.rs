use std::{fmt, str};

use cdf_kernel::{CdfError, Result};

use crate::{message::HttpRequest, support::set_header};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SecretUri(String);

impl SecretUri {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if !value.starts_with("secret://") {
            return Err(CdfError::contract(
                "secret reference must use the secret:// scheme",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for SecretUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SecretValue {
    bytes: Vec<u8>,
}

impl SecretValue {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            bytes: value.into().into_bytes(),
        }
    }

    pub fn as_str(&self) -> Result<&str> {
        str::from_utf8(&self.bytes).map_err(|_| CdfError::auth("secret value is not valid UTF-8"))
    }
}

impl Drop for SecretValue {
    fn drop(&mut self) {
        self.bytes.fill(0);
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl fmt::Display for SecretValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

pub trait SecretProvider {
    fn resolve(&self, uri: &SecretUri) -> Result<SecretValue>;
}

pub trait AuthRefreshHook {
    fn refresh(&mut self, uri: &SecretUri, provider: &dyn SecretProvider) -> Result<SecretValue>;
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProviderRefreshHook;

impl AuthRefreshHook for ProviderRefreshHook {
    fn refresh(&mut self, uri: &SecretUri, provider: &dyn SecretProvider) -> Result<SecretValue> {
        provider.resolve(uri)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthScheme {
    Bearer { token_uri: SecretUri },
    Header { name: String, value_uri: SecretUri },
}

impl AuthScheme {
    fn uri(&self) -> &SecretUri {
        match self {
            Self::Bearer { token_uri } => token_uri,
            Self::Header { value_uri, .. } => value_uri,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct AuthSession {
    scheme: AuthScheme,
    value: Option<SecretValue>,
    refreshed: bool,
}

impl AuthSession {
    pub fn new(scheme: AuthScheme) -> Self {
        Self {
            scheme,
            value: None,
            refreshed: false,
        }
    }

    pub fn apply(
        &mut self,
        provider: &dyn SecretProvider,
        request: &mut HttpRequest,
    ) -> Result<()> {
        if self.value.is_none() {
            self.value = Some(provider.resolve(self.scheme.uri())?);
        }
        let value = self.value.as_ref().expect("secret value was just resolved");
        match &self.scheme {
            AuthScheme::Bearer { .. } => {
                set_header(
                    &mut request.headers,
                    "authorization",
                    format!("Bearer {}", value.as_str()?),
                );
            }
            AuthScheme::Header { name, .. } => {
                set_header(&mut request.headers, name, value.as_str()?);
            }
        }
        Ok(())
    }

    pub fn refresh_once(
        &mut self,
        provider: &dyn SecretProvider,
        hook: &mut dyn AuthRefreshHook,
    ) -> Result<()> {
        if self.refreshed {
            return Err(CdfError::auth(
                "auth refresh was already attempted for this session",
            ));
        }
        self.value = Some(hook.refresh(self.scheme.uri(), provider)?);
        self.refreshed = true;
        Ok(())
    }
}

impl fmt::Debug for AuthSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthSession")
            .field("scheme", &self.scheme)
            .field("value", &self.value.as_ref().map(|_| "[REDACTED]"))
            .field("refreshed", &self.refreshed)
            .finish()
    }
}
