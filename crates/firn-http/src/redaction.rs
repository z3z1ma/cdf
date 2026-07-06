use std::{collections::BTreeSet, str};

use firn_kernel::Result;

use crate::{
    auth::SecretValue,
    message::HeaderMap,
    support::{canonical_header_name, is_sensitive_name},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Redactor {
    secrets: Vec<String>,
    sensitive_headers: BTreeSet<String>,
}

impl Default for Redactor {
    fn default() -> Self {
        Self {
            secrets: Vec::new(),
            sensitive_headers: [
                "authorization",
                "proxy-authorization",
                "x-api-key",
                "api-key",
                "cookie",
                "set-cookie",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        }
    }
}

impl Redactor {
    pub fn register_secret_value(&mut self, value: &SecretValue) -> Result<()> {
        self.register_secret(value.as_str()?)
    }

    pub fn register_secret(&mut self, value: &str) -> Result<()> {
        if value.is_empty() {
            return Ok(());
        }
        self.secrets.push(value.to_owned());
        Ok(())
    }

    pub fn redact_text(&self, value: &str) -> String {
        self.secrets
            .iter()
            .fold(value.to_owned(), |redacted, secret| {
                redacted.replace(secret, "[REDACTED]")
            })
    }

    pub fn redact_headers(&self, headers: &HeaderMap) -> HeaderMap {
        headers
            .iter()
            .map(|(name, value)| {
                let canonical = canonical_header_name(name);
                let value = if self.sensitive_headers.contains(&canonical)
                    || is_sensitive_name(&canonical)
                {
                    "[REDACTED]".to_owned()
                } else {
                    self.redact_text(value)
                };
                (name.clone(), value)
            })
            .collect()
    }

    pub fn redact_url(&self, url: &str) -> String {
        let Some((base, query_and_fragment)) = url.split_once('?') else {
            return self.redact_text(url);
        };
        let (query, fragment) = query_and_fragment
            .split_once('#')
            .map_or((query_and_fragment, ""), |(query, fragment)| {
                (query, fragment)
            });
        let redacted_query = query
            .split('&')
            .filter(|part| !part.is_empty())
            .map(|part| {
                let (name, value) = part.split_once('=').unwrap_or((part, ""));
                let redacted_value = if is_sensitive_name(name) {
                    "[REDACTED]".to_owned()
                } else {
                    self.redact_text(value)
                };
                format!("{name}={redacted_value}")
            })
            .collect::<Vec<_>>()
            .join("&");
        let mut redacted = format!("{}?{}", self.redact_text(base), redacted_query);
        if !fragment.is_empty() {
            redacted.push('#');
            redacted.push_str(&self.redact_text(fragment));
        }
        redacted
    }
}
