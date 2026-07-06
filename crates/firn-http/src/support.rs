use std::str;

use firn_kernel::{ErrorKind, FirnError, Result};

use crate::message::{HeaderMap, HttpResponse};

pub(crate) fn set_header(
    headers: &mut HeaderMap,
    name: impl Into<String>,
    value: impl Into<String>,
) {
    headers.insert(canonical_header_name(&name.into()), value.into());
}

pub(crate) fn header_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

pub(crate) fn canonical_header_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

pub(crate) fn parse_next_link(link_header: &str) -> Option<String> {
    link_header.split(',').find_map(|part| {
        let part = part.trim();
        let rel_next = part.split(';').skip(1).any(|param| {
            param.trim().eq_ignore_ascii_case("rel=\"next\"")
                || param.trim().eq_ignore_ascii_case("rel=next")
        });
        if !rel_next {
            return None;
        }
        let start = part.find('<')?;
        let end = part[start + 1..].find('>')?;
        Some(part[start + 1..start + 1 + end].to_owned())
    })
}

pub(crate) fn set_query_param(url: &str, name: &str, value: &str) -> String {
    let (without_fragment, fragment) = url
        .split_once('#')
        .map_or((url, ""), |(url, fragment)| (url, fragment));
    let (base, query) = without_fragment
        .split_once('?')
        .map_or((without_fragment, ""), |(base, query)| (base, query));
    let mut params = query
        .split('&')
        .filter(|part| !part.is_empty())
        .filter(|part| {
            let candidate = part
                .split_once('=')
                .map_or(*part, |(candidate, _)| candidate);
            candidate != name
        })
        .map(str::to_owned)
        .collect::<Vec<_>>();
    params.push(format!("{name}={value}"));
    let mut next_url = format!("{base}?{}", params.join("&"));
    if !fragment.is_empty() {
        next_url.push('#');
        next_url.push_str(fragment);
    }
    next_url
}

pub(crate) fn retry_after_ms(response: &HttpResponse) -> Option<u64> {
    header_value(&response.headers, "retry-after")
        .and_then(parse_u64)
        .map(|seconds| seconds.saturating_mul(1_000))
}

pub(crate) fn parse_u64(value: &str) -> Option<u64> {
    value.trim().parse::<u64>().ok()
}

pub(crate) fn is_retryable_kind(kind: &ErrorKind) -> bool {
    matches!(kind, ErrorKind::Transient | ErrorKind::RateLimited)
}

pub(crate) fn retry_exhausted_error(error: &FirnError, reason: &str) -> FirnError {
    let message = format!("{reason}: {}", error.message);
    match error.kind {
        ErrorKind::RateLimited => FirnError::rate_limited(message, error.retry_after_ms),
        _ => FirnError::new(error.kind.clone(), message),
    }
}

pub(crate) fn normalize_host(host: impl Into<String>) -> String {
    let value = host.into();
    let host = value
        .trim()
        .trim_end_matches('.')
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let host = host.split('/').next().unwrap_or(host);
    let host = host
        .strip_prefix('[')
        .and_then(|value| value.split_once(']'))
        .map_or(host.split(':').next().unwrap_or(host), |(inside, _)| inside);
    host.to_ascii_lowercase()
}

pub(crate) fn host_from_url(url: &str) -> Result<String> {
    let (_, rest) = url
        .split_once("://")
        .ok_or_else(|| FirnError::contract("HTTP request URL must include a scheme"))?;
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .ok_or_else(|| FirnError::contract("HTTP request URL must include a host"))?;
    let host_port = authority.rsplit('@').next().unwrap_or(authority);
    let host = if let Some(stripped) = host_port.strip_prefix('[') {
        stripped
            .split_once(']')
            .map(|(inside, _)| inside)
            .ok_or_else(|| FirnError::contract("IPv6 host is missing closing bracket"))?
    } else {
        host_port.split(':').next().unwrap_or(host_port)
    };
    if host.trim().is_empty() {
        return Err(FirnError::contract("HTTP request URL must include a host"));
    }
    Ok(normalize_host(host))
}

pub(crate) fn host_matches(host: &str, allowed: &str) -> bool {
    host == allowed
        || allowed
            .strip_prefix("*.")
            .is_some_and(|suffix| host.ends_with(&format!(".{suffix}")))
}

pub(crate) fn is_sensitive_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    [
        "token",
        "secret",
        "password",
        "authorization",
        "api_key",
        "apikey",
    ]
    .iter()
    .any(|needle| name.contains(needle))
}
