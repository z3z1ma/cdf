use std::fmt;

use crate::{
    message::HttpResponse,
    support::{header_value, parse_next_link, set_query_param},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PaginationKind {
    Cursor,
    Page,
    Offset,
    LinkHeader,
    NextToken,
}

impl fmt::Display for PaginationKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Cursor => "cursor",
            Self::Page => "page",
            Self::Offset => "offset",
            Self::LinkHeader => "link_header",
            Self::NextToken => "next_token",
        };
        f.write_str(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PaginationConfig {
    Cursor {
        query_param: String,
        response_field: String,
        initial: Option<String>,
    },
    Page {
        query_param: String,
        start_page: u64,
    },
    Offset {
        offset_param: String,
        limit_param: String,
        start_offset: u64,
        limit: u64,
    },
    LinkHeader,
    NextToken {
        query_param: String,
        response_field: String,
        initial: Option<String>,
    },
}

impl PaginationConfig {
    pub fn kind(&self) -> PaginationKind {
        match self {
            Self::Cursor { .. } => PaginationKind::Cursor,
            Self::Page { .. } => PaginationKind::Page,
            Self::Offset { .. } => PaginationKind::Offset,
            Self::LinkHeader => PaginationKind::LinkHeader,
            Self::NextToken { .. } => PaginationKind::NextToken,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageRequest {
    pub url: String,
    pub kind: PaginationKind,
    pub plan_note: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Paginator {
    config: PaginationConfig,
    current_page: u64,
    current_offset: u64,
    last_marker: Option<String>,
}

impl Paginator {
    pub fn new(config: PaginationConfig) -> Self {
        let current_page = match &config {
            PaginationConfig::Page { start_page, .. } => *start_page,
            _ => 0,
        };
        let current_offset = match &config {
            PaginationConfig::Offset { start_offset, .. } => *start_offset,
            _ => 0,
        };
        Self {
            config,
            current_page,
            current_offset,
            last_marker: None,
        }
    }

    pub fn first_request(&self, base_url: &str) -> PageRequest {
        let url = match &self.config {
            PaginationConfig::Cursor {
                query_param,
                initial: Some(initial),
                ..
            }
            | PaginationConfig::NextToken {
                query_param,
                initial: Some(initial),
                ..
            } => set_query_param(base_url, query_param, initial),
            PaginationConfig::Page {
                query_param,
                start_page,
            } => set_query_param(base_url, query_param, &start_page.to_string()),
            PaginationConfig::Offset {
                offset_param,
                limit_param,
                start_offset,
                limit,
            } => {
                let url = set_query_param(base_url, offset_param, &start_offset.to_string());
                set_query_param(&url, limit_param, &limit.to_string())
            }
            PaginationConfig::Cursor { .. }
            | PaginationConfig::NextToken { .. }
            | PaginationConfig::LinkHeader => base_url.to_owned(),
        };
        PageRequest {
            url,
            kind: self.config.kind(),
            plan_note: format!("pagination={}", self.config.kind()),
        }
    }

    pub fn next_request(
        &mut self,
        current_url: &str,
        response: &HttpResponse,
    ) -> Option<PageRequest> {
        let next_url = match &self.config {
            PaginationConfig::Cursor {
                query_param,
                response_field,
                ..
            }
            | PaginationConfig::NextToken {
                query_param,
                response_field,
                ..
            } => {
                let marker = response.page.fields.get(response_field)?.trim();
                if marker.is_empty() || self.last_marker.as_deref() == Some(marker) {
                    return None;
                }
                self.last_marker = Some(marker.to_owned());
                set_query_param(current_url, query_param, marker)
            }
            PaginationConfig::Page { query_param, .. } => {
                if response.page.item_count == 0 {
                    return None;
                }
                self.current_page = self.current_page.saturating_add(1);
                set_query_param(current_url, query_param, &self.current_page.to_string())
            }
            PaginationConfig::Offset {
                offset_param,
                limit,
                ..
            } => {
                if response.page.item_count == 0 || response.page.item_count < *limit as usize {
                    return None;
                }
                self.current_offset = self.current_offset.saturating_add(*limit);
                set_query_param(current_url, offset_param, &self.current_offset.to_string())
            }
            PaginationConfig::LinkHeader => {
                parse_next_link(header_value(&response.headers, "link")?)?
            }
        };

        Some(PageRequest {
            url: next_url,
            kind: self.config.kind(),
            plan_note: format!("pagination={}", self.config.kind()),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutoDetectionResult {
    pub kind: Option<PaginationKind>,
    pub evidence: Vec<String>,
}

impl AutoDetectionResult {
    pub fn plan_summary(&self) -> String {
        let kind = self
            .kind
            .map(|kind| kind.to_string())
            .unwrap_or_else(|| "none".to_owned());
        format!("pagination={kind}; evidence={}", self.evidence.join(", "))
    }
}

pub fn detect_pagination(response: &HttpResponse) -> AutoDetectionResult {
    if response
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("link"))
        .and_then(|(_, value)| parse_next_link(value))
        .is_some()
    {
        return AutoDetectionResult {
            kind: Some(PaginationKind::LinkHeader),
            evidence: vec!["Link header contains rel=next".to_owned()],
        };
    }

    let field_specs = [
        ("next_token", PaginationKind::NextToken),
        ("nextToken", PaginationKind::NextToken),
        ("next_cursor", PaginationKind::Cursor),
        ("nextCursor", PaginationKind::Cursor),
        ("next_page", PaginationKind::Page),
        ("nextPage", PaginationKind::Page),
        ("next_offset", PaginationKind::Offset),
        ("nextOffset", PaginationKind::Offset),
    ];
    for (field, kind) in field_specs {
        if response
            .page
            .fields
            .get(field)
            .is_some_and(|value| !value.trim().is_empty())
        {
            return AutoDetectionResult {
                kind: Some(kind),
                evidence: vec![format!("response field `{field}` is present")],
            };
        }
    }

    AutoDetectionResult {
        kind: None,
        evidence: vec!["no supported pagination marker found".to_owned()],
    }
}
