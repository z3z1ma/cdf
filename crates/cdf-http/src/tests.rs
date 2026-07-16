use std::sync::Arc;

use cdf_kernel::{ErrorKind, Result};
use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};

use super::*;

fn test_response_budget() -> HttpResponseBudget {
    let memory: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(1024 * 1024, Default::default()).unwrap());
    HttpResponseBudget::new(1024 * 1024, memory, Arc::new(|| Ok(()))).unwrap()
}

#[test]
fn paginators_cover_cursor_page_offset_link_and_next_token() {
    let mut cursor = Paginator::new(PaginationConfig::Cursor {
        query_param: "cursor".to_owned(),
        response_field: "next_cursor".to_owned(),
        initial: None,
    });
    let current = cursor.first_request("https://api.example.com/items");
    assert_eq!(current.url, "https://api.example.com/items");
    let response = HttpResponse::new(200).with_field("next_cursor", "abc");
    assert_eq!(
        cursor.next_request(&current.url, &response).unwrap().url,
        "https://api.example.com/items?cursor=abc"
    );

    let mut pages = Paginator::new(PaginationConfig::Page {
        query_param: "page".to_owned(),
        start_page: 1,
    });
    let first = pages.first_request("https://api.example.com/items");
    assert_eq!(first.url, "https://api.example.com/items?page=1");
    let response = HttpResponse::new(200).with_item_count(10);
    assert_eq!(
        pages.next_request(&first.url, &response).unwrap().url,
        "https://api.example.com/items?page=2"
    );

    let mut offsets = Paginator::new(PaginationConfig::Offset {
        offset_param: "offset".to_owned(),
        limit_param: "limit".to_owned(),
        start_offset: 0,
        limit: 50,
    });
    let first = offsets.first_request("https://api.example.com/items");
    assert_eq!(first.url, "https://api.example.com/items?offset=0&limit=50");
    let response = HttpResponse::new(200).with_item_count(50);
    assert_eq!(
        offsets.next_request(&first.url, &response).unwrap().url,
        "https://api.example.com/items?limit=50&offset=50"
    );

    let mut links = Paginator::new(PaginationConfig::LinkHeader);
    let response = HttpResponse::new(200).with_header(
        "Link",
        r#"<https://api.example.com/items?page=2>; rel="next""#,
    );
    assert_eq!(
        links
            .next_request("https://api.example.com/items?page=1", &response)
            .unwrap()
            .url,
        "https://api.example.com/items?page=2"
    );

    let mut tokens = Paginator::new(PaginationConfig::NextToken {
        query_param: "page_token".to_owned(),
        response_field: "next_token".to_owned(),
        initial: None,
    });
    let response = HttpResponse::new(200).with_field("next_token", "tok-2");
    assert_eq!(
        tokens
            .next_request("https://api.example.com/items", &response)
            .unwrap()
            .url,
        "https://api.example.com/items?page_token=tok-2"
    );
}

#[test]
fn auto_detection_is_plan_visible() {
    let response = HttpResponse::new(200).with_field("next_token", "tok-2");
    let detected = detect_pagination(&response);
    assert_eq!(detected.kind, Some(PaginationKind::NextToken));
    assert_eq!(
        detected.plan_summary(),
        "pagination=next_token; evidence=response field `next_token` is present"
    );
}

#[test]
fn rate_limiter_respects_retry_after_and_quota_headers() {
    let mut limiter = RateLimiter::new(
        RateLimitPolicy {
            requests_per_minute: Some(60),
            quota_headers: vec![QuotaHeaderPolicy::remaining_until_reset(
                "X-RateLimit-Remaining",
                "X-RateLimit-Reset",
                ResetHeaderSemantics::DelaySeconds,
            )],
        },
        0,
    );

    assert!(limiter.before_request(0).allowed);
    let retry_after = HttpResponse::new(429).with_header("Retry-After", "2");
    let decision = limiter.observe_response(&retry_after, 10, 10);
    assert_eq!(decision.wait_ms, 2_000);
    assert!(!limiter.before_request(1_000).allowed);

    let quota = HttpResponse::new(200)
        .with_header("X-RateLimit-Remaining", "0")
        .with_header("X-RateLimit-Reset", "3");
    let decision = limiter.observe_response(&quota, 5_000, 5_000);
    assert_eq!(decision.wait_ms, 3_000);
}

#[test]
fn response_classification_preserves_typed_retry_metadata_without_policy() {
    let response = HttpResponse::new(500);
    let error = classify_response(&response).unwrap();
    assert_eq!(error.kind, ErrorKind::Transient);

    let rate_limited = HttpResponse::new(429).with_header("Retry-After", "4");
    let error = classify_response(&rate_limited).unwrap();
    assert_eq!(error.kind, ErrorKind::RateLimited);
    assert_eq!(error.retry_after_ms, Some(4_000));
}

#[test]
fn auth_refresh_hooks_and_traces_do_not_format_secrets() {
    struct Provider {
        value: String,
    }

    impl SecretProvider for Provider {
        fn resolve(&self, _uri: &SecretUri) -> Result<SecretValue> {
            Ok(SecretValue::new(self.value.clone()))
        }
    }

    let uri = SecretUri::new("secret://env/API_TOKEN").unwrap();
    let provider = Provider {
        value: "super-secret-token".to_owned(),
    };
    let mut session = AuthSession::new(AuthScheme::Bearer {
        token_uri: uri.clone(),
    });
    let mut request = HttpRequest::new(
        HttpMethod::Get,
        "https://api.example.com/items?token=super-secret-token",
    );
    session.apply(&provider, &mut request).unwrap();
    assert_eq!(
        request.headers.get("authorization").unwrap(),
        "Bearer super-secret-token"
    );

    let mut redactor = Redactor::default();
    redactor.register_secret("super-secret-token").unwrap();
    let trace = TraceEvent::from_request(&request, &redactor);
    let trace_text = format!("{trace:?}");
    assert!(!trace_text.contains("super-secret-token"));
    assert!(trace_text.contains("[REDACTED]"));
    assert!(!format!("{request:?}").contains("super-secret-token"));
    assert!(!format!("{session:?}").contains("super-secret-token"));

    let refreshed_provider = Provider {
        value: "rotated-secret-token".to_owned(),
    };
    let mut hook = ProviderRefreshHook;
    session
        .refresh_once(&refreshed_provider, &mut hook)
        .unwrap();
    let mut refreshed = HttpRequest::new(HttpMethod::Get, "https://api.example.com/items");
    session.apply(&refreshed_provider, &mut refreshed).unwrap();
    assert_eq!(
        refreshed.headers.get("authorization").unwrap(),
        "Bearer rotated-secret-token"
    );
    assert!(
        session
            .refresh_once(&refreshed_provider, &mut hook)
            .is_err()
    );
}

#[test]
fn allowlist_denies_before_transport_send() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct CountingTransport {
        sends: AtomicUsize,
    }

    impl HttpTransport for CountingTransport {
        fn send(&self, _request: HttpRequest, _budget: HttpResponseBudget) -> Result<HttpResponse> {
            self.sends.fetch_add(1, Ordering::SeqCst);
            Ok(HttpResponse::new(200))
        }
    }

    let allowlist = EgressAllowlist::from_hosts(["api.example.com"]);
    let transport = CountingTransport::default();
    let denied = send_with_policy(
        &transport,
        &allowlist,
        HttpRequest::new(HttpMethod::Get, "https://evil.example.net/items"),
        test_response_budget(),
    )
    .unwrap_err();
    assert_eq!(denied.kind, ErrorKind::Auth);
    assert_eq!(transport.sends.load(Ordering::SeqCst), 0);

    send_with_policy(
        &transport,
        &allowlist,
        HttpRequest::new(HttpMethod::Get, "https://api.example.com/items"),
        test_response_budget(),
    )
    .unwrap();
    assert_eq!(transport.sends.load(Ordering::SeqCst), 1);
}
