BEGIN {
    OFS = "\t"
    print "file", "line", "syntax", "primary_owner", "retry", "redaction", "rationale", "source"
}

{
    first = index($0, ":")
    file = substr($0, 1, first - 1)
    rest = substr($0, first + 1)
    second = index(rest, ":")
    line = substr(rest, 1, second - 1) + 0
    source = substr(rest, second + 1)
    gsub(/\t/, " ", source)
    test = file ~ /\/tests\.rs$/ \
        || (file ~ /\/error\.rs$/ && line >= error_test_line) \
        || (file ~ /\/runtime\.rs$/ && line >= runtime_test_line) \
        || (file ~ /\/session\.rs$/ && line >= session_test_line)

    if (match(source, /CdfError::[a-z_]+/)) {
        syntax = substr(source, RSTART, RLENGTH)
    } else {
        match(source, /ErrorKind::[A-Za-z]+/)
        syntax = substr(source, RSTART, RLENGTH)
    }
    kind = syntax
    sub(/^.*::/, "", kind)

    if (test) {
        owner = syntax ~ /^ErrorKind::/ || source ~ /assert_eq!/ ? "test_assertion" : "test_fixture"
        retry = syntax == "CdfError::rate_limited" ? "preserved_875_ms_fixture" : "none"
        redaction = "test_only"
        rationale = "Test-only fixture or assertion verifies the production ownership boundary."
    } else if (syntax == "CdfError::contract") {
        owner = "caller_contract"
        retry = "none"
        redaction = "bounded_configuration_plan_or_identifier_context"
        rationale = "Invalid configuration, plan, schema mapping, identifier, policy, or unsupported request crosses the connector contract."
    } else if (syntax == "CdfError::data") {
        owner = "finalized_package_data"
        retry = "none"
        redaction = "bounded_package_segment_or_schema_context"
        rationale = "Malformed, inconsistent, over-bound, or noncanonical finalized package evidence belongs to the supplied data."
    } else if (syntax == "CdfError::destination") {
        owner = "durable_clickhouse_destination"
        retry = "none"
        redaction = "bounded_target_receipt_or_segment_context"
        rationale = "Missing, duplicated, contradictory, or incompletely published target and mirror evidence belongs to the durable destination."
    } else if (syntax == "CdfError::internal") {
        owner = "cdf_invariant"
        retry = "none"
        redaction = "operation_context_only"
        rationale = "A CDF-owned lifecycle, serialization, generated-shape, arithmetic, or validated-plan invariant failed."
    } else if (syntax == "CdfError::auth") {
        owner = "clickhouse_auth"
        retry = "none"
        redaction = "fixed_message_no_secret_value"
        rationale = "Resolved destination credentials are absent or invalid and require credential repair."
    } else if (syntax == "CdfError::new") {
        owner = "dynamic_foreign_classifier"
        retry = "kind_dependent"
        redaction = "controlled_message_and_stable_code_only"
        rationale = "The central client boundary applies stable variant, numeric-code, and nested typed-error ownership."
    } else if (kind == "Transient") {
        owner = "remote_transport"
        retry = "host_policy_only"
        redaction = "controlled_message_and_stable_code_only"
        rationale = "Network, timeout, or a stable transient server code may recover after external state changes."
    } else if (kind == "RateLimited") {
        owner = "clickhouse_rate_limit"
        retry = "host_policy_only"
        redaction = "controlled_message_and_stable_code_only"
        rationale = "A stable server concurrency, quota, or memory-limit code identifies externally imposed pressure."
    } else if (kind == "Auth") {
        owner = "clickhouse_auth"
        retry = "none"
        redaction = "controlled_message_and_stable_code_only"
        rationale = "A stable server authentication code requires credential repair."
    } else if (kind == "Environment") {
        owner = "local_host_environment"
        retry = "none"
        redaction = "controlled_message_and_stable_code_only"
        rationale = "A nested host permission, resource, resolver, runtime, or TLS facility failure belongs to the local environment."
    } else if (kind == "Internal") {
        owner = "cdf_invariant"
        retry = "none"
        redaction = "controlled_message_and_stable_code_only"
        rationale = "An official-client request/serialization API invariant failed after CDF validation."
    } else {
        owner = "clickhouse_destination_response"
        retry = "none"
        redaction = "controlled_message_and_stable_code_only"
        rationale = "A malformed, unsupported, or schema-contradicting destination response is non-retryable remote evidence."
    }

    print file, line, syntax, owner, retry, redaction, rationale, source
}
