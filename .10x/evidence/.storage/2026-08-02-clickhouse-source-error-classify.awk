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
    test = file ~ /\/tests\.rs$/ || (file ~ /\/error\.rs$/ && line >= error_test_line)

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
        retry = syntax == "CdfError::rate_limited" ? "preserved_250_ms_fixture" : "none"
        redaction = "test_only"
        rationale = "Test-only fixture or assertion verifies the production ownership boundary."
    } else if (syntax == "CdfError::contract") {
        owner = "caller_contract"
        retry = "none"
        redaction = "bounded_configuration_or_field_context"
        rationale = "Invalid configuration, plan, identifier, predicate, checkpoint, or effective-schema binding crosses the connector contract."
    } else if (syntax == "CdfError::data") {
        owner = "clickhouse_source_data"
        retry = "none"
        redaction = "bounded_resource_field_or_schema_context"
        rationale = "Missing, malformed, drifting, unsupported, over-bound, or physically inconsistent remote data belongs to the selected source."
    } else if (syntax == "CdfError::internal") {
        owner = "cdf_invariant"
        retry = "none"
        redaction = "operation_context_only"
        rationale = "A validated query, serialization, partition, schema-probe, or cursor invariant failed inside CDF."
    } else if (syntax == "CdfError::auth") {
        owner = "clickhouse_auth"
        retry = "none"
        redaction = "fixed_message_no_secret_value"
        rationale = "Resolved credentials are absent or invalid and require credential repair."
    } else if (syntax == "CdfError::new") {
        owner = "dynamic_foreign_classifier"
        retry = "kind_dependent"
        redaction = "controlled_message_and_stable_code_only"
        rationale = "Central foreign boundary applies the stable variant, code, and nested-I/O classification."
    } else if (kind == "Transient") {
        owner = "remote_transport"
        retry = "host_policy_only"
        redaction = "controlled_message_and_stable_code_only"
        rationale = "Network, timeout, or stable transient server code may recover after external state changes."
    } else if (kind == "RateLimited") {
        owner = "clickhouse_rate_limit"
        retry = "host_policy_only"
        redaction = "controlled_message_and_stable_code_only"
        rationale = "Stable server quota or limit code identifies externally imposed rate pressure."
    } else if (kind == "Auth") {
        owner = "clickhouse_auth"
        retry = "none"
        redaction = "controlled_message_and_stable_code_only"
        rationale = "Stable server authentication code requires credential repair."
    } else if (kind == "Environment") {
        owner = "local_host_environment"
        retry = "none"
        redaction = "controlled_message_and_stable_code_only"
        rationale = "Nested host facility, permission, resource, resolver, or TLS-construction failure belongs to the local environment."
    } else if (kind == "Internal") {
        owner = "cdf_invariant"
        retry = "none"
        redaction = "controlled_message_and_stable_code_only"
        rationale = "Official-client parameter or decode API invariant failed after CDF validation."
    } else {
        owner = "clickhouse_source_data"
        retry = "none"
        redaction = "controlled_message_and_stable_code_only"
        rationale = "Malformed, unsupported, or schema-contradicting remote response belongs to the selected source."
    }

    print file, line, syntax, owner, retry, redaction, rationale, source
}
