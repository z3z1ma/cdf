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
    test = file ~ /\/tests\.rs$/

    if (match(source, /CdfError::[a-z_]+/)) {
        syntax = substr(source, RSTART, RLENGTH)
    } else if (match(source, /=> ErrorKind::[A-Za-z]+/)) {
        syntax = substr(source, RSTART + 3, RLENGTH - 3)
    } else if (match(source, /MongoErrorKind::[A-Za-z]+/)) {
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
        owner = "mongodb_source_data"
        retry = "none"
        redaction = "bounded_resource_field_or_schema_context"
        rationale = "Missing, malformed, drifting, unsupported, over-bound, or physically inconsistent BSON/source data belongs to the selected source."
    } else if (syntax == "CdfError::internal") {
        owner = "cdf_invariant"
        retry = "none"
        redaction = "operation_context_only"
        rationale = "A validated serialization, counter, partition, batch, query, or cursor invariant failed inside CDF."
    } else if (syntax == "CdfError::new") {
        owner = "dynamic_foreign_classifier"
        retry = "kind_dependent"
        redaction = "controlled_message_without_endpoint_or_credentials"
        rationale = "The central SDK boundary applies the stable variant and nested typed/I/O classification."
    } else if (syntax == "MongoErrorKind::Authentication") {
        owner = "mongodb_auth"
        retry = "none"
        redaction = "controlled_message_without_endpoint_or_credentials"
        rationale = "The official driver authentication variant maps to the CDF authentication owner."
    } else if (syntax == "MongoErrorKind::InvalidArgument") {
        owner = "caller_contract"
        retry = "none"
        redaction = "controlled_message_without_endpoint_or_credentials"
        rationale = "The official driver rejected a caller-supplied or compiled request argument."
    } else if (syntax == "MongoErrorKind::Shutdown") {
        owner = "remote_transport"
        retry = "host_policy_only"
        redaction = "controlled_message_without_endpoint_or_credentials"
        rationale = "Driver shutdown is a transient external availability condition."
    } else if (syntax ~ /^MongoErrorKind::/) {
        owner = "dynamic_foreign_classifier"
        retry = "kind_dependent"
        redaction = "controlled_message_without_endpoint_or_credentials"
        rationale = "This official-driver variant participates in the adjacent exhaustive typed mapping branch."
    } else if (kind == "Transient") {
        owner = "remote_transport"
        retry = "host_policy_only"
        redaction = "controlled_message_without_endpoint_or_credentials"
        rationale = "Connection, selection, shutdown, or stable transient server failure may recover after external state changes."
    } else if (kind == "RateLimited") {
        owner = "mongodb_rate_limit"
        retry = "embedded_delay_preserved_or_host_policy"
        redaction = "controlled_message_without_endpoint_or_credentials"
        rationale = "A stable server quota code or embedded typed error identifies externally imposed rate pressure."
    } else if (kind == "Auth") {
        owner = "mongodb_auth"
        retry = "none"
        redaction = "controlled_message_without_endpoint_or_credentials"
        rationale = "Authentication failure requires credential or authorization repair."
    } else if (kind == "Contract") {
        owner = "caller_contract"
        retry = "none"
        redaction = "controlled_message_without_endpoint_or_credentials"
        rationale = "The compiled request or selected server capability is outside the connector contract."
    } else if (kind == "Environment") {
        owner = "local_host_environment"
        retry = "none"
        redaction = "controlled_message_without_endpoint_or_credentials"
        rationale = "A local permission, resource, DNS, resolver, or TLS-construction failure belongs to the host environment."
    } else if (kind == "Internal") {
        owner = "cdf_or_official_driver_invariant"
        retry = "none"
        redaction = "controlled_message_without_endpoint_or_credentials"
        rationale = "The official driver reported an internal invariant failure after CDF validation."
    } else {
        owner = "mongodb_source_data"
        retry = "none"
        redaction = "controlled_message_without_endpoint_or_credentials"
        rationale = "Malformed, unsupported, missing, or schema-contradicting MongoDB response data belongs to the selected source."
    }

    print file, line, syntax, owner, retry, redaction, rationale, source
}
