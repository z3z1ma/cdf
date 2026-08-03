Status: recorded
Created: 2026-08-03
Updated: 2026-08-03

# ClickHouse destination error-ownership audit

## Observation

The frozen `cdf-dest-clickhouse` Rust scope contains thirteen files, ten site-bearing files, and
242 constructor/direct-kind rows. The 206 production rows comprise 78 Contract constructors, 46
Destination constructors, 34 Data constructors, 26 Internal constructors, two dynamic `new`
classifiers, one Auth constructor, and nineteen direct `ErrorKind` branches. Those direct branches
are five Destination, four Internal, four Transient, two Data, two Environment, one Auth, and one
RateLimited. The remaining 36 rows are test fixtures and assertions. No direct `CdfError` struct
literal bypasses the constructors; the two textual `CdfError {` matches are function return types.

Package hash, segment, schema, ordinal, and finalized-package inconsistencies are Data. Invalid
URI, identifier, policy, merge key, mapping, and unsupported-disposition inputs are Contract.
Missing, duplicated, malformed, or contradicted target, stage, mirror, and receipt evidence is
Destination. Session lifecycle, generated physical-batch shape, state/receipt serialization,
validated-plan state, and arithmetic impossibilities are Internal.

The official-client wrapper recursively checks ClickHouse boxed errors, generic source chains,
and nested `std::io::Error` payloads before applying a coarse classification. An embedded typed
`CdfError` retains its kind, retry delay, and primary message while receiving controlled operation
context. Raw permission/resource/TLS facility failures are Environment; transport failures are
Transient; stable authentication and pressure codes are Auth and RateLimited. Missing/drifting
destination objects and unstructured server rejection are Destination, malformed supplied Arrow
payload is Data, and generated SQL syntax/client-serialization contradictions are Internal.

Foreign server text is never forwarded. Stable server failures expose only controlled action text
and an optional numeric code. Tests include credential-like server detail and prove it is absent.
Resolved URI userinfo is accepted only from a secret reference, and both connection and
destination `Debug` implementations redact credential values.

## Procedure

```sh
LC_ALL=C rg --files -0 -g '*.rs' crates/cdf-dest-clickhouse | LC_ALL=C sort -z \
  > .10x/evidence/.storage/2026-08-03-clickhouse-destination-error-files.nul
error_test_line=$(rg -n '^#\[cfg\(test\)\]' crates/cdf-dest-clickhouse/src/error.rs | cut -d: -f1)
runtime_test_line=$(rg -n '^#\[cfg\(test\)\]' crates/cdf-dest-clickhouse/src/runtime.rs | cut -d: -f1)
session_test_line=$(rg -n '^#\[cfg\(test\)\]' crates/cdf-dest-clickhouse/src/session.rs | cut -d: -f1)
xargs -0 rg -n --no-heading -- \
  'CdfError::(new|transient|rate_limited|auth|contract|data|destination|environment|internal|from)|ErrorKind::(Transient|RateLimited|Auth|Contract|Data|Destination|Environment|Internal)' \
  < .10x/evidence/.storage/2026-08-03-clickhouse-destination-error-files.nul | \
  LC_ALL=C sort | awk -v error_test_line="$error_test_line" \
    -v runtime_test_line="$runtime_test_line" -v session_test_line="$session_test_line" \
    -f .10x/evidence/.storage/2026-08-03-clickhouse-destination-error-classify.awk \
  > .10x/evidence/.storage/2026-08-03-clickhouse-destination-error-sites.tsv
LC_ALL=C rg -n --no-heading -g '*.rs' -- \
  'CdfError::internal|\.internal\(|ErrorKind::Internal|\bInternal\b' \
  crates/cdf-dest-clickhouse | LC_ALL=C sort
LC_ALL=C rg -n --no-heading -g '*.rs' -- 'CdfError \{' crates/cdf-dest-clickhouse
```

Frozen outputs:

- `.10x/evidence/.storage/2026-08-03-clickhouse-destination-error-files.nul` — thirteen
  null-delimited Rust paths; SHA-256
  `2b85e3ea504233bc331a5d54ba86fa5673b860201b9a265282cb73953ce9f40a`.
- `.10x/evidence/.storage/2026-08-03-clickhouse-destination-error-classify.awk` — semantic
  classification rules; SHA-256
  `83f742af8ff71d44243bbcdf874cdb5809d632f91d5f447ea7e158a88c62e545`.
- `.10x/evidence/.storage/2026-08-03-clickhouse-destination-error-sites.tsv` — one header plus
  242 classified rows; SHA-256
  `f6cd5e57324630a21fe151e8291b68c574d508293187569b1992ceb432c7f679`.

## What it supports or challenges

This supports the connector's typed-wrapper provenance, retry ownership, durable-destination
ownership, and secret-redaction requirements. The audit challenged and repaired source-derived
destination mappings: missing/drifting targets and malformed destination responses are no longer
reported as source Data, while generated SQL syntax rejection is now CDF-owned Internal.

## Limits

The inventory is exact for the frozen thirteen-file destination crate. Stable-code fixtures are
synthetic; the owning ticket separately records the digest-pinned live target, merge, replace,
crash-recovery, dependency-rejection, and roofline observations. The user explicitly prohibited
graphify for this workstream, so no graph update was run; that is a requested validation limit,
not evidence about module relationships. The audit does not sample every ClickHouse server code,
TLS implementation, or platform network failure.
