Status: recorded
Created: 2026-08-02
Updated: 2026-08-02

# ClickHouse source error-ownership audit

## Observation

The frozen `cdf-source-clickhouse` Rust scope contains twelve files and 162 `CdfError`
constructor sites. The 146 production constructors are 63 Contract, 61 Data, nineteen Internal, two
dynamic `new`, and one Auth. The sixteen test-only constructors are eight Internal, five Data, one
Auth, one Environment, and one RateLimited fixture. The direct-kind supplement adds eighteen
production `ErrorKind` branches (eight Data, four Transient, two Environment, two Internal, one
Auth, and one RateLimited) and 25 test assertions. No direct `CdfError` struct literal bypasses the
constructors.

The official-client boundary first walks nested ClickHouse boxed errors, generic source chains,
and nested `std::io::Error` payloads. Embedded typed CDF failures retain kind, retry delay, and
primary message while receiving controlled operation context. Raw ClickHouse failures are then
classified only by stable enum variant or parsed numeric server code: credentials are Auth,
quota/limit codes are RateLimited, network/timeouts and named transient server codes are
Transient, malformed or schema-contradicting source responses are Data, and only generated
parameter/official-client API contradictions are Internal. Unknown server codes fail closed as
non-retryable Data. No private retry loop exists.

Raw I/O classification preserves the wrapper boundary as well as the nested error kind. Timeout
and socket failures remain Transient. Permission, resource, resolver, and TLS-construction errors
inside the client's Network variant are Environment, including raw `InvalidData` certificate
construction. `InvalidData` or `UnexpectedEof` inside the client's Other/Arrow response variant is
source Data. Embedded typed CDF errors take precedence over both raw paths.

The opt-in official-client allocation ceiling has its own stable typed variant. A raw error body,
HTTP chunk, compressed/decoded frame, Arrow metadata message, or Arrow record body that crosses
its admitted limit maps to non-retryable source Data with a fixed connector message; the foreign
stage label and size are not forwarded. Decode-envelope arithmetic and missing lease authority are
CDF-owned Internal invariants because they are fixed by connector constants before source contact.

Foreign `Display` text is never forwarded. Server failures emit a controlled message and, when
available, the numeric code; a regression fixture includes credential-like server text and proves
it is absent. Resolved connection `Debug` output explicitly redacts both username and password.
Configuration/schema messages carry only bounded endpoint-shape, resource, field, physical-type,
or schema context. The audit also replaced two production `expect` calls with typed Internal
invariant failures and reclassified a server that violates `LIMIT 0` as source Data rather than
Internal.

Live discovery against ClickHouse 25.8.28.1 established that its ArrowStream format rejects a raw
`UUID` with server code 50 (`UNKNOWN_TYPE`). The user-ratified mapping instead generates
`toString()` for fields pinned with physical `UUID` metadata and validates the resulting canonical
Binary bytes before adapting them to CDF `Utf8`. Invalid UTF-8, noncanonical text, or a physical/
logical mapping contradiction remains source Data without forwarding server text. Ordinary
ClickHouse `String` is not reinterpreted and remains arbitrary-byte Arrow `Binary`.

Timezone/type metadata is treated as hostile source evidence rather than SQL. Invalid timezone
literals, unsupported physical declarations, catalog/effective metadata drift, and malformed Arrow
schema semantics fail as non-retryable Data before query execution or infallible conversion.
Missing or undersized cursor-state authority remains Internal because CDF fixes that lease before
contact and retains it for the complete decoder lifetime.

Malformed record-batch offsets, lengths, field-node values, variadic counts, union prefixes,
compressed-output declarations, schema ownership estimates, and configured row ceilings all fail
through the same non-retryable Arrow source-Data boundary. A fixed-width projection whose aligned
validity/value buffers cannot fit even one row under the compiled body ceiling is likewise source
Data. Missing compiled projection fields or impossible sizing/lease arithmetic remain Internal;
zero `max_block_rows` remains caller Contract.

The final physical/effective-schema repair keeps ownership at the same boundary: an absent or
drifting source observation, absent cataloged physical schema, missing projected physical field,
or planned/observed physical-hash disagreement is Data because the selected source no longer
matches verified discovery. A projection name absent from the compiled effective schema is
Contract because it contradicts the compiled scan. Physical field and schema metadata are
preserved rather than silently rebuilt, and none of these paths is classified as Internal.

The nineteen production Internal constructors are CDF-owned client/lease, decode-envelope,
serialization, cursor extraction, validated stable-key, and inline-partition invariants. The two
classifier Internal branches cover CDF-generated invalid parameters and impossible official-client
serialization/deserialization API use after validation. None represents an external server,
caller-contract, credential, or host-facility failure.

## Procedure

```sh
LC_ALL=C rg --files -g '*.rs' crates/cdf-source-clickhouse | LC_ALL=C sort \
  > .10x/evidence/.storage/2026-08-02-clickhouse-source-error-files.txt
error_test_line=$(rg -n '^#\[cfg\(test\)\]' crates/cdf-source-clickhouse/src/error.rs | cut -d: -f1)
LC_ALL=C rg -n --no-heading -g '*.rs' -- \
  'CdfError::(new|transient|rate_limited|auth|contract|data|destination|environment|internal|from)|ErrorKind::(Transient|RateLimited|Auth|Contract|Data|Destination|Environment|Internal)' \
  crates/cdf-source-clickhouse | LC_ALL=C sort | \
  awk -v error_test_line="$error_test_line" \
    -f .10x/evidence/.storage/2026-08-02-clickhouse-source-error-classify.awk \
  > .10x/evidence/.storage/2026-08-02-clickhouse-source-error-sites.tsv
LC_ALL=C rg -n --no-heading -g '*.rs' -- \
  'CdfError::internal|ErrorKind::Internal' crates/cdf-source-clickhouse | sort
LC_ALL=C rg -n --no-heading -g '*.rs' -- 'CdfError \{' crates/cdf-source-clickhouse
```

Frozen outputs:

- `.10x/evidence/.storage/2026-08-02-clickhouse-source-error-files.txt` — twelve Rust files;
  SHA-256 `e936abf959d8c40c8d23cd570e636e1f0b9f0d3f7ec3b1085432dab569253a98`.
- `.10x/evidence/.storage/2026-08-02-clickhouse-source-error-classify.awk` — frozen semantic
  classification rules used by the preceding no-temporary-file construction command.
- `.10x/evidence/.storage/2026-08-02-clickhouse-source-error-sites.tsv` — one header plus 205
  classified constructor/direct-kind rows; SHA-256
  `d216d7889d2372d04db0c13d5f78f05b0ce38b46f6e298c854aecded907e0204`.

## What it supports or challenges

This supports the shared error taxonomy, typed-wrapper provenance, retry ownership, and
credential-redaction requirements of `.10x/specs/clickhouse-table-source.md`. It challenges any
claim that arbitrary official-client or server text is safe to emit: only stable variants/codes
and controlled connector messages cross the boundary.

## Limits

The inventory is exact for the frozen twelve-file Rust crate. Stable-code fixtures are synthetic;
the owning ticket separately records digest-pinned live transport, type, cursor, partial-stream,
matrix, and roofline observations. Those runs still do not sample every ClickHouse code or
platform network failure. Affected lint, integration, live, roofline, and certification results
belong in the owning ticket rather than being inferred here.
