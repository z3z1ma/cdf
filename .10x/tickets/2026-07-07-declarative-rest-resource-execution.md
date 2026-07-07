Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-conformance-chaos-golden.md
Depends-On: .10x/tickets/done/2026-07-05-http-toolkit.md, .10x/tickets/done/2026-07-06-resource-conformance-suite-foundation.md

# Add declarative REST resource execution

## Scope

Implement the first openable Tier-0 declarative REST resource execution path over the existing `cdf-http` toolkit and public `ResourceStream` contract.

Owns:

- `crates/cdf-declarative/**` for REST runtime construction, JSON record selection, batch production, and tests.
- `crates/cdf-http/**` only for small additive toolkit helpers required to make REST execution deterministic and testable.
- `crates/cdf-conformance/**` only for reusable execution conformance cases or assertion helpers needed by REST resources.
- `.10x/` evidence, review, and ticket records for this child.

Keep crate roots thin. Add focused modules such as `rest_runtime` rather than expanding `lib.rs` or `compiled.rs` into a monolith.

## Acceptance criteria

- `CompiledResource` REST plans can be converted into an openable REST resource using explicit runtime dependencies: an `HttpTransport`, an optional `SecretProvider`, and any required deterministic clock/sleep substitutes. Tests MUST use deterministic in-memory transports; no live internet or ambient network dependency is allowed.
- The default `CompiledResource::open` behavior for REST without runtime dependencies remains a clear error rather than silently using ambient network access.
- REST execution builds safe `GET` requests from `base_url`, `path`, static query params, cursor query params, and pagination config. Query construction MUST preserve existing params and MUST reject malformed or non-HTTP(S) base URLs before transport use.
- `cdf-http::EgressAllowlist` is enforced before transport use. A denied request MUST leave the mock transport untouched.
- Bearer and header auth declarations resolve only through `SecretProvider`, apply to requests without formatting secret values into errors/debug output, and support one auth refresh retry on `401`/`403` when a refresh hook is configured.
- Pagination uses `cdf-http::Paginator` for link-header, cursor, page-number, offset, and next-token modes. The runtime MUST stop when the paginator returns no next request and MUST guard against duplicate cursor/token loops through the existing paginator semantics.
- JSON responses are decoded into Arrow `RecordBatch` payloads for declared schemas. This first slice MUST support record selectors `$` for a top-level array and `$.<field>` for an object field containing an array. Missing selector targets, non-array records, unsupported field types, and uncoercible values MUST fail as data errors without emitting partial batches.
- Produced batches satisfy resource execution conformance: correct resource id, partition id, unique batch id, row count, byte count, observed schema hash, and `RecordBatch` payload.
- Cursor-bearing REST resources emit a `SourcePosition::Cursor` from the maximum observed cursor field in each emitted page when the cursor value is representable by the current `CursorValue` shapes. Cursor fields missing from accepted records MUST fail closed.
- Request filters negotiated as cursor pushdown MUST affect the first request when the resource has a `cursor.param`; unsupported predicates MUST remain unsupported and MUST NOT be smuggled into URLs.
- Focused negative self-tests prove the runtime fails closed for allowlist denial, missing secrets, non-JSON response body, selector mismatch, cursor field absence, schema coercion failure, and pagination loop/empty termination.
- Existing declarative planning-level REST behavior and conformance expectations remain source-compatible.

## Evidence expectations

Record focused checks:

- `cargo fmt --all -- --check`
- `git diff --check -- . ':(exclude).gitignore'`
- `cargo test -p cdf-declarative --locked --no-fail-fast`
- `cargo test -p cdf-conformance --locked resource -- --nocapture` if conformance helpers change
- `cargo clippy -p cdf-declarative -p cdf-http -p cdf-conformance --all-targets --locked -- -D warnings`
- `cargo nextest run -p cdf-declarative -p cdf-http -p cdf-conformance --locked`

Before closure, run relevant `QUALITY.md` gates, parallelized where practical: workspace check/test/clippy, docs, cargo-hack feature checks, cargo deny/audit/vet/OSV, Semgrep over touched crates, source-only gitleaks, direct unsafe/FFI/raw-pointer scan, dependency hygiene, and bounded mutation testing over the new REST runtime and conformance assertions where feasible. Skip CodeQL for this checkpoint per the active goal instruction; do not recreate the CodeQL database.

## Explicit exclusions

No live GitHub/API integration test, no CLI `run` widening to REST resources, no package/checkpoint lifecycle changes, no SQL source execution, no DataFusion `TableProvider` HTTP scan provider, no REST transform escape-hatch execution, no OAuth flow, no streaming supervisor, no run ledger/default ids, no `resume`, no `replay package`, no destination changes, no CI workflow changes, and no `.gitignore` edits.

The MVP killer-demo path remains parent scope until this REST resource can be connected through `cdf run` with explicit runtime inputs in a separate child.

## References

- `VISION.md` D-1, D-2, D-7, D-17, Chapter 8, Chapter 9, Chapter 15, Chapter 20, and Chapter 22.
- `.10x/specs/resource-authoring-planning-batches.md`
- `.10x/specs/conformance-governance-roadmap.md`
- `.10x/tickets/done/2026-07-05-http-toolkit.md`
- `.10x/tickets/done/2026-07-06-resource-conformance-suite-foundation.md`
- `.10x/tickets/done/2026-07-06-resource-execution-conformance-file-sources.md`
- `.10x/knowledge/quality-gate-execution.md`
- `.10x/knowledge/rust-crate-organization.md`

## Progress and notes

- 2026-07-07: Split from the conformance parent after local DuckDB lifecycle chaos closed. Current declarative REST resources compile and negotiate cursor pushdown, and `cdf-http` has pure toolkit primitives, but `CompiledResource::open` still returns an explicit unsupported error for REST/SQL. This child makes REST openable through explicit runtime dependencies and deterministic conformance tests without adding live network or CLI run orchestration.
- 2026-07-07: Do not implement in the ticket-creation turn. Assign to a worker in a later turn with the bounded write boundary above; parent owns integration review, evidence, and final commit.

## Blockers

None for the deterministic declarative REST resource execution slice.
