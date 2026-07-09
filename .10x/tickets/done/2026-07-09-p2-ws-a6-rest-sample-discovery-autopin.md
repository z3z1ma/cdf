Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-a-discovery-compiler-stage.md
Depends-On: .10x/decisions/data-onramp-schema-discovery-reconciliation.md, .10x/specs/data-onramp-schema-intelligence.md, .10x/tickets/done/2026-07-09-p2-ws-a5-generic-discover-autopin-postgres-run.md

# P2 WS-A6 REST sample discovery auto-pin

## Scope

Implement the next bounded discovery compiler slice for declarative REST resources: one-page sample discovery that produces a pinned schema snapshot and lets `cdf schema discover`, `cdf plan`, `cdf preview`, and `cdf run` use that snapshot without requiring hand-declared REST schema fields.

REST discovery is a compiler-stage probe, not execution. It may perform exactly one GET through the existing REST runtime dependencies, auth, retry, rate-limit, and egress allowlist path. It MUST apply the configured record selector to that response and infer a schema from the selected record objects. Cursor semantics remain declared by the resource; this ticket MUST NOT guess a cursor for REST resources.

## Acceptance criteria

- `cdf schema discover <rest-resource>` uses the generic project discovery dispatcher and prints a deterministic schema snapshot report without package, destination, checkpoint, or run-ledger writes.
- `cdf plan`, `cdf preview`, and `cdf run` auto-pin unpinned REST discover-mode resources before package-producing execution, write `.cdf/schemas/<resource>@<hash>.json`, and report `schema_source = discovered` in their existing JSON surfaces.
- REST execution accepts `SchemaSource::Discovered { .. }` and uses the pinned snapshot schema hash for batch/package evidence instead of failing with "requires a declared schema hash".
- Discovery uses the existing auth/secret/allowlist/request-validation path and does not leak resolved secret values in CLI output, snapshot artifacts, evidence, or errors.
- Inference is deterministic for supported JSON scalar fields: boolean, signed integer, unsigned integer where needed, float64, utf8, and nullable fields when any sampled record is null or missing. Unsupported nested arrays/objects may be discovered as `utf8` only if execution can serialize them through the existing REST value path; otherwise they fail discovery with an actionable error naming the field.
- REST cursor fields must still be declared. A discover-mode REST run with no cursor remains governed by existing run-spine cursor validation, not by guessed discovery behavior.

## Evidence expectations

- Focused unit tests for REST sample-page discovery inference, selector failure, auth/allowlist use, and `SchemaSource::Discovered` REST execution.
- CLI tests for `cdf schema discover` and plan/preview/run auto-pin using a local HTTP fixture, including no package/destination/checkpoint writes for discover-only and no secret leakage.
- Project/runtime tests proving a discovered REST resource can commit a checkpoint and package with the pinned snapshot hash.
- Required quality gates: focused tests, affected-crate clippy/test, `cargo fmt --all -- --check`, `git diff --check`, jscpd, rust-code-analysis, Semgrep, Gitleaks, cargo deny/audit/vet/machete/OSV, and reusable CodeQL.

## Explicit exclusions

This ticket does not implement multi-page discovery sampling, pagination-wide union, cursor inference, `cdf schema pin|show|diff`, `cdf add`, REST conformance S5 closure, remote object/file discovery, Python generator discovery, WASM boundary discovery, Avro-like file discovery, or full WS-B validation-program serialization for coercion verdicts.

## Progress and notes

- 2026-07-09: Opened after A5 closed and the user reasserted that discovery is not remotely done. Source inspection found `crates/cdf-project/src/schema_discovery.rs` still has a REST unsupported branch, and `crates/cdf-declarative/src/rest_runtime.rs` still rejects `SchemaSource::Discovered` with the declared-schema-only error.
- 2026-07-09: Implemented REST one-page sample discovery through the existing REST request/auth/allowlist path, generic dispatcher REST support, CLI `schema discover` no-write reporting, plan/preview/run first-use auto-pin for REST discover-mode resources, and REST execution with pinned discovered snapshot hashes. Focused runtime/project/CLI tests, affected-crate clippy, `cargo fmt --all -- --check`, and `git diff --check` passed in the worker tree.
- 2026-07-09: Parent review fixed late-field nullable inference, paginator first-request discovery, helper shape clippy, and discovery snapshot duplication before closure.
- 2026-07-09: Closure evidence recorded in `.10x/evidence/2026-07-09-p2-ws-a6-rest-sample-discovery-autopin.md`; adversarial review recorded in `.10x/reviews/2026-07-09-p2-ws-a6-rest-sample-discovery-autopin-review.md`.

## Blockers

None at closure.
