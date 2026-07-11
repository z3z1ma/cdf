Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-ws-b-schema-reconciliation-arrow-vocabulary.md
Depends-On: .10x/tickets/done/2026-07-09-p2-ws-b6-json-family-observed-reconciliation.md, .10x/specs/data-onramp-schema-intelligence.md, .10x/specs/data-onramp-source-experience-cli.md, .10x/knowledge/schema-coercion-evidence-provenance.md

# P2 WS-B7 REST observed-first reconciliation

## Scope

Route declarative REST JSON pages through the shared observed-physical-schema reconciliation front end established for local JSON/NDJSON. REST discovery, preview, and package-producing execution must not retain a separate declared-schema decoder truth.

This ticket owns REST response record materialization and reconciliation only. Reuse B6 helpers where their extraction does not widen behavior; preserve REST pagination, selector, cursor, retry, auth, egress, and redaction semantics.

## Acceptance criteria

- Each bounded REST page observes accepted JSON record types before applying the pinned or declared schema constraint.
- Lossless width/decimal widenings materialize automatically; parse coercions and lossy mappings remain governed by the existing explicit type policy.
- Row-local scalar drift produces deterministic `source_type_mismatch` quarantine without aborting accepted records or changing their order.
- Extra/missing fields, `cdf:source_name`, physical provenance, and exact coercion decisions match the local JSON/NDJSON reconciliation semantics.
- The trusted coercion plan reaches package validation evidence only through the dual-channel provenance invariant; source response metadata cannot synthesize it.
- Preview and run share the same REST response reconciliation front end, including multi-page consistency and fail-closed inconsistent plans.
- Existing cursor advancement, selector, pagination, retry, secret redaction, and egress tests remain green.

## Evidence expectations

Focused `cdf-declarative`, `cdf-contract`, `cdf-engine`, CLI preview/run, and conformance REST tests; multi-page mixed-width/drift cases; package artifact evidence; malformed/adversarial JSON; redaction checks; and the applicable `QUALITY.md` input-boundary/security/test profiles.

## Explicit exclusions

This ticket does not change REST discovery sample bounds, cursor inference, pagination policy, HTTP retries, `cdf add`, source authentication syntax, CSV/Arrow/SQL reconciliation, or contract flag vocabulary.

## Progress and notes

- 2026-07-09: Opened after B6 closed the local JSON/NDJSON split and left REST as the next live JSON-family path still requiring the same shared observed-first compiler semantics.
- 2026-07-09: Replaced the REST-only declared-schema array materializer with the B6 shared observed-first NDJSON reconciliation front end. Each non-empty decoded REST page now observes its physical JSON schema, applies the pinned or declared constraint with the active `TypePolicy`, and carries the reconciled Arrow batch, localized pre-contract quarantine facts, and exact coercion-plan header evidence forward.
- 2026-07-09: Added `RestRuntimeDependencies::with_type_policy` so REST uses the same governed parse/lossy policy surface as local JSON without changing declarative contract vocabulary. The dependency default retains the pre-existing REST parse-coercion policy; width and decimal widenings remain automatic, and lossy signed-to-unsigned materialization requires the explicit allowance.
- 2026-07-09: Enforced one byte-identical reconciliation plan across non-empty pages and fail closed on page-to-page plan changes. Pagination, selector, retry, authentication, egress rechecks, request redaction, and source schema identity remain on the existing path. Cursor positions continue to use the maximum over every source record in the page, including rows quarantined for a non-cursor field, so quarantine cannot cause the cursor to refetch an already-observed row.
- 2026-07-09: Added focused REST cases for source-name projection, physical-type metadata, lossless integer-to-decimal widening, extra-field evidence, deterministic row-local `source_type_mismatch` quarantine, accepted-row ordering, stable multi-page plans, fail-closed inconsistent page plans, and page-level cursor advancement. Strengthened the CLI run case to prove the package validation program contains the exact same coercion plan written to `schema/coercion-plan.json`.
- 2026-07-09: Verification passed: `cargo fmt --all -- --check`; `cargo test -p cdf-declarative --locked` (82 passed); `cargo nextest run -p cdf-declarative --locked` (82 passed); `cargo test -p cdf-conformance rest --locked` (2 passed); focused `cdf-formats declared_ndjson` (11 passed), `cdf-contract schema_coercion` (3 passed), and `cdf-engine package_artifacts` (3 passed); CLI REST preview, package-producing run, and discover/plan/preview/run secret-redaction cases (1 passed each); targeted five-crate Clippy; and `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`.
- 2026-07-09: Security/input checks passed: `cargo deny check`; `cargo audit` with only the repository-allowed `RUSTSEC-2024-0436` unmaintained `paste` warning; and individual `gitleaks dir --no-banner --redact` scans of the three B7-touched Rust files plus this ticket. A repository-root gitleaks invocation was interrupted after it spent prolonged time traversing the shared build tree without output; the scoped B7 scans found no leaks, and integrated repository-wide secret scanning remains part of the parent program's closure evidence.
- 2026-07-09: Closure review found that the runtime-only `RestRuntimeDependencies::with_type_policy` builder could mint parse/lossy authority without binding that choice into the compiled plan or package identity. This supersedes the earlier progress note describing that builder as an acceptable policy surface. Removed the type-policy field and builder entirely from production REST dependencies; live REST now calls B6's strict declared-NDJSON reader directly, with `coerce_types = false` and `allow_lossy_mapping = false`. No trust preset implicitly grants either allowance. A future allowance requires a separately ratified compiled/TOML surface that participates in plan and package identity.
- 2026-07-09: Updated unrelated REST fixtures to use physically matching JSON scalar and cursor types. String-to-date/timestamp behavior is now a strict fail-closed test, signed-to-unsigned no longer receives a test-only runtime allowance, row-local string drift remains quarantine, and the acceptance demo uses an integer cursor because project checkpoint window-close semantics do not ratify UTF-8 cursors. Automatic lossless integer-to-decimal widening, cursor safety, multi-page consistency, and exact dual-channel evidence remain unchanged.
- 2026-07-09: Added the production CLI regression `run_rest_runtime_defaults_cannot_authorize_parse_or_lossy_coercion`. It proves a parse-like row is quarantined while the committed package contains neither `CoercedByPolicy` nor `LossyAllowed`, and proves signed-to-unsigned REST reconciliation fails with `allow_lossy_mapping` guidance rather than emitting lossy package evidence. Post-repair verification passed: full `cdf-declarative` tests (82), focused REST runtime tests (37), CLI strict-authority regression, CLI REST run/preview/discover paths, REST conformance including the MVP acceptance demo (2), formatting/diff checks, and workspace all-target/all-feature Clippy.
- 2026-07-09: Closed after parent-observed integration verification passed 773/773 workspace tests plus formatting, check, strict Clippy, doc tests/docs, dependency policy, advisory, and scoped secret scans. Closure evidence: `.10x/evidence/2026-07-09-p2-b7-f2-integration.md`. Adversarial review: `.10x/reviews/2026-07-09-p2-b7-f2-integration-review.md`. Retrospective learning is preserved in `.10x/knowledge/type-policy-authority.md`.

## Blockers

None. B6, the active schema-intelligence spec, and the evidence-provenance knowledge record fully govern this slice.
