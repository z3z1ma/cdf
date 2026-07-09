Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-i-conformance-parity-friction-suite.md
Depends-On: .10x/tickets/done/2026-07-09-p2-ws-a7-schema-pin-show-diff-cli.md, .10x/tickets/done/2026-07-09-p2-ws-d3-file-manifest-incremental-noop.md, .10x/tickets/done/2026-07-09-p2-ws-d4-gzip-zstd-file-decode-foundation.md, .10x/tickets/done/2026-07-09-p2-ws-e2-http-file-runtime-and-discovery.md, .10x/tickets/done/2026-07-09-p2-ws-g1-source-diagnostics-and-deep-validate-foundation.md, .10x/tickets/done/2026-07-09-p2-ws-h2-cdf-add-single-file-parquet.md, .10x/specs/data-onramp-conformance.md

# P2 WS-I3 conformance matrix and friction-registry reconciliation

## Scope

Repair the P2 conformance registry after A7/D3/D4/E2/G1/H2/B5 closed. The current matrix still points at terminal ticket paths as active owners, omits concrete later tests, and overstates S8 as globally covered.

## Acceptance criteria

- S1-S8 rationales accurately describe current implementation and evidence limits.
- Terminal child tickets are references/evidence only, never active blockers.
- Friction rows 11-16 name the concrete G1/D4/E2/H2 tests now guarding their closed slices.
- S8 is represented as partial/pending until every required source archetype uses the same full compiler front end; the existing local file/REST/Postgres law remains recorded as partial evidence.
- Active-owner validation reads ticket files and fails when an owner is missing, terminal, or not a ticket record.
- The durable friction evidence registry is updated consistently with the executable matrix.
- No scenario is promoted to covered solely by registry maintenance.

## Evidence expectations

Focused `cdf-conformance` P2 registry tests, record-link validation, `git diff --check`, and review that attempts to find stale/terminal owners or overclaimed scenarios.

## Explicit exclusions

This ticket does not implement missing S1-S8 runtime behavior, add new transports, or close the P2 program. It repairs ownership truth so later implementation cannot hide behind stale strings.

## Progress and notes

- 2026-07-09: Opened after read-only audit found stale A7/D3/D4/E2/G1/H2 owners and missing later test names in `crates/cdf-conformance/src/run_matrix/data_onramp.rs`.
- 2026-07-09: Reconciled S1-S8 against the current closed slices. S1 now records deterministic HTTPS discovery/run plus HTTP `cdf add` as partial evidence; S2 records local append manifest incrementality/no-op; S3 records local gzip/zstd decode; S4/S5/S6 rationales name their remaining acceptance gaps; and S8 is pending because the existing local-file/REST/Postgres row/schema fingerprint law does not prove full compiler-front-end parity for every required archetype. No scenario was promoted to covered.
- 2026-07-09: Removed closed A7/D3/D4/E2/G1/H2 child tickets from active-owner arrays. Remaining blockers point only at open P2 workstream tickets. Active-owner validation now reads the record at each path, accepts only `open | active | blocked`, and rejects missing files, terminal `done | cancelled` records, non-ticket paths, missing statuses, and unsupported statuses.
- 2026-07-09: Added executable validation that every scenario/friction test string resolves to an existing source file and concrete Rust test function. Friction rows 11-16 now name the current G1, E2, and D4 guards, including command-correct plan wording, source-specific remediation, deep validate, production HTTP Parquet discovery/run, and local compressed NDJSON decode/mismatch tests. Updated `.10x/evidence/2026-07-08-p2-friction-regression-registry.md` to the same evidence and active-owner boundaries.
- 2026-07-09: Focused verification passed: `cargo test -p cdf-conformance --locked p2_registry_named_tests_resolve_to_test_functions -- --nocapture`; `cargo test -p cdf-conformance --locked p2_active_owner_validation_reads_status_and_rejects_invalid_owners -- --nocapture`; `cargo test -p cdf-conformance --locked p2_friction_registry_maps_closed_slices_to_tests_and_open_rows_to_tickets -- --nocapture`; and `cargo test -p cdf-conformance --locked p2_data_onramp_scenario_matrix_records_s1_through_s8 -- --nocapture` (one test passed in each command). `cargo fmt -p cdf-conformance -- crates/cdf-conformance/src/run_matrix/data_onramp.rs` and scoped `git diff --check` also passed. A shell cross-check over every quoted `crates/...::test` reference reported no missing source or function.
- 2026-07-09: Verification limit: this registry-maintenance slice did not rerun the Postgres-backed `p2_preview_run_parity_law_covers_supported_archetypes`; its previously recorded local-file/REST/Postgres evidence remains partial by design, and I3 changes only its status/rationale and registry ownership. An early sequential test retry briefly encountered concurrent B6 compilation before B6 added its dependency; the final focused I3 commands above compiled and passed after that shared-tree change settled.
- 2026-07-09: Closure repair removed the positive owner test's hardcoded `open` expectation. It now remains valid across the intentionally accepted nonterminal transitions `open | active | blocked`, while the missing, terminal, and non-ticket negative assertions are unchanged. `cargo test -p cdf-conformance --locked p2_active_owner_validation_reads_status_and_rejects_invalid_owners -- --nocapture`, `cargo test -p cdf-conformance --locked p2_friction_registry_maps_closed_slices_to_tests_and_open_rows_to_tickets -- --nocapture`, scoped formatting, and scoped `git diff --check` all passed.
- 2026-07-09: Closed after `.10x/evidence/2026-07-09-p2-a8-b6-i3-integration.md` and `.10x/reviews/2026-07-09-p2-a8-b6-i3-integration-review.md`. Retrospective: executable registries must validate both the referenced artifact and its lifecycle state, and tests of accepted state sets must remain transition-safe.

## Blockers

None.
