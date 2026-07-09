Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-i-conformance-parity-friction-suite.md
Depends-On: .10x/specs/data-onramp-conformance.md, .10x/specs/data-onramp-schema-intelligence.md, .10x/specs/data-onramp-source-experience-cli.md, .10x/tickets/done/2026-07-09-p2-ws-a6-rest-sample-discovery-autopin.md, .10x/tickets/done/2026-07-09-p2-ws-b7-rest-observed-reconciliation.md, .10x/tickets/done/2026-07-09-p2-ws-f2-s7-key-disposition-experience.md

# P2 WS-I4 standalone S5 and S7 conformance

## Scope

Add deterministic, standalone `cdf-conformance` scenarios for the already-implemented S5 REST discover/pin/package flow and S7 key/disposition flow. Promote only S5 and S7 in the executable P2 scenario matrix after the exact conformance tests pass and no longer depend on active implementation tickets.

The S5 fixture must exercise a declarative REST resource with no declared schema, an explicitly declared cursor, one bounded recorded sample page, deterministic snapshot pinning, preview, package-producing run, package evidence, receipt verification, and committed checkpoint schema identity. The S7 fixture must exercise keyless append through the operator path and merge-without-key failure before source contact or project mutation, including both remediations.

## Acceptance criteria

- `cdf-conformance` contains a named S5 test that runs a deterministic REST fixture from discover mode through pin, preview, package, verified destination receipt, and committed checkpoint.
- S5 proves the snapshot metadata records the REST sample-page probe, the pinned hash is reused by run, cursor advancement is committed, `cdf:source_name` survives, and secrets are absent from output/artifacts.
- `cdf-conformance` contains a named S7 test proving append needs no key and emits no key nudge, while merge without `merge_key` fails once before source contact or filesystem mutation with the two ratified fixes.
- The S5 and S7 matrix rows become `Covered`, name only the standalone conformance tests (plus supporting tests when useful), and carry no active-ticket blockers.
- Friction row 17 names the standalone S7 scenario; registry validation continues to resolve every named test and reject terminal owners.
- No other S1-S8 row is promoted.

## Evidence expectations

Focused `cdf-conformance` tests, the P2 matrix/owner/named-test checks, deterministic rerun evidence, package verification assertions for S5, no-contact/no-write assertions for S7, workspace formatting/Clippy, and parent integration verification.

## Explicit exclusions

This ticket does not add REST support to `cdf add`, infer a cursor, change discovery sample bounds, add type-policy syntax, implement exact-row dedup, or promote S1-S4/S6/S8. S5 requires the cursor to be explicitly declared, as the active specification says APIs cannot be guessed safely.

## Progress and notes

- 2026-07-09: Opened after A6/B7/F2 established the runtime behavior and I3 made the registry executable. Existing CLI integration tests are supporting evidence, but the matrix deliberately retained S5/S7 as pending until standalone conformance owned the complete scenarios.
- 2026-07-09: Implemented standalone CLI-driven S5 and S7 scenarios in the executable P2 matrix. S5 uses a deterministic recorded HTTP fixture to pin the same discover-mode REST sample twice, proves byte-stable snapshot/lock output, previews without writes, runs to DuckDB, verifies the package and destination receipt, checks committed cursor/schema identity, preserves `cdf:source_name`, and scans generated artifacts for the resolved secret. S7 runs keyless append through validate/plan/preview/run and proves merge-without-key returns the command-specific catalog error with both fixes before HTTP contact or any file/directory mutation.
- 2026-07-09: Promoted only S5 and S7 to `Covered`, removed their active-ticket blockers, registered the standalone S7 test on friction row 17, and updated the durable friction registry with the same exact-row-dedup exclusion boundary. Focused verification is in progress; the first conformance check encountered a transient out-of-scope `cdf-cli` compile mismatch while the parallel C3 lane was changing the scan planner signature, so I4 did not edit that surface and will rerun after the owning lane is coherent.
- 2026-07-09: Focused verification passed after C3 restored the ratified schema identity: `cargo test -p cdf-conformance p2_s5_rest_discover_pin_preview_run_package_checkpoint_conformance --locked -- --nocapture` (1 passed), `cargo test -p cdf-conformance p2_s7_keyless_append_and_precontact_merge_failure_conformance --locked -- --nocapture` (1 passed), `cargo clippy -p cdf-conformance --all-targets --locked --no-deps -- -D warnings`, `cargo fmt --all -- --check`, scoped `rustfmt --edition 2024 --check`, and `git diff --check`. A second run through `cargo test -p cdf-conformance 'run_matrix::data_onramp::p2_' --locked -- --nocapture` passed both standalone scenarios again plus every matrix/owner/named-test registry check, providing deterministic rerun evidence.
- 2026-07-09: The same broader command exposed an S8 parity integration failure after the parallel destination-normalization change: the run-matrix helper planned a Postgres 63-byte identifier policy for a DuckDB cell, then the resolved DuckDB destination rejected it. Seven P2 tests passed and only `p2_preview_run_parity_law_covers_supported_archetypes` failed at the destination commit. This was repaired and closed by `.10x/tickets/done/2026-07-09-p2-ws-c3-live-destination-normalization-duckdb-postgres.md`; the final combined P2 and workspace suites pass.
- 2026-07-09: Parent review hardened S7's pre-contact proof: the merge fixture now points at a deliberately nonexistent file-secret sentinel, asserts that path is absent before planning, and proves neither the sentinel nor full `secret://` reference appears anywhere in the raw JSON error envelope. Zero HTTP requests and a byte-identical project tree remain required. The exact S7 test passed (1/1). The required combined `cargo test -p cdf-conformance 'run_matrix::data_onramp::p2_' --locked -- --nocapture` initially exposed a fixture-only S5 race because the recorded server captured a single partial TCP read under parallel load; the helper now reads through the HTTP header terminator. The combined rerun passed all 8/8 P2 tests, including S5, S7, registry validation, and preview/run parity after C3's repair.
- 2026-07-09: Closed after independent review passed and parent-observed workspace verification passed 781/781 tests plus the full quality profile. Closure evidence: `.10x/evidence/2026-07-09-p2-c3-i4-integration.md`. Review: `.10x/reviews/2026-07-09-p2-c3-i4-integration-review.md`.

## Blockers

None. The active conformance, schema-intelligence, and source-experience specifications fully define this test-only slice.
