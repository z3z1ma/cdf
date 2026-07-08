Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-07-p0-workstream-c-spine-conformance-harness.md
Depends-On: .10x/tickets/2026-07-08-p0-c1-run-spine-matrix-foundation.md

# P0 C2: REST and SQL run-spine matrix cells

## Scope

Extend the run-spine scenario matrix from C1 to deterministic REST-fixture and table-backed Postgres SQL source archetypes across DuckDB, filesystem Parquet, and Postgres destinations and append, replace, and merge where destination sheets support them.

Owns:

- deterministic REST transport fixtures and runtime dependencies in conformance;
- deterministic Postgres SQL source fixtures using the existing local Postgres harness conventions;
- source-specific matrix fixture construction;
- matrix output evidence updates for REST and SQL executed/excluded cells.

## Acceptance Criteria

- REST cells use deterministic in-process/fake HTTP transport and do not contact the public network.
- SQL cells use local ephemeral Postgres source setup consistent with existing destination/source tests.
- REST and SQL resources enter `run_project` through `ProjectRunSource::rest` and `ProjectRunSource::sql`, respectively.
- Each executed REST/SQL cell uses the same matrix assertions required by C1: plan honesty, package validity, trait-level receipt verification, checkpoint gating, replay identity, and duplicate behavior.
- Sheet-excluded cells are recorded with a reason in the matrix output, not skipped.
- Secret values used by REST/SQL fixtures are not written into evidence records or serialized artifacts except as redacted references.

## Evidence Expectations

Run focused REST/SQL matrix tests, relevant declarative runtime tests if helpers are touched, `cargo fmt --all --check`, `cargo check -p cdf-conformance -p cdf-project -p cdf-declarative --all-targets --locked`, `cargo clippy -p cdf-conformance -p cdf-project -p cdf-declarative --all-targets --locked -- -D warnings`, `cargo nextest run -p cdf-conformance --locked`, `git diff --check`, redaction-focused Gitleaks if fixture secrets are introduced, and updated matrix evidence with executed/excluded cells.

## Explicit Exclusions

No live GitHub/public HTTP API execution. No arbitrary SQL query source support beyond the already-ratified table-backed SQL resource path. No new destination/source archetype. No cross-destination chaos or property/fuzz work.

## Progress And Notes

- 2026-07-08: Split from P0 Workstream C. Existing project runtime tests already prove deterministic REST and table-backed SQL can run into DuckDB; this child moves that coverage into conformance and expands it across current MVP destinations/dispositions.

## Blockers

C1 should land first so REST/SQL cells reuse the same matrix model and assertion surface.
