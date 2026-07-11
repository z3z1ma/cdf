Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-ws-c-source-identity-normalization.md
Depends-On: .10x/decisions/data-onramp-source-identity-preview-disposition.md, .10x/specs/types-contracts-normalization.md, .10x/tickets/done/2026-07-08-p2-ws-c1-declarative-schema-normalization.md, .10x/tickets/done/2026-07-09-p2-ws-c2-destination-identifier-policy-adapter.md

# P2 WS-C3 live DuckDB and Postgres destination normalization

## Scope

Use the resolved DuckDB or Postgres destination sheet's `IdentifierRules` to derive the live column identifier policy at plan time, and carry the resulting normalized output schema consistently through preview, package production, destination commit, and package evidence.

This is the first live destination-policy join. It owns namecase-backed DuckDB and Postgres only. The source-facing pinned snapshot remains observed/source evidence; destination normalization is a compiled output-schema transformation and must not silently rewrite the source snapshot or replace its declared/pinned hash in the existing shared `StateDelta`/package/receipt/checkpoint `schema_hash` field. Until a separate observed/output hash artifact contract is ratified, the effective destination policy and normalized output schema are additional identity evidence, not a second meaning smuggled into that field.

## Acceptance criteria

- Plan-time destination resolution adapts DuckDB/Postgres sheet rules through the C2 adapter; no live path hard-codes `IdentifierPolicy::default()` after the destination is known.
- The same destination-normalized schema governs plan rendering, preview batches, package Arrow schema/segments, and destination validation/commit. The validation program, package schema artifacts, and policy evidence record that output identity while the existing shared `schema_hash` remains the declared/pinned resource hash.
- Automatic renames preserve the physical/source identifier as `cdf:source_name`; explicit `source_name` remains an ambiguity override rather than a per-field requirement.
- DuckDB accepts mixed/camel-case source columns such as `VendorID` without manual mappings.
- Postgres applies its 63-byte identifier limit deterministically, including stable hash suffixing, and the committed table/package agree on the truncated name.
- Post-normalization collisions fail before extraction or destination writes and include both source names plus explicit rename guidance.
- Plans/packages record the effective normalizer version and destination policy parameters needed to explain an output-schema change.
- Deterministic conformance covers DuckDB and Postgres plan/preview/run behavior; existing default declarative and snapshot normalization tests remain green.

## Evidence expectations

Focused project/engine/contract/destination tests, plan JSON and package artifact assertions, live local DuckDB plus fixture-backed or local-test Postgres conformance, collision/no-write checks, hash stability, and applicable `QUALITY.md` schema/identity profiles.

## Explicit exclusions

Parquet destination column policy is excluded because its current sheet declares `object-key-component-v1`, which C2 correctly classifies as a non-column normalizer. This ticket must not reinterpret that rule. A later record must define whether Parquet columns use source `namecase-v1`, a distinct sheet field, or another explicit policy. The normalizer algorithm itself and source snapshot format are unchanged.

## Progress and notes

- 2026-07-09: Opened after C1 established automatic source-name/default normalization and C2 established a fail-closed destination-rule adapter. Source inspection confirmed the project destination planning facade has the resolved sheet but currently passes the resource schema directly into destination planning.
- 2026-07-09: Bound the resolved DuckDB/Postgres destination sheet to contract compilation before live plan/run. `ValidationProgram` now serializes the full effective `IdentifierPolicy` (version, maximum length, charset, and allowed pattern), while the existing normalizer version remains in the operator chain. Parquet returns no column policy without dereferencing its intentionally unmaterialized protocol, preserving the explicit object-key exclusion.
- 2026-07-09: Plan and run now resolve destination policy before compiling the validation program; backfill and the lower-level run matrix do the same. Preview invokes the same exported engine record-batch normalizer used by package production and reports the effective fields/policy. Contract evaluation and NormalizeExec resolve columns through `cdf:source_name`, allowing DuckDB's unbounded policy to restore a source-derived name previously truncated by C1's destination-neutral 63-byte default.
- 2026-07-09: DuckDB/Postgres planning validates destination-normalized schemas, Postgres derives load columns from that schema, and package segments/commits consume the exact NormalizeExec output. Automatic source provenance remains on every renamed field. The pinned/declared resource schema hash remains the shared StateDelta/package/receipt/checkpoint identity per the active discovery decision; destination output identity is evidenced by the serialized IdentifierPolicy, validation program column outputs, and `schema/output.json` rather than replacing the pinned hash.
- 2026-07-09: Added direct regressions: `duckdb_destination_policy_normalizes_plan_preview_package_and_commit` proves `VendorID` requires no manual mapping and plan/preview/package/DuckDB columns agree, including an unbounded >63-byte identifier; `postgres_destination_policy_truncates_package_and_committed_column_identically` proves stable 63-byte hash suffixing and package/table agreement against local Postgres; `destination_normalization_collision_fails_before_writes` proves both source names and rename guidance appear before package/state/destination writes; the Parquet destination-planning test now explicitly proves no column policy lookup or protocol panic.
- 2026-07-09: Focused evidence passed: `cdf-contract` 35/35; `cdf-engine` 34/34; run-matrix conformance 9/9; DuckDB direct regression 1/1; collision/no-write 1/1; Postgres live regression 1/1; Parquet-safe planning 1/1; affected trust-ring 5/5, dedup 1/1, quarantine 1/1, HTTP Parquet 1/1, and backfill 3/3 regressions. An earlier full affected run found only the now-repaired legacy planner bindings; the parent retains final workspace verification and closure review.
- 2026-07-09: Integration with I4 exposed an identity ambiguity. Resolved from `.10x/decisions/data-onramp-schema-discovery-reconciliation.md`, the package/state preimage contract, and resource-scoped checkpoint semantics: destination normalization must not replace the pinned resource hash in the one existing shared `schema_hash` field. A future distinct observed/output hash model would require its own specification and migration.
- 2026-07-09: Integration review found that equality of the serialized destination policy alone was insufficient: a stale plan could copy the new `IdentifierPolicy` while retaining column outputs compiled under an older policy. Project preflight now fail-closes before writes unless `normalizer_version` equals the policy version and the column program count, order, source names, and output names exactly match re-normalization of the current resource schema under that policy. Adversarial long-name policy-spoof and stale-version tests prove package, state, and destination files remain absent. Legacy validation-program JSON without `identifier_policy` still deserializes to the version-consistent default.
- 2026-07-09: Repaired all conformance plan builders to compile against the resolved destination policy, including live local-file, drift-quarantine, REST MVP, and run-matrix file plans; the run-matrix helper now recompiles the complete validation program rather than mutating policy evidence on a default program. Refreshed live DuckDB, Postgres, and Parquet golden identities only for the newly serialized policy bytes after printing verified fixture evidence. Final evidence: focused legacy-serde/stale-plan tests 3/3; original live-run integration regressions 4/4; MVP and run-matrix focused tests 2/2; full `cargo nextest run -p cdf-conformance --locked --no-fail-fast` 77/77, including DuckDB and Parquet 100-repeat determinism and bounded live Postgres.
- 2026-07-09: Final affected-crate verification passed: `cargo nextest run -p cdf-contract -p cdf-project --locked --no-fail-fast` 151/151; `cargo fmt --all -- --check`; `git diff --check`; and Clippy for `cdf-contract`, `cdf-project`, and `cdf-conformance` across all targets with locked dependencies and warnings denied.
- 2026-07-09: Closed after parent-observed workspace verification passed 781/781 tests, docs, strict Clippy, dependency/advisory/semver gates, changed-file secret scanning, and 83.38% line coverage. Closure evidence: `.10x/evidence/2026-07-09-p2-c3-i4-integration.md`. Adversarial review: `.10x/reviews/2026-07-09-p2-c3-i4-integration-review.md`.

## Blockers

None for DuckDB and Postgres. Parquet is an explicit exclusion, not an inferred default.
