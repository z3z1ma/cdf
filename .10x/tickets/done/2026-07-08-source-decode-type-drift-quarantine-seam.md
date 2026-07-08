Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/done/2026-07-08-p1-e6-drift-quarantine-conformance.md
Depends-On: .10x/decisions/source-decode-type-drift-quarantine.md

# Source decode type-drift quarantine seam

## Scope

Implement the narrow pre-contract source decode quarantine seam ratified by `.10x/decisions/source-decode-type-drift-quarantine.md` so P1 E6 can use a literal source scalar type-drift fixture.

Likely write scope:

- `crates/cdf-kernel/**` for a source/runtime-neutral pre-contract quarantine fact shape if needed;
- `crates/cdf-formats/**` and/or `crates/cdf-declarative/**` for declared-schema NDJSON localized row-field mismatch handling;
- `crates/cdf-engine/**` for folding pre-contract quarantine facts into package quarantine artifacts and verdict/quarantine summaries;
- `crates/cdf-conformance/**` for the E6 numeric type-drift scenario and malformed-input negative coverage.

## Governing records

- `VISION.md` Chapters 11, 20, and 23.3.
- `.10x/specs/types-contracts-normalization.md`.
- `.10x/specs/package-lifecycle-determinism.md`.
- `.10x/specs/run-orchestration-ledger.md`.
- `.10x/specs/conformance-governance-roadmap.md`.
- `.10x/decisions/contract-live-verdict-execution-semantics.md`.
- `.10x/decisions/source-decode-type-drift-quarantine.md`.
- `.10x/tickets/done/2026-07-08-p1-e6-drift-quarantine-conformance.md`.

## Acceptance criteria

- A declared-schema NDJSON fixture with a localized scalar type mismatch, such as numeric `event_type: 42` under a frozen string contract, produces a package quarantine record with `error_code = "source_type_mismatch"` and deterministic field-scoped `rule_id`.
- Accepted rows from the same fixture continue through package data segments, destination commit, trait receipt verification, and receipt-gated checkpoint commit.
- Package evidence includes `quarantine/part-*.parquet`, `stats/verdict-summary.json`, `stats/quarantine-summary.json`, the validation program, destination quarantine mirror outcome, and trust-ring events where triggered.
- Malformed JSON and unlocalizable decode errors still fail closed before package finalization and destination mutation.
- The E6 conformance scenario is updated from the temporary domain-value drift fixture to the literal type-drift fixture and passes for unsupported mirror exclusion and Postgres supported mirror coverage.

## Evidence expectations

Record focused unit tests for the decoder/runtime fact path, E6 conformance output, package artifact inspection, destination receipt verification, checkpoint evidence, redaction checks for observed values, jscpd and `rust-code-analysis-cli` metrics, direct unsafe scan, security scans, and adversarial review.

## Explicit exclusions

No broad schema-on-read replacement, no DataFusion multi-output plan, no silent scalar coercion as acceptance, no public demo script, and no new source archetype or destination.

## Progress and notes

- 2026-07-08: Opened from P1 E6 review. A numeric JSON scalar in the drift fixture currently fails in `cdf-formats` before `ContractExec` with `expected string got 42`; this prevents E6 from closing with literal type-drift quarantine.
- 2026-07-08: Activated for implementation. Parent read the governing decision and identified the narrow preferred seam as source/runtime-owned pre-contract quarantine facts carried from declared-schema file decode into `cdf-engine` package quarantine evidence.
- 2026-07-08: Implemented the passive `cdf-kernel` pre-contract quarantine fact shape and `cdf-engine` package folding slice. Focused tests construct the source-decode fact manually; declared-schema NDJSON decoding and E6 fixture updates remain outside this slice. Evidence: `.10x/evidence/2026-07-08-source-decode-pre-contract-kernel-engine.md`.
- 2026-07-08: Completed the seam end to end. Declared-schema JSON/NDJSON reads now localize scalar source type mismatches into pre-contract quarantine facts, preserve accepted rows, fail closed for malformed/unlocalizable input, redact `pii:*` observed values, and feed package quarantine/verdict summaries. E6 now uses literal numeric type drift. Evidence: `.10x/evidence/2026-07-08-source-decode-type-drift-quarantine-seam.md`; review: `.10x/reviews/2026-07-08-source-decode-type-drift-quarantine-seam-review.md`.

## Blockers

None.
