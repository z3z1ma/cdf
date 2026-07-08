Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-e3-merge-dedup-live-path.md
Verdict: pass

# P1 E3 merge dedup review

## Target

Review the E3 implementation that adds deterministic pre-merge dedup to the live contract execution path.

## Findings

- Pass: Dedup semantics are centralized in `cdf-contract` and keyed by the compiled validation program. Destination crates do not infer merge keys or silently invent dedup policy.
- Pass: `cdf-engine` applies dedup after row verdict/quarantine filtering and before package segment writing. Append and replace plans with compiled dedup rules keep all accepted rows and do not write a dedup summary.
- Pass: `keep = first`, `keep = last`, and `keep = fail` have focused tests. `keep = fail` and NULL key handling fail before package finalization, which is the right pre-destination mutation boundary.
- Pass: Package identity includes `stats/dedup-summary.json`, and `PackageReader` exposes the JSON so replay/inspect surfaces can read recorded evidence without re-evaluating source data.
- Pass: Live project coverage proves the package contains deduped segments, replay into a second database is byte/row equivalent, and duplicate replay/redrive returns the existing duplicate receipt path without a second mutation.
- Pass after repair: The broad conformance run caught a fixture compatibility regression from the new `EnginePlan.write_disposition` field. The final implementation defaults legacy missing values to `Append`, adds a focused serde compatibility test, and the full touched-core-plus-conformance nextest run passes.
- Minor accepted residual: `rust-code-analysis-cli` reports `evaluate_package_order_dedup` with cognitive complexity 26. The function is a single semantic unit over package-order grouping and fail-closed key checks; splitting it further would add indirection without reducing the acceptance risk in this slice. Keep this visible if future dedup variants are added.
- Minor accepted residual: `jscpd` reports 8.00% duplicated lines over the touched file set, dominated by existing test/helper repetition. I did not find duplicated production dedup semantics that should be abstracted before closure.
- Out-of-scope owner created: full-workspace `cargo machete` still reports `cdf-cli`'s `cdf-dest-parquet` dependency as unused. That is unrelated to E3 and is owned by `.10x/tickets/2026-07-08-cdf-cli-unused-parquet-dependency.md`.

## Verdict

Pass. Acceptance criteria are backed by focused unit tests, live run/replay/duplicate-redrive coverage, broad conformance nextest, package identity evidence, security/supply-chain scans, jscpd metrics, complexity metrics, and a repaired compatibility regression found by the suite.

## Residual risk

E3 does not prove destination-wide merge semantics for every driver cell. That remains covered by the broader run-spine conformance matrix and the ordered P1 E6 drift-quarantine conformance scenario. No unresolved E3 blocker remains.
