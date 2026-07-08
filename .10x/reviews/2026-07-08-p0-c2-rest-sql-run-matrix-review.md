Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p0-c2-rest-sql-run-matrix.md
Verdict: pass

# P0 C2 REST and SQL run matrix review

## Target

Review of the C2 implementation in `crates/cdf-conformance/src/run_matrix/**`, `crates/cdf-conformance/Cargo.toml`, `Cargo.lock`, and ticket graph updates.

## Findings

No blocking findings remain.

The C1 harness split is complete. The prior 1,173-line `tests.rs` was split into focused modules for core execution, destination handles, FILE/REST/SQL fixtures, assertions, plan JSON construction, local Postgres, and test transport/secrets. The largest focused module is now 297 lines, and the executable test surface remains readable enough for future Workstream C children to extend.

REST cells use only injected fake transport and runtime dependency injection. The test asserts one GET request to the fixture URL and verifies that the serialized matrix output does not contain the fixture REST secret. No public HTTP path is used.

SQL cells use the local/`TEST_DATABASE_URL` Postgres harness. The source table is created per cell, and the source enters runtime through `ProjectRunSource::sql`, not through a file or prebuilt package shortcut.

The shared assertion surface covers the C1 obligations for every executed cell: plan honesty, package verification, trait-level destination receipt verification, checkpoint gating after receipt verification, committed checkpoint head, source position evidence, replay input identity, artifact replay identity, and duplicate no-op behavior.

Minor accepted residual: `jscpd` reports two small clones between REST and SQL fixture files, totaling 20 duplicated lines / 1.05%. These are parallel declarative fixture shapes and resource-identity checks. Refactoring them now would add an abstraction around two readable fixtures, so the duplication is accepted for C2 and should be revisited only if a third non-file fixture repeats the same shape.

Minor accepted residual: `osv-scanner` returns nonzero because it reports the already-ratified `paste 1.0.15` / `RUSTSEC-2024-0436` unmaintained advisory. `cargo audit`, `cargo deny`, and `cargo vet` all match the existing advisory posture; no C2-specific supply-chain regression was found.

## Acceptance mapping

- REST cells use deterministic in-process transport and no public network.
- SQL cells use the local Postgres harness.
- REST and SQL resources enter `run_project` through `ProjectRunSource::rest` and `ProjectRunSource::sql`.
- Matrix output records 24 executed cells and 3 explicit Parquet merge exclusions.
- Executed cells assert plan, package, receipt, checkpoint, replay, duplicate, and source-position behavior.
- Fixture secret values are not serialized into the matrix output or evidence.
- Quality evidence includes fmt/check/clippy/test/nextest, jscpd, rust-code-analysis, scc, Semgrep, Gitleaks, cargo deny/audit/vet/tree, OSV scanner, and unsafe text scan.

## Verdict

Pass. C2 is closable. Workstream C remains active because C3 cross-destination chaos, C4 per-destination live-run goldens, C5 property/fuzz targets, and C6 aggregate closure are still open.
