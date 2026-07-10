Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-a10e-file-quarantine-processed-positions.md, .10x/tickets/done/2026-07-10-p2-rp2-residual-verdict-runtime-package.md

# P2 A10e and RP2 runtime outcome evidence

## What was observed

CDF now has total file- and row-level outcomes over the shared plan/package/receipt/checkpoint spine.

For multi-file schema variance, compatible observations execute against immutable baseline/effective authority while incompatible or freeze-deviating observations produce typed terminal quarantine evidence. Their exact source positions advance only after a verified destination receipt and checkpoint commit, independently of output segments. All-quarantine packages contain zero data segments, a real receipt, typed processed-observation evidence, and a committed checkpoint; unchanged identities are skipped and changed identities retry. Append retains removed historical manifest entries. DuckDB, Parquet, and Postgres treat zero-segment append/replace commits as receipt/state work without target data or replace-pointer mutation.

For residual schema variance, readers emit source-neutral candidates and the validation program compiles capture versus quarantine. Safe values preserve the conforming row projection in a final nullable `_cdf_variant`; required/control fields quarantine. PII is redacted before package serialization. Different batches under one effective schema retain distinct observation, batch, row, path, physical-type, verdict, rule, and redaction evidence. Unsupported encodings become `cdf.residual_encode_unsupported` quarantine. Package verification rejects unsupported evolution versions and tampered schema authority.

The final integration also removed duplicate schema models and fixture-only plans: the kernel owns the recursive canonical Arrow IR, destinations use the shared framework-variant classifier, and live/run-matrix conformance uses the real compiler/planner.

## Procedure

Parent-observed verification on 2026-07-10:

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo nextest run --workspace --all-features --no-fail-fast`: passed 883/883, 0 skipped, 4 slow, in 120.370 seconds. The run included 100-rebuild prepared-package goldens, 100-run DuckDB and Parquet live goldens, bounded live Postgres, the file/REST/SQL run matrix, residual properties, all-quarantine replay, zero-data destinations, and source-free recovery.

Executor-observed supporting gates, reconciled by the parent against the final worktree:

- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: passed.
- Affected all-feature nextest matrix over kernel, contract, formats, declarative, engine, package, all destinations, and project: 479/479 passed.
- Focused formats and declarative suites: 34/34 and 88/88 passed.
- A10e destination suites: package 34/34, DuckDB 13/13, Parquet 21/21, Postgres 35/35 including the local live fixture.
- Semver checks passed completely for package, Parquet, and Postgres. Intentional pre-1.0 migrations were reported for non-exhaustive authority/result/header types and explicit zero-data commit effects; they are recorded in the child tickets and review rather than characterized as compatible.

## What this supports

The observations support every A10e and RP2 acceptance criterion: named drift verdicts, exact redacted evidence, deterministic processed positions, gate-backed all-quarantine advancement and recovery, no-data destination semantics, total residual capture/quarantine, exact final variant shape, package/replay version checks, baseline/effective/output authority separation, multi-batch decision preservation, and source/destination-neutral extension seams.

## Limits

Cloud transports and remote listing are not part of these children and remain owned by WS-E. Explicit sampled discovery and promotion/correction execution remain owned by A10g and RP5-RP10. Preview still has an open parity obligation for terminal file quarantine under A10f/WS-I; this evidence does not claim that later conformance closure.
