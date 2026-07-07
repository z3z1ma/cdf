Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-package-archive-persistence-cli.md
Verdict: pass

# Package archive persistence and CLI review

## Target

Review of the package archive persistence and CLI implementation for `.10x/tickets/done/2026-07-06-package-archive-persistence-cli.md`.

## Assumptions tested

- Archive metadata is non-identity manifest metadata and does not change canonical package hashes, receipts, signing input, lifecycle status, or IPC replay/read paths.
- Full package verification validates archives, while `--force` can still repair corrupted archive metadata by using canonical-only identity verification.
- Archive writes are status-gated to the ratified lifecycle states and do not silently recover corrupted final archive state without `--force`.
- The current supply-chain rule remains in force: no direct arrow-rs `parquet`/`paste` path is introduced.

## Findings

No blocking findings.

Minor residual risk: final mutation testing left 7 missed mutants in low-level error-injection/platform guards in `crates/firn-package/src/archive.rs`. The surviving cases cover temp-dir collision/cleanup and synthetic missing-file error-string branches. Focused behavior tests cover the user-visible archive contract, and this residual does not block this ticket.

Minor residual risk: CodeQL continues to show the known Rust extractor macro-warning profile recorded by earlier CodeQL evidence, but the current SARIF has 0 findings and the reusable database was not gratuitously recreated.

Architectural follow-up at review time: the direct native Arrow/DataFusion Parquet question was unresolved by design and tracked by `.10x/tickets/done/2026-07-06-native-arrow-parquet-policy.md`. It was later ratified by `.10x/decisions/native-arrow-datafusion-parquet-policy.md`. This was not a blocker for this ticket because the active acceptance criteria explicitly preserved the then-current no-`parquet`/`paste` supply-chain constraint.

## Verdict

Pass. The implementation satisfies the ratified package archive persistence and CLI contract, has focused package/CLI coverage plus broad quality evidence, and leaves the remaining architectural Parquet backend decision with a durable owner.

## Residual risk

Rare filesystem race/error-message branches remain mutation-survivor territory. If those become operationally important, add platform-specific fault-injection coverage rather than widening the product archive contract.
