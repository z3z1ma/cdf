Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-09-p2-ws-d2-file-manifest-run-aggregation.md
Verdict: pass

# P2 WS-D2 file manifest run aggregation review

## Target

Implementation and records for `.10x/tickets/done/2026-07-09-p2-ws-d2-file-manifest-run-aggregation.md`.

## Findings

- Significant, found and resolved during parent review: the initial live multi-file runtime test checked only `file_name()` values and would have allowed absolute temp-dir-dependent paths in checkpoint manifests. The parent patch moved file-manifest normalization to the engine partition boundary and tightened the assertion to exact source-root-relative paths (`events-a.ndjson`, `events-b.ndjson`).
- Minor residual, accepted: touched-file jscpd reports duplication in `crates/cdf-project/src/runtime_tests.rs`. The implementation-only jscpd scan over `cdf-engine/src/execution.rs` and `cdf-project/src/runtime/artifacts.rs` is clean with 0 clones; refactoring the existing runtime integration harness would exceed D2.
- Minor residual, accepted: CodeQL continues to report three pre-existing hard-coded cryptographic value findings in `crates/cdf-cli/src/tests.rs`. They are outside the D2 write scope and are owned by `.10x/tickets/done/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`.
- Minor residual, accepted: OSV continues to report the ratified `RUSTSEC-2024-0436` `paste` advisory. D2 does not change dependencies or the DataFusion/PyO3 tuple posture.

## Verdict

Pass. The implementation is bounded, preserves cursor and non-file behavior, fails closed on ambiguous file evidence, and has focused plus workspace-wide verification. The remaining P2 file-source work is correctly outside this ticket: manifest comparison/filtering, no-op reruns, compression, remote transports, and schema variance.

## Residual risk

The engine now normalizes file-manifest positions at the partition boundary for `ScopeKey::File`. That is the right owner because only the engine has both partition scope and batch source position, but future source-position variants should not be added to this helper by analogy without a decision or focused ticket.

