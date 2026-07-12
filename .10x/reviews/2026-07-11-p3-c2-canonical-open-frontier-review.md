Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: crates/cdf-engine/src/execution.rs, crates/cdf-project/src/runtime/orchestration.rs, crates/cdf-cli/src/run_command.rs
Verdict: pass

# C2 canonical partition-open frontier review

## Findings

- Significant, resolved: the initial implementation duplicated scheduler arithmetic in `cdf-engine` and omitted source useful-concurrency and transport authority. It was deleted. `cdf-runtime::resolve_runtime_scheduler` is the sole resolver; project orchestration only carries its nonidentity result, and engine execution only consumes `effective_jobs`.
- Significant, resolved: immediate frontier replenishment opened a later payload before the current partition satisfied a global limit, breaking attempted-input authority. Limited runs now use a serial frontier and replenish only after proving remaining rows are required. The existing observation/attestation regression test catches this class.
- Significant, resolved: terminal schema quarantines could have been treated like admitted payload work. Frontier entries preserve canonical outcome order but skip `ResourceStream::open` for terminal quarantines.
- Minor, accepted in owning ticket: the milestone overlaps open/download establishment but does not yet execute transform outcomes concurrently. C2 explicitly remains open for the byte-bounded outcome frontier, admission permits, retry, and file-unit completion.

## Verdict

Pass for this bounded milestone. It improves production concurrency without adapter leakage, duplicate policy, scheduling-dependent artifacts, speculative limit I/O, or a legacy compatibility path. It is not closure evidence for C2.

## Residual risk

Drivers whose `open` future returns before meaningful I/O will see little speedup until unit decode/output concurrency lands. Python/foreign resources without typed compiled source capabilities remain serial rather than receiving guessed scheduler declarations; their migration remains owned by the active source/FFI tickets.
