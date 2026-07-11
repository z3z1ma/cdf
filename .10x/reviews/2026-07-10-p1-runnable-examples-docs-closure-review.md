Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: examples/, crates/cdf-conformance/src/run_matrix/examples.rs, docs/quickstart.md
Verdict: pass

# P1 runnable examples and docs review

## Findings

No critical or significant finding remains. The examples reuse ordinary project compilation and CLI execution, not a parallel demonstration path. REST uses loopback only. Postgres uses the existing conformance service harness and a secret reference; the DSN is never embedded in TOML or output. Checked-in projects are the test inputs, so documentation drift breaks conformance.

## Verdict

Pass.

## Residual risk

None requiring a follow-up ticket.
