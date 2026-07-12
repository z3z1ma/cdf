Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-a5-streaming-operator-graph.md, .10x/tickets/done/2026-07-11-p3-a5e-streaming-graph-integration.md

# A5 streaming graph closeout

## What was observed

Generic production engine/project/runtime sources contain no `read_all_segments`, `read_commit_segments`, `Vec<CommitSegment>`, or equivalent whole-package data materialization. Remaining `Vec<RecordBatch>` owners are bounded canonical segment/microbatch assembly and encode-task payloads under shared memory admission.

All graph children are complete: edge contracts, fused transform kernel, durable segment stream, spill-backed metadata finalization, external dedup, and end-to-end integration. The full engine suite passes 93 non-ignored tests. Recorded TLC milestones reduced package execution from 1.240s to 1.178s and local staged end-to-end median to approximately 1.53s while preserving package/receipt/checkpoint identity.

## Procedure

```text
rg -n 'read_all_segments|read_commit_segments|Vec<CommitSegment>|collect::<Vec<.*CommitSegment' crates/cdf-engine/src crates/cdf-project/src/runtime crates/cdf-runtime/src -g '*.rs'
cargo test -p cdf-engine --locked
cargo clippy -p cdf-engine --all-targets --locked -- -D warnings
```

The static scan returned no production whole-package segment collection. Tests and lint passed.

## What this supports or challenges

This supports constant-memory streaming across transform, package persistence, and destination ingress with deterministic canonical registration. It challenges any claim that A5 includes partition fan-out; the active specification and ticket explicitly place that in C2.

## Limits

Partition/unit concurrency, retry, global-limit speculation, and jobs scaling remain C2/C4. Format-specific decode and remote overlap remain B/G.
