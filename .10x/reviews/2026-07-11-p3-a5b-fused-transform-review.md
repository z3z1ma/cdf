Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-11-p3-a5b-fused-transform-kernel.md
Verdict: pass

# P3 A5b adversarial closure review

## Target

The fused contract/normalization kernel, memory ownership through durable persistence, and bounded quarantine/residual evidence path.

## Findings

- No critical or significant correctness finding remained. Fused and unfused controls produce identical rows, segments, package hash/signature, quarantine bytes, contract evolution, verdict summaries, lineage, and source positions.
- Transform reservation precedes allocation, reconciles to retained Arrow bytes, crosses canonical segmentation, and releases after durable publication. Undersized budgets fail with a typed data error and no retained bytes.
- The accepted no-residual path remains Arrow/vector based and reconstructs no scalar rows. The release control improved 64k-row throughput from 1.426 GiB/s to 3.912 GiB/s (2.743x).
- High-cardinality evidence no longer accumulates package-global vectors. Residual ordering uses bounded runs and deterministic fan-in merge under shared budgets; quarantine artifact names derive from the part sequence and stream into the atomic summary.
- Error paths cannot publish partial success artifacts: unfinished sinks remove their temporary sibling, spill owners remove scratch on drop, and package finalization follows evidence completion.

## Residual risk

External merge runs retain intermediate spill files until the reader drops, so worst-case disk amplification exceeds the final sorted evidence size. E2 owns intermediate-run reclamation and the package-wide spill/durability envelope. This does not weaken A5b memory bounds or artifact correctness.

## Verdict

Pass. A5b acceptance criteria are supported by implementation, tests, measured throughput, and bounded evidence ownership.
