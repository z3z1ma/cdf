Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md, .10x/decisions/terabyte-scale-performance-envelope.md, .10x/specs/performance-lab-and-envelope.md
Verdict: pass

# P3 terabyte-scale activation review

## Findings

- Architecture authority: pass. P3 implements the existing Chapter 6 and active runtime specification rather than inventing a benchmark-only fast path.
- Measurement honesty: pass. Absolute ambition is paired with same-host roofline ratios, bias labels, warm/cold separation, median/variance, and an explicit rule against baseline resets.
- Scale breadth: pass. The acceptance set includes multi-file/row-group Parquet, remote transports, row formats, destination bulk protocols, foreign boundaries, 100 GB memory stress, and a 1 TB run. It is not shaped around one local file.
- Correctness guardrails: pass. Package identity, SHA-256, verdicts, receipts, checkpoint gating, crash recovery, redaction, and deterministic replay cannot be traded away for throughput.
- Sequencing: pass. WS-L is explicitly exclusive until the before picture exists; later plans depend on it.
- Significant, resolved in records: the old backlog treated the P0 harness as sufficient precursor but had no ratified close envelope. The new decision and specification distinguish foundation from P3 baseline and bind every later optimization to before/after evidence.

## Residual risk

Host variance and long stress-run cost can make CI comparisons noisy. The focused spec makes unlike hosts inconclusive and reserves the 100 GB case for the slow tier; implementation must still prove the measurement method itself is reliable.

## Verdict

Pass for activation. No optimization is authorized before WS-L produces the complete baseline evidence.
