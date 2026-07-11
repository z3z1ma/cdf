Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-10-p3-ws-l2-phase-telemetry.md
Verdict: pass

# P3 L2 architecture review

## Target

Kernel event vocabulary, engine instrumentation boundary, project runtime recorder, SQLite migration, CLI consumers, tests, and closure evidence for L2.

## Findings

The initial implementation exposed a telemetry-specific execution function and paid timer costs when collection was disabled. Both were architectural defects: the public seam was replaced with a general `EngineExecutionOptions` value on the existing pre-finalize execution boundary, and disabled collection now avoids `Instant::now`, metric-map population, and measured package-write calls entirely.

The first runtime test also proved that adding an enum alone was insufficient: the append-only SQLite ledger correctly rejected the new kind. Schema v5 now rebuilds the constrained event table and preserves prior events rather than removing database validation.

No unresolved critical, significant, or minor finding remains. Phase types live in the kernel and import no engine/runtime types. Engine measurements aggregate by semantic stage and do not enter package artifacts. Runtime phases emit only terminal facts, so rate limiting cannot leave a persisted begin without an end. Failure drains active phases before `run_failed`. Existing non-telemetry callers use defaults, retain event order, and avoid measurement overhead. CLI display contains no raw metric strings or credential-bearing fields, and the existing secret guard still runs before durable or live publication.

## Verdict

Pass.

## Residual risk

Nanosecond duration resolution and byte definitions are now stable observation vocabulary, but their eventual concurrent attribution and overhead ratios must be measured against the L3-L5 lab. Process termination cannot append an interrupted event after the process is already dead; durable crash reconstruction remains a later runtime/chaos concern, not an L2 correctness gap.
