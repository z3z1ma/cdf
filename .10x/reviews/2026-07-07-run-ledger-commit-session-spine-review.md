Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/decisions/run-ledger-commit-session-spine.md, .10x/specs/run-orchestration-ledger.md
Verdict: pass

# Run ledger and commit-session spine review

## Target

Review of the active decision and specification ratifying CDF's general run spine.

## Assumptions tested

- The decision must not weaken the checkpoint commit gate.
- The run ledger must not become a second state authority.
- Destination sessions must not allow generic runtime code to synthesize or bypass receipts.
- Resume/replay behavior must follow the book's crash matrix rather than invent a new lifecycle.
- The decision must be specific enough to unblock executable child tickets while leaving exact schema mechanics to implementation tickets.

## Findings

- Significant risk avoided: The decision explicitly makes the run ledger an operational index, not the commit gate. `CheckpointStore::commit` remains the only state advancement path.
- Significant risk avoided: The decision requires duplicate receipts to verify and cover the state delta before checkpoint commit, preserving idempotent replay without treating duplicate as a magic success flag.
- Significant risk avoided: `RunId` is opaque. This avoids hard-coding an unratified string format while still ratifying default minting and collision behavior.
- Minor residual uncertainty: The exact SQLite table shape and migration fixtures are intentionally left to the run-ledger implementation ticket. That is acceptable because the decision/spec constrain authority, event families, redaction, and recovery behavior.
- Minor residual uncertainty: CLI flag spelling for caller-supplied run ids is left to CLI implementation. The semantic contract is caller-supplied ids may exist but must fail closed on collision.

## Verdict

Pass. The records turn book-backed requirements into implementation authority without laundering unratified storage details or weakening the commit gate.

## Residual risk

The next implementation wave changes public Rust API surface and serialized operational artifacts. Closure of those tickets should require semver checks, migration/compatibility review, and inspect-run redaction tests.
