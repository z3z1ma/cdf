Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: RP9C implementation through current working tree
Verdict: pass

# RP9C adversarial review

## Findings

No critical or high-severity finding remains.

The review challenged four architectural failure modes:

1. Destination proliferation in the promotion engine. The initial live Postgres test exposed that raw protocol planning bypassed adapter preparation. The fix is at the shared boundary: `ProjectDestinationRuntime::prepare_correction_commit`; Postgres catalog inspection and request binding remain inside the Postgres adapter, and promotion has no destination-name branch.
2. Sibling checkpoints for multi-target promotion. Packages now form one deterministic `H0 -> T0 -> ... -> Tn` chain, and fenced settlement checks exact current parent/input in the committing transaction.
3. A stale run checkpoint winning after schema publication. SQLite checkpoint commit now consults the latest matching resource publication in the same transaction and rejects a different schema hash. Schema-contract correction checkpoints are intentionally exempt because they establish authority before publication.
4. Recovery evidence becoming authority or encoding small-target assumptions. Journal entries are append-only derived evidence, never consulted for settlement. Target sequences use checked `u64` space with the final two values reserved for lock/publication, so there is no fixed target-count threshold or collision.

## Verdict

Pass. The destination/source extensibility boundary is stronger than before RP9C, commit and publication authority remain typed and transactional, and all material failure and migration paths have direct regression ownership.

## Residual risk

The journal can conservatively lag an authoritative mutation if the process is killed between the two filesystem/database operations. No action is required: making the journal authoritative would introduce a distributed transaction and weaken the design; recovery already verifies and reconstructs current truth from the authoritative artifacts.
