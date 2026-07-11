Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-10-p2-rp9b-atomically-fenced-promotion-settlement.md, .10x/specs/schema-promotion-corrections.md

# RP9B atomically fenced promotion settlement evidence

## Observation

Promotion now receives one `PromotionSettlementStore` rather than independent lease, checkpoint, and ledger handles. SQLite checks the exact lease row inside the same immediate transaction that commits a promotion checkpoint or appends a publication event. Destination settlement remains idempotent and may leave a receipt after expiry, but the stale executor cannot advance state. Exact lock CAS remains the intentional boundary between checkpoint and publication.

The deterministic clock regression expires owner A before checkpoint commit, proves the checkpoint remains proposed, lets higher-token owner B commit, expires B before publication, proves no event exists, then lets owner C append exactly once. Equal publication replay remains observable after expiry; conflict remains fail-closed.

## Verification

```text
cargo test -p cdf-state-sqlite
# 36/36
cargo test -p cdf-project --lib
# 163/163
cargo test -p cdf-cli --lib
# 255/255
cargo clippy -p cdf-kernel -p cdf-state-sqlite -p cdf-project -p cdf-cli --all-targets -- -D warnings
cargo semver-checks check-release -p cdf-kernel --baseline-rev HEAD
# 196/196
cargo fmt --check
git diff --check
```

## Limits

This proves the local transactional store and promotion integration. Remote stores must implement the same trait in one consistency domain and pass equivalent conformance. Multi-target ordering and command-level race composition remain RP9C.
