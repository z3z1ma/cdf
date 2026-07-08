Status: active
Created: 2026-07-08
Updated: 2026-07-08

# Status Freshness Authority Precedence

## Context

`.10x/tickets/done/2026-07-07-cli-status-runtime-ledger-freshness.md` requires `cdf status` to combine committed checkpoint heads with run-ledger/package receipt timestamps where those timestamps are authoritative freshness evidence.

The active run-ledger specification states that the run ledger is an append-only observability and recovery index, not state-advancement authority. The checkpoint gate specification states that source state advances only through `CheckpointStore::commit`, and the SQLite checkpoint store persists `committed_at_ms` from the durable destination receipt accepted by that commit.

## Decision

For `cdf status`, a committed checkpoint head is the freshness authority whenever exactly one committed head exists for the serving resource scope. Its `committed_at_ms` value is the receipt commit timestamp accepted by the checkpoint gate and MUST remain the timestamp used for fresh/stale classification.

Run-ledger and package receipt facts are supporting observability evidence. They MUST NOT override a committed checkpoint head or advance state. They MAY make status non-evaluable when the run ledger, package path, or package receipt artifact needed to corroborate a receipt is missing or contradictory, and they MAY provide fresh/stale classification only for receipt-only states where no committed checkpoint head exists and the active specifications allow recovery from a durable receipt.

Status MUST remain read-only and MUST NOT contact sources or destinations while evaluating these facts.

## Alternatives Considered

- Use the newest run-ledger receipt event as the freshness authority. Rejected because the run ledger is explicitly not state-advancement authority and can lag or disagree with durable package/checkpoint facts.
- Use package receipt artifacts as the freshness authority even when a committed head exists. Rejected because package artifacts corroborate the commit path but do not supersede the checkpoint gate.
- Keep checkpoint-only status. Rejected because it fails the CLI status ticket's requirement for distinct missing-ledger, missing-receipt, stale-receipt, and fresh-receipt states.

## Consequences

- Existing committed-head freshness behavior remains compatible.
- Receipt-ledger checks can surface corrupted or incomplete observability state without inventing a live health probe.
- Recovery and status continue to share the same authority model: durable package/receipt/checkpoint facts outrank run-ledger events when they disagree.
