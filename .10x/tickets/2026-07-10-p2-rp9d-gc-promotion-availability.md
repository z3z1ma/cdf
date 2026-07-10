Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p2-rp9-promotion-execution-recovery-gc.md
Depends-On: .10x/tickets/done/2026-07-10-p2-rp1-residual-envelope-codec.md, .10x/tickets/done/2026-07-10-p2-rp5-schema-promote-dry-planner-cli.md

# RP9D truthful GC promotion availability

## Scope

Make `cdf package gc` report whether the proposed collection would remove the last locally executable residual-promotion authority, using the same canonical inventory semantics as the planner.

## Acceptance criteria

- GC delegates residual/package/receipt/resource classification to a shared typed promotion-availability service; CLI code does not independently scan Arrow strings or reimplement planner evidence rules.
- Promotable bytes are counted only after package verification, resource attribution, canonical `residual-json-v1` decoding, and exact structural receipt/target association. Malformed envelopes, unreceipted packages, tombstones, corrupt/missing packages, and noncanonical variant fields are not labeled promotable.
- Reports distinguish `contains_local_residual_bytes`, `locally_promotable`, `last_locally_promotable_for_resource`, and `collection_removes_last_local_promotable_copy`. The final flag is true only when the artifact is actually planned for collection and no retained executable local authority remains.
- Retained/collectible/protected mixtures and multiple packages per resource are deterministic. No destination readback is inferred, and retention policy is not silently changed.
- Human/JSON output names exact affected resource/package/bytes/action and remediation before an eventual destructive GC mode can remove the final authority.

## Evidence expectations

Shared-service unit tests, canonical/malformed residual fixtures, exact receipt association tests, multi-package/action matrix, tombstone/corrupt/missing cases, CLI human/JSON assertions, strict Clippy/formatting, and independent review.

## Explicit exclusions

No destructive GC execution, retention-policy change, destination readback probe, correction execution, or source re-extraction.

## Progress and notes

- 2026-07-10: Opened from the inaccurate promotability finding in `.10x/reviews/2026-07-10-p2-rp9-promotion-execution-independent-review.md`. The current byte-counting CLI implementation is not authority and must be replaced by shared planner semantics.

## Blockers

None.
