Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-10-p2-rp9-promotion-execution-recovery-gc.md
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
- 2026-07-10: Activated and assigned to `/root/impl_d5`. The repair is limited to a shared typed promotion-availability service, GC rendering/classification integration, and focused fixtures. Existing dry-run retention reachability remains authoritative for planned actions; RP9D only reports the consequence of those actions and must not alter retention or infer destination readback.
- 2026-07-10: Added shared `cdf-project` local promotion availability and collection-assessment APIs. The service shares canonical residual scanning and exact structural receipt verification with RP5 inventory, distinguishes byte presence from executable local authority, and computes last-copy/final-collection consequences from caller-supplied retention actions without changing them.
- 2026-07-10: Replaced CLI Arrow/string scanning with the shared service. Human/JSON output now carries independent contains/promotable/last/removes-last facts, exact bytes/action/authority, receipt targets, artifact provenance, remediation, and an explicit no-destination-readback statement. Canonical, unreceipted, malformed, noncanonical-field, tombstone, invalid-receipt, corrupt, missing-manifest, and multi-package action cases are covered. Evidence: `.10x/evidence/2026-07-10-p2-rp9d-gc-promotion-availability.md`. Implementer review: `.10x/reviews/2026-07-10-p2-rp9d-gc-promotion-availability-review.md`.
- 2026-07-10: Closed after independent review `.10x/reviews/2026-07-10-p2-rp9d-gc-promotion-availability-independent-review.md` passed and confirmed the CLI only projects the shared typed assessment. Retrospective: promotion availability is a read-authority property, distinct from retention reachability and byte presence, and must be authenticated by canonical residual plus exact receipt/preimage semantics. Conservative limits require no follow-up: tombstones without manifests intentionally retain no resource attribution or promotability claim; unknown future actions default to retain; the shared verifier covers every receipt dimension even though the focused mismatch fixture mutates one. No destructive GC behavior or retention change is claimed.

## Blockers

None.
