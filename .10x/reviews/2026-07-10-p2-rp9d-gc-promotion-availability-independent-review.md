Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-10-p2-rp9d-gc-promotion-availability.md
Verdict: pass

# RP9D GC promotion availability independent review

## Target

Independent adversarial review of the RP9D ticket, its evidence and implementer review, the shared availability implementation in `crates/cdf-project/src/promotion.rs`, and the `cdf package gc` integration and fixtures in `crates/cdf-cli`.

## Assumptions tested

- A readable package or a field named `_cdf_variant` is not by itself promotion authority.
- Canonical residual bytes and executable local promotion authority are separate facts.
- A receipt is authority only when it exactly agrees with package, state, schema, target, disposition, package-hash idempotency token, and ordered segment acknowledgements.
- Corrupt packages, malformed envelopes, invalid receipts, tombstones, and missing manifests fail closed without suppressing unrelated packages.
- “Only locally promotable package” is distinct from “the proposed collection removes all locally promotable packages.”
- Mixed retain/collect action sets are evaluated across the complete resource set, not one row at a time.
- GC action projection cannot mutate existing retention decisions or manufacture destination-readback availability.
- The ticket and evidence must not imply that a destructive GC mode or command-level final-removal case exists.

## Findings

- Pass: `inspect_local_package_promotion_availability` is the shared typed classification boundary. It verifies the package before any positive promotion claim, obtains resource attribution from the typed state preimage, calls the same canonical residual scanner used by the RP5 inventory, and calls the same structural receipt verifier used by planning. The CLI does not import Arrow, decode residual strings, or reconstruct receipt authority.
- Pass: positive classification requires exactly one framework-semantic residual column per batch, canonical `residual-json-v1` decoding of every non-null envelope, nonzero canonical byte count, and at least one structurally verified receipt. A canonical but unreceipted package retains byte-presence reporting while remaining non-promotable; malformed envelopes, nonsemantic lookalikes, invalid receipts, corrupt identity data, missing manifests, and tombstones cannot become promotable authority.
- Pass: receipt verification binds package hash, schema, commit target/disposition, package-hash idempotency token, exact ordered acknowledgements, and state-delta coverage. Receipt targets exposed to GC are derived only after this verifier succeeds and are deterministically sorted.
- Pass: collection assessment first counts all locally promotable packages by resource, then separately records whether any promotable package survives the supplied action set. Mixed retain/collect sets do not raise the final-removal flag; a single collected copy and an all-collect set do. The final flag additionally requires that the row itself is locally promotable and planned as `would_collect`.
- Pass: the CLI projects its existing `retain`, `would_collect`, and `restore_required` decisions into the shared assessment without changing package classification or retention. Protected receipts and checkpoint packages remain retained, corrupt/tombstone artifacts remain protected or retained under the pre-existing rules, and no destination readback is inferred.
- Pass: output carries resource, package, artifact, canonical local/promotable byte counts, typed authority, receipt targets, action, last-copy facts, and remediation in JSON; the human table exposes the actionable subset and explicitly states that destination readback was not inferred.
- Pass: the evidence correctly limits the command-level claim. Current retention protects receipted packages, so the live CLI fixture can truthfully demonstrate `last_locally_promotable_for_resource = true` with `planned_action = retain` and final-removal false. The future destructive consequence is tested only through the shared action matrix; neither the implementation nor evidence claims that collection occurred.

## Verification

The following focused checks passed on the reviewed working tree:

```text
cargo test -p cdf-project --lib local_promotion_ -- --nocapture
2 passed; 0 failed

cargo test -p cdf-cli package_gc -- --nocapture
3 passed; 0 failed
```

Code inspection additionally confirmed that the service reuses `scan_canonical_package_residuals` and `structurally_verified_package_receipts`, while `package_gc_plan` supplies only the already-computed GC action projection.

## Residual risk and evidence limits

- An actual retention tombstone removes the state preimage, so the low-level availability record can classify it as `tombstone_only` and retain its package hash but cannot produce a resource-specific collection assessment. This is fail-closed and does not affect last-promotable-copy accounting because tombstones are never locally promotable; richer tombstone attribution would require a retention-format change outside RP9D.
- The RP9D invalid-receipt fixture directly mutates the schema hash. The remaining target, disposition, token, acknowledgement, duplicate, and state-coverage rejection branches are evident in the shared verifier but are not each re-mutated by a dedicated RP9D fixture. This is a branch-coverage limitation, not an observed classification defect.
- `assess_local_promotion_collection` defaults an absent caller action to `retain`. The current CLI produces action entries from the same artifact paths enumerated under the same root, so the reviewed integration is complete and fail-safe. Any future caller that treats the API as a destructive authorization boundary should require complete action-map coverage rather than relying on that conservative default.

## Verdict

Pass. RP9D now reports verified local promotion availability and proposed collection consequences from shared planner semantics, preserves existing retention, fails closed on damaged or noncanonical authority, and does not overclaim destructive GC or destination readback. Ticket closure and parent-graph reconciliation remain separate.
