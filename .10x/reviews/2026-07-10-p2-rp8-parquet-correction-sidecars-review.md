Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-10-p2-rp8-parquet-correction-sidecars.md
Verdict: pass

# P2 RP8 Parquet correction-sidecar adversarial review

## Target

RP8 kernel receipt extensions and the Parquet/object-store correction-sidecar implementation.

## Findings

- Pass: settlement remains on the existing `DestinationProtocol` correction session, canonical `Receipt`, receipt verification, and future checkpoint gate. No parallel receipt or destination-specific orchestration model was introduced.
- Pass: generic validation dispatches on `CorrectionStrategy`, not a destination id. Adding another sidecar-capable destination consumes the closed kernel evidence contract and implements only its physical publication mechanics.
- Pass: the exact compiler-produced `residual-json-v1` envelope remains value authority. The adapter revalidates the operation digest and exact values instead of parsing display JSON or destination literals.
- Pass: atomicity is stated narrowly and truthfully. `AtomicTarget` applies only to create-only publication of the immutable correction manifest/receipt scope; typed and human-readable evidence says the base target is unchanged. The adapter does not advertise atomic rematerialization.
- Pass: content-addressed object and manifest hashes, package/token identity, schema transition, provenance addresses, counts, and receipt marker are independently re-read and cross-checked. Object-only and manifest-without-receipt crash states are not accepted as committed effects.
- Pass: replay is package-token idempotent and returns the canonical stored receipt. The base Parquet manifest and objects are never silently rewritten.
- Pass: destination capability claims remain conservative: no in-place update, provenance persistence/targetability, or residual readback is claimed.

## Verdict

Pass. The implementation satisfies RP8 without weakening package identity, receipt settlement, replay, or the source/destination extension invariant.

## Residual risk

Live cloud stores may differ operationally in latency and failure presentation even though the pinned `object_store` create-only contract is shared. That risk is already owned by P2 WS-E/WS-I and does not weaken the RP8 protocol proof. Executable versioned rematerialization remains correctly unavailable until a destination/store proves atomic pointer compare-and-swap.
