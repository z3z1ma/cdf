Status: active
Created: 2026-07-10
Updated: 2026-07-10

# Correction receipts distinguish operation from resource disposition

## Context

Schema-promotion corrections must settle through the canonical destination `Receipt` so the existing receipt verification and checkpoint gate remain authoritative. `Receipt` records a `WriteDisposition`, but addressed schema correction is not an ordinary resource disposition. Reusing `cdc_apply` would falsely claim CDC support and conflate source operation semantics with framework correction. Adding a correction-only receipt model would split settlement and force RP9 to translate evidence back into the checkpoint store.

## Decision

A correction request carries the resource's actual target disposition, and the canonical `CommitPlan` and `Receipt` preserve that disposition as target context. The closed, versioned `DestinationCorrectionReceiptEvidence` carries a distinct typed operation kind, initially `addressed_correction`, and is the sole authority that the receipt represents correction rather than an ordinary append, replace, or merge commit.

Correction planning and verification MUST validate both dimensions: the receipt disposition equals the request's resource disposition, and the correction evidence operation kind equals `addressed_correction`. A destination advertises and authorizes correction through `DestinationCorrectionCapabilities`, never by claiming ordinary `cdc_apply` support. Append remains keyless; an append resource's correction receipt may report append context while its typed evidence reports addressed correction.

The correction request's operation digest is derived and recomputed by shared kernel code from the exact canonical operation set. Receipt evidence binds that digest. Caller-supplied digest text, destination-native SQL, or driver-local operation labels are not authority.

## Alternatives considered

- Encode correction as `WriteDisposition::CdcApply`: rejected because schema promotion is not source CDC and destinations may support corrections without supporting ordinary CDC ingestion.
- Add `WriteDisposition::Correction`: rejected because disposition describes the resource's target behavior; correction is a separate commit operation authorized by a separate capability family.
- Return a separate correction receipt and translate it before checkpoint commit: rejected because it creates a second settlement protocol and weakens the ordinary commit gate.
- Infer correction from update counts or transaction strings: rejected because those are not closed typed operation authority.

## Consequences

- Existing checkpoint and receipt infrastructure remains the single settlement path.
- Generic RP9 orchestration verifies typed correction evidence without destination-name branches.
- Destination adapters implement physical correction mechanics only; they do not reinterpret dispositions.
- Receipt readers must inspect the versioned correction evidence when distinguishing an ordinary resource commit from a promotion correction.
