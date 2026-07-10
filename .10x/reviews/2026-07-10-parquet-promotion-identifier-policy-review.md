Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-10-parquet-promotion-identifier-policy.md
Verdict: pass

# Parquet promotion identifier-policy adversarial review

## Assumptions tested

- A column policy cannot silently reinterpret an object-key policy: the capabilities are separate typed fields and the existing adapter rejection remains tested.
- Promotion cannot gain a destination-name special case: policy is obtained from the generic runtime sheet hook and compiled through the shared adapter.
- Accessing Parquet policy during planning cannot create destination files: the filesystem runtime returns the static validated sheet without protocol materialization.
- Normalizing columns cannot rename storage objects: tests assert normalized ordinary/sidecar fields and the pre-existing manifest/sidecar key shapes independently.
- A CLI execution report cannot stand in for destination evidence: the source fixture creates and verifies a real Parquet receipt before correction execution, and execution subsequently records correction receipt, checkpoint, lock, and event.

## Findings

### Resolved — significant: one identifier field represented two incompatible namespaces

The Parquet sheet previously exposed `object-key-component-v1` through `identifier_rules`, causing the shared column adapter to reject promotion correctly. `identifier_rules` is now column-only and a typed optional `ObjectKeyRules` capability owns object components. Old sheets omit the optional capability and retain backward-compatible serialization.

### Resolved — significant: the first CLI fixture forged destination identity

The first fixture changed a DuckDB receipt id to look like Parquet. RP9A's strict live verification rejected it. The fixture now removes the DuckDB fixture receipt, commits the unchanged source package through `ParquetDestination`, and verifies the resulting canonical Parquet receipt before invoking promotion.

### Resolved — minor: Parquet planning could have required destination materialization

The generic sheet hook defaults to the active protocol sheet, but filesystem Parquet overrides it with the static destination sheet. Identifier planning therefore remains read-only.

## Verdict

Pass. The implementation follows the ratified namespace split, keeps column normalization generic and fail-closed, preserves object-key identity, and provides a real end-to-end Parquet correction-sidecar execution proof.

## Residual risk

- Future object-key policy versions require explicit enum/version expansion; unknown values fail deserialization or artifact validation rather than degrading silently.
- Remote object-store transports use the same Parquet protocol but are not backed by a live external service in this ticket.
