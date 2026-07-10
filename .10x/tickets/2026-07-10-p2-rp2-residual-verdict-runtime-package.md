Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p2-residual-schema-promotion-program.md
Depends-On: .10x/tickets/2026-07-10-p2-rp1-residual-envelope-codec.md, .10x/tickets/2026-07-09-p2-ws-a10d-effective-schema-runtime-evidence.md, .10x/tickets/done/2026-07-09-p2-ws-b5-validation-program-coercion-evidence.md

# P2 RP2 residual verdict compiler, runtime, and package evidence

## Scope

Compile safe residual capture versus row/file quarantine at field/path grain; execute it for unknown fields, scalar mismatches, and isolated parse/coercion failures; materialize final nullable `_cdf_variant`; and serialize total evolution evidence into packages/replay.

## Acceptance criteria

- Discover/evolve defaults and freeze opt-in behavior match the active residual spec.
- Cursor, merge/primary key, required non-null, source-position, and operation-field violations quarantine rather than partial-admit.
- Safe nullable mismatches null only the typed field; unknown fields remain absent from typed output; conforming fields continue.
- `_cdf_variant` is last, nullable, semantically/version tagged, and null on clean rows.
- Encoding failure becomes `cdf.residual_encode_unsupported` quarantine with redaction, not an internal crash.
- Validation program, schema/output evidence, contract-evolution artifact, package identity, verification, and replay carry residual decisions and baseline/effective schema hashes.
- Different files/batches may emit different residual decisions under one verified effective schema without collapsing evidence.

## Evidence expectations

Compiler/runtime tests, mixed clean/residual/quarantine batches, package/tamper/replay checks, PII residual redaction, multi-file integration, and adversarial review.

## Explicit exclusions

No schema promotion command, row correction, destination capability changes, or retention behavior.

## Progress and notes

- 2026-07-10: Opened after exact safety and encoding ratification.

## Blockers

Depends on RP1 and A10d.
