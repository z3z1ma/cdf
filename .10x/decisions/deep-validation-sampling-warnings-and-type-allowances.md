Status: active
Created: 2026-07-10
Updated: 2026-07-10

# Deep validation sampling, warnings, and Tier-0 type allowances

## Context

P2 WS-G2 was blocked on three source-experience semantics. The user granted autonomous ratification authority and had already confirmed the recommended direction.

## Decision

- JSON-family compiler probes stop at the first of 4,096 records or 8 MiB of decoded/source input. Plans and discovery manifests record the configured limits and actual probe bytes; bounded sampling is evidence, not a claim of exhaustive row conformance.
- `cdf validate --deep` reports row-local mismatches that the governing contract can quarantine as typed warnings. Plan, preview, and run retain those mismatches as non-fatal governed outcomes with quarantine/residual evidence.
- Tier 0 accepts `types = { coerce_types = <bool>, allow_lossy_mapping = <bool> }` on a resource. Both values default to `false` and are the only declaration-level overrides. They compile into resource runtime policy and the serialized validation program; they are not CLI-only hints.
- Deep mismatch diagnostics name the resource, safe source location, field, observed type, constraint type, and the applicable fixes. URL query values are removed wholesale before rendering.

## Alternatives considered

- Unbounded deep reads were rejected because validation must remain a compiler front-end operation.
- Treating quarantinable JSON rows as command failures was rejected because it contradicts total verdict semantics.
- Error text that recommends unavailable configuration was rejected; the Tier-0 allowance surface must exist before the remediation is emitted.

## Consequences

Sampling can miss later drift, but run-time contract evaluation remains authoritative and quarantines it. Explicit allowances affect all execution paths through resource policy, so adding a source or destination does not require special CLI handling.
