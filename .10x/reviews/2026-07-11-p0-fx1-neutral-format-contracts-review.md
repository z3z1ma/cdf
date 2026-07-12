Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: crates/cdf-runtime/src/format.rs
Verdict: pass

# Review: neutral format contracts

## Assumptions tested

- The boundary is genuinely neutral rather than a renamed filesystem/parser API.
- Accounted payload ownership survives asynchronous streaming and driver handoff.
- A driver can own discovery, deterministic unit planning, and physical decoding without owning shared schema reconciliation.
- Registry mutation cannot partially install a duplicate/conflicting driver.
- Capabilities needed for projection, predicates, random access, expansion defense, checksum behavior, cancellation, and memory admission are explicit.

## Findings

No critical or significant finding remains in this slice. The registry initially replaced an existing transform before returning a duplicate-id error; this was corrected to preflight all conflicts before mutation. Descriptor validation was strengthened to reject repeated aliases/extensions, incoherent range/random-access claims, empty identity authorities, zero expansion bounds, and conflicting strong magic.

## Verdict

Pass. The new contract is appropriately located in `cdf-runtime`, object-safe, parser/transport/executor neutral, and strict enough to support the migration without source-runtime special cases.

## Residual risk

The old enum/match implementation still owns production behavior until the subsequent FX1 migration slices replace it. That is explicitly incomplete acceptance under the same open ticket, not an accepted parallel or compatibility surface.
