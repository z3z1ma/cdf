Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Scheduler source-boundary readiness is narrower than SX1 closure

## Context

P3 C1 originally depended on closure of the entire P0 SX1 source-extension ticket. SX1 includes two separable concerns: the hot execution/scheduling boundary, and product/compiler hook migration for discovery, add, deep validation, doctor, and open declarative parsing.

The scheduler needs only the former. All file, REST, and Postgres executable resources now carry version/hash-bound neutral plans, scheduler-facing working-set/concurrency/retry/attestation/order declarations, type/effective-schema policy, and injected execution services. Production CLI runtime construction resolves exclusively through `SourceRegistry`; scheduler code receives `dyn QueryableResource` plus neutral capabilities and contains no source-name branch.

Discovery/product-hook cleanup does not affect scheduler admission semantics or hot-path execution. Keeping C1 blocked on it would couple enterprise performance work to compiler/UX migration without reducing scheduler risk.

## Decision

C1 MAY proceed against the implemented and evidenced SX1 runtime boundary while SX1 remains open for parser/schema/discovery/product-hook acceptance criteria. C1 depends on the neutral registry and first-party driver evidence records rather than terminal SX1 status.

C1 MUST consume only neutral source capabilities and queryable resources. It MUST NOT import source implementations, inspect driver ids/kinds/schemes, or add a compatibility branch for any source. Any missing scheduler declaration discovered by C1 is repaired in the neutral source contract and all first-party drivers before C1 closes.

## Alternatives considered

- Wait for all SX1 product hooks: rejected because those hooks are not inputs to admission and would unnecessarily serialize P3.
- Mark SX1 done early: rejected because its open parser, discovery dispatch, schema composition, and conformance criteria are real and remain incomplete.
- Let C1 read compatibility plans temporarily: rejected because that would recreate the architectural leak SX1 exists to remove.

## Consequences

Scheduler work can proceed now without weakening SX1 closure. SX1 remains an active P0 owner. The separation is falsifiable: any C1 source-id import or capability gap violates this decision and blocks C1 review.
