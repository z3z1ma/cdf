Status: open
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-09-p2-ws-a10-multi-file-schema-discovery-pin.md
Depends-On: .10x/decisions/multi-file-discovery-aggregation-and-budget.md, .10x/tickets/done/2026-07-09-p2-ws-b2-schema-reconciliation-core.md

# P2 WS-A10b aggregate schema join core

## Scope

Implement the pure, format-neutral aggregate-schema join and per-file schema verdict model over ordered physical Arrow schemas. It must reuse the ratified reconciliation widening lattice rather than create a second type system.

## Acceptance criteria

- Equal fields preserve; one-direction transitive lossless widening selects the wider type; pairs without a ratified path are incompatible.
- List/struct/map children recurse through the same rule.
- Fields missing from any compatible file become nullable, retain deterministic null-origin evidence, and produce a per-file missing-null decision.
- Source field identity is the unnormalized source name. Candidate order is canonical location order and aggregate field order is first appearance.
- Reserved CDF metadata is regenerated; non-reserved metadata is retained only when identical across applicable candidates; conflicts produce deterministic per-file metadata-variance evidence.
- The result contains one aggregate/effective schema plus total per-file/field verdicts suitable for the discovery manifest and later coercion compilation.
- Unsupported signed/unsigned, integer/float, decimal/timezone, dictionary, extension, or nested joins fail as incompatible unless already present in the active widening lattice.
- Input permutation after canonical sorting produces identical schema, verdict, and canonical evidence.

## Evidence expectations

Unit and property tests over generated Arrow schemas, widening composition reuse, nested/missing/metadata/collision adversarial cases, canonical serialization fixtures, and no-I/O review.

## Explicit exclusions

No file I/O, candidate listing, snapshot writes, batch array materialization, package changes, file quarantine, or CLI behavior.

## Progress and notes

- 2026-07-09: Opened as the pure semantic lane parallel to A10a.

## Blockers

None.
