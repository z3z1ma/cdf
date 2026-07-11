Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/2026-07-10-p3-ws-l5-preoptimization-baseline.md

# P3 B6: Avro object-container and single-object codecs

## Scope

Add native Avro OCF block planning/parallel decode and explicit-schema single-object framing with logical types, unions, schema resolution evidence, block codecs, and physical provenance.

## Acceptance criteria

- OCF blocks are bounded deterministic units; single-object requires explicit fingerprint/schema authority.
- Writer/reader schema resolution compiles into shared reconciliation; no ambient registry inference occurs.
- Nullable/general unions, logical types, sync corruption, block compression, and malformed records obey catalog semantics.
- Native reference ratio, memory, jobs, and package determinism are green.

## Evidence expectations

Dependency review, Apache/reference corpus, schema-evolution matrix, malformed/fuzz blocks, logical-type/union goldens, and profiles.

## Explicit exclusions

No network schema registry client.

## Blockers

Depends on FX1 and L5.

## References

- `.10x/specs/native-enterprise-format-catalog.md`
- `.10x/specs/native-format-codec-runtime.md`
