Status: active
Created: 2026-07-11
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md

# P3 B10: descriptor-bound length-delimited Protobuf codec

## Scope

Add native dynamic Protobuf decode from explicit descriptor set/message, length-delimited framing, Arrow mapping, presence/oneof/enum/map/repeated/well-known handling, and unknown-field residual policy.

## Acceptance criteria

- Unframed or descriptorless streams fail plan; message limits are bounded.
- Field-number/presence/oneof/unknown provenance survives; unknowns never drop silently.
- Schema evolution, malformed varints/messages, random chunks, and jobs remain deterministic.
- Native reference ratio and memory/security evidence are green.

## Evidence expectations

Dependency review, protoc/reference cross-checks, descriptor evolution matrix, malformed/fuzz corpus, unknown-field goldens, and profiles.

## Explicit exclusions

No gRPC transport or ambient schema registry.

## Blockers

None. FX1 and L5 are done.

## References

- `.10x/specs/native-enterprise-format-catalog.md`
- `.10x/specs/native-format-codec-runtime.md`
- `.10x/research/2026-07-18-protobuf-arrow-decoder-admission.md`

## Journal

- 2026-07-18: Revalidated the executable dependencies and active Arrow tuple. Rejected `ptars-core 0.0.21` because its direct path skips unknown wire fields, does not enforce oneof last-wins, and pins Arrow 59. Admitted `prost-reflect 0.16.5` for descriptor authority and conformance only. The production path will be one CDF-owned bounded wire-to-Arrow decoder; exact unknown occurrences enter the existing compiled residual-verdict program. See `.10x/research/2026-07-18-protobuf-arrow-decoder-admission.md`.
