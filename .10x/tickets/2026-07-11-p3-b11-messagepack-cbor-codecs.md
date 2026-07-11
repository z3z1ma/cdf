Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/2026-07-10-p3-ws-l5-preoptimization-baseline.md

# P3 B11: MessagePack and CBOR codecs

## Scope

Add native sequence/top-array MessagePack and CBOR decoding with exact scalar/binary/tag/extension/nested provenance, bounded sampling, framing pinning, and residual/quarantine handling.

## Acceptance criteria

- Framing is explicit/pinned; concatenated sequences never rely on EOF ambiguity.
- Integer width/sign, binary/string, maps, timestamps/tags/extensions, depth/record limits, and malformed input match catalog semantics.
- Random chunking and in-memory/spooled/local/remote paths produce identical packages.
- Native reference ratios and memory profiles are recorded separately per codec.

## Evidence expectations

Dependency reviews, standard corpora, extension/tag matrix, fuzzing, schema/discovery goldens, memory, and profiles.

## Explicit exclusions

No application-specific extension interpretation without explicit option/schema.

## Blockers

Depends on FX1 and L5.

## References

- `.10x/specs/native-enterprise-format-catalog.md`
- `.10x/specs/native-format-codec-runtime.md`
