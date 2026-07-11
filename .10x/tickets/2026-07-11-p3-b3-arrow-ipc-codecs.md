Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md

# P3 B3: Arrow IPC file and stream codecs

## Scope

Implement separate file/stream drivers with bounded batch yields, remote range/seek and spool fallback for file framing, sequential stream framing, dictionary/compression support, and discovery/runtime parity.

## Acceptance criteria

- Remote Arrow IPC file no longer hard-rejects solely for being remote.
- No IPC path collects every record batch; dictionaries/order/schema remain exact.
- File/stream framing mismatch, truncation, continuation, compression, and schema changes fail with precise evidence.
- Throughput/reference ratio and zero-copy/copy counts are recorded.

## Evidence expectations

Arrow reference comparison, remote range/spool fixtures, malformed/fuzz corpus, memory, schema/dictionary goldens, and profiles.

## Explicit exclusions

No subprocess protocol changes.

## Blockers

Depends on L5 and FX1.

## References

- `.10x/specs/native-enterprise-format-catalog.md`
- `.10x/specs/native-format-codec-runtime.md`
