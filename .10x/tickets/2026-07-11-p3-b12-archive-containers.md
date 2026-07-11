Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md

# P3 B12: ZIP/TAR member-partition containers

## Scope

Implement archive-container drivers, member glob/plans/identities, ZIP ranged central-directory/member reads, TAR streaming, nested limits, safe scratch, and whole-archive manifest completion.

## Acceptance criteria

- Traversal, links, duplicate names, bombs, corruption, nested depth, counts, and expanded budgets fail safely with exact member evidence.
- Selected members are deterministic logical children and compose with transforms/codecs without full-archive buffering where format permits.
- Outer file position advances only after all selected members complete.
- Local/remote, jobs, memory, and reference performance are green.

## Evidence expectations

Security/adversarial archive corpus, range traces, member identity/checkpoint tests, nested compression, cancellation/cleanup, memory, and profiles.

## Explicit exclusions

No RAR/7z in catalog v1.

## Blockers

Depends on transforms, FX1, and L5.

## References

- `.10x/specs/native-enterprise-format-catalog.md`
- `.10x/specs/native-format-codec-runtime.md`
- `.10x/decisions/logical-file-partitions-executor-packing-and-zip-trigger.md`
