Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-g-remote-io-overlap.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/2026-07-11-p0-sx1-source-extension-boundary.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md, .10x/specs/remote-local-io-overlap.md

# P3 G1: async streaming local/HTTP/cloud byte sources

## Scope

Implement injected async `ByteSource` providers for local, HTTP(S), S3, GCS, Azure; streaming bodies/listings; pooled clients; generation/precondition/reattest; typed retries; and migrate private runtime/mutex/Vec APIs through compatibility adapters.

## Acceptance criteria

- No production transport constructs a runtime or materializes full list/response solely for its API.
- Every ranged/parallel read is generation-bound; weak identity chooses sequential spool or fails.
- Millions-entry listing remains bounded/deterministic.
- Secret/egress/redaction/retry/cancellation laws pass across providers.
- Mock transport addition requires no source/format/scheduler branch.

## Evidence expectations

Recorded provider fixtures, generation-change/conditional request tests, high-cardinality RSS, connection reuse, dependency review, static runtime gates, and before/after transport benchmarks.

## Explicit exclusions

No adaptive range controller or codec optimization.

## Blockers

Depends on L5, SX1, FX1, and A4.

## References

- `.10x/decisions/generation-bound-overlapped-io.md`
- `.10x/research/2026-07-11-remote-io-overlap-audit.md`
- `.10x/specs/remote-local-io-overlap.md`
