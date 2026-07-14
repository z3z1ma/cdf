Status: open
Created: 2026-07-13
Updated: 2026-07-13
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/2026-07-11-p3-g1-streaming-transport-byte-sources.md

# P0 single-crossing schema admission program

## Scope

Replace preliminary runtime source observation with metadata-only inventory, compiled deferred admission, fused observation/extraction, immutable-generation observation caching, and one-invocation dynamic producers under `.10x/specs/single-crossing-schema-admission.md`.

## Children and sequence

1. `.10x/tickets/2026-07-13-p0-sa1-deferred-admission-plan-ir.md` defines the source/codec-neutral compiled operation and package evidence.
2. `.10x/tickets/2026-07-13-p0-sa2-metadata-inventory-observation-cache.md` removes payload reads from inventory and adds exact cache identity.
3. `.10x/tickets/2026-07-13-p0-sa3-fused-codec-admission.md` fuses row/binary observation with extraction and retains first windows.
4. `.10x/tickets/2026-07-13-p0-sa4-dynamic-producer-admission.md` applies the same law to Python/Lua/WASM.
5. `.10x/tickets/2026-07-13-p0-sa5-single-crossing-conformance.md` owns adversarial transport/process counters, preview parity, and closure.

SA1 and SA2 may proceed in parallel after FX1/G1. SA3 depends on both. SA4 depends on SA1 and may proceed alongside SA3. SA5 depends on all implementation children.

## Acceptance criteria

- Every child is terminal with passing review and evidence.
- No ordinary run pre-probes every current candidate before extraction.
- Remote row collections and dynamic producers cross their expensive source boundary once per partition absent retry/replay.
- The P2 sampled-discovery, P3 streaming/remote-I/O, and extension-boundary ticket references/statuses are reconciled at closure.

## Non-goals

No same-run typed schema epoch, implicit promotion, or cache-as-authority behavior.

## References

- `.10x/decisions/single-crossing-expensive-source-boundary.md`
- `.10x/specs/single-crossing-schema-admission.md`
- `.10x/specs/sampled-schema-discovery-coverage.md`
- `.10x/specs/residual-variant-capture.md`

## Journal

- 2026-07-13: Opened from the user's 100-remote-JSON-file counterexample. Current runtime-baseline selection explicitly sets `sample_files` to `None`, probes every candidate, then extraction opens the source again. The current FX1 refactor is preserving descriptor/version/options/probe-policy authority so SA1 can compile admission without format branches; it must not add a generic pre-extraction probe loop.
- 2026-07-13: FX1 delivered the first prerequisite without claiming this program complete: ordinary remote file inventory now resolves format/compression from metadata and registered descriptors with zero payload ranges, and executable format bindings pin all codec semantics required by deferred admission. SA2 remains open for local payload-free identity and caching; SA3 remains open for retained-window discovery/extraction. Evidence: `.10x/evidence/2026-07-13-fx1-compiled-format-binding-metadata-inventory.md`.

## Blockers

Children own implementation. FX1 and G1 must publish their neutral codec/source seams first.

## Evidence

Pending child completion.

## Review

Pending.

## Retrospective

Pending.
