Status: active
Created: 2026-07-13
Updated: 2026-07-14
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md

# P0 fixed-schema discovery and stream-admission program

## Scope

Separate cold discovery from pinned execution: freeze a persistent or run-local schema before final planning, remove ordinary pinned current-schema pre-scans, encode independent file/within-file coverage, fuse physical admission with extraction, reuse same-command payload spools, cache strongly identified observations, and preserve one-invocation dynamic producers under `.10x/specs/schema-discovery-and-stream-admission.md`.

## Children and sequence

1. `.10x/tickets/done/2026-07-13-p0-sa0-cold-discovery-final-plan-lifecycle.md` makes cold discovery feed final planning directly and deletes the second pinned-preparation pass. Done.
2. `.10x/tickets/done/2026-07-13-p0-sa1-compiled-stream-admission-plan.md` defines the source/codec-neutral compiled stream-admission operation and package evidence. Done.
3. `.10x/tickets/done/2026-07-13-p0-sa2-metadata-inventory-observation-cache.md` removes payload reads/hashing from inventory, encodes both coverage axes, and adds exact cache identity. Done.
4. `.10x/tickets/done/2026-07-13-p0-sa3-fused-codec-admission.md` fuses row/binary observation with extraction, retains first windows, and hands discovery spools to execution. Done.
5. `.10x/tickets/2026-07-13-p0-sa4-dynamic-producer-admission.md` applies the bootstrap-barrier law to Python/Lua/WASM.
6. `.10x/tickets/2026-07-13-p0-sa5-fixed-schema-admission-conformance.md` owns adversarial transport/process counters, preview parity, and closure.

SA0, SA1, and SA2 may proceed independently because the required registry, source-generation, and byte-source seams already exist. SA3 depends on SA0-SA2. SA4 depends on SA0-SA1 and may proceed alongside SA3. SA5 depends on every implementation child.

## Acceptance criteria

- Every child is terminal with passing review and evidence.
- No ordinary run pre-probes every current candidate before extraction.
- Cold commands freeze before final planning without re-entering pinned discovery; pinned commands perform no current-schema pre-scan.
- Remote row collections and dynamic producers perform no hidden full pre-scan or repeated full transfer; materialized payload spools are reused absent retry/replay.
- Discovery evidence distinguishes file coverage from within-file coverage and never uses unqualified exhaustive claims.
- The P2 sampled-discovery, P3 streaming/remote-I/O, and extension-boundary ticket references/statuses are reconciled at closure.

## Non-goals

No same-run typed schema epoch, implicit promotion, or cache-as-authority behavior.

## References

- `.10x/decisions/fixed-schema-discovery-and-stream-admission.md`
- `.10x/specs/schema-discovery-and-stream-admission.md`
- `.10x/specs/residual-variant-capture.md`

## Journal

- 2026-07-13: Opened from the user's 100-remote-JSON-file counterexample. Current runtime-baseline selection explicitly sets `sample_files` to `None`, probes every candidate, then extraction opens the source again. The current FX1 refactor is preserving descriptor/version/options/probe-policy authority so SA1 can compile admission without format branches; it must not add a generic pre-extraction probe loop.
- 2026-07-13: FX1 delivered the first prerequisite without claiming this program complete: ordinary remote file inventory now resolves format/compression from metadata and registered descriptors with zero payload ranges, and executable format bindings pin all codec semantics required by deferred admission. SA2 remains open for local payload-free identity and caching; SA3 remains open for retained-window discovery/extraction. Evidence: `.10x/evidence/2026-07-13-fx1-compiled-format-binding-metadata-inventory.md`.
- 2026-07-13: The user corrected the absolute single-crossing model. A final plan still requires a fixed schema, so cold discovery may consume its explicit two-axis budget before plan finalization and may reread a small unspooled prefix. The prohibited behavior is a hidden full pre-scan, a second pinned-preparation discovery pass, or discarding a materialized payload spool. Ordinary pinned runs compile total admission and observe physical reality only while extracting. The active decision/spec were superseded and this graph was reshaped around those exact semantics.
- 2026-07-14: SA0 and SA1 closed after exact cross-crate lifecycle and replay review. Cold discovery now feeds the final plan directly; pinned preparation performs no current-file schema discovery; the compiled stream-admission program is source/codec-neutral; and kernel-owned partition observation identity is enforced across planning, preview, execution, package evidence, and replay before mutation. SA2 is the next active child.
- 2026-07-14: SA2 closed after four cumulative adversarial passes and a final boundedness repair. Inventory is payload-free; discovery records independent coverage axes; strong observations cache under exact semantic identity; weak inputs become content-attested only at EOF; logical/access identity and completion evidence are invocation-safe; and superseded file range/body surfaces are deleted. SA3 is now active and owns retained discovery windows/spools plus fused codec admission.
- 2026-07-14: SA3 closed after the closure audit repaired one transport accounting regression and corrected the child boundary rather than falsely absorbing still-open engines. Sequential file codecs and REST now retain exact discovery input through the final-plan barrier; materialized transformed/weak Parquet spools are consumed once; pinned physical drift follows compiled residual/quarantine behavior without mutating snapshots. Selective Parquet planning remains B2/G2, and full-content JSON inference remains B5. SA4 is the next implementation child; SA5 remains blocked on SA4.

## Blockers

None external. Children own the remaining implementation graph; required neutral seams are already committed.

## Evidence

Pending child completion.

## Review

Pending.

## Retrospective

Pending.
