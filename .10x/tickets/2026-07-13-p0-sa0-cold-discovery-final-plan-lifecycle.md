Status: open
Created: 2026-07-13
Updated: 2026-07-13
Parent: .10x/tickets/2026-07-13-p0-fixed-schema-discovery-stream-admission.md

# P0 SA0: cold discovery to final-plan lifecycle

## Scope

Make an unpinned package-producing command consume its bounded discovery result exactly once, freeze the persistent or run-local snapshot, and compile the final execution plan directly from that result. Delete the post-auto-pin re-entry into ordinary pinned preparation.

## Non-goals

No observation cache, payload-spool handoff, decoder-loop fusion, dynamic producer lifecycle, or destination behavior.

## Acceptance criteria

- Cold `run|plan|preview` with persistent auto-pin performs one discovery lifecycle and compiles from its returned normalized schema/evidence.
- `--no-pin` freezes the identical run-local schema/plan authority without project writes.
- Snapshot/lock persistence does not trigger a second current-file discovery or alter the already compiled observation evidence.
- Ordinary pinned preparation loads/verifies the snapshot without source payload probes; current physical observations are not required to finalize the plan.
- Transport counters and regression tests replace the current behavior that calls pinned preparation after auto-pin.

## References

- `.10x/decisions/fixed-schema-discovery-and-stream-admission.md`
- `.10x/specs/schema-discovery-and-stream-admission.md`
- `.10x/specs/data-onramp-schema-intelligence.md`

## Assumptions

The user ratified fixed schema before final plan, direct cold-result reuse, and no current-schema pre-scan on pinned runs in the 2026-07-13 discovery-lifecycle correction.

## Journal

- 2026-07-13: Inspection identifies two concrete regressions: `prepare_resource_schema_for_cli` writes a new pin and immediately calls pinned effective-schema preparation, while the ordinary pinned branch calls current-file discovery before extraction. This ticket owns removing those lifecycle re-entries without weakening final-plan schema authority.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
