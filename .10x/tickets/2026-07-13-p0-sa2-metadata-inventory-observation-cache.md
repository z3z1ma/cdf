Status: active
Created: 2026-07-13
Updated: 2026-07-14
Parent: .10x/tickets/2026-07-13-p0-fixed-schema-discovery-stream-admission.md

# P0 SA2: metadata inventory, two-axis coverage, and observation cache

## Scope

Make local/remote file inventory payload-free, remove local whole-file hashing from planning, encode independent file and within-file coverage, and add a versioned observation cache keyed by immutable generation plus codec/options/normalizer/contract identity.

## Non-goals

No fused decoder changes or dynamic producer lifecycle.

## Acceptance criteria

- Local/object-store/HTTP inventory reads no payload bytes.
- `sample_files` selection occurs before any probe for every registered format.
- Manifests encode `all_files|sampled_files` separately from `format_metadata|bounded_content|full_content`; unqualified exhaustive evidence is deleted.
- Local whole-file hashing occurs while extraction/spooling reads content, never during inventory.
- Cache exact hits avoid schema I/O; weak/mismatched/corrupt entries miss safely.
- Cache storage, bounds, cleanup, and telemetry are explicit and remain outside package identity.

## References

- `.10x/specs/schema-discovery-and-stream-admission.md`

## Assumptions

Cache keys and authority limits are fixed by the governing spec.

## Journal

- 2026-07-14: Execution began after SA0/SA1 closed. Initial source audit confirms two remaining inventory violations: local transport metadata computes `file_sha256`, and file-resource planning computes it again. Discovery selection already precedes registered-format probes, providing the seam for exact two-axis evidence. No observation cache exists yet; this ticket will add one keyed only by strong generation/checksum plus compiled format/options, normalizer, and admission identity, with weak local metadata forced to miss.

## Blockers

None. Generation-strength and neutral byte-source prerequisites are committed.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
