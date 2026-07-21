Status: done
Created: 2026-07-21
Updated: 2026-07-21
Parent: .10x/tickets/done/2026-07-21-p0-iceberg-execution-robustness.md

# P0: byte-adaptive Iceberg Parquet batches

## Scope

Replace Iceberg's brittle fixed-row decode request and post-decode hard failure with source-owned,
byte-adaptive Parquet batching that honors the compiled memory/frontier authority by default.

## Non-goals

No unbounded batch, silent memory oversubscription, destination-specific workaround, lower global
performance cap, or requirement that users tune valid tables before first execution.

## Acceptance Criteria

- The reader derives an effective row target from immutable task/schema evidence and the configured
  byte target while retaining `parquet_batch_rows` as an explicit upper tuning knob.
- The decoder acquires its complete configured envelope before polling, the emitted frontier has a
  distinct recorded maximum, and optional all-null canonicalization occurs only under a second
  transition lease; no post-decode copy/split path can deadlock or allocate outside the ledger.
- Narrow projections retain the configured vectorized row ceiling instead of inheriting whole-file
  width, while wide/sparse schemas shrink the requested rows and complete within shared authority.
- Focused skew/large-value tests and the real `flolake.transactions` Parquet-destination run pass
  without per-resource batch overrides.

## References

- `.10x/specs/iceberg-source.md`
- `.10x/specs/constant-memory-proof.md`
- `.10x/specs/deterministic-parallel-scheduler.md`
- `.10x/tickets/done/2026-07-19-iceberg-i2-scan-execution.md`

## Assumptions

- User-ratified 2026-07-21: the byte setting is a resource target/authority, not an excuse to fail a
  representable valid table under defaults.
- Record-backed: the source must emit preaccounted batches within its compiled frontier; generic
  orchestration must not infer or repair Iceberg row widths.

## Journal

- 2026-07-21: Root cause confirmed: the Iceberg reader requests the configured 65,536-row ceiling,
  reserves 32 MiB, then rejects the resulting retained Arrow batch after decoding. The observed
  105,606,290-byte batch is normal wide-row variance, not invalid source data.
- 2026-07-21: Split the decode reservation, emitted-batch frontier, and total source working set
  into distinct authorities. The reader derives its row request once per task from immutable schema,
  projection, file bytes, and record count; it pre-acquires the full decoder envelope and rejects
  only an observation outside that explicit envelope. Duplicate all-null buffers are canonicalized
  only when the ledger grants a second transition lease.
- 2026-07-21: Adversarial review falsified the draft copy-compaction path: it could self-deadlock
  while holding the allocation needed to satisfy its own reserve, and pressure-dependent copying
  risked changing batch identities. Deleted it entirely. Added a distinct generic
  `maximum_emitted_batch_bytes` capability so the scheduler retains the source's complete working
  set while the engine frontier admits exactly the emitted batch bound.
- 2026-07-21: Review also found that a narrow projection could inherit a wide file's compressed
  bytes-per-row estimate. Compressed width is now scaled by projected/top-level field coverage;
  the 1-of-2,048-field case retains the 65,536-row ceiling.
- 2026-07-21: The real wide smoke exposed two pre-existing quadratic validation/normalization
  lookups that made the repaired source look much slower than it was. Nullability restoration now
  consumes the schema-bound validation plan by ordinal, and field normalization uses one alias
  index per batch instead of scanning all 2,052 column programs for every field. Exact release wall
  time fell from 62.08 seconds to 50.89 seconds; validation accounting fell 52.75 to 28.76 seconds.

## Blockers

None.

## Evidence

- `/tmp/cdf-iceberg-validation-indexed-smoke.log`: fresh optimized release run of the exact user
  command against FQ12 completed 3,513,266 rows, 84 tasks, 1,188 segments, verified receipt and
  checkpoint, `real 50.89`, `user 69.91`, maximum RSS 987,447,296 bytes, with no source overrides.
- Focused source tests cover wide and narrow-projection width estimation, all-null sharing, exact
  transition-lease release, and the discovery-to-pinned execution lifecycle. Final aggregate
  command/result is recorded by the parent barrier. All 41 Iceberg tests and the 733-test aggregate
  suite passed on the integrated checkout.

## Review

Pass. The final implementation has no post-decode copy/split path or new Arrow dependency. It
pre-acquires the complete decoder envelope, exposes the emitted frontier independently, preserves
the narrow projection row ceiling, and canonicalizes duplicate all-null storage only with a second
ledger lease.

## Retrospective

Compressed Parquet bytes and schema layout can seed an efficient request but cannot prove the
variable-width result. The decoder envelope is therefore acquired before payload polling and the
live memory ledger remains authoritative. A wide integration fixture also exposes algorithmic
complexity that narrow row-rate fixtures conceal; schema program resolution must be compiled or
indexed, never repeated as a nested field scan.
