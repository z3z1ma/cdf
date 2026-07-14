Status: active
Created: 2026-07-13
Updated: 2026-07-13

# Single-crossing schema admission

## Purpose and scope

This specification governs the join between discovery coverage, runtime schema observation, source extraction, and observation caching. It refines `.10x/specs/sampled-schema-discovery-coverage.md`, `.10x/specs/data-onramp-schema-intelligence.md`, `.10x/specs/residual-variant-capture.md`, and `.10x/decisions/single-crossing-expensive-source-boundary.md`.

It applies to file, REST, SQL cursor streams, Python, Lua, and WASM sources. It does not change explicit replay/retry semantics or authorize implicit typed-schema promotion.

This specification supersedes the timing clauses in `.10x/specs/native-format-codec-runtime.md` and `.10x/specs/data-onramp-file-sources-transports.md` only where they required payload magic confirmation during ordinary inventory or partition planning. Their signal-combination, mismatch, and fail-closed requirements remain binding, but content evidence is now obtained from the admitted stream (or a reusable bounded metadata observation) before any partial package or destination mutation.

## Inventory and pinning

File inventory MUST enumerate every matched candidate without payload reads. Each entry MUST contain its canonical location and available bounded identity facts. Missing optional identity facts MUST remain explicit nulls rather than triggering a content hash/download.

`sample_files = N` MUST use `stratified-hash-v1` across every registered file format. It selects files, not records within a selected file. A selected row-oriented file is still bounded by the configured per-file byte/record probe budget. `exhaustive` MUST be explicit for row-oriented remote collections when every file would require payload sampling. Parquet and Arrow IPC retain exhaustive metadata discovery by default because their schema metadata does not require row-data reads.

Pin evidence MUST state matched, selected/probed, and unobserved counts and identities. A sampled pin is a valid immutable baseline and MUST NOT claim exhaustive conformance.

## Compiled deferred admission

The plan MUST carry a versioned deferred schema-admission operation containing:

- the pinned baseline and effective typed projection identities;
- format driver id/version and canonical options;
- normalization version;
- trust/type/coercion/residual/quarantine policy;
- control-critical fields;
- the total set of permitted verdicts;
- the observation-cache key shape.

Execution MUST NOT reparse, reoptimize, or expand that operation. It instantiates the operation with the exact physical observation obtained from the admitted source stream and records the selected verdict.

## Runtime stream behavior

For sequential sources, the execution order MUST be:

```text
open once
→ read/decode one accounted observation window
→ reconcile against the compiled admission operation
→ retain and emit that same window
→ continue the same stream through validate/package/deliver
```

The observation window MUST remain under the memory ledger. If it cannot fit, normal flush/backpressure/spill/clean-fail policy applies. It MUST NOT be discarded and reread.

For exhaustive content discovery performed as part of a package-producing run, CDF MUST either continue the same live stream or extract from the exact ledger-accounted spool created by that stream. A second source download is forbidden.

For Parquet/Arrow IPC, bounded metadata observations MAY be reused from pin/cache. A new or changed generation may receive one bounded schema-metadata probe before extraction, but data pages MUST be transferred only by the extraction access plan. Full/high-coverage remote Parquet uses one verified sequential spool; selective scans use the plan-recorded generation-bound ranges.

## Observation cache

An observation cache key MUST include:

```text
source generation or cryptographic checksum
+ format driver id and semantic version
+ canonical decoding options hash
+ normalization version
+ pinned contract identity
```

Weak identity MUST NOT admit a cached observation as exact. Cache hits MUST be visible in telemetry and remain outside package identity; the physical observation and selected verdict remain package evidence. Cache corruption or mismatch falls back to fresh fused observation without weakening generation checks.

## Verdicts

Compatible physical types admit. Lossless compiled coercions coerce and record. Isolated unknown or incompatible nullable values follow `_cdf_variant` residual capture. Control-critical mismatches quarantine the row or partition. Reliably isolated malformed records may quarantine at record grain. Broken framing or unresynchronizable streams quarantine the partition. An explicit strict contract aborts before destination mutation.

An unknown field observed after compilation MUST NOT become a new typed destination field within the same schema epoch. It remains residual until explicit rediscovery/promotion/backfill.

## Scenarios

Given 100 remote JSON files and `sample_files = 10`, when the schema is pinned, then CDF lists 100 metadata identities, samples exactly 10 deterministic files within the configured bounds, and records 90 unobserved entries without opening their payloads.

Given the same resource later runs, when an unobserved file is extracted, then its first accounted decode window both instantiates schema admission and flows downstream; the remote generation is not downloaded a second time.

Given an unchanged Parquet generation with a cached footer observation, when it runs, then no schema probe occurs and payload access follows only the selected full-scan or selective-range plan.

Given a Python producer without a schema handshake, when it runs, then the producer process starts once and its retained first batches continue into the package after admission.

## Acceptance criteria

- A transport fixture with counters proves metadata-only inventory and at most one payload transfer per partition for JSON/CSV/NDJSON.
- A sampled runtime baseline does not disable `sample_files` or pre-probe unobserved candidates.
- Fused observation retains exact rows/bytes and produces the same package as an equivalent predeclared schema.
- Cache hit/miss/generation-change/corruption tests prove key completeness and fail-closed behavior.
- Parquet full scans never combine a full payload download with a second full read; unchanged footer observations are reused.
- Python/Lua/WASM tests prove one invocation per partition when no explicit retry occurs.
- Preview and run share the same admission front end; preview bounds downstream consumption without starting a second source.

## Explicit exclusions

This specification does not introduce same-run typed schema epochs, implicit pin mutation, hidden adaptive file sampling, or cache authority over source generation.
