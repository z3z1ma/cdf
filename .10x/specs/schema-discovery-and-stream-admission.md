Status: active
Created: 2026-07-13
Updated: 2026-07-13

# Schema discovery and stream admission

## Purpose and scope

This specification governs cold schema discovery, coverage, schema freezing, pinned execution, physical-observation admission, observation caching, payload-spool reuse, and dynamic-producer bootstrap. It refines `.10x/specs/data-onramp-schema-intelligence.md`, `.10x/specs/residual-variant-capture.md`, `.10x/specs/remote-local-io-overlap.md`, and `.10x/decisions/fixed-schema-discovery-and-stream-admission.md`.

It supersedes `.10x/specs/superseded/single-crossing-schema-admission.md` and `.10x/specs/superseded/sampled-schema-discovery-coverage.md`.

## Invariants

A package-producing command MUST freeze one output schema and final execution plan before package or destination mutation. That schema MUST remain immutable for the run. Physical observations discovered after final plan compilation MUST select a serialized admission verdict and MUST NOT invent a typed field, coercion, or schema epoch.

Inventory MUST NOT read payload bytes for format confirmation, compression confirmation, schema inference, or whole-file hashing. It MAY read transport/filesystem metadata and MUST label identity strength. Local identity based on path, size, modification time, and platform generation facts remains weak until content is read and hashed.

An ordinary pinned run MUST NOT execute a current-schema discovery pass. It MAY reuse a strongly keyed observation cache, but absence of a cache entry causes in-stream observation, not a preliminary probe.

## Cold discovery and final planning

For an unpinned `Discover` or unpinned `Hints` resource, a package-producing command MUST execute:

```text
metadata inventory
→ deterministic discovery selection
→ bounded discovery observations
→ aggregate/reconcile observations and hints
→ freeze persistent or run-local schema snapshot
→ compile final plan and total stream-admission program
→ execute
```

The discovery result returned by that command MUST feed final plan compilation directly. Persistent auto-pin MAY write snapshot and lock artifacts, but MUST NOT re-enter pinned preparation or rediscover current files afterward. `--no-pin` MUST freeze the same run-local snapshot in plan/package identity without project writes.

An explicit `cdf schema discover` ends after reporting. An explicit `cdf schema pin` ends after validated artifact persistence. A later command is a separate source interaction and MAY read the extraction payload while reusing a valid observation cache entry.

No destination preparation that can mutate external state, package segment creation, checkpoint proposal, or producer side effect beyond the bounded discovery invocation MAY begin before final plan compilation.

## Independent coverage axes

Every discovery manifest MUST record both axes:

### File coverage

- `all_files`: every matched candidate is selected;
- `sampled_files`: explicit positive `sample_files = N` selects a deterministic subset.

Sampling MUST never activate implicitly from file count, elapsed time, memory pressure, executor topology, or transport cost. If matched files are at most `N`, evidence records `all_files`.

`sampled_files` uses `stratified-hash-v1`. Candidates are sorted by canonical location. For `K = min(M, N)`: zero candidates fail; one chooses the lowest selector score; two choose first and last; three or more choose first/last and the lowest-score candidate from each balanced contiguous interior stratum. The score is SHA-256 over length-prefixed selector version, resource id, canonical location, and bounded metadata identity. Ties resolve by canonical location and then canonical identity bytes.

Selection evidence MUST include selector/version, configured `N`, matched/selected/unobserved counts, selected scores, strata, and bounded identities. Selection occurs before payload probes and is invariant to probe concurrency/completion order.

### Within-file coverage

- `format_metadata`: bounded format schema metadata, such as Parquet footer or Arrow IPC schema block, without row-data scanning;
- `bounded_content`: configured maximum input bytes and records from a selected row-oriented file;
- `full_content`: every record/value is observed.

Parquet and Arrow IPC default to `all_files + format_metadata`. CSV/JSON/NDJSON and other row formats default to `all_files + bounded_content` unless `sample_files` explicitly narrows file coverage. Their default bounded-content limits remain the format's recorded probe limits. `full_content` MUST be explicit and MUST not be inferred from the word `all_files`.

Artifacts, CLI reports, and diagnostics MUST NOT use unqualified `exhaustive`. They MAY say `file-exhaustive`, `metadata-exhaustive`, or `content-exhaustive` only when the corresponding axis proves it.

Each selected candidate records bytes and records actually observed plus a physical-schema hash/verdict. Each unselected candidate records only bounded metadata identity and `unobserved`; it MUST NOT carry placeholder schema facts.

## Pinned execution and stream admission

A pinned command MUST execute:

```text
metadata inventory and incremental selection
→ load/verify fixed snapshot
→ compile final plan and total admission program
→ open selected partitions
→ observe/reconcile while decoding each same stream
→ validate/package/deliver
```

The compiled admission program MUST contain baseline/effective schema identities, format-driver id/version/options and observation contract, normalization version, type/coercion allowances, residual/quarantine policy, control-critical fields, and every permitted verdict. Execution MUST NOT reparse or reoptimize these inputs.

The first physical schema/window emitted by a partition instantiates the compiled program. The exact observation and chosen verdict MUST be package evidence. The outcomes are:

- compatible: admit;
- compiled lossless widening: coerce and record;
- isolated unknown field or nullable mismatch: preserve exact value in `_cdf_variant` under the compiled residual rule;
- control-critical mismatch: quarantine row or partition;
- reliably isolated malformed record: quarantine record when the driver declares record isolation;
- broken framing/unresynchronizable input: quarantine partition;
- explicit strict contract: abort before destination mutation.

An unknown field MUST NOT become a typed destination column during the run. Promotion/backfill is an explicit later schema epoch.

Preview MUST use the same inventory, fixed schema, format interpretation, admission program, and first-stream observation as run, then stop at its bounded downstream row/byte limit. A clean preview MUST not hide a failure visible within those same bounds.

## Observation cache and payload spool

An observation-cache key MUST contain:

```text
strong source generation or cryptographic checksum
+ format driver id and semantic version
+ canonical format/transform options hash
+ normalization version
+ pinned contract/admission-program identity
```

Weak identity, key mismatch, corruption, or unsupported cache version MUST miss safely. Cache hits/misses are telemetry and do not replace extraction generation preconditions or package observation evidence. Cache bounds, retention, cleanup, and location MUST be explicit.

Large bytes MUST NOT be stored as ordinary observation-cache values. Payload materialization uses a disk/memory-ledger-accounted spool with source, generation, transform, owner, cleanup, and content-hash metadata.

If cold discovery fully downloads, fully decompresses, or otherwise writes a candidate into a payload spool during the same package-producing command, extraction MUST consume that exact verified spool and MUST NOT contact/download/decompress the source generation again. If discovery retains decoded batches instead, those exact batches MUST enter the execution stream after final plan compilation.

Small bounded prefix/footer/sample reads that were not placed in the payload spool MAY be read again as part of extraction. The duplicate bytes MUST remain within the recorded discovery budget and MUST be reported separately from extraction payload bytes. Implementations SHOULD reuse cached facts/ranges when doing so is simpler than rereading, but MUST NOT create an unbounded retention obligation for a tiny probe.

## Dynamic and unbounded producers

A dynamic producer with a declared schema handshake MAY freeze the plan without consuming data. Otherwise CDF MUST start one invocation, retain or spool its bounded bootstrap batches, freeze a run-local schema and final plan at a bootstrap barrier, emit the retained batches into that plan, and continue the same invocation.

The producer MUST NOT be invoked once for discovery and again for extraction in one command. Unbounded sources use the same fixed schema epoch until an explicit controlled restart/replan; they do not mutate schema mid-stream.

## Errors and evidence

Discovery failures MUST name resource, candidate, both coverage axes, measured/configured bytes and records, and the fix. A selected probe that exceeds budget MUST fail without substituting another candidate. An incompatible selected cold-discovery aggregate MUST fail schema freezing with every selected candidate verdict.

Runtime drift MUST produce a named compiled verdict rather than an inference stack trace. Package evidence MUST preserve baseline/effective schema hashes, physical observation hash/type provenance, admission-program identity, residual/quarantine rule, and source generation.

Telemetry MUST distinguish metadata operations, discovery prefix/footer/sample bytes, payload-spool bytes, extraction source bytes, spool reuse, observation-cache hit/miss, and duplicate bounded probe bytes. A bounded range probe MUST NOT be reported as a full-object transfer.

## Scenarios

Given 100 enormous remote JSON files and `sample_files = 10`, when cold discovery runs, then inventory lists 100 metadata identities, `stratified-hash-v1` selects exactly 10, each selected file is read only within the configured content byte/record budget, evidence says `sampled_files + bounded_content`, and 90 candidates remain explicitly unobserved.

Given that snapshot is pinned, when the resource later runs, then CDF performs no current-schema pre-scan; each new/changed file's first decode window is reconciled within its extraction stream and every compatible/residual/quarantine verdict follows the frozen program.

Given 100 Parquet files, when cold discovery uses defaults, then every footer/schema metadata block is observed without reading data pages and evidence says `all_files + format_metadata`, not content-exhaustive.

Given an unpinned cold `cdf run` on a transformed seekable file, when discovery must fully decompress to a spool, then the final plan consumes that spool and the remote compressed generation is transferred/decompressed once.

Given an unpinned cold `cdf run` on NDJSON whose 8 MiB prefix is sampled without a payload spool, when extraction begins, then rereading that bounded prefix is permitted and separately measured; there is no second full-file pre-scan.

Given a pinned baseline and an unobserved nullable field mismatch, when extraction observes the row, then the typed field is null, the original value is captured in `_cdf_variant`, and the pin remains byte-identical.

Given a Python producer without a schema handshake, when a run starts, then one invocation emits retained bootstrap batches, the plan freezes, and that invocation continues without re-executing user code.

## Acceptance criteria

- Cold auto-pin counter tests prove discovery feeds final planning directly with no subsequent pinned discovery pass.
- Pinned run counter tests prove zero schema-probe payload opens before extraction and no override of `sample_files`.
- Coverage artifacts encode and validate both axes; legacy unqualified exhaustive evidence is removed rather than shimmed.
- Local inventory counters prove no whole-file hash/read; extraction hashes and reattests the generation.
- Observation-cache hit/miss/generation/version/options/normalizer/contract/corruption tests fail closed.
- Full or transformed discovery spools are reused by same-command extraction; bounded unspooled probe duplication is capped and separately measured.
- JSON/NDJSON/CSV retained-window tests produce byte/row-identical packages to declared-schema execution.
- Parquet full-scan data pages transfer once; selective ranges stay generation-bound and deterministic.
- Residual/quarantine/strict outcomes are serialized, replayable, and never mutate the schema epoch.
- Preview/run share the admission front end, and Python/Lua/WASM producer counters prove one invocation absent retry/replay.

## Explicit exclusions

This specification does not introduce implicit schema promotion, same-run typed schema epochs, adaptive hidden sampling, cache authority over weak source identity, unbounded payload retention, destination-specific admission behavior, or a backwards-compatibility reader for superseded discovery evidence.
