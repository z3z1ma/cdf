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
- 2026-07-14: Removed both inventory-time whole-file SHA-256 passes. Local plans now carry an explicit weak `source_generation` derived from bounded filesystem metadata and label `identity_strength = weak`; remote plans label strong/content-addressed identity without claiming the full object was transferred. `FilePosition` preserves the metadata generation separately from checksum/ETag/version so append incrementality remains possible without laundering weak evidence into a strong field. Fixed object-version-only manifest comparison, which previously always classified the object as changed. Parquet source validation now correctly uses the provider's `exact_ranges` capability for within-run random-access safety instead of conflating that capability with cross-command cache authority. Extraction-time content hashing remains the next part of this acceptance criterion.
- 2026-07-14: Replaced the ambiguous `exhaustive|sampled` discovery label with two independent, serialized axes: `all_files|sampled_files` and `format_metadata|bounded_content|full_content`. Format drivers now declare whether their schema observation comes from format metadata or bounded content, keeping the project compiler free of format-name branching. Candidate, manifest, kernel, source-identity, promotion, and CLI evidence records exact selected/unobserved counts plus observed bytes and records. The executor budget now carries independent byte and record ceilings; the prior hidden 1,000-record decoder limit is explicit evidence. `sample_files` selection remains upstream of every registered-format probe.
- 2026-07-14: Ratified the observation-cache mechanical policy within the active spec: `.cdf/cache/schema-observations/v1`, current-format-only entries, 4,096 entries, 64 MiB total, and 8 MiB per entry, with oldest-write eviction after successful writes. Exact keys bind canonical source location plus strong generation/checksum, format driver id/version, canonical format+transform interpretation, discovery byte/record contract, `namecase-v1`, and contract/baseline identity. Weak identity bypasses; absent, corrupt, unsupported, mismatched, oversized, or unavailable entries miss without affecting discovery correctness. Cache observations retain their original coverage counts for deterministic artifacts while actual source bytes and hit/miss telemetry remain non-identity report data.
- 2026-07-14: Implemented the versioned observation store and wired it through every CLI/project discovery entry point. Exact strong-generation hits reuse canonical schema observations without source payload I/O; local weak identities bypass without creating cache state. Entries are installed atomically, immutable under their content-addressed key, validated against their canonical schema hash, bounded by entry/count/byte limits, and evicted oldest-first after successful writes. Corrupt, unsupported, oversized, mismatched, unavailable, or conflicting entries safely fall back to discovery. Cache telemetry is attached only to non-identity source reporting; replay/package identity retains the original observation coverage rather than the cache access path.

## Blockers

None. Generation-strength and neutral byte-source prerequisites are committed.

## Evidence

- `CARGO_BUILD_JOBS=12 cargo check --workspace --all-targets` passed after the metadata-identity change.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files -p cdf-kernel --lib` initially exposed the strength/capability conflation in Parquet; after separating those concepts, `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files --lib` passed all 32 tests and the kernel run passed all 33 tests.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-project --lib http_parquet_auto_pin_plan_preview_and_run_use_file_runtime` passed the HTTP Parquet plan/preview/run integration and proves bounded discovery no longer appears as a full-object `bytes_loaded` inventory claim.
- `CARGO_BUILD_JOBS=12 cargo check --workspace --all-targets` passed after the two-axis artifact/API change, including every downstream evidence constructor.
- The current `cdf-project` test binary passed all 8 `discovery_manifest` tests, `tests::local_ndjson_discovery_is_bounded_and_writes_nothing_until_pin`, and `tests::local_parquet_discover_autopin_persists_all_file_metadata_manifest`; these observations prove deterministic pre-probe file selection, bounded row-oriented evidence, and all-file metadata-only Parquet evidence respectively.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel --lib` passed all 33 tests, including `two_axis_discovery_coverage_evidence_is_total_and_round_trips`.
- `CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2 cargo check -p cdf-project --all-targets` passed after the observation-cache integration; disabling incremental debug state avoided host swap thrash while retaining parallel compilation.
- `CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2 cargo test -p cdf-project --lib observation_cache -- --nocapture` passed 5 tests. The remote integration proves an exact hit performs no schema GET and records zero discovery source bytes while preserving schema/manifest hashes; an ETag change performs a new GET. Unit coverage proves corruption removal, immutable-key conflict rejection, generation/options/normalizer/admission mismatch misses, and bounded cleanup.
- `CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2 cargo test -p cdf-project --lib local_ndjson_discovery_is_bounded_and_writes_nothing_until_pin -- --nocapture` passed and proves a weak local identity bypasses the cache without creating its directory.
- `CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2 cargo check -p cdf-cli --all-targets` passed, covering the cache wiring in add, schema, scan, and deep-validation command paths.

## Review

Pending.

## Retrospective

Pending.
