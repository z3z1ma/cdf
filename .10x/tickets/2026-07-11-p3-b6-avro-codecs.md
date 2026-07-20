Status: blocked
Created: 2026-07-11
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md

# P3 B6: Avro object-container and single-object codecs

## Scope

Add native Avro OCF block planning/parallel decode and explicit-schema single-object framing with logical types, unions, schema resolution evidence, block codecs, and physical provenance.

## Acceptance criteria

- OCF blocks are bounded deterministic units; single-object requires explicit fingerprint/schema authority.
- Writer/reader schema resolution compiles into shared reconciliation; no ambient registry inference occurs.
- Nullable/general unions, logical types, sync corruption, block compression, and malformed records obey catalog semantics.
- Native reference ratio, memory, jobs, and package determinism are green.

## Evidence expectations

Dependency review, Apache/reference corpus, schema-evolution matrix, malformed/fuzz blocks, logical-type/union goldens, and profiles.

## Explicit exclusions

No network schema registry client.

## Blockers

- `arrow-avro` 58.3.0 performs codec decompression and nested collection
  expansion inside unbounded dependency-owned allocations. CDF now reserves and
  enforces the complete retained block-output authority and stops before publishing
  any partial block, but an adversarial compressed block can still exceed that
  authority transiently before the dependency yields an Arrow batch. Closure needs
  either a bounded upstream decoder surface or execution under C5's enforceable
  isolated-worker memory/CPU authority. This is a closure blocker, not a reason to
  weaken the catalog's expansion guarantee.

## Journal

- 2026-07-18: Activated after H3 closure and the H4/H5 dependency audit. The native
  `arrow-avro` 58.3.0 codec is the implementation engine: it is the Arrow project’s
  vectorized Avro reader used by DataFusion 54, supports OCF ranges, logical types,
  dense unions, schema resolution, single-object fingerprints, and native block
  compression. CDF retains byte-source access, deterministic unit assignment,
  memory accounting, schema authority, and batch identity. This keeps Avro isolated
  in its own codec crate and avoids either a row-wise `apache-avro` conversion or an
  Avro dependency in generic runtime.
- 2026-07-18: Rejected the first nominal-byte-range splitter after adversarial
  review. It could infer framing from a sync-shaped payload substring, publish an
  earlier batch before a late fatal record, and materialize unbounded planning
  metadata. Replaced it rather than retaining a compatibility path.
- 2026-07-18: OCF planning now parses the exact count/encoded-size framing of each
  block, verifies its trailing sync marker, emits one retryable unit per block with
  an exact extent, and bounds header bytes, encoded bytes, records per block, and
  total blocks with explicit format knobs. Adjacent units overlap only on the
  framing sync marker required by `arrow-avro` range decoding.
- 2026-07-18: OCF decode now reserves one decoded-block output authority, fully
  decodes and validates the block, and only then partitions the exclusive memory
  lease into independently owned batches. This generic `MemoryLease::into_partitions`
  transfer avoids readmission and proves exact release. A malformed record after a
  valid first Arrow batch now returns before a `PhysicalDecodeStream` is published.
- 2026-07-18: Single-object input is one explicit-schema/fingerprint datum per file;
  multi-chunk records are retained under the record authority, while concatenated
  datums fail rather than relying on absent message boundaries. Transport errors now
  cross both Avro and Arrow adapters as typed `CdfError` values, preserving auth,
  transient, rate-limit, cancellation, and retry evidence.
- 2026-07-18: Reconciled the tranche after the concurrent Iceberg lane committed.
  The actual shared tree is workspace-format-clean and strict-Clippy-clean across
  every crate, target, and feature. The exact fast-quality test slices are green.
  `cargo deny` and `cargo audit` are green; `cargo vet` admits every Avro addition
  but remains red solely on the independently committed Iceberg git dependency,
  whose supply-chain admission is outside this ticket.
- 2026-07-18: Audited C5 before claiming it as the resource-containment fix.
  C5 is a serialized authority/equivalence boundary whose local host executes in
  process; it does not impose an allocator or CPU fence around dependency-owned
  decode. Upstream `arrow-avro` 58.3.0 exposes no decompression or nested-value
  limit. The ticket therefore moves to `blocked` instead of laundering C5 into
  closure evidence or adding a destination/source-specific subprocess workaround.
- 2026-07-18: F2 found that the blocked codec had nevertheless been registered in
  the standard CLI catalog. Removed both Avro framings from product registration
  and removed the CLI's direct Avro dependency until this ticket has enforceable
  containment evidence. The codec crate, native tests, and focused ticket remain;
  test-only registries may still exercise it under explicit deterministic budgets.
  A product-catalog regression test now rejects accidental re-admission.

## Evidence

- Exact block framing, projection, scheduler-order invariance, all native OCF block
  codecs, Apache reference output, logical types/general unions, sync corruption,
  encoded/decoded/block-count limits, late-record atomic failure, SOE fingerprint,
  SOE projection, truncation, concatenation, and multi-chunk cases: `cargo test -p
  cdf-format-avro --locked` passed 15 tests on 2026-07-18.
- Exclusive lease transfer and exact release: `cargo test -p cdf-memory --locked`
  passed 23 tests, including the new partition/no-double-release case.
- Owned crates are formatted and strict-clean: `cargo fmt -p cdf-format-avro -p
  cdf-memory -- --check` and `cargo clippy -p cdf-format-avro -p cdf-memory
  --all-targets --all-features --locked -- -D warnings` passed.
- Full-workspace static gates passed on the settled shared tree: `cargo fmt --all --
  --check` and `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo clippy --workspace
  --all-targets --all-features --locked -- -D warnings`.
- Exact fast-quality tests passed on the settled shared tree: core library results
  were contract 90/2 ignored, kernel 70/0, package 64/4 ignored, and runtime 144/2
  ignored; CLI core passed 35/35 and its artifact feature passed 37/37.
- `cargo deny --locked check` and `cargo audit --deny warnings --ignore
  RUSTSEC-2024-0436` passed. After `cargo vet fmt`, `cargo vet --locked
  --no-minimize-exemptions` reports exactly one unrelated missing admission:
  `iceberg:0.10.0@git:fd1c546...`; the new `arrow-avro`, `crc`, `crc-catalog`, and
  `strum_macros` dependencies are covered.
- Gitleaks 8.18.4 scanned a clean checkout with the exact staged patch applied and
  reported `no leaks found`.

## Review

- 2026-07-18 adversarial architecture review verdict: **fail**. Critical findings
  were partial fatal-window publication and unbounded dependency-owned expansion;
  significant findings were heuristic non-block units, unbounded unit metadata, and
  lost typed transport errors. Exact block units, bounded unit count, atomic
  publication, and typed error transport are repaired and regression-tested. Native
  decompression/collection expansion remains the explicit blocker above. No second
  review will be commissioned until that blocker and the remaining performance/jobs/
  package evidence are resolved.

## References

- `.10x/specs/native-enterprise-format-catalog.md`
- `.10x/specs/native-format-codec-runtime.md`
