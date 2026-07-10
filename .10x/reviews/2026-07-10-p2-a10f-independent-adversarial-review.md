Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-09-p2-ws-a10f-multifile-discovery-runtime-conformance.md
Verdict: fail

# A10f independent adversarial review

## Target and authority

This review independently inspected the completed A10f implementation against `.10x/tickets/done/2026-07-09-p2-ws-a10f-multifile-discovery-runtime-conformance.md` and the active decision `.10x/decisions/preview-global-budget-and-payload-selection.md`. No implementation was modified.

## Assumptions tested

- `stratified-hash-v1` has one canonical score contract after selector extraction into `cdf-kernel`, rather than separate discovery and preview interpretations.
- Selection membership and fair quotas are fixed before payload I/O and are independent of enumeration, row/byte exhaustion, completion order, and executor topology.
- The global row, decoded-byte, and batch defaults and overrides match the ratified semantics.
- Decoded-byte admission is batch-atomic and separate from rendered-output accounting.
- Evidence distinguishes planned, eligible, selected, opened, metadata-attested, inspected, partial, selected-but-uninspected, and payload-uninspected coverage without claiming unseen conformance.
- Terminal quarantine uses the same attestation rule as run, preview remains no-write, and the shared engine front end—not source-specific CLI branches—owns payload execution.
- Large-N selection bounds payload opens and leaves enough deterministic evidence for a future distributed executor to reproduce membership and quotas.

## Findings

### Open — significant: preview does not use the established canonical bounded-identity bytes for `stratified-hash-v1`

`cdf-project` preserves the historical discovery selector contract by converting `DiscoveryBoundedIdentity` to a JSON value, recursively sorting object keys, and hashing the compact canonical JSON bytes. Its committed golden for resource `events.raw`, location `s3://acme/events/2026/01.parquet`, and identity `(size=42, modified=null, value=etag-value, strength=stable_etag)` is:

```text
1caaf43016737e252f12a8b0568d67951ecfd702010906cc8a7eaddb7b1caa27
```

Preview reconstructs the same logical identity in a private `PreviewBoundedIdentity` and passes `serde_json::to_vec(&identity)` directly to the kernel selector. That preserves Rust field declaration order rather than the established key-sorted canonical order. Recomputing the exact score inputs produced:

```text
canonical identity bytes: {"modified_at_ms":null,"size_bytes":42,"strength":"stable_etag","value":"etag-value"}
canonical score:          1caaf43016737e252f12a8b0568d67951ecfd702010906cc8a7eaddb7b1caa27

preview identity bytes:   {"size_bytes":42,"modified_at_ms":null,"value":"etag-value","strength":"stable_etag"}
preview score:            e5afb1e8a95fcc82c95c39e74f90e8af961f09e1d318633ec59023c9449da0b8
```

Because score determines the lowest member within each stratum, this can change preview membership while both reports claim `stratified-hash-v1`. The kernel extraction itself preserves discovery's golden, but the preview adapter does not preserve the selector's canonical bounded-identity input. Existing tests independently prove the discovery golden and preview determinism; none compares equal logical discovery/preview candidates or their scores. This violates the versioned selector identity and the ticket's golden-compatibility claim. A10f should remain active until preview uses the canonical bounded-identity representation and a cross-adapter golden/regression proves identical score and membership.

## Checks that passed

- `K=min(N,B)` is implemented by the kernel selector's `min(candidate_count, membership_limit)` and preview passes the global batch budget as membership limit. The 10,000-partition test selects and opens 64 payloads.
- Fair quotas are computed before opening streams as `floor(B/K)` plus one for the first `B mod K` selector-ordered members. The focused `B=8,K=3` test reports `3,3,2`.
- Kernel selection sorts candidates and selected indices canonically; permutation tests pass. Preview processes that precomputed order sequentially, so current execution has no completion-order or concurrency-dependent membership/output order.
- The row default is 500, decoded-input default is 64 MiB, and batch default is 64. `EnginePreviewLimits::new` rejects zero values. CLI `--limit` changes only the row bound; byte and batch defaults remain unchanged and all limits are serialized and rendered.
- A batch's Arrow memory size is checked before schema reconciliation or contract processing. An oversized batch is rejected atomically with zero admitted batches, rows, decoded bytes, and rendered bytes; opened and uninspected evidence remains distinct.
- Selection and quotas are fixed before row/byte exhaustion. Selected-but-uninspected and partially inspected IDs, per-selected-member quota/score/identity hash/inspected-batch count, payload-uninspected IDs, and aggregate opened/attested/inspected counts are serialized.
- Terminal quarantines call the same `ResourceStream::attest_partition` physical-schema-hash rule as run, cache repeated observation attestations, and do not open payload streams.
- `preview_resource` is generic over `ResourceStream`; CLI file/REST/SQL paths call the shared engine front end. The inspected CLI and conformance scenarios preserve project-tree state across preview.
- Large-N payload opens are bounded by selected membership. The current implementation is sequential, so at most one selected stream is live in this executor. Unselected file partitions may still receive metadata attestation/footer work; they do not receive `ResourceStream::open` payload calls or payload-conformance claims.
- The serialized selector name, candidate count, selected canonical locations, scores, bounded-identity hashes, quotas, and inspected batches are sufficient for a future distributed executor to shard the selected set deterministically, provided it uses the corrected canonical identity encoding.

## Exact verification

The following commands passed:

```text
cargo test -p cdf-kernel stratified_selection -- --nocapture
# 3/3

cargo test -p cdf-engine preview -- --nocapture
# 7/7

cargo test -p cdf-project stratified_hash_selector -- --nocapture
# 4/4, including the canonical score golden

cargo test -p cdf-cli --lib sampled_discovery_renders_every_cli_path_and_routes_unseen_drift_to_package_quarantine -- --nocapture
# 1/1

cargo test -p cdf-cli --lib preview_multi_match_file_glob_reads_every_sorted_match_without_writes -- --nocapture
# 1/1

cargo test -p cdf-conformance --lib p2_s8_multifile_preview_traverses_the_same_planned_partitions_as_run -- --nocapture
# 1/1

cargo test -p cdf-conformance --lib p2_preview_run_parity_law_covers_supported_archetypes -- --nocapture
# 1/1 across file, REST, and Postgres archetypes
```

The score comparison used the exact length-prefixed SHA-256 domain from `cdf_kernel::stratified_hash_v1_score` with the discovery golden's resource, location, and logical identity, once with canonical key-sorted identity JSON and once with preview's field-order JSON.

## Verdict

Fail. Most budget, quota, bounded-open, byte-admission, evidence, parity, and no-write behavior is well-supported, but the preview adapter emits a different score for the same logical bounded identity while claiming the existing versioned `stratified-hash-v1` selector. That is a core deterministic-membership contract, not a documentation-only discrepancy.

## Limits and residual risk

- This review did not run a public-network or cloud transport; those remain excluded and owned by WS-E/WS-I.
- The current executor is sequential. The review established that selection and quotas are precomputed and serializable, but did not execute a concurrent or distributed preview scheduler because none exists in scope.
- The 64 MiB bound applies to decoded batches admitted to contract processing, as ratified. A decoder must materialize a batch before the engine can reject it, so this is not a hard cap on one upstream decoder allocation.
- Metadata attestation may scale with the number of payload-uninspected file partitions and can probe Parquet/Arrow metadata. This does not violate the explicit payload-open bound, but remote metadata-latency policy is not proven here.

## Subsequent resolution

The canonical-identity finding was repaired and independently re-reviewed in `.10x/reviews/2026-07-10-p2-a10f-canonical-identity-repair-review.md` with verdict `pass`. This review's original `fail` verdict remains the historical assessment of the pre-repair implementation.
