Status: active
Created: 2026-07-10
Updated: 2026-07-10

# Preview global budget and deterministic payload selection

## Context

P2 requires `cdf preview` to share resolution, decoding, reconciliation, validation, residual capture/quarantine, normalization, and effective-schema behavior with `run` while remaining bounded and no-write. Multi-file preview cannot payload-scan every partition at production cardinality, but first-file or executor-resource-dependent behavior would hide detectable drift and make evidence nondeterministic.

The user ratified explicit global defaults after reviewing the production-scale tradeoff. This decision completes `VISION.md` sections 8.6 and 18, `.10x/decisions/data-onramp-source-identity-preview-disposition.md`, and `.10x/specs/data-onramp-source-experience-cli.md`. It does not change discovery coverage: exhaustive versus sampled schema discovery remains governed separately, and resource pressure never silently changes discovery coverage.

## Decision

Preview uses a versioned executor policy. The first policy is `preview-balanced-stratified-v1`.

Default global decoded-inspection limits per preview invocation are:

- 500 rendered output rows;
- 64 MiB of decoded input batch bytes admitted to contract processing;
- 64 admitted input batches.

All three limits MUST be executor-configurable, validated as positive values, serialized into machine evidence, and rendered for operators. An explicit CLI row limit overrides the default rendered-row limit only; it does not silently change the byte or batch bounds.

Preview MUST resolve and plan the same partition set as run. Terminal file/schema observations already proved during planning MAY be metadata-attested through the same exact identity/schema-verdict rule as run without opening payload bytes. Other candidates participate in deterministic payload selection.

For `N` payload-eligible candidates and a global batch budget `B`, membership size is `K = min(N, B)`. Candidate membership uses the existing canonical `stratified-hash-v1` selector over resource id, canonical partition location, and bounded transport identity. Selection MUST be independent of enumeration order, concurrency, worker topology, byte/row exhaustion, and completion order. Selector name, candidate count, selected membership, and selected-but-uninspected membership MUST be serialized.

Selected partitions receive deterministic fair-share batch quotas before payload I/O: each receives `floor(B / K)` batches and the first `B mod K` selector-ordered partitions receive one additional batch. Partitions are processed without requiring all selected streams to remain open concurrently. Global row or decoded-byte exhaustion may stop before every quota is consumed; those selected partitions remain reported as selected-but-uninspected or partially inspected. Unselected planned partitions remain metadata-attested where exact attestation is available and are reported as payload-uninspected. No uninspected partition receives a decode, reconciliation, or row-conformance claim.

Decoded-byte accounting is over batches admitted to contract processing, not rendered output bytes. Output bytes are reported separately. A decoded batch that would exceed the remaining byte budget MUST NOT enter contract processing; the report MUST distinguish payload-opened from payload-inspected evidence and mark truncation. Batch admission is atomic; preview does not fabricate partial-batch byte precision.

Preview success means only that planning/attestation succeeded for the full resolved set and the admitted payload bytes passed through the same front end as run. It does not claim unseen rows or payload-uninspected partitions conform. A later run MUST NOT fail for a reason preview already observed and admitted without preview having emitted the corresponding verdict.

## Alternatives considered

Open one batch from every partition.

- Rejected because payload I/O and open/poll work scale with partition count and are not bounded at production cardinality.

Inspect canonical partitions sequentially until a global limit is exhausted.

- Rejected because a large early partition can consume the entire budget and make later-file coverage systematically poor.

Let concurrency, elapsed time, or resource pressure choose membership.

- Rejected because equal source/configuration would produce different evidence across executors and retries.

Reuse discovery metadata budgets as preview payload limits.

- Rejected because metadata probing and decoded row inspection are different ledger categories with different cost and semantics.

## Consequences

Preview reports separate planned, selected, payload-opened, metadata-attested, payload-inspected, partially inspected, and uninspected counts. It reports decoded input bytes separately from rendered output bytes and names the policy/selector versions.

The executor may implement bounded concurrency, but concurrency MUST NOT change membership, quotas, output order, verdicts, or serialized evidence. Future Spark/Flink/container executors can shard the selected set while preserving the same policy identity and deterministic reduction.

Changing defaults, selector membership, quota allocation, or byte-accounting semantics requires a new versioned policy and decision rather than silently changing `preview-balanced-stratified-v1`.
