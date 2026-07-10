Status: active
Created: 2026-07-10
Updated: 2026-07-10

# Sampled schema discovery coverage

## Purpose and scope

This specification governs explicit sampled file-schema discovery. It refines `.10x/specs/data-onramp-schema-intelligence.md`, `.10x/specs/data-onramp-file-sources-transports.md`, and `.10x/decisions/explicit-sampled-discovery-and-residual-promotion.md` without weakening exhaustive discovery as the Parquet/Arrow IPC default.

It defines selection, evidence, pinning, runtime obligations, and errors. It does not define row-level residual encoding or promotion execution.

## Coverage modes

Discovery coverage MUST be one of:

- `exhaustive`: every matched candidate is probed;
- `sampled`: an explicit deterministic subset is probed and every other matched candidate is recorded as unprobed.

Parquet and Arrow IPC default to `exhaustive`. Sampling MUST NOT activate implicitly because of file count, metadata bytes, elapsed time, memory pressure, concurrency, executor topology, or transport cost.

Sampled mode requires an explicit positive `sample_files = N`. There is no hidden default for `N`. If the matched count is at most `N`, the result is exhaustive and MUST be recorded as exhaustive.

## Deterministic selector

The first selector is named `stratified-hash-v1`.

Candidates are first sorted by canonical transport location. Let `M` be matched candidates and `K = min(M, N)`.

- `K = 0`: discovery fails with the existing no-match diagnostic.
- `K = 1`: select the candidate with the lowest selector score.
- `K = 2`: select the first and last canonical candidates.
- `K >= 3`: select the first and last canonical candidates; divide the `M - 2` interior candidates into `K - 2` contiguous strata whose sizes differ by at most one, assigning remainder entries to earlier strata; select the lowest-score candidate from each stratum.

The selector score is SHA-256 over canonical length-prefixed bytes for:

```text
cdf-sample:stratified-hash-v1
resource_id
canonical_location
bounded_identity
```

`bounded_identity` is the canonical transport-neutral discovery identity, including explicit nulls for unavailable size, modification time, ETag/checksum, or strength fields. It MUST NOT require a full object read. Score ties resolve by canonical location and then bounded-identity canonical bytes.

The complete selector name, `N`, matched count, selected locations/scores, stratum boundaries, and candidate identities MUST be serialized into the discovery manifest. Candidate enumeration order or probe completion order MUST NOT change selection.

Adding, removing, renaming, or changing the bounded identity of a candidate MAY change the sample and MUST change the fresh discovery-manifest identity. Repeated discovery over unchanged candidates MUST select identical entries and produce byte-identical selector evidence.

## Probe budgets and failures

Selection occurs before probe scheduling. Executor budgets control only concurrency, in-flight bytes, and explicit resource-limit failure. They MUST NOT change selected membership.

If a selected probe exceeds the resolved per-file or in-flight budget, discovery fails with the selected location, measured/allowed bytes, coverage mode, and override guidance. It MUST NOT substitute another candidate.

Malformed selected candidates receive ordinary per-file probe verdicts. Initial sampled pinning requires the selected schemas to have one compatible aggregate under the active widening/join contract. A visible incompatibility in the selected sample fails pinning rather than choosing an arbitrary baseline. Unprobed runtime incompatibility is governed by residual capture or quarantine.

## Manifest and snapshot evidence

Every matched candidate MUST have one participation state:

- `probed` with physical-schema hash, measured probe bytes, and schema verdict;
- `unprobed` with no physical-schema hash, no measured probe bytes, and no discovery-time schema verdict.

An unprobed entry MUST NOT carry placeholders that look like observed facts. Serialization validation MUST reject physical-schema hashes or schema verdicts on `unprobed` entries and reject their absence on successfully `probed` entries.

Snapshot/package evidence MUST distinguish:

- baseline snapshot hash;
- effective schema hash;
- discovery manifest hash;
- coverage mode and selector version.

A sampled baseline remains immutable until explicit pin refresh. Ordinary execution MUST NOT rewrite it after observing an unprobed file.

## Runtime behavior

Every runtime partition is reconciled regardless of discovery participation. A clean unprobed file may flow normally. New compatible fields/widenings follow the effective-schema policy. Unknown or incompatible paths follow the compiled residual-capture/quarantine program. No runtime path may treat `unprobed` as admitted without validation.

Preview and run MUST use the same sampled manifest, baseline/effective schema, and reconciliation front end. Preview remains bounded in rows/bytes, but it MUST NOT use a different candidate selector.

## Scenarios

Given 10,000 canonically ordered Parquet candidates and `sample_files = 100`, when two executors enumerate the same bounded identities under different concurrency budgets, then both select the same 100 candidates and emit identical selector evidence.

Given a sampled pin and an unprobed file with an unexpected scalar type, when the resource runs under an evolve/discover contract, then the runtime emits a residual or quarantine verdict according to the compiled program and does not mutate the pin.

Given selected metadata exceeds the executor budget, when discovery runs, then it fails without probing a replacement or writing snapshot/lock artifacts.

## Acceptance criteria

- Selector edge cases and permutation invariance are property-tested.
- Sampled manifests round-trip and reject false probed/unprobed evidence.
- A sampled first pin and later unseen drift complete through the ordinary plan/package/receipt/checkpoint pipeline.
- Repeated unchanged selection is byte deterministic.
- Exhaustive default behavior and existing one-file evidence remain unchanged.

## Explicit exclusions

This specification does not define row residual serialization, destination correction, source-record sampling inside one text file, statistical confidence claims, or adaptive sampling.
