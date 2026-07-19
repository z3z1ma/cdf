Status: active
Created: 2026-07-11
Updated: 2026-07-16
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md, .10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md, .10x/tickets/done/2026-07-11-p0-bx1-kernel-stream-extent-artifacts.md, .10x/specs/portable-partition-task-protocol.md

# P0 WX1: portable partition task/result protocol

## Scope

Implement neutral canonical task, attempt/fence, typed artifact reference, worker result/attestation protocol values and validation/hash fixtures; integrate capability declarations without transport or a remote scheduler.

## Acceptance criteria

- Protocol has no engine/runtime/driver/CLI/store/path/secret-value implementation types.
- Canonical serialization/hash/version/tamper/stale-fence validation is fixture-backed.
- Task is sufficient for mock isolated reconstruction through registries/injected services.
- Package/receipt/checkpoint authority remains absent from worker results.

## Evidence expectations

API/dependency checks, golden/tamper/compatibility fixtures, mock resolution, secret/path scans, and adversarial protocol/authority review.

## Explicit exclusions

No RPC, worker daemon, framework adapter, remote store, or placement.

## Blockers

None. SX1, DX1, and BX1 are done; this ticket is executable.

## References

- `.10x/decisions/portable-partition-task-capsule.md`
- `.10x/research/2026-07-11-portable-partition-task-audit.md`
- `.10x/specs/portable-partition-task-protocol.md`
- `.10x/decisions/datafusion-analysis-scheduling-identity-boundary.md`
- `.10x/specs/datafusion-currency-bridges.md`

## Progress and notes

- 2026-07-12: `.10x/tickets/2026-07-12-p3-j5-execution-plan-marshaling-metrics.md` will translate native CDF operators into DataFusion plan/metrics shells only after WX1 establishes canonical task authority. DataFusion/Ballista serialization cannot replace the capsule or gain credential, package-finalization, receipt, or checkpoint authority.
- 2026-07-19: Activated as the P0 half of the critical path to C5. Implementation will establish the smallest neutral canonical capsule/result authority first; A7/A8 can then complete the independent stream-epoch half of the dependency chain before C5 proves isolated-worker equivalence.
- 2026-07-19: Implemented the protocol authority in neutral `cdf-runtime`: versioned canonical task/result digests; exact CDF/artifact/Arrow/relational-engine/normalizer compatibility; content-addressed compiled-source, partition, input, segment, quarantine, residual, verdict, and lineage references; portable resource/control budgets; runtime-resolved host capability admission; task-bound fenced attempts; semantic results whose operational telemetry is explicitly nonidentity; and coordinator admission that rejects expired/stale fences, unauthorized output scopes, wrong partition ordinals, count/hash mismatches, and non-success terminal states. Control metadata bounds are task knobs rather than hard-coded throughput ceilings.
- 2026-07-19: Kept task payloads out of control messages. The compiled source and partition plans are immutable typed artifact references, so the protocol contains no borrowed plan/resource, local filesystem path, store implementation, open handle, or transport. Added source-registry admission for exact driver version/option-schema identity and proved a serialized plan can be dropped, reloaded by content hash, and resolved through the ordinary registry plus injected execution services.
- 2026-07-19: Tightened the reconstruction boundary before review: explicit redacted-options identity; exact reconstructed source/partition hash, driver, resource, scope, and source-position binding; rejection of coordinator-absolute local file positions; exact declared worker services plus CPU/I/O/memory/disk and runtime-resolved lane capabilities; portable relative artifact keys; and unique contiguous per-partition segment ordinals/ids. The task's secret-reference list must exactly equal the references found after the compiled source artifact is reconstructed.
- 2026-07-19: Completed the task authority tuple with explicit decode-unit and segment authority hashes plus a task-recorded retry/duration policy enforced by attempt envelopes. Empty immutable input artifacts remain legal; message/reference and byte limits are configurable task authority rather than implicit constants. Replaced the positional task constructor with one named input structure so C5 and future hosts cannot silently transpose protocol fields.
- 2026-07-19: Corrected the embedded-interpreter lane lifecycle before closure: portable tasks admit `RuntimeResolvedRequired` ceilings and reject host-resolved values; worker admission requires a `RuntimeResolved` lane that is a validated tightening. This preserves one portable task hash across GIL and free-threaded hosts while making the executable ceiling exact at attachment time. Reconstructed partition scopes and metadata now also reject coordinator-absolute local paths recursively.

## Evidence

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime worker_protocol::tests --locked` — 7/7 passed. Covers the checked-in canonical task fixture, round trip and hash stability, task/result tampering, unsupported compatibility/services, portable-to-runtime-resolved lane tightening, reconstructed partition identity and coordinator-path rejection, stale/expired fencing, contiguous segment authority, nonidentity telemetry, explicit metadata-budget failure, output-scope authorization, and absence of payload/commit authority fields.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime source_registry_compiles_hashes_and_resolves_mock_without_order_authority --locked` — 1/1 passed. The mock source plan is serialized to a typed immutable artifact, coordinator objects are dropped, content identity is rechecked, and the plan resolves through the registered driver and injected services; stale driver versions fail before resolution.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-runtime --all-targets --locked -- -D warnings` — passed.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime --lib --locked` — 93 passed, 1 explicit performance test ignored.
- `cargo fmt --all` and `git diff --check` — passed.
