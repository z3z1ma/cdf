Status: active
Created: 2026-07-15
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-14-p3-d8-parquet-staged-parallel-ingress.md

# P3 D9: Generic immutable-content reachability authority

## Scope

Specify and implement one destination-neutral authority for reclaiming immutable content-addressed objects that are not reachable from committed manifests and are not protected by any live staged-publication claim. The authority must compose with runtime-owned `StagingLease` fencing, object-store generation primitives, manifest roots, long-running attempts, concurrent same-content writers, and retention policy without destination-specific heartbeats, modification-time guesses, or full-store scans on the hot path.

## Non-goals

- No Parquet-owned garbage collector, liveness worker, retention clock, or process-local registry.
- No deletion based only on object age, path naming, list consistency, or a passive lease observation.
- No change to immutable object bytes, package identity, receipt verification, or committed-manifest retention semantics.

## Acceptance Criteria

- The kernel/runtime boundary exposes typed opaque content identity, publication claim, committed-root, and reclamation-candidate records without importing Parquet or any concrete destination.
- A publisher establishes a fenced live claim before an object can become a reclamation candidate; concurrent attempts sharing the same content cannot cause premature deletion.
- Manifest settlement atomically or monotonically converts claimed content into committed reachability evidence; crash recovery can finish or release the exact claim without guessing from modification time.
- Reclamation requires proof that no committed root and no live claim can reference the exact immutable object generation. Races with a new claim fail closed through provider CAS/fencing.
- The policy is incremental and bounded: normal runs do not scan all manifests or objects, and cleanup memory is independent of retained dataset size.
- Parquet enrolls through the generic protocol; conformance covers concurrent identical writers, crash before/after object publication, crash before/after manifest settlement, stale cleanup versus renewed claim, retained historical manifests, and object-store conflict/readback behavior.

## References

- `.10x/specs/streaming-destination-ingress.md`
- `.10x/specs/immutable-content-reachability.md`
- `.10x/decisions/destination-staged-ingress-final-package-binding.md`
- `.10x/tickets/done/2026-07-14-p3-d8-parquet-staged-parallel-ingress.md`

## Assumptions

- Record-backed: immutable content objects may outlive one attempt and therefore cannot be deleted by exact-attempt staging cleanup.
- User-ratified: liveness and reclamation eligibility are generic storage/runtime concerns; destination-specific heartbeat and modification-time policies are forbidden.
- Record-backed: object stores expose only per-object atomic generation/CAS primitives, so the protocol must remain correct without a multi-object transaction.

## Journal

- 2026-07-15 ownership: D8 eliminated whole-run attempt staging and final-copy amplification by immediately publishing create-or-verify immutable group objects. That architecture makes shared object lifetime independent of one attempt lease. This ticket owns the required generic reachability/claim protocol; D8 must not reintroduce Parquet-specific cleanup to hide the gap.
- 2026-07-18 shaping: Added `.10x/specs/immutable-content-reachability.md`, which defines destination-neutral content identity, publication claim, committed root, reclamation candidate, settlement, and cleanup semantics. The ticket is now behaviorally shaped; implementation still remains open and must not begin by adding a Parquet-owned cleanup loop or heartbeat.
- 2026-07-18 implementation slice: Added destination-neutral kernel records for immutable content identity, publication claims, committed roots, reclamation candidates, expired-claim evidence, root checks, and reclamation proofs. Added a runtime `StagingLease::content_publication_claim` helper so adapters derive claims from the existing fenced lease generation instead of inventing destination-local liveness metadata. The proof validator rejects deletion without exact provider-generation evidence, rejects live same-content claims without matching expired-lease proof, rejects other concurrent same-content live claims, and rejects any consulted committed root that still references the candidate. This is the generic boundary needed by Parquet/object-store enrollment, but D9 remains active until storage/index enrollment and destination conformance exist.
- 2026-07-19 durable-index slice: Added the current-only `ContentReachabilityStore` protocol and SQLite v1 implementation. Claims retain their complete source `ScopeLease`, so cleanup can re-prove the exact lease generation rather than guessing from an attempt id and token. The indexed store atomically installs/plans/publishes/releases claims, prepares roots before destination-manifest publication, commits roots and settles claims in one transaction, and emits bounded candidates from reverse indexes rather than scanning objects or manifests. An atomic reclamation reservation blocks racing claims/root intents until deletion is completed or explicitly released. Prepared roots protect the manifest-publication crash window; the governing spec now records that monotonic settlement explicitly. No destination code or hot-path behavior changed in this slice.
- 2026-07-19 enrollment slice: Injected the generic reachability store beside the runtime staging-lease authority for run, replay, and resume. Parquet now installs a fenced claim before immutable object publication, records exact provider generation when the provider exposes one, prepares a destination-neutral root before publishing the manifest, atomically settles that root after manifest verification, and releases or aborts authority on rollback. Duplicate/concurrent same-content writers merge their claims into the same deterministic root; replay verifies the existing manifest against that root before settlement. Cleanup is a generic bounded runtime operation over indexed candidates and durable reservations. The filesystem deleter serializes publication/deletion by content address and rechecks the exact SHA-256 generation under the lock. The generic `object_store` API cannot conditionally delete an observed generation, so those stores deliberately report unsupported and retain content rather than emulate unsafe HEAD-then-DELETE. This is a safe capability boundary, not a destination identity branch.

## Blockers

None. The focused claim/root/reclamation protocol is now governed by `.10x/specs/immutable-content-reachability.md`.

## Evidence

- 2026-07-18 neutral record/proof slice:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel content_reclamation --locked -j 12 -- --nocapture` — passed, 4 passed. Covers exact provider generation, expired-claim proof, concurrent same-content live-claim retention, and committed-root retention.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime staging_lease_builds_fenced_content_publication_claim --locked -j 12 -- --nocapture` — passed, 1 passed. Proves runtime-owned `StagingLease` generations can construct content publication claims without destination-specific heartbeat or liveness fields.
  - `cargo fmt --all -- --check` — passed.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-kernel -p cdf-runtime --all-targets --locked -j 12 -- -D warnings` — passed.
- 2026-07-19 durable index:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-state-sqlite content_reachability --locked -j 12` — passed, 2 passed. Proves prepared and committed roots both protect content, retained-root release exposes a bounded candidate, and an atomic reclamation reservation rejects a racing same-address publisher until released.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime staging_lease_builds_fenced_content_publication_claim --locked -j 12` — passed, 1 passed after strengthening the record to retain the complete `ScopeLease`.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-kernel -p cdf-state-sqlite -p cdf-runtime --all-targets --locked -j 12 -- -D warnings` — passed.
- 2026-07-19 Parquet enrollment and bounded reclamation:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-state-sqlite content_reachability --locked -j 12` — passed, 3 passed. Covers monotonic prepared/committed-root protection, concurrent same-content claim convergence into one root, bounded candidate exposure after root release, and reclamation reservations fencing a racing publisher.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-parquet --lib --locked -j 12` — passed, 36 passed and 1 ignored. Covers live-claim retention, abort/release, durable reservation recovery, exact local-generation deletion, changed-generation retention, remote conditional-delete refusal, duplicate manifest replay, and existing Parquet publication/readback behavior.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-project general_project_run_commits_file_resource_to_parquet_with_ledger_order --lib --locked -j 12` — passed, 1 passed. Proves normal project execution receives both state authorities and commits through Parquet.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-project parquet_artifact --lib --locked -j 12` — passed, 2 passed. Proves replay/recovery destination construction receives the reachability authority.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-kernel -p cdf-state-sqlite -p cdf-runtime -p cdf-dest-parquet -p cdf-project -p cdf-cli --all-targets --locked -j 12 -- -D warnings` — passed.
  - `CARGO_BUILD_JOBS=12 cargo test --release -p cdf-dest-parquet local_streaming_parquet_reaches_sixty_percent_of_write_roofline --lib --locked -j 12 -- --ignored --nocapture` — passed at 1,567.9 MiB/s, 0.919x the measured raw sequential-write roofline (1,706.4 MiB/s). The authority work remains outside the batch/row hot path and preserves the P3 Parquet throughput gate.
  - `cargo fmt --all` and `git diff --check` — passed.

## Review

Verdict: pass. Fresh adversarial review traced publication failure, duplicate replay, manifest-before-settlement crash, cleanup-reservation crash recovery, racing same-content publication, renewed/live leases, and changed provider generations. Generic runtime code names only the conditional-delete capability and destination-neutral identities; it contains no Parquet branch, heartbeat, age heuristic, object listing, or unbounded collection. SQLite transitions are indexed and transactional. Parquet performs authority I/O per immutable output object/root, never per row or batch, and the release roofline remains 0.919x raw. The residual limitation is explicit and safe: `object_store` 0.12 exposes no conditional-delete contract, so remote reclamation retains content until a provider-specific implementation supplies that capability. This does not weaken publication, retention, or runtime throughput.

## Retrospective

The useful seam is "prove exact identity and non-reachability" rather than "clean up Parquet files." The prepared-root intent is the critical monotonic bridge across two stores that cannot share a transaction: it closes the manifest publication window without inventing a destination heartbeat. Durable reservations similarly turn provider deletion into a recoverable state transition. Provider capability must remain honest: retaining an orphan on a backend without conditional delete is preferable to a racy emulation. Keeping `LoadAttemptId` in runtime avoided a disruptive crate move; the kernel record uses an opaque claim attempt id populated from the lease helper, and future adapters can enroll without depending on Parquet or SQLite.
