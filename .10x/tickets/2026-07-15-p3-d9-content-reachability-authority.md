Status: active
Created: 2026-07-15
Updated: 2026-07-18
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

## Blockers

None. The focused claim/root/reclamation protocol is now governed by `.10x/specs/immutable-content-reachability.md`.

## Evidence

- 2026-07-18 neutral record/proof slice:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel content_reclamation --locked -j 12 -- --nocapture` — passed, 4 passed. Covers exact provider generation, expired-claim proof, concurrent same-content live-claim retention, and committed-root retention.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime staging_lease_builds_fenced_content_publication_claim --locked -j 12 -- --nocapture` — passed, 1 passed. Proves runtime-owned `StagingLease` generations can construct content publication claims without destination-specific heartbeat or liveness fields.
  - `cargo fmt --all -- --check` — passed.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-kernel -p cdf-runtime --all-targets --locked -j 12 -- -D warnings` — passed.

## Review

Pass for the neutral record/proof and lease-binding slice. The implementation is kernel/runtime-generic, imports no Parquet/object-store destination crate, and does not add a cleanup loop, heartbeat, hard cap, hot-path scan, or performance-sensitive default. D9 remains active because no production reachability index, Parquet claim/root enrollment, or object-store CAS cleanup conformance has been implemented yet.

## Retrospective

The useful seam is "prove exact identity and non-reachability" rather than "clean up Parquet files." Keeping `LoadAttemptId` in runtime avoided a disruptive crate move; the kernel record instead uses an opaque content-claim attempt id populated from the runtime lease helper. If that indirection becomes annoying once multiple adapters enroll, the right refactor is moving the opaque attempt id into kernel as a shared id type, not adding adapter-local claim fields.
