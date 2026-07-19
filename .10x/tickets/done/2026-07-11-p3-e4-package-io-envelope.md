Status: done
Created: 2026-07-11
Updated: 2026-07-19
Parent: .10x/tickets/done/2026-07-10-p3-ws-e-hashing-package-io.md
Depends-On: .10x/tickets/done/2026-07-11-p3-e3-streaming-verification-replay-io.md

# P3 E4: package I/O envelope and triage closeout

## Scope

Run small/large/many-segment/high-cardinality package build/verify/replay cases, publish roofline/hash/sync breakdowns, prove crash/golden/memory laws, and close the original package-I/O triage into measured evidence.

## Acceptance criteria

- Package build reaches ≥70% write roofline and hashing ≤5% wall on named hosts.
- Production construction reports zero redundant content reread bytes.
- High-cardinality build/verify memory is bounded.
- Triage hypotheses are each closed with before/after or measured no-action rationale.

## Evidence expectations

Host reports/profiles/syscall counts, crash/golden suite, memory stress, triage reconciliation, and adversarial filesystem review.

## Explicit exclusions

No hash/artifact semantic change.

## Blockers

None. E3 is complete with capability-rooted bounded verification, retained opened-object destination ingress, and dedicated-host non-regression/high-cardinality evidence.

## References

- `.10x/tickets/done/2026-07-07-package-io-hashing-overhead-triage.md`
- `.10x/specs/package-io-hashing-durability.md`

## Journal

- 2026-07-19: Added a permanent release benchmark around the exact production LZ4 IPC writer, a hash-free control with identical framing/durability, and exact-byte raw writes. The initial one-core result exposed a 1.453 GiB/s SHA ceiling and a 44% cache-sized hash fraction; `aws-lc-rs` and `ring` both tied `sha2/asm` at 1.453 GiB/s, so both temporary dependencies and their probe code were deleted.
- 2026-07-19: Corrected the aggregate benchmark before accepting it: every worker and batch now carries distinct high-entropy values, the raw control writes the exact encoded payloads, and worker count comes from host parallelism rather than a fixed cap. Added environment-controlled sustained size/sample/volume knobs so closure can exceed RAM without burdening ordinary tests.
- 2026-07-19: Dedicated-host closure passed three alternating 32 GiB samples on the tuned gp3 filesystem. Hash-free median was 31.695912041 seconds; production hash-on median was 31.714295798 seconds, a 0.06% attributable wall fraction. Production sustained 1,033.8 MiB/s against an 8 GiB direct-I/O `fio` roofline of 1,145.25 MiB/s (0.903x). The package writer is storage-bound, so no hash/backend/product change is retained.
- 2026-07-19: Reconciled the whole triage. E1 proves exact receipts, zero post-write segment rereads, failure cleanup, and unchanged goldens; E2 proves bounded streaming manifests, receipt-only production finalization, buffered trace durability, crash boundaries, and million-entry construction; E3 proves bounded capability-rooted verify/replay and opened-object destination consumption. E4 adds large/many-segment sustained build and exact hash/roofline evidence. Every original hypothesis is now measured, implemented safely, or rejected with a trigger.
- 2026-07-19: Activated after E3 closure. The package envelope now owns only measured build/write/hash/verify rooflines, crash/golden coverage reconciliation, and the original triage closeout; it will not reopen E3's capability or destination-ingress design.
- 2026-07-14: Corrected the FineWeb critical-path attribution before tuning package I/O. A release run spent 4.118 seconds in the package interval, but sampling showed its main thread blocked on staged-destination backpressure while DuckDB flushed/checkpointed each of 115 segments. Raw warm fsynced write was 6.87 GB/s and SHA-256 was 2.96 GiB/s with 2.01% measured write overhead. The destination regression is owned by `.10x/tickets/done/2026-07-14-p3-d7-persistent-staged-ingress-stream.md`; E4 will remeasure the package-only roofline after D7 removes the confounder.
- 2026-07-14: Activated with a measured package critical-path improvement. Removing a four-worker encode cap initially failed because completed encoder output and staged destination input independently reserved the same Arrow allocations. Canonical pressure relief plus an owned batch-and-lease handoff completed the 2.147 GB FineWeb-to-DuckDB fixture and reduced package execution from 5.008 to 4.168 seconds (16.8%). Evidence: `.10x/evidence/2026-07-14-p3-f2-accounted-staged-payload-handoff.md`.

## Evidence

- Package build and hash-share envelope: `.10x/evidence/2026-07-19-p3-e4-package-io-envelope.md`.
- Production zero-reread, atomic receipt, and golden evidence: `.10x/tickets/done/2026-07-11-p3-e1-hashing-artifact-sink.md` and `.10x/tickets/done/2026-07-11-p3-e2-streaming-manifest-durability.md`.
- Bounded million-entry explicit verification and fresh full-year non-regression: `.10x/tickets/done/2026-07-11-p3-e3-streaming-verification-replay-io.md`.
- Current critical-path accounted handoff: `.10x/evidence/2026-07-14-p3-f2-accounted-staged-payload-handoff.md`.

Acceptance mapping:

- **≥70% write roofline and hashing ≤5% wall:** 0.903x and 0.06%, respectively, on the named dedicated host.
- **Zero redundant production reread:** E1/E2 delete post-write/finalize rereads; E3 fresh-run authority consumes receipts without re-verification.
- **Bounded high cardinality:** E2 constructs one million entries at 175.8 MB RSS; E3 verifies 1,000,001 entries at 207.5 MB RSS under a 2 GiB cgroup.
- **Triage closeout:** the journal and evidence record classify every original hypothesis; alternate SHA backends are a measured no-action and mmap/consumer-read false hashing are rejected by E3.

## Review

### Findings

None. Fresh review checked that the retained benchmark uses production IPC options and durability ordering, alternates hash-on/hash-off order, uses unique high-entropy worker/batch payloads, exceeds RAM in the sustained cell, and excludes cleanup from both controls. The direct roofline and package writer use the same tuned filesystem. The test-only additions do not change package bytes, product selection, dependencies, or hot-path code.

### Verdict

**pass**. Every acceptance criterion maps to current evidence, and closure retains no speculative optimization or compatibility path.

### Residual risk

Storage faster than the observed approximately 5.5 GiB/s cache-sized aggregate hash capacity may make SHA visible again. That is an evidence trigger, not an active defect on the named host. Object-store multipart throughput is measured under its transport/destination owners rather than inferred from local files.

## Retrospective

- Fixed worker caps can conceal broken resource ownership. Concurrency should be bounded by measured CPU, memory, disk, and destination authorities; when widening it fails, first test whether the same physical allocation is being counted at multiple pipeline stages.
- A one-core or cache-sized hashing percentage can be true and operationally irrelevant. The correct closure cell must exceed RAM and compare exact production semantics against the sustained device roofline.
- Benchmark temp placement is part of the workload contract. `/tmp` was a 16 GiB tmpfs; making the spill root explicit turned an ENOSPC false start into a reusable host-independent harness knob.
- Backend folklore was unhelpful: three SHA implementations tied exactly. Measure candidate dependencies before accepting their build/supply-chain cost, then delete the losers completely.
