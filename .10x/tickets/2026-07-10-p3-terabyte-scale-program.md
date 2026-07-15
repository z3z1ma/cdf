Status: open
Created: 2026-07-10
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-08-p2-data-onramp-program.md

# P3 terabyte scale: the performance architecture

## Scope

Implement `VISION.md` Chapter 6 in full and make CDF run at hardware speed with constant, budgeted memory. P3 builds the performance lab first, then the streaming runtime, format decoders, deterministic parallel execution, destination bulk paths, hash-while-write package I/O, memory stress law, remote overlap, and measured interop boundaries.

This parent is an aggregate plan. Workstream records own sequencing and integration; each broad workstream MUST be split into bounded executable children before implementation.

## Governing records

- `VISION.md` Chapter 6 and Chapters 10, 12, and 14.
- `.10x/decisions/terabyte-scale-performance-envelope.md`.
- `.10x/specs/performance-lab-and-envelope.md`.
- `.10x/specs/architecture-layering-runtime.md`.
- `.10x/specs/package-lifecycle-determinism.md`.
- `.10x/specs/destination-receipts-guarantees.md`.
- `.10x/specs/resource-authoring-planning-batches.md`.
- `.10x/specs/data-onramp-file-sources-transports.md`.
- `.10x/knowledge/runtime-conformance-throughput-rule.md`.
- `.10x/knowledge/source-destination-extension-invariant.md`.
- `.10x/tickets/done/2026-07-07-performance-investigation-backlog.md` and its open triage children, which this program absorbs rather than duplicates.

## Hard guardrails

- Deterministic package identity MUST be invariant across `--jobs 1` and `--jobs N` for fixed plans and inputs.
- Package verification, receipts, checkpoint gating, crash recovery, redaction, and P1 progress remain unchanged in meaning.
- Every data-bearing buffer MUST participate in the single memory ledger once WS-A lands; no adapter or destination may retain an informal side budget.
- Parallel scheduling MUST NOT change partition-to-segment assignment, segment content, manifest order, or replay.
- Every optimization requires same-harness before/after evidence. Correctness mechanisms are optimized, never disabled to meet a target.
- New dependencies follow cargo-vet/deny and the pinned-tuple policy. `unsafe` requires a focused active decision, safety comment, and fuzz target.
- Source and destination performance behavior belongs behind shared runtime traits and capability sheets. No ticket-local concrete-source/destination branch may enter generic orchestration.
- Native format performance and behavior belong behind the registry/byte-source contract in `.10x/specs/native-format-codec-runtime.md`; adding a codec MUST NOT extend generic compiler/runtime match trees or force unrelated parser build domains.

## Workstreams

- `.10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md`
- `.10x/tickets/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md`
- `.10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md`
- `.10x/tickets/2026-07-10-p3-ws-c-deterministic-parallelism.md`
- `.10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md`
- `.10x/tickets/2026-07-10-p3-ws-e-hashing-package-io.md`
- `.10x/tickets/2026-07-10-p3-ws-f-constant-memory-guarantee.md`
- `.10x/tickets/2026-07-10-p3-ws-g-remote-io-overlap.md`
- `.10x/tickets/2026-07-10-p3-ws-h-interop-boundaries.md`
- `.10x/tickets/2026-07-11-p3-ws-v-vectorized-validation.md`
- `.10x/tickets/2026-07-12-p3-ws-j-datafusion-currency-bridges.md`
- `.10x/tickets/2026-07-13-p0-fixed-schema-discovery-stream-admission.md`

## Sequencing

WS-L runs first and alone until a full pre-optimization baseline evidence record exists. WS-A then owns the exclusive runtime-spine migration window. WS-B, WS-D, and WS-E may proceed in crate-bounded lanes against the baseline where they do not touch the frozen runtime surface. WS-C follows WS-A channels. WS-F is integrated with WS-A's ledger. WS-G builds on P2 transports. WS-H is independent after baseline. Existing performance triage tickets close only by absorption into a P3 child with evidence or by a measured no-action rationale.

After all workstreams close, program proof/closure proceeds through:

- `.10x/tickets/2026-07-11-p3-z1-envelope-evidence-reconciliation.md`
- `.10x/tickets/2026-07-11-p3-z2-scale-demo-adversarial-review.md`
- `.10x/tickets/2026-07-11-p3-z3-program-closure-retrospective.md`

## Acceptance criteria

- Every target in `.10x/decisions/terabyte-scale-performance-envelope.md` is green on recorded host classes.
- Correctness/evidence overhead is at most 10% of equivalent raw read-plus-write.
- Jobs-invariance and the 100 GB/2 GiB constant-memory law are permanent CI gates.
- The 1 TB synthetic scenario completes under the default budget and is I/O-bound in the attached profile.
- All ten current data-plane gaps in the P3 directive have before/after evidence.
- The generated envelope document is published and README claims link to it.
- Every open performance-triage ticket is terminal with evidence or an explicit measured no-action rationale.
- Coverage rows for `VISION.md` 6.1 through 6.6 leave pending where P3 owns the implementation.
- Final adversarial review includes a workload intended to embarrass the envelope and finds no critical/high unresolved issue or architecture leak.

## Evidence expectations

Each workstream records host-labeled before/after benchmarks, focused correctness/conformance output, and review. Parent closure requires the complete envelope, profiles, memory traces, jobs-invariance hashes, chaos results, triage reconciliation, coverage updates, and before/after TLC demo.

## Explicit exclusions

P3 does not implement a distributed scheduler, remote worker leases, resident streaming supervisor, WASM Tier 3 runtime, new lakehouse destinations, or weaken artifact semantics. Its runtime boundaries MUST remain embeddable in later Spark/Flink/container execution.

## Progress and notes

- 2026-07-10: Opened from the user-ratified P3 directive. The performance target and measurement doctrine are active in `.10x/decisions/terabyte-scale-performance-envelope.md`; the lab contract is active in `.10x/specs/performance-lab-and-envelope.md`. This activation starts no optimization. WS-L remains the only workstream eligible to execute until its baseline evidence exists.
- 2026-07-11: The user explicitly reprioritized enterprise performance ahead of CI/release stabilization while keeping CLI excellence active. Removed the stale whole-P1 closure dependency. WS-L is eligible immediately; P1 tails may proceed independently but may not optimize or otherwise mutate the pre-baseline P3 data plane.
- 2026-07-11: The user expanded native input scope beyond the original Parquet/CSV/JSON envelope. `.10x/decisions/native-enterprise-format-catalog-v1.md` and `.10x/specs/native-enterprise-format-catalog.md` make the WS-B closeout catalog finite and testable; FX1 prevents those codecs from extending generic compiler/runtime match trees or one monolithic parser build domain.
- 2026-07-11: VISION 6.5–6.6 are now explicitly owned within P3 drain mode. BX1 and A7–A9 move stream extent/watermark meaning into kernel artifacts, compile complete policies, execute finite frontier-closed epochs on the ordinary graph, and conformance-test late data/recovery/jobs invariance. Resident lifecycle and concrete CDC remain in the later supervisor ticket and must reuse this path.
- 2026-07-11: WS-H is split through IX1 and H1–H5. Python/subprocess must become incremental implementations of one neutral foreign-stream contract with falsifiable copy/memory/cancellation semantics; WASM remains a prospective interface/cost model in P3, with no invented runtime claim.
- 2026-07-11: The future distribution seam is now executable architecture rather than prose: WX1 defines a canonical operational partition task/result capsule and C5 proves direct-local versus serialize/reconstruct isolated-worker equivalence. P3 still ships no remote scheduler/framework adapter.
- 2026-07-11: Envelope ownership audit found the ≥1 GB/s/core validation target had no implementation child while the current evaluator is per-rule/per-row scalar. WS-V now owns engine-neutral vector kernels, bitmap verdict algebra, graph integration, scalar differential proof, and the target closeout.
- 2026-07-11: WS-L completed before any P3 data-plane optimization. The immutable report and honest failing envelope are recorded at `.10x/evidence/2026-07-11-p3-l5-preoptimization-baseline.md`; WS-A now owns the exclusive runtime-spine migration window, while crate-bounded WS-B/WS-D/WS-E work remains eligible only where it does not cross that frozen surface.
- 2026-07-12: Added WS-J under `.10x/decisions/datafusion-analysis-scheduling-identity-boundary.md` and `.10x/specs/datafusion-currency-bridges.md`. DataFusion is the standard currency for pruning, expressions, catalogs, memory/object-store sessions, plans, and metrics, while deterministic native CDF operators exclusively produce identity-bearing bytes and verdicts. The sequence reuses completed A2 memory authority, open FX1/WX1/G1 seams, and prevents P3 APIs from hardening private substitutes.
- 2026-07-13: Added the P0 fixed-schema discovery and stream-admission program after the 100-remote-JSON-file counterexample exposed runtime pre-observation as a second source execution. The corrected authority requires a fixed schema before final planning, removes current-schema pre-scans from pinned execution, represents file and within-file discovery coverage independently, and reuses materially downloaded payloads during the same command. SA0-SA5 join FX1, G1/G2/G3, B4/B5, and H2/H4 without adding source-format branches.
- 2026-07-14: Closed WS-D after D7 replaced the superseded per-segment staged-ingress API with one generic bounded acknowledgement stream and completed the reopened wide-string investigation. The exact 2.205 GB package path measures 2.017 seconds; the only public Arrow alternative is 41% slower and unbounded by upstream contract, while TLC remains 9.51M rows/s. All destination-specific behavior remains behind adapter ingress capabilities. Active backlog moved from 89 to 87 in this closure audit; C2 is the next high-impact executable frontier because it unlocks C3/C4, G4, V3, and F4.

## Blockers

None at program level. Workstream dependencies and the WS-A runtime-spine freeze govern sequencing.
