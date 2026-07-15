Status: active
Created: 2026-07-11
Updated: 2026-07-14
Parent: .10x/tickets/2026-07-10-p3-ws-c-deterministic-parallelism.md
Depends-On: .10x/tickets/done/2026-07-11-p3-c1-scheduler-admission-contract.md, .10x/tickets/done/2026-07-11-p3-a5-streaming-operator-graph.md, .10x/tickets/done/2026-07-11-p3-a3-canonical-segmentation-adaptive-batching.md

# P3 C2: parallel partition/unit execution and canonical frontier

## Scope

Execute admitted partitions/format units concurrently through the streaming graph, implement bounded reorder/frontier, canonical limit/slicing/speculative discard, partition retry/reattest, file completion, and scope-safe state assembly.

## Acceptance criteria

- Random completion/delay/retry/jobs produce jobs=1-identical packages/evidence/state.
- Reorder memory remains bounded with a stalled first partition; later admission backpressures.
- Limits at every row/batch/unit/partition boundary select the exact jobs=1 prefix at the compiled limit node and never advance discarded positions.
- Retried identity/schema changes fail/replan; duplicate attempts cannot cross the frontier.
- File positions advance only after every selected row-group/block/member unit completes.

## Evidence expectations

Randomized/property scheduler harness, jobs goldens, limit/position matrix, retry/identity chaos, slow-frontier memory traces, cancellation/leak tests, and scaling profiles.

## Explicit exclusions

No DataFusion/Python-specific tuning or distributed work.

## Blockers

The lifecycle-bearing canonical stream frontier prerequisite is implemented for registered format units. Retry execution remains semantically blocked because `SourceExecutionCapabilities` declares granularity and typed retryable errors but no ratified attempt budget, deadline, or backoff authority. Inventing a one-off retry count in the engine would violate the scheduler spec's policy/capability split. A shared nested admission authority must also replace independent partition/unit ceilings before `--jobs` can be claimed as a global work ceiling.

## References

- `.10x/specs/deterministic-parallel-scheduler.md`

## Progress and notes

- 2026-07-12 shaping: `.10x/research/2026-07-12-c2-frontier-retry-architecture.md` recommends a scheduler-owned one-step `FuturesUnordered` frontier: each admitted ordinal owns exactly one open/next-batch poll or one ready batch, later ready ordinals are not re-polled, and canonical acknowledgement alone rearms/advances the window. This structurally bounds stalled-head retention by jobs/items/ordinal distance plus accounted payload bytes, keeps limited runs at lookahead one, and reuses C1 admission, A3 positions/segmentation, and A5 canonical persistence. The current opaque `BatchStream` still lacks awaitable cancel/join authority, so a neutral lifecycle-bearing opened-stream seam is a prerequisite. Retry remains blocked on three confirm-or-correct decisions covering budget/backoff, typed eligibility/reattest/duplicate authority, and source/operator precedence; exact candidates are in the research record.
- 2026-07-12: Began the next bounded C2 milestone after confirming the engine/runtime worktree is clean. Governing records establish canonical outcome release and exact limited-run authority, while the current source capability contract declares retryable error classes but no attempt budget/backoff policy. This execution will therefore add bounded production partition batch lookahead for unlimited runs, retain exact serial semantics for all limited runs, and add fail-closed retry identity/duplicate acceptance machinery without inventing retry timing or counts.
- 2026-07-12: Blocked-on-design after a bounded prefetch prototype exposed that the current `FuturesOrdered` open frontier does not provide a safe production ownership point for concurrent stream polling. The prototype wrapped each admitted `BatchStream` in an injected-host task with a one-item channel and retained serial execution for every limited run. A deterministic test stalled partition 0 until partition 1 was polled. The existing engine only polls the canonical open future before entering that partition's drain loop; later immediately-ready open futures therefore do not necessarily spawn their stream producers. A second prototype polled a whole admitted wave before consuming its head, but the focused test still did not complete and it would also make canonical progress depend on every later open in the wave, which is an unacceptable head-of-line regression. All source/test edits were reverted; only this journal remains.
- 2026-07-14: Implemented the missing lifecycle-bearing primitive as a generic runtime canonical stream frontier and used it below the registered format-session boundary. It lazily owns at most the admitted unit count, gives each active ordinal one pending poll or one retained result, does not re-poll later streams behind a stalled head, stops admission on a later error, and drops scoped child streams on terminal failure/cancellation. Decode-unit admission joins memory, CPU, I/O, source usefulness, and unit count; the one-unit path stays direct. A three-row-group production Parquet fixture and stalled-head/later-error tests pass. Release FineWeb improved 7.27 -> 5.37 seconds while the isolated source/package path remained about 943 MiB peak footprint. Evidence: `.10x/evidence/2026-07-14-p3-c2-b2-canonical-decode-unit-frontier.md`.
- 2026-07-14: This milestone deliberately does not claim C2 closure. The generic unit frontier is real, but partition opening and unit children do not yet consume one shared admission permit set; `--jobs` therefore remains a partition ceiling rather than a global nested-work ceiling. Retry/reattest, limits, file-unit completion authority, jobs goldens, and cancellation chaos remain open.

## Journal

- 2026-07-12 inspection: `git status --short crates/cdf-engine crates/cdf-runtime crates/cdf-project .10x/tickets/2026-07-11-p3-c2-parallel-frontier-execution.md` showed the assigned source boundary clean. Unrelated CLI, package, and DX files were already dirty and were not touched.
- 2026-07-12 focused observation: `cargo test -p cdf-engine operator_graph_compiles_from_capabilities_without_driver_name_dispatch --locked` passed for the initial non-gated prototype, but that invocation began before the deterministic gate was compiled and did not substantiate concurrent stream polling.
- 2026-07-12 adversarial observation: after compiling the stalled-first gate, the same focused command did not complete until interrupted. Forcing the test scheduler's effective jobs to two ruled out the single-CPU test environment as the cause. Polling the full admitted open wave also failed to yield a completing focused test and introduced an invalid dependency on later opens. Both attempts were interrupted and reverted.
- 2026-07-12 limit: no property, retry, or position claim is supported. Existing limited-run serial behavior was inspected but not changed or reverified after the prototype was reverted. No workspace-wide checks were run.

## Retrospective

- Concurrent open establishment is not a reusable concurrency boundary for stream outcomes: once the canonical open resolves, the current consumer stops polling the open frontier and drains that stream.
- A bounded channel is only a payload bound after every producer has a scheduler-owned lifecycle and the admitted producer count itself remains live and replenished; detached per-partition prefetch scopes do not supply canonical completion ownership.
- A stalled-first gate was the right falsification test: it distinguished real outcome concurrency from fast immediate mock opens and prevented a concurrency claim based only on package identity.
- The reusable frontier belongs below source/format identity and must own the child stream lifecycle, not merely the future that opens it. One pending poll per admitted ordinal is the structural bound; channel capacity alone is insufficient.
- Nested parallelism needs one shared admission authority. Per-frontier arithmetic can be locally safe while still violating a process-wide `--jobs` promise across multiple open partitions.

## Review

- 2026-07-14 decode-unit slice — adversarial self-review traced stalled-head behavior, later errors, lazy admission, stream drop/cancellation, memory retention, single-unit overhead, format/transport layering, and concurrent telemetry semantics. The first pass found that a later ready error could otherwise admit more streams before reaching canonical order; admission now stops as soon as any active poll reports an error, with a regression test proving later openers remain untouched. No significant or critical finding remains for the bounded unit slice. Verdict: **pass for this slice**. Residual risk is explicitly open: shared nested admission, CPU/I/O separation, retry, global limits, and jobs invariance.

- 2026-07-11: Landed the first production canonical frontier below partition scheduling: segment IPC encode/hash/durable publication executes concurrently through the injected host, while registration and downstream release remain submission-ordinal ordered. Inline/parallel package identities, source rechunking, staged final binding, failure cancellation/join, and zero residual memory are covered. This does not close C2 partition/unit concurrency, retry, global-limit, or file-completion criteria. Evidence: `.10x/evidence/2026-07-11-p3-a5e-parallel-segment-frontier.md`.
- 2026-07-11: Added the first production partition frontier milestone. Typed CLI sources resolve effective jobs once through `cdf-runtime`; project orchestration carries that nonidentity resolution into the engine; the engine opens source streams concurrently in canonical `FuturesOrdered` order without re-deriving source, destination, host, transport, lane, or memory policy. The window is bounded by the resolved effective jobs and partition count. Global-limit runs deliberately remain serial so discarded partitions cannot perform I/O or acquire attestation authority, and terminal schema quarantines never open payload streams. Jobs 1/4 produce identical manifest identity and lineage with zero residual managed memory. This milestone overlaps open/download latency only; transform concurrency, admission permits, retry/reattest, bounded outcome reordering, and atomic file-unit completion remain open under C2. Evidence: `.10x/evidence/2026-07-11-p3-c2-canonical-open-frontier.md`; review: `.10x/reviews/2026-07-11-p3-c2-canonical-open-frontier-review.md`.
- 2026-07-11: Exposed `cdf run --jobs N` as a strict nonzero user ceiling into the existing runtime scheduler resolution. Omission retains capability-driven auto; the value does not enter plan/package identity and cannot bypass source, CPU, memory, lane, or destination ceilings. Evidence/review: `.10x/evidence/2026-07-11-p3-c2-jobs-cli-ceiling.md`, `.10x/reviews/2026-07-11-p3-c2-jobs-cli-ceiling-review.md`.
- 2026-07-11: G2 removed the file-transport mutex and confined remote validate/spool/decode forwarding to injected per-partition I/O scopes, making C2's canonical open frontier effective for independent HTTP/object-store partitions instead of merely queueing serialized blocking calls. Local native streams receive no extra edge. C2 remains open for transform/outcome concurrency and retry/frontier completion. Evidence: `.10x/evidence/2026-07-11-p3-g2-concurrent-transport-spool.md`.
