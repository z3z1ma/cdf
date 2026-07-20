Status: active
Created: 2026-07-20
Updated: 2026-07-20

# Partition-separable isolated segment canonicalization

## Context

CDF's canonical `_cdf_package_row_ord` is a dense package-global ordinal assigned after every
row-selecting operation. An isolated partition worker cannot know its package prefix until earlier
canonical partitions have completed. The two-barrier prepared-segment protocol solves that prefix
problem without rereading a source or moving row payloads through control messages.

That protocol does not make a package-global operator partition-separable. A limit, package-global
deduplication, or multi-partition drain frontier can select rows based on other partitions. Running
such an operator independently inside each partition task changes semantics before the ordinal
barrier is reached. The prior decision incorrectly described every row-selecting operation as part
of partition preparation without distinguishing operators whose state is package-global.

P3 WS-C concerns deterministic parallel execution of safe partitions and explicitly excludes a
distributed scheduler. The portable partition capsule must preserve that boundary rather than
pretend a whole-transfer graph is a partition.

## Decision

The two-barrier isolated protocol applies only after the compiler proves that the task's
row-selecting operator chain is partition-separable, or when one bounded package/drain epoch has
exactly one selected partition.

1. A partition worker executes the complete partition-separable chain once and publishes fenced,
   ledger-accounted prepared rows plus source and evidence artifacts.
2. The coordinator admits preparations in canonical partition order, computes dense checked row
   prefixes, and issues source-free canonical-segment finalization tasks. Finalization may run in
   parallel and the coordinator alone assembles package identity and advances commit authority.

For a multi-partition plan containing a package-global selector or drain frontier, the partition
task compiler MUST fail before writing compiler artifacts or contacting the source. P3 continues to
execute that graph through the ordinary coordinator-owned whole-transfer runtime. A later
distributed implementation may add a distinct typed global-operator or epoch task, but it MUST bind
the recorded compiled plan, bounded state/spill authority, canonical input frontier, output
artifacts, and fencing explicitly. It MUST NOT overload `PortablePartitionTask` or interpret a
package-global operator as partition-local.

Direct local execution remains fused and creates no prepared artifact. Finite streaming epochs
reset the dense ordinal at zero; the current isolated law covers a finite one-partition epoch.
Multi-partition epoch distribution remains part of the later distributed-execution authority, not
an implicit behavior of the P3 partition capsule.

## Alternatives considered

- **Treat every operator as partition-local and merge later.** Rejected because limit, deduplication,
  and frontier selection are observably different after independent execution.
- **Call the full plan one partition task.** Rejected because it falsifies partition identity,
  retry scope, source-position authority, and scheduler accounting.
- **Centralize all isolated preparation in the coordinator.** Rejected because it removes the
  distribution seam and duplicates the direct runtime without solving portable execution.
- **Invent a global task inside C5.** Rejected because P3 excludes the remote/distributed scheduler
  and C5 needs no speculative task kind to prove safe-partition equivalence. The existing
  distributed-execution ticket and J5 plan-marshaling work own that later whole-plan boundary.
- **Sparse per-partition ordinals or pre-count/reread.** Rejected for the provenance and performance
  reasons recorded in the superseded decision.

## Consequences

The portable protocol is total over its declared topology: supported safe partitions are
byte-identical at jobs 1/N, while nonseparable graphs fail before side effects rather than silently
changing meaning. Source and destination extension boundaries remain registry-driven. C5 may close
after proving the supported topology, explicit pre-I/O rejection, bounded control/data separation,
and the full tamper/fence/capability admission matrix.

Future whole-plan distribution has an honest owner in
`.10x/tickets/2026-07-05-distributed-execution-and-remote-state.md`, with DataFusion plan translation
work in `.10x/tickets/2026-07-12-p3-j5-execution-plan-marshaling-metrics.md`. Either may introduce a
new global task only through a separately ratified contract; neither may weaken this partition
capsule or move identity-bearing bytes into DataFusion.
