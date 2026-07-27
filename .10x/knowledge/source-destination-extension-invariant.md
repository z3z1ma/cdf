Status: active
Created: 2026-07-10
Updated: 2026-07-26

# Source and destination extension invariant

CDF program work must optimize for the cost and correctness of adding one source or one destination, not for closing one ticket through a local shortcut. This is a P0 architectural closure invariant for P2, P1, and subsequent work.

A source- or destination-specific implementation is valid only at its adapter/driver boundary. Shared semantics belong in kernel contracts, compiler/runtime traits, plans, packages, receipts, checkpoints, capability sheets, and conformance laws. The generic orchestration path must consume those abstractions without branching on concrete source formats or destination names.

Every non-trivial integration review must ask:

- What files and crates would change to add one new source with the same capabilities?
- What files and crates would change to add one new destination with the same capabilities?
- Did the change add a concrete-source or concrete-destination branch to generic orchestration?
- Did behavior that should be falsifiable through a capability sheet or trait become an ad-hoc helper or free function?
- Are lifecycle, evidence, replay, and commit semantics expressed once through kernel/runtime contracts?
- Can conformance falsify a new adapter's claims without editing the conformance engine for that adapter?
- Did a ticket-specific convenience leak filesystem, CLI, executor, parser, or driver types below its proper boundary?

Repeated edits across generic orchestration for each driver, copied source pipelines, source-specific command branches, parallel receipt/checkpoint logic, and one-off helpers in shared crates are stop-line findings. They require repair in the owning ticket when in scope or a bounded P0 owner before the parent program may close. Passing focused tests does not waive this invariant.

Format-specific parsing and destination-specific physical commit code are expected inside adapters. The smell is not specificity itself; it is specificity controlling shared semantics outside the adapter boundary.

Wrappers are part of the extension boundary. When a source or destination trait gains a semantic
override—not merely an accessor—every registry, validation, scope, policy, and instrumentation
wrapper must deliberately forward it. Relying on the trait default can silently erase an
adapter's optimized or correctness-bearing behavior while still compiling.

Dynamic sources declare schema acquisition through the neutral producer descriptor. A metadata
handshake performs no row-producing invocation. A stream bootstrap starts the real invocation
once, retains its first accounted batches through the compiler's schema-freeze barrier, and
continues that exact stream during execution. Project/compiler orchestration may retain the opaque
prepared payload but must not branch on the language runtime.

Shared implementation archetypes are also part of the invariant. Once two adapters duplicate an
identity-, memory-, retry-, receipt-, mirror-, or task-lifecycle implementation, the next adapter
must not copy it again. Extract the proven lower mechanism while keeping source/destination
semantics in the adapter. A universal trait that merely mirrors `SourceDriver` or
`DestinationRuntime` is not a valid extraction.

First-party enrollment has one explicit construction leaf: `cdf-builtin-drivers`. Product,
benchmark, conformance, and integration-test owners consume or extend that catalog instead of
recreating shipped driver lists. The catalog leaf never flows downward into neutral production
crates. Permanent graph gates use all-feature Cargo metadata package identities and dependency
kinds—not manifest key spelling—so aliases, optional edges, target-specific edges, and build
dependencies cannot evade the boundary. Data-driven catalog fixtures hash complete descriptors,
source option schemas, and destination inspection artifacts; their inspection rows come from the
same construction table that installs destinations, so a test cannot silently omit a newly
enrolled driver.

Driver concurrency follows `.10x/decisions/driver-session-concurrency-canon.md`: drivers are
thread-safe factories, resolved runtimes are run-owned, finalized sessions may borrow
thread-affine state, staged sessions are movable and bounded, and all actual concurrency comes
from injected host/stage authorities. Adapter authors must never acquire `Send + Sync` through a
mutex wrapper solely to satisfy an imagined universal runtime.

Make that law executable in both directions. Positive compile assertions cover driver, staged
session, and host trait objects; deliberately non-`Send` synthetic runtimes and finalized
sessions (for example an `Rc`-backed thread-affinity marker) make accidental blanket bounds fail
to compile. Merely omitting a `Send` assertion proves nothing. Native handles may be confined by
the exclusively owned ingress protocol or a declared blocking lane; an injected `Send + Sync`
host does not make the handle movable.

Shared external-task readers own an entire retained decoded payload as one `Arc`-shared unit:
typed model, canonical identity, encoded bytes, and parse lease. Sharing only the lease while
deep-cloning the decoded model makes the memory ledger false. The reader may centralize
authority/task decoding and identity verification, but each adapter retains its position,
authorization, retry, schema-observation, and partition-plan semantics through a typed codec
boundary.

Parse-memory policy is part of adapter behavior, not an implementation detail of the byte
formula. A shared extraction must preserve the adapter's memory class, admission mode
(fail-fast or blocking), and cancellation boundary explicitly. Changing Iceberg discovery
parsing from fail-fast discovery admission to blocking control admission, for example, would be
a semantic and liveness regression even if the reservation size were identical.

Shared external-task planning admits its complete lifetime overlap once. A spill-backed index
must not hold scratch memory and then block while reacquiring writer memory from the same finite
coordinator; reserve and partition the combined authority before accepting work. Exact duplicate
and conflict checks precede fresh spill admission so idempotence remains available even at the
disk ceiling. Publication checks cancellation at the atomic-install boundary, and every error
path releases the scratch directory, spill reservation, and memory partitions.

Journal-free SQLite scratch is valid only with a fail-before-mutation capacity proof. The current
canonical builder bounds one insertion from the bundled SQLite B-tree maximum depth, maximum net
split pages per level, and record overflow pages, admits that disk through the shared spill
coordinator, and poisons rather than retries on unexpected `SQLITE_FULL` or insertion failure.
Any SQLite upgrade must revalidate those mirrored structural constants. A retry after
`SQLITE_FULL` with journaling disabled is forbidden because the failed statement may already have
discarded accepted rows.

One source-owned canonical task encoder is the byte and content-identity authority. Planning
hashes exactly the bounded bytes that encoder emits; reading decodes/validates the model, re-encodes
through the same codec, and compares the resulting content digest before execution. Separate
planning hash callbacks can drift and publish an artifact their paired reader rejects; hashing or
serializing the same production task twice merely to reconcile duplicate authorities is also a
performance defect.

Source compilation and driver-owned portable-plan validation are contact-free. Health and
discovery remain explicit bounded, contact-capable stages, while row execution and retry happen
after resolution. Portability conformance must traverse the worker admission path that
reconstructs authority and calls the owning driver; a direct registry-helper unit test cannot
prove an isolated host did not bypass that validation. A serializable plan whose driver retains
the default validator must fail before isolated execution.

Module boundaries must be compiler-visible. Splitting a monolith into sibling files while the
parent glob-imports every sibling and each child glob-imports the parent preserves the monolith's
dependency graph and is not modularization. Extract shared value/authority models as leaf-owned
types, keep orchestration in an explicit upper composition layer, orient production imports into
an acyclic graph, and make public facades enumerate their exports. Line counts and file counts are
diagnostics only; an adversarial review must be able to trace every cross-module edge from an
explicit import. White-box test modules may aggregate internal surfaces, but that convenience
must not leak into production ownership.

The operational consequences are expanded in:

- `.10x/knowledge/runtime-performance-authorities.md`
- `.10x/knowledge/remote-discovery-and-io-lifecycle.md`
- `.10x/knowledge/pre-production-current-only-policy.md`
- `.10x/knowledge/product-integration-and-closure-gate.md`
