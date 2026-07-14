Status: active
Created: 2026-07-13
Updated: 2026-07-13

# Fixed-schema discovery and stream admission

## Context

CDF must finalize an execution plan with one fixed output schema before package or destination mutation. It must also support large remote collections, evolving future files, and dynamic producers without downloading or invoking every partition once to discover its current schema and again to extract it.

The first single-crossing decision correctly rejected exhaustive pre-execution source observation on pinned runs, but it made the source-crossing rule too absolute for cold discovery. An unpinned resource cannot produce a final plan until a deterministic discovery budget has been observed and frozen. A bounded prefix may therefore be read during cold discovery and later appear again as the prefix of extraction; correctness does not require retaining every small probe. The architecture must instead prevent hidden full pre-scans, repeated full transfers, and discarded materialized spools.

The former discovery model also used one overloaded `exhaustive|sampled` axis. File coverage and within-file coverage are independent: Parquet can inspect metadata from every file without reading data pages, while JSON can sample bounded records from every file or fully scan only by reading all content.

## Decision

Every package-producing execution uses a final plan with one immutable output schema epoch. An unpinned cold command performs metadata inventory, deterministic discovery under explicit coverage, aggregation/reconciliation, and persistent or run-local schema freezing before final plan compilation. No package or destination mutation begins before that plan is frozen.

A pinned command loads the fixed schema and compiles a total stream-admission program without a current-file schema discovery pass. Each partition's physical observation is obtained during its extraction stream and selects only a precompiled outcome: admit, lossless coerce, residual capture, row/partition quarantine, or strict failure. Runtime observation never expands the typed schema epoch.

Discovery coverage has two independent recorded axes:

- file coverage: every candidate file, or a deterministic explicit `sample_files = N` subset using `stratified-hash-v1`;
- within-file coverage: format metadata, bounded content sample with byte/record limits, or full content.

Unqualified `exhaustive` is forbidden in new artifacts and diagnostics. Evidence uses `all_files|sampled_files` plus `format_metadata|bounded_content|full_content` and records matched, selected, unobserved, bytes, records, and identities. Defaults remain format-driven and explicit: Parquet/Arrow IPC use all-file format metadata; row formats use the configured bounded byte/record sample for every selected file; full-content discovery is opt-in.

Inventory is payload-free. Local inventory does not compute a whole-file hash; it records bounded filesystem identity and reattests it at open. Cryptographic content hashing occurs while extraction or an explicit discovery spool reads the content.

Discovery reuse has two stores with different authority:

- an observation cache stores bounded schema/footer/sample facts keyed by generation or checksum, format-driver version/options, normalizer version, and contract identity;
- a payload spool stores ledger-accounted content bytes or decoded batches under run/source/generation identity.

Observation-cache hits avoid schema I/O when identity is strong; misses never change semantics. Any full file or transformed payload materialized into a payload spool during a package-producing cold command MUST be handed to extraction under the same generation rather than downloaded/decompressed again. Small bounded metadata or content probes that were not materialized into the payload spool MAY be reread during extraction; their duplicate bytes remain bounded, measured, and outside claims of single-transfer payload reuse.

Cold auto-pin uses the discovery result directly to compile the final plan. It MUST NOT write the snapshot and then invoke ordinary pinned preparation, because that performs the discovery lifecycle twice. Explicit `cdf schema discover|pin` commands end after discovery/persistence; a later independent run may read the source payload normally while reusing cached observations.

Dynamic Python/Lua/WASM sources follow a bootstrap barrier. A declared schema handshake can freeze the plan without data. Otherwise the producer starts once, its first bounded batches are retained or spooled while the run-local schema is frozen, and the same invocation continues after final plan compilation.

## Alternatives considered

Pre-discover every current file on every pinned run.

- Rejected. It makes incremental execution proportional to observation plus extraction, overrides explicit sampling, and requires exact knowledge of physical schemas that the total admission program exists to classify.

Allow the output schema to evolve during extraction.

- Rejected. It creates same-package schema epochs, scheduling-dependent identity, and destination mutation before the final contract is known. Unknown fields remain residual until explicit rediscovery/promotion.

Forbid any duplicate source byte in one command.

- Rejected. It forces retained-buffer machinery for tiny bounded probes even when rereading that prefix is cheaper and equally correct. The meaningful invariant is no hidden full pre-scan, no repeated full transfer, and mandatory reuse of materialized payload spools.

Use one general key-value cache for observations and payloads.

- Rejected. Small structured observations and large governed byte payloads require different accounting, retention, cleanup, and access patterns.

Hash every local file during inventory to obtain strong identity.

- Rejected. It turns planning into a full payload scan. Local metadata identity remains explicitly weak until extraction hashes and reattests the file.

## Consequences

The compiler needs an explicit cold-discovery barrier followed by final plan compilation, plus a pinned path that skips current-schema discovery. Coverage configuration/evidence must migrate from one axis to two. The source/format boundary needs a stream-admission handoff and reusable spool/session handle. Existing tests that require a pinned baseline to disable `sample_files` and pre-open every file are invalid and must be replaced with transport-counted in-stream admission laws.

This decision supersedes `.10x/decisions/superseded/single-crossing-expensive-source-boundary.md`. The anti-convergence rules remain: discovery freezes one plan identity; runtime drift is verdict-bearing; all paths still produce the ordinary package, receipt, and gate evidence.
