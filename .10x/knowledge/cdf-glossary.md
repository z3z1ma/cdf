Status: active
Created: 2026-07-05
Updated: 2026-08-04

# cdf glossary

`cdf` is the CLI binary and crate prefix; CDF in prose is the project.

Resource: the smallest stateful extraction unit. In a D3 project its canonical identity comes from
`cdf/<namespace>/<resource>.cdf.sql`; it declares schema, keys, cursor, state scope,
disposition, contract, trust, and capabilities, and produces Arrow record batches. Its namespace,
configured source, and logical target are independent.

Configured source: one project-named upstream instance in `[sources.<name>]`. It owns shared typed
connection, secret-reference, policy, egress, quota, and driver configuration. It is selected
explicitly by `upstream(source => '<name>', ...)` and is not the unit of runtime state.

Source type: the immutable connector kind on a configured source, such as `postgres`, `files`, or
`mongodb`, used to select an internal driver.

Source driver: the internal Rust implementation selected by source type. Its registry is process
composition authority, not a project namespace or configured-source catalog.

Upstream relation: the driver-owned table, collection, REST path, catalog table, file selector, or
equivalent object selected by the remaining typed `upstream(...)` arguments.

Resource namespace: the first path component below `cdf/`. It organizes canonical CDF
resource identity and never infers a configured source.

Logical target: the destination-side object name declared by `TARGET` or defaulted to the resource
id. The selected environment independently owns the physical destination connection.

Batch: Arrow payload plus resource, partition, schema hash, rows, bytes, source position, watermarks, stats, and optional CDC operation information.

Scan plan: a negotiated read plan containing projection, classified filters, limits, partitioning, ordering, estimates, and pushdown fidelity.

Contract: a policy compiled into a validation program with a total verdict lattice.

Package: hash-addressed evidence of one attempted state transition. Package data is canonical Arrow IPC; stats, quarantine, and lineage are Parquet; manifests and receipts are canonical JSON. A package contains either ordinary rows or finalized keyed effects, never an optional mixture of both.

Keyed effect: one final `upsert` or `delete` for an exact declared key. Upserts carry one complete output row; deletes carry only the mechanically derived key tuple. Package construction selects at most one effect per key before finalization.

Delete capture: the source-side decision and coverage evidence describing whether native deletion facts are unsupported, optionally observed, or inherent in the selected stream.

Delete application: the explicit destination-side `ignore`, `hard`, or Boolean-marker `soft` behavior applied to captured package deletes. Capture and application are independent authorities.

Receipt: a destination's durable, independently verifiable acknowledgment that a package or segment set was committed.

Checkpoint: a typed, append-only state transition committed only after receipt verification.

Commit gate: the commit boundary enforced by `CheckpointStore::commit`; a source cursor may advance only after all data represented by the cursor is durably committed and the destination receipt is recorded.

Scope: a sub-resource state key such as a partition, window, file, stream, schema-contract, or destination-load scope.

Sheet: a declared and lockfile-snapshotted capability table for a resource or destination.

Trust level: planner preset expanding operator intent into contract, validation, promotion/demotion, and retention policy. Values are `experimental`, `governed`, `financial`, and `serving`.

Disposition: destination write semantics. MVP dispositions are `append`, `replace`, and `merge`; `cdc_apply` arrives with log CDC.

Ice: committed state in the checkpoint ledger.

Snowfall: raw extraction batches before validation and packaging.

CDF: a load package: compacted evidence that can still melt if it never commits.

Compiled expression plan: the parsed, resolved, optimized, version-bound expression program
recorded in the plan/package. Execution and replay consume it exactly; they never reparse or
reoptimize source expressions.

Pinned schema: the hash-addressed fixed effective schema used to compile one execution plan.
“Pinned” may mean project-persisted or run-local; either form is immutable during that run.

Observed schema: physical schema facts learned from source metadata or streamed payload. It
constrains admission against the pin and never silently replaces the effective schema.

Discovery coverage: the explicit pair of candidate-file coverage and within-file
metadata/byte/record coverage used to select a schema before the final plan.

Prepared source session: run-owned source state carrying resolved generation, reusable discovery
observations or payload, transport/decoder handles, leases, I/O metrics, and terminal cleanup
through execution.

Admission program: the compiled total classification of observed physical data into exact,
widened, coerced, residual-captured, quarantined, or failed outcomes.

Task authority: a content-addressed, bounded source-owned description of partition work. Generic
runtime schedules it without interpreting catalog, file-format, query, or provider semantics.

Destination ingress: the closed execution category exposed by a destination runtime:
`FinalizedPackage` or `StagedSegments`. Generic orchestration branches on this capability, never
on a destination name.

Staging lease: the generic externally durable staging liveness authority. It proves ownership,
renews while an attempt is live, releases on completion, and permits cleanup only after provable
expiry. It is not a Parquet heartbeat.

Runtime-resolved blocking lane: a portable plan lane whose executable concurrency is tightened
against the actual embedded runtime and cgroup/host authority at attachment. The canonical
bindings are `Static`, `RuntimeResolvedRequired`, and `RuntimeResolved`; “host-bound” is
superseded vocabulary.

Memory lease: active ownership of resident bytes against the run ledger. A planning reservation or
compiled demand estimate is not a live lease, and transient free memory is not a planning
authority.

Stage capacity: local queue, worker, retained-object, or writer capacity. It produces
backpressure at that stage and must not become the run-wide job ceiling.

Package row ordinal: the internal non-null `UInt64` field `_cdf_package_row_ord`, dense and
zero-based after every row-selecting operation. Destinations use it to derive transaction-owned
row keys or manifest-bound physical provenance.

Planned bytes: a typed estimate of logical work selected by the compiler. Transferred bytes:
physical transport I/O observed by runtime metrics. Package bytes and destination bytes are
separate facts; none may be substituted for another.

Current-only compatibility: CDF preserves current external protocol interoperability and all
correctness fences, but no runtime compatibility with obsolete pre-production CDF artifacts,
internal APIs, or execution paths.
