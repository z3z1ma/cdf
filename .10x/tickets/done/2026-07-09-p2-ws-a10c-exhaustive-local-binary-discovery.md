Status: done
Created: 2026-07-09
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-09-p2-ws-a10-multi-file-schema-discovery-pin.md
Depends-On: .10x/tickets/done/2026-07-09-p2-ws-a10a-discovery-manifest-artifact-budget.md, .10x/tickets/done/2026-07-09-p2-ws-a10b-aggregate-schema-join-core.md, .10x/tickets/done/2026-07-09-p2-ws-a9-local-arrow-ipc-discover-run.md, .10x/tickets/done/2026-07-09-p2-ws-d5-binary-format-autodetection.md

# P2 WS-A10c exhaustive local binary discovery and pin lifecycle

## Scope

Replace the Parquet/Arrow single-candidate discovery gates with the generic discovery-set orchestrator for local files. Enumerate deterministically, probe every binary metadata block under the resolved per-executor budget, aggregate with A10b, persist A10a's sidecar, and expose the result through discover/pin/diff/no-pin/auto-pin.

## Acceptance criteria

- Multi-file local Parquet and Arrow IPC globs discover, pin, diff, and first-use auto-pin without narrowing to one file.
- Every matched candidate is probed; no result contains `unprobed` or sampled membership.
- Probe scheduling never exceeds the resolved 64 MiB per-file, 128 MiB in-flight, or 8-probe defaults; executor options can override and resolved values are recorded.
- Budget exhaustion fails before snapshot/lock writes and names the measured/allowed bytes and override/remediation path.
- First pin writes nothing if any initial file is malformed or schema-incompatible and reports every candidate verdict.
- Compatible widening, new/missing fields, nested schemas, metadata variance, deterministic order, no-change identity, add/remove/change manifest diff, and normalizer collisions are covered.
- The cardinality-one compatibility case produces the same artifact/evidence shape as a one-entry discovery set.
- Discovery does not compute runtime full-file SHA or read row/data pages; measured probe evidence proves the bound.
- Existing v1 pins hydrate unchanged. Ordinary commands verify/hydrate baseline authority before current-file observation, never rewrite it, and may reuse unchanged manifest probe evidence; non-file pinned resources retain existing no-probe behavior.

## Evidence expectations

Multi-file Parquet/IPC fixtures, measured-byte tests, budget/concurrency instrumentation, snapshot/manifest golden bytes, CLI no-write failures, legacy compatibility, full affected tests, and adversarial review for hidden full reads/single-file branches.

## Explicit exclusions

No mixed-schema package execution, nullable array materialization, effective-schema package stamping, file quarantine, remote Arrow, cloud transport, HTTP enumeration, text sampling, or preview traversal changes.

## Progress and notes

- 2026-07-09: Opened as the first I/O integration child after A10a/A10b.
- 2026-07-10: Implemented one exhaustive local-binary discovery orchestrator over adapter-owned Parquet footer and Arrow IPC schema probes. Deterministic candidate enumeration, bounded per-file metadata reads, checked aggregate byte accounting, A10b aggregate joins, post-join namecase normalization, and A10a manifest construction are shared across both formats; generic orchestration contains no source-format parsing branches.
- 2026-07-10: Integrated manifest-linked v2 snapshots through schema discover/pin/diff, first-use no-pin/auto-pin, add, and run preparation. Sidecars publish before snapshots; unchanged refreshes compare discovery observations while retaining the existing content-addressed snapshot; changed refresh manifests bind the exact verified prior lock snapshot hash. `effective_schema_hash` is explicitly the schema-only identity because binding the final manifest-linked v2 hash in its own manifest would be circular.
- 2026-07-10: Preserved the runtime exact `FileManifest` path and SHA identities independently from bounded discovery identities. The deprecated cardinality-one Parquet compatibility helper now refuses multi-file input without producing partial evidence; product paths use the exhaustive artifact API.
- 2026-07-10: Added measured Parquet/Arrow metadata-budget tests; exhaustive widening/missing-field/metadata-variance/set-identity tests; malformed, incompatible, budget, and normalizer-collision no-write tests; exact-baseline/schema-only-effective-hash regression; legacy no-partial-evidence regression; multi-file CLI lifecycle/no-pin/auto-pin stability; and run-time exact-manifest preservation coverage. Focused project and CLI lifecycle gates pass.
- 2026-07-10: Verification: `cdf-formats` 33/33, `cdf-declarative` 87/87, and `cdf-project` 132/132 pass; the focused A10c CLI lifecycle/run regressions pass; strict `clippy -D warnings` passes for all four affected crates. The full CLI suite reached 241/242; the sole failure is outside A10c in the concurrently changed RP4 SQLite migration expectation (test expects schema version 2 while the lease migration now produces 3).
- 2026-07-10: Hardened the new `SchemaDiscoveryExecutionOptions` extension boundary before publication: it is `#[non_exhaustive]`, its fields are private, and callers use `new`/`default`, budget and verified-baseline builders, and read-only accessors. Baseline hashes can no longer be supplied arbitrarily; `SchemaSnapshotStore::read_with_verified_baseline` first hydrates and verifies the locked snapshot plus linked discovery manifest, then returns an opaque, resource-bound authority token accepted by discovery options. Cross-resource token use fails before source observation. Schema and plan/no-pin CLI paths now use that verified authority. Scoped check, focused lifecycle/missing-artifact tests, `cdf-project` 132/132, and strict project/CLI clippy pass. `cargo semver-checks` against `HEAD` completed 195/196 checks; its sole failure is the unrelated concurrently changed `LockedDestination` non-exhaustive transition, while the A10c options type is net-new relative to the baseline.
- 2026-07-10: Parent integration verification and P0 extension-cost review passed. Evidence: `.10x/evidence/2026-07-10-p2-a10c-rp3-rp4-integration.md`. Review: `.10x/reviews/2026-07-10-p2-a10c-rp3-rp4-integration-review.md`.

## Blockers

None. A10a and A10b are complete.
