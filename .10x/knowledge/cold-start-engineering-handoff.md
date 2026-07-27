Status: active
Created: 2026-07-26
Updated: 2026-07-26

# Cold-start engineering handoff

## Purpose

This is the entry point for an engineer inheriting CDF without conversational context. It indexes
current authority, names the operational traps that are easy to rediscover incorrectly, and gives
the first safe sequence of work. It is not a substitute for the referenced decisions,
specifications, tickets, source, or evidence.

## Authority order

When records disagree, use this order:

1. The user's current explicit direction.
2. Accepted, non-superseded decisions and active specifications.
3. Active knowledge records and the current executable ticket.
4. Current source, tests, workflows, generated schemas, and tool behavior.
5. Current dated evidence, within its recorded host/workload/revision limits.
6. Terminal ticket journals and cancelled tickets as historical evidence only.
7. Chat recollection, comments, examples, and old generated documents.

Terminal tickets often contain accurate history and obsolete conclusions in the same journal.
Never promote a terminal journal sentence over a newer active decision, workflow, or source
inspection. A cancelled ticket preserves an idea or falsification; it is not executable work.

## Knowledge catalog

Every active knowledge record was reviewed during the 2026-07-26 handoff audit. Use this catalog
instead of guessing from filenames:

| Record | Use it for |
|---|---|
| `active-backlog-and-future-roadmap.md` | Active program and deliberate activation triggers for parked ambitions |
| `cdf-glossary.md` | Canonical project/runtime vocabulary |
| `cdf-product-objective.md` | Product objective and differentiating guarantees |
| `cold-start-engineering-handoff.md` | This current-authority entry point |
| `content-addressed-sidecar-publication.md` | Safe sidecar publication and content-addressed ownership |
| `dependency-tuple-migration-guard.md` | Coordinated dependency-version tuple changes |
| `developer-build-duckdb-linkage.md` | Dynamic developer versus static release DuckDB boundary |
| `fenced-lease-lock-publication.md` | Cross-process fenced lease and lock publication |
| `historical-gitleaks-findings.md` | Exact historical scanner false-positive fingerprints only |
| `incremental-commit-discipline.md` | Coherent commit cadence and shared-tree ownership |
| `performance-evidence-and-regression-triage.md` | Comparable measurements and first-bad isolation |
| `pre-production-current-only-policy.md` | Delete obsolete CDF paths while preserving external compatibility |
| `product-integration-and-closure-gate.md` | Product smoke, representative fixtures, and ticket closure |
| `quality-gate-execution.md` | Proportionate `QUALITY.md` execution and tool pitfalls |
| `remote-discovery-and-io-lifecycle.md` | Fixed-schema discovery, payload reuse, ranges/spools, and I/O identity |
| `runtime-conformance-throughput-rule.md` | Conformance ownership and measured-default rule |
| `runtime-performance-authorities.md` | CPU, memory, stage pressure, spill, segmentation, and provenance |
| `rust-crate-organization.md` | File/module organization for Rust crates |
| `schema-coercion-evidence-provenance.md` | Physical versus effective schema and coercion evidence |
| `source-destination-extension-invariant.md` | No source/destination identity leakage into generic orchestration |
| `type-policy-authority.md` | Closed Arrow vocabulary and governed mapping authority |
| `vision-coverage-matrix.md` | VISION implementation/verification coverage |

Canonical executable runbooks:

| Skill | Trigger |
|---|---|
| `run-cdf-ec2-benchmarks` | Provision, reuse, measure, fetch, or tear down the governed benchmark host |
| `build-and-install-cdf` | Build locally, refresh a binary, or validate static release packaging |
| `investigate-cdf-performance-regressions` | Throughput, CPU, memory, I/O, build, or latency unexpectedly worsens |

Skills live canonically under `.10x/skills/` and are mirrored under `.claude/skills/`. Change the
canonical copy first; the mirror has no independent procedure.

## Current program state

The stabilization and CPU-saturation programs are terminal. The active program is:

- `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`

Its bounded children are the entire active graph. The intended sequence starts with the composition
root and compiler-enforced walls, then catalog-task source commons, destination common services,
typed CLI reports/error taxonomy, holistic CLI work, and a final extension-authoring proof.

Before taking a ticket:

```bash
git status --short
find .10x/tickets -maxdepth 1 -type f -print | sort
sed -n '1,260p' .10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md
```

Read the child completely and every referenced record before editing. Other workers may share the
checkout. A dirty worktree is not authority to discard, reset, reformat, or absorb unrelated work.

The longer product roadmap is deliberately parked, not forgotten:

- `.10x/knowledge/active-backlog-and-future-roadmap.md`
- `VISION.md`
- `.10x/knowledge/vision-coverage-matrix.md`

Do not reopen a historical monolith merely because the product vision remains ambitious. Activate
the smallest current program when a real consumer or ratified priority makes it valuable.

## The system in one page

CDF compiles declarative sources, contracts, transformations, destination capabilities, state
positions, and evidence requirements into an identity-bearing plan. Execution produces canonical
Arrow IPC segments, a package manifest, verdict/evidence artifacts, a destination receipt, and a
checkpoint committed only after receipt verification.

The non-negotiable shape is:

```text
project + lock + pinned schema + source observations
→ compiler
→ immutable plan and portable partition tasks
→ bounded source/decode/validate/segment pipeline
→ canonical package
→ capability-selected destination ingress
→ verified receipt
→ checkpoint/commit gate
```

Convenience front ends, source drivers, destination adapters, foreign runtimes, distributed
schedulers, and query engines may plug into this calculus. They may not bypass package identity,
verdicts, receipts, or the commit gate.

Core invariants:

- A final plan always has one fixed effective schema.
- Discovery may choose that schema before the final plan; execution never mutates it in flight.
- Observed physical schemas constrain admission and produce recorded coercion/quarantine verdicts.
- Source and destination identities never leak into generic orchestration branches.
- Canonical package bytes and identities are produced by CDF's deterministic native path.
- DataFusion may analyze, prune, query, and schedule; it does not produce identity-bearing bytes.
- Drivers are thread-safe factories; run-local runtimes own mutable execution state exclusively.
- The run has one CPU authority and one memory authority; stage capacities are local pressure
  signals, not global concurrency ceilings.
- Actual physical I/O comes from runtime metrics. Planned logical bytes and selected-task bytes are
  estimates, never relabelled as transferred bytes.
- Current external protocol compatibility matters. Compatibility with old CDF artifacts, internal
  APIs, and superseded execution paths does not.

Read these focused records before changing the spine:

- `.10x/knowledge/source-destination-extension-invariant.md`
- `.10x/knowledge/runtime-performance-authorities.md`
- `.10x/knowledge/remote-discovery-and-io-lifecycle.md`
- `.10x/knowledge/pre-production-current-only-policy.md`
- `.10x/knowledge/product-integration-and-closure-gate.md`
- `.10x/knowledge/schema-coercion-evidence-provenance.md`
- `.10x/knowledge/type-policy-authority.md`

## Current performance floors, not universal promises

The strongest current one-TiB Parquet evidence is:

- `.10x/evidence/2026-07-26-parquet-parallel-one-tib-rerun.md`

On the recorded `c7i.4xlarge` host, the exact 1.0086-TiB synthetic acceptance completed in
499.07 seconds at 2.222 GB/s logical throughput, 10.892 million rows/s, 678% average CPU, and
3.923 GB peak RSS under the default 4-GiB managed budget. This was a 5.40x improvement over the
prior 411.5-MB/s result, with identical workload identity and verified receipt/checkpoint/package
facts.

The generated `docs/performance-envelope.md` was refreshed from its reconciliation manifest on
2026-07-26 and reports this result with its repeated-content/hard-link and physical-I/O limits.
Future updates must change the manifest and regenerate the document together.

The controlled full-year TLC-to-DuckDB path and the live FineWeb HTTP path have useful evidence,
but they are workload-specific floors, not proof that every schema, transport, or destination
performs similarly. Wide DuckDB tables around two thousand columns remain an acknowledged
destination pathology; ordinary schemas have demonstrated multi-million-row/s ingress.

Use:

- `.10x/skills/investigate-cdf-performance-regressions/SKILL.md`
- `.10x/knowledge/performance-evidence-and-regression-triage.md`
- `.10x/specs/performance-lab-and-envelope.md`

Do not promote a performance default from intuition or laptop timing.

## EC2 benchmark host

The supported interface is:

- `tools/p3-ec2-benchmark-host.sh`
- `.10x/skills/run-cdf-ec2-benchmarks/SKILL.md`

The helper owns provisioning, SSH preparation, preflight, synchronization, optimized build,
measurement, evidence fetch, and teardown. Do not recreate this protocol with ad-hoc AWS/SSH
commands.

At this handoff there is no active benchmark state file under the tool's target state directory,
and a 2026-07-26 read-only `us-west-2` query for pending/running
`Project=CDF,Purpose=P3Benchmark` instances returned none. Reusable-resource metadata may remain,
but it is not proof that an instance is live or owned. Always rerun the helper's `status` and AWS
tag query before a future tranche; never infer “already torn down” from a missing local state file
alone.

The known host class is a `c7i.4xlarge` with 16 logical CPUs, 250-GiB gp3, 16,000 IOPS, and
1,000 MiB/s configured throughput. A changed instance type, volume, cgroup memory limit, CPU
governor, dependency tuple, or timed region defines a different benchmark cell.

## DuckDB build and linkage boundary

Read:

- `.10x/knowledge/developer-build-duckdb-linkage.md`
- `.10x/skills/build-and-install-cdf/SKILL.md`

Routine local and EC2 developer builds set `DUCKDB_DOWNLOAD_LIB=1`. That downloads and dynamically
links the exact library version encoded by `libduckdb-sys`, avoiding the dominant source-build
cost. The resulting binary is not portable by itself.

Published release artifacts deliberately do not use this environment variable. They enable
`bundled-duckdb`, compile the full static library, inspect native dependencies, and fail if a
dynamic DuckDB dependency remains. This cost is intentional at the release boundary.

The repository Cargo config does not set `DUCKDB_DOWNLOAD_LIB`; callers must set it explicitly.
The downloaded archive is not checksum-verified by `libduckdb-sys`, so it is a developer/benchmark
acceleration, not the release supply-chain path. Do not copy only
`target/release/cdf` into a global bin directory and assume it carries its downloaded library.

## Remote discovery and execution lifecycle

Cold discovery and pinned execution are different:

```text
cold
metadata inventory
→ explicit file/content coverage
→ bounded discovery
→ aggregate observations
→ freeze schema
→ compile final plan
→ execute without a second schema pre-scan

pinned
metadata inventory
→ load fixed schema
→ compile admission/coercion/quarantine program
→ stream each selected partition once
→ reconcile physical observations in that stream
```

Never restore the old behavior that pre-opened every file on every pinned run. File coverage and
within-file record/byte coverage are independent dimensions. Parquet footer coverage may be
metadata-exhaustive without reading data pages; JSON/CSV completeness requires content scanning.

Range reads are correct for bounded discovery and selective, strongly versioned scans. They are
pathological as the unconditional full-scan strategy. Finite high-coverage remote Parquet uses
sequential streaming into an accounted spool with overlap; sequential row formats decode directly
under bounded backpressure. Unbounded input must never create an unbounded spool.

Read `.10x/knowledge/remote-discovery-and-io-lifecycle.md` before changing this area.

## Package, provenance, and destination ingress

The logical package-row provenance field is `_cdf_package_row_ord`. It is materialized after
filtering/dedup/quarantine and before canonical segment write. It resets per finite package, which
also makes it compatible with indefinitely running inputs that rotate finite packages.

Destination-wide `_cdf_row_key` remains destination allocated because it depends on transactional
state. Destinations derive it from their allocated range and package ordinal. Segment/package
metadata maps rows back to package and segment identity. Do not reintroduce destination-specific
row enumeration or depend on asynchronous segment completion order.

Destination ingress is capability-shaped:

```rust
DestinationIngress::FinalizedPackage(...)
DestinationIngress::StagedSegments(...)
```

Generic orchestration branches on this closed capability, never on DuckDB/Postgres/Parquet names.
Staged durable objects use the generic staging lease authority; no destination-specific heartbeat
thread owns liveness.

## Correctness and closure

Focused tests establish only their assertions. Core lifecycle work requires a product smoke that
crosses the actual compiler → package → destination → receipt → checkpoint path. The minimum
stabilization matrix is maintained in
`.10x/knowledge/product-integration-and-closure-gate.md`.

Run `QUALITY.md` proportionately. Do not serialize every expensive check by habit and do not run
heavy laptop workloads that can exhaust the machine. The primary agent should own workspace-wide
Cargo checks when parallel workers exist; individual workers should run focused checks.

Ticket state is part of product state. Journal evidence as it happens, record limitations, repair
references when moving records, and close or cancel tickets only when their terminal rationale is
true. Open tickets are executable owners, not reminders.

## First 30 minutes for a cold agent

1. Inspect the worktree and current branch without mutating it.
2. Read this handoff, the active program parent, the first selected child, and all its references.
3. Read `QUALITY.md`, `VISION.md` sections named by the ticket, and relevant graph/source authority.
4. Run the smallest read-only repository queries needed to trace the current composition.
5. Check whether another worker owns overlapping files.
6. Make the bounded ticket change, journal as evidence appears, and run focused verification.
7. Review the diff adversarially against architecture and performance invariants.
8. Update the ticket, graph, generated artifacts if owned, then commit a coherent tranche.

Useful initial commands:

```bash
git status --short
git log -12 --oneline
find .10x/tickets -maxdepth 1 -type f -print | sort
rg -n '^Status: (open|active|blocked)$' .10x/tickets
sed -n '1,260p' QUALITY.md
graphify query "the selected ticket's architectural boundary"
```

If `graphify` is unavailable, record that tooling limit and use `rg`/source inspection. Do not
pretend the graph was consulted.

## Things not to “simplify” accidentally

- Do not collapse discovered, declared, and observed schemas into one mutable runtime truth.
- Do not turn destination queue depth into global job count.
- Do not hard-code a low concurrency/memory/segment cap in place of an adaptive plan-recorded
  default and authoritative operator knob.
- Do not make correctness evidence disappear to save time; vectorize, stream, or parallelize it.
- Do not add a compatibility fallback when the current happy path can be fixed directly.
- Do not make a source or destination work by adding an identity-specific branch to the CLI,
  project compiler, engine, or generic runtime.
- Do not use planned bytes as actual transfer evidence.
- Do not retain entire packages, remote objects, or decompressed row streams in memory.
- Do not mutate destination state before a fixed plan and all plan-time fail-closed checks exist.
- Do not claim closure from unit tests when the changed lifecycle is only observable end to end.
