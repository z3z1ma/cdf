Status: recorded
Created: 2026-07-26
Updated: 2026-07-26

# Knowledge and skill handoff audit

## Observation

CDF had detailed architectural decisions, specifications, ticket journals, and performance
evidence, but no operational skills and no single cold-start index. Several high-cost lessons were
only reconstructible by combining terminal tickets, current scripts/workflows, source behavior,
and dated evidence.

The most material gaps were:

- the complete EC2 benchmark lifecycle and cost/identity safeguards;
- the split between dynamically downloaded developer DuckDB and static published DuckDB;
- the absence of checksum verification in the downloaded-library developer path;
- the current one-TiB performance floor and stale generated performance envelope;
- a single statement of cold discovery versus pinned in-stream admission;
- stage-local versus global CPU/memory/backpressure authority;
- the customer-zero deletion/current-only rule and its external-compatibility limit;
- the mandatory product smoke needed to catch cross-lifecycle identity regressions;
- a repeatable regression-bisection procedure.

## Procedure

The audit:

1. Read every active file under `.10x/knowledge/`.
2. Enumerated the entire terminal ticket set and searched every recorded Retrospective section by
   the dominant architecture, performance, discovery, remote I/O, DuckDB, packaging, product-smoke,
   CLI, connector, and closure themes.
3. Read the active pre-wave program and enumerated all fifteen active parent/child tickets.
4. Inspected the EC2 benchmark helper's command surface, defaults, state model, current reusable
   resource state, terminal benchmark-host tickets, and current one-TiB evidence.
5. Inspected the workspace DuckDB version/features, Cargo loader configuration,
   `libduckdb-sys` download build script, EC2 build helper, hosted release workflow, install
   documentation, and hosted release evidence.
6. Reconciled current source/workflow authority against stale statements in terminal ticket
   journals.
7. Authored the canonical knowledge records and executable skills named below.
8. Ran the helper's local `status` and a read-only `us-west-2` AWS query for pending/running
   `Project=CDF,Purpose=P3Benchmark` instances. The helper had no active state and the AWS query
   returned no instance, independently confirming no benchmark compute remained cost-bearing.

## New durable authorities

Knowledge:

- `.10x/knowledge/cold-start-engineering-handoff.md`
- `.10x/knowledge/developer-build-duckdb-linkage.md`
- `.10x/knowledge/performance-evidence-and-regression-triage.md`
- `.10x/knowledge/remote-discovery-and-io-lifecycle.md`
- `.10x/knowledge/runtime-performance-authorities.md`
- `.10x/knowledge/pre-production-current-only-policy.md`
- `.10x/knowledge/product-integration-and-closure-gate.md`

Skills:

- `.10x/skills/run-cdf-ec2-benchmarks/SKILL.md`
- `.10x/skills/build-and-install-cdf/SKILL.md`
- `.10x/skills/investigate-cdf-performance-regressions/SKILL.md`

Existing knowledge records were cross-linked where the new authority changes future execution:

- `.10x/knowledge/active-backlog-and-future-roadmap.md`
- `.10x/knowledge/quality-gate-execution.md`
- `.10x/knowledge/runtime-conformance-throughput-rule.md`

## What this supports

The audit supports a cold-start handoff in which:

- the next executable program and its boundaries are immediately legible;
- developer builds avoid unnecessary DuckDB source compilation without weakening release
  portability;
- EC2 measurements can be reproduced and cost-bearing resources terminated;
- performance regressions are isolated through comparable cells and binary search;
- source discovery does not regress to duplicate full reads;
- concurrency and memory changes preserve one authority and deterministic identity;
- core changes cannot close on crate-local tests alone;
- obsolete CDF paths are deleted without weakening external compatibility.

## Validation

- The cold-start catalog was mechanically compared with all 22 files under `.10x/knowledge/`; the
  sets matched exactly.
- Every `.10x/*.md` path referenced by the new knowledge, skills, and this evidence record resolved
  to an existing repository file.
- `quick_validate.py` from the system skill creator passed all three canonical `.10x` skills and
  all three `.claude` mirrors.
- `git diff --cached --check` passed.
- `gitleaks git --staged --redact --no-banner` scanned the staged tranche and reported no leaks.
- The read-only tagged EC2 query returned no pending/running CDF benchmark instance.

## Limits

- Terminal tickets were mined systematically through their Retrospective sections and themed
  references; not every journal line in all 448 terminal tickets was reread sequentially.
- Live AWS state was read but not mutated during this record audit. The empty tagged-instance query
  is a dated observation, not a durable guarantee; the EC2 skill requires a fresh status/AWS check
  for every future tranche.
- At audit time, the newest one-TiB evidence had not regenerated `docs/performance-envelope.md`.
  Later on 2026-07-26, the reconciliation manifest and generated document were refreshed together
  and the committed golden-generation test passed.
- The pre-wave hardening tickets remain open and executable. This audit does not claim their
  architectural outcomes are implemented.
