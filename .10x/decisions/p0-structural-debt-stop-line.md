Status: active
Created: 2026-07-07
Updated: 2026-07-07

# P0 Structural Debt Stop-Line

## Context

The current CDF tree has a working run spine, but an external architecture review identified structural debt that would compound as soon as new destination, source-archetype, streaming, distribution, and ecosystem lanes widen the runtime.

The review found six stop-line workstreams:

- Commit sessions must accept segments incrementally rather than being preloaded with fully materialized packages.
- The project orchestrator must be opened to driver registration and generic trait composition rather than closed enums and destination-specialized wrapper families.
- Conformance, chaos, golden, property, and fuzz harnesses must catch up to the run spine.
- The remaining DuckDB Arrow 58 residual and DataFusion git-pin publication constraints must be closed or explicitly accepted.
- The contract lane must become live-path behavior, not schema-only planning vocabulary.
- The benchmark gate must become a maintained opt-in harness before performance work proceeds from intuition.

The directive is user-ratified on 2026-07-07 and supplements the CDF 1.0 standing goal. It does not replace `VISION.md`; it front-loads book-aligned structural work that is cheap now and expensive later.

## Decision

CDF adopts a P0 structural-debt stop-line program owned by `.10x/tickets/done/2026-07-07-p0-structural-debt-program.md`.

Until Workstreams A, B, and C close with evidence and adversarial review, the project MUST NOT open new:

- enterprise or lakehouse destination lanes, including BigQuery, Snowflake, Databricks, Iceberg, and Delta;
- new source archetype lanes, including CDC and Kafka;
- resident streaming-supervisor implementation lanes.

Work already in flight may finish its current child ticket, then pause before widening. Workstreams D, E, and F may proceed concurrently with A-C when file ownership and acceptance criteria do not conflict.

This stop-line does not block record maintenance, user-ratified decisions, existing child-ticket closure, vault/secret-provider work that does not open a new destination lane, or quality-tooling work.

The destination session contract is no longer optional. The kernel destination protocol must require `begin`, must stream or accept package segments incrementally, and must expose trait-level receipt verification. Fully materialized package replay must feed recorded segments through the same session shape used by future streaming package-to-destination commit.

## Alternatives considered

Continue ordinary program sequencing and treat these debts as follow-ups.

Rejected. Every new destination or source would multiply the non-streaming session shape, closed enum resolution, specialized replay/recover wrappers, and harness gaps.

Fix only the CLI `commands.rs` shape.

Rejected as insufficient. The command module architecture issue already has a closed owner, but the deeper debt is the lower-layer runtime shape that future CLI commands would consume.

Defer benchmarks until after optimization work starts.

Rejected. The standing goal requires P4 to be evidence-ordered; optimization without baselines produces anecdotal performance work.

## Consequences

`.10x/specs/destination-receipts-guarantees.md` and `.10x/specs/run-orchestration-ledger.md` are updated to make segment-streaming sessions and trait-level receipt verification required, not optional.

Destination/source/streaming parent tickets must show the stop-line in their blockers so future agents do not start blocked lanes by accident.

The P0 parent ticket is the sequencing authority until its exit criteria are met. The stop-line lifts only by an explicit parent progress note after all six workstreams close and the coverage matrix is updated.
