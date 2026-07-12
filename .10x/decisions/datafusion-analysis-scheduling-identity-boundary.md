Status: active
Created: 2026-07-12
Updated: 2026-07-12

# DataFusion analysis, scheduling, and identity boundary

## Context

CDF already uses DataFusion as the required Tier-B planning substrate, shares its finite memory authority, and adapts queryable resources through `TableProvider`. P3 is now hardening native decode, validation, canonical segmentation, hashing, and package sinks near their hardware rooflines. Those operators produce bytes and verdicts that enter package identity. Replacing them with generic engine operators would risk package drift, weaken format-specific performance, and make DataFusion upgrades artifact migrations.

DataFusion nevertheless exposes mature, ecosystem-standard interfaces for expressions, statistics/pruning, catalogs, memory, object-store registration, physical plans, metrics, and serialization. CDF already owns equivalent facts—schema-aware expressions, evidence statistics, artifact stores, a unified memory ledger, secret-resolved transports, and portable partition tasks—but does not consistently marshal them into those interfaces.

This decision refines `.10x/decisions/datafusion-tier-b-delegation-boundary.md`; it does not make DataFusion optional or restore a parallel ad-hoc engine.

## Decision

DataFusion MAY analyze, prune, query, explain, and schedule CDF work. DataFusion MUST NOT produce identity-bearing CDF bytes or verdicts.

Anything hashed into a package or used as receipt/checkpoint evidence—including decoded physical batches, effective-schema materialization, row verdicts, canonical segment bytes, manifests, and quarantine artifacts—MUST be produced by deterministic native CDF operators. Those native operators MAY be exposed as DataFusion `ExecutionPlan` nodes so DataFusion can schedule them and collect metrics, but their semantic and byte output remains governed by CDF contracts and golden fixtures.

Plan-time DataFusion results that affect execution MUST be canonicalized and recorded in the CDF plan/package authority. They MUST NOT be silently re-derived at run or replay time. This includes simplified expressions, pruning predicates, selected statistics, partitioning decisions, and optimizer-derived properties.

DataFusion types remain contained in engine/adaptor crates. The kernel, `cdf-runtime`, source/format/destination extension contracts, and package artifact schemas MUST NOT expose DataFusion types. One pinned Arrow/DataFusion tuple per CDF minor and existing golden/supply-chain gates continue to apply.

CDF will marshal existing facts into DataFusion currencies in this order:

1. evidence statistics into `PruningStatistics` with a permanent pruned-versus-unpruned equivalence law;
2. the existing `cdf-memory` authority and secret-resolved object stores into DataFusion execution/session registries;
3. declarative derive/filter/contract expressions into a shared DataFusion `Expr` representation, with native fused lowering for identity-bearing execution and recorded optimizer/linter output;
4. ledger/package evidence into DataFusion catalog providers and standard query interfaces;
5. native pipeline stages into `ExecutionPlan` shells with metrics and portable-plan marshaling for later distributed evaluation.

Native Parquet/Arrow/CSV/JSON codecs, schema reconciliation, validation, dedup, statistics, segmentation, and hashing remain primary. An FX1 adapter MAY host a DataFusion `FileFormat` for an exotic non-primary format only behind the same neutral driver laws and measured acceptance. `physical-expr-adapter` is not an execution authority; its coercion semantics will be audited against CDF's richer reconciliation lattice. Selected DataFusion functions or aggregate algorithms may be reused only when benchmarks and differential conformance justify them.

## Alternatives considered

### Replace CDF's native data plane with DataFusion operators

Rejected. It would put package identity and the measured roofline path under a quarterly dependency's implementation details, discard format-specific advantages, and make replay stability depend on DataFusion execution versions.

### Keep DataFusion limited to the existing TableProvider adapter

Rejected. It strands mature pruning, expression analysis, catalogs, metrics, scheduling, and ecosystem interoperability behind duplicate CDF-only interfaces.

### Expose DataFusion types throughout kernel and extension APIs

Rejected. It would contaminate source/destination addition paths with the engine build graph and violate the established Arrow-only kernel and neutral runtime boundaries.

### Hand-roll pruning, expression optimization, and catalogs

Rejected. NULL/cast pruning soundness, expression simplification, and catalog interoperability are high-maintenance semantics already production-falsified in DataFusion.

## Consequences

Deep DataFusion integration now means a scheduling and semantic-analysis shell around native identity operators, not generic replacement of the identity path. Each bridge has a narrow crate owner and independent conformance law. DataFusion upgrades can change nonidentity plans only when recorded plan authority and golden tests admit the change; they cannot silently alter package bytes.

