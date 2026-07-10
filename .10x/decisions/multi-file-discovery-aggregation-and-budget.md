Status: active
Created: 2026-07-09
Updated: 2026-07-09

# Multi-file discovery aggregation and executor budget

## Context

The first P2 discovery slices exposed one-file Parquet and Arrow IPC probes. The user explicitly rejected treating those slices as the product architecture and required multi-file discovery/pinning that remains suitable for massive standalone or container execution, remote Azure/object-store ingestion, custom Python/WASM parsing, and eventual Spark/Flink-style distributed embedding.

The governing VISION already fixes the important invariants: discovery is a pinned compiler stage; files are partitions; Arrow is the closed type vocabulary; drift is a total contract verdict; plans/packages survive every convenience; and distributed workers reuse the same partition/package/checkpoint calculus.

Research in `.10x/research/2026-07-09-multi-file-discovery-aggregation-contract.md` found that sampled binary auto-pins, a singular resource coercion plan, segment-only processed positions, and one hash standing for baseline/effective/manifest identity cannot meet those invariants.

## Decision

Multi-file discovery is one format-neutral resource-level aggregation stage. Parquet and Arrow IPC adapters MUST enumerate every matched candidate and probe every footer/schema block. Binary discovery MUST NOT silently sample. A cardinality-one resource uses the identical aggregator and evidence shape.

The aggregate schema is the least upper bound induced by the ratified lossless widening lattice. Equal types preserve; a one-direction lossless widening chooses the wider type; no ratified path is incompatible. Nested children recurse. A field absent from any compatible file becomes nullable and materializes a typed null for that file. Aggregation identifies fields by unnormalized source name and normalizes only after the join. Candidate order is canonical transport-location order; field order is first appearance in that order.

Reserved `cdf:*` metadata is regenerated. Non-reserved metadata is retained on the aggregate only when identical across every candidate where it appears. Conflicts are retained as per-file metadata-variance evidence.

Discovery records three distinct identities:

- baseline snapshot hash: the immutable pin in `cdf.lock`;
- effective schema hash: the verdict-bearing schema a plan/package will execute;
- discovery manifest hash: the content address of candidates, probes, identities, and per-file verdicts.

First pin requires an exhaustively compatible aggregate. It fails with the complete report when any initial candidate is incompatible. Once pinned, the verified baseline authority MUST be established before any current-file contact. `evolve` may then derive a compatible effective output schema as a serialized contract result without rewriting or replacing the baseline; `freeze` keeps the baseline effective and quarantines deviations. Only explicit `cdf schema pin` refreshes the baseline. Current-file listing/probing for a file run is runtime front-end observation, not discovery-pin refresh; non-file pinned resources retain their existing no-probe ordinary-command behavior.

A terminal file quarantine advances that exact runtime file identity only after its quarantine package and destination receipt pass the ordinary checkpoint gate. An unchanged bad identity is not retried forever; a changed identity is evaluated again. File removal changes future discovery authority only and never deletes destination data or historical state.

Discovery identities are strength-labeled bounded observations. Runtime `FileManifest` identity remains authoritative for incrementality. Weak/multipart ETags are not treated as strong checksums.

The standalone/default executor budget is:

- 64 MiB maximum metadata bytes for one file;
- 128 MiB maximum total in-flight discovery metadata;
- 8 concurrent probes.

These are configurable, plan-recorded per-executor defaults—not cluster-wide ceilings or claims of universal production sufficiency. Embedders and future memory-ledger policy MAY supply larger or smaller values through executor options. Every resolved value is serialized in discovery evidence. Budget changes may alter scheduling or cause an explicit resource-limit failure, but MUST NOT alter candidate membership, schema joins, verdict semantics, or trigger sampling.

All models below the product layer MUST remain executor-neutral. They MUST NOT depend on CLI, local filesystem, Tokio task ownership, Spark/Flink APIs, Python, WASM, or a specific object-store SDK. Transports and parser tiers adapt into canonical candidate/probe facts; plans, manifests, packages, receipts, and checkpoints remain the integration protocol.

## Alternatives considered

Sample after a fixed file count or byte threshold.

- Rejected because unprobed binary files have no physical-schema hash or verdict and a sampled auto-pin is knowingly incomplete.

Hard-code 128 MiB as a global limit.

- Rejected because executor topology and available memory differ across local processes, containers, and distributed workers. The semantic invariant is explicit boundedness, not one universal capacity.

Hash every complete file during discovery.

- Rejected because it turns schema probing into row-data/full-object I/O. Runtime exact `FileManifest` identity already owns data incrementality.

Continuously rewrite the pin under `evolve`.

- Rejected because it collapses CDF into perpetual inference and weakens lockfile review, replay determinism, and package identity.

Choose the first file as the initial `freeze` baseline.

- Rejected because candidate order would become arbitrary semantic authority. First pin must describe the complete compatible resource set.

## Consequences

The implementation needs a canonical discovery manifest sidecar, backward-compatible snapshot/lock references, a pure aggregate-schema join, exhaustive candidate adapters, per-file coercion/verdict evidence, effective-schema plan/package fields, nullable missing-field materialization, file-level quarantine, and processed positions independent of accepted output segments.

Large-scale execution scales through candidate partitions, bounded concurrent probes, cache reuse, and executor/worker replication. The same compiled facts can be embedded under a future Spark/Flink scheduler without moving correctness into that scheduler.

Per-file or total budget exhaustion is a named plan-time resource error with remediation. It never silently narrows discovery coverage.
