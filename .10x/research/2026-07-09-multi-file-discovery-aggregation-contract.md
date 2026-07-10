Status: done
Created: 2026-07-09
Updated: 2026-07-09

# Multi-file discovery aggregation contract

## Question

What exact resource-level contract lets CDF discover, pin, reconcile, execute, and evidence multi-file Parquet and Arrow IPC without a single-file product path, silent sampling, perpetual pin mutation, or weakened runtime `FileManifest` identity?

This research supports `.10x/tickets/2026-07-09-p2-ws-a10-multi-file-schema-discovery-pin.md`. It separates record-backed invariants from semantics that still require user ratification.

## Sources and methods

- Read `VISION.md` Chapters 7, 8, 11-13, and 19 plus the P2 directive.
- Read the active data-onramp discovery, transport/manifest, and source-experience decisions and specs.
- Traced candidate selection, Parquet/Arrow probes, snapshot/lock hydration, runtime identities, reconciliation, engine coercion evidence, quarantine, package identity, and checkpoint aggregation. The detailed source inventory is in A10.
- Reviewed the mandatory official dlt comparison context: https://dlthub.com/docs/dlt-ecosystem/verified-sources/filesystem, https://dlthub.com/docs/general-usage/schema-contracts, and https://dlthub.com/docs/general-usage/naming-convention.

## Findings

### Record-backed invariants

1. Discovery is a compiler stage and its baseline output is pinned; runtime cannot continuously rewrite the lockfile.
2. The resource is the schema authority boundary. Every matched file is a candidate/partition within it; one file is only the cardinality-one case of the same abstraction.
3. Binary discovery can inspect every candidate without row data: Parquet reads footer/schema metadata and Arrow IPC reads file framing/schema blocks. Text inference may need sampling; binary metadata aggregation does not.
4. Runtime `FileManifest` identity decides incrementality/checkpoint honesty. Discovery identity decides schema-evidence reuse. They compose but are not the same artifact.
5. Baseline pin, current effective output schema, and discovery-set manifest are three distinct facts and need distinct hashes.
6. Per-file preserved, widened, missing-null, extra, incompatible, and quarantine decisions cannot fit one resource-wide coercion plan.
7. Quarantined files may yield no accepted batch. Processed-file positions must therefore exist independently of output segments, and an all-quarantine package must still be gateable.
8. Exhaustive schema discovery detects variance in every candidate, but S8 separately needs bounded multi-partition preview coverage for decode failures schema probes cannot see.

### dlt lessons without convergence

dlt validates transport-neutral metadata enumeration, managed file incrementality, and resource-level `evolve`/`freeze` contracts across Arrow inputs. Its filesystem source separates descriptor enumeration from content readers. CDF retains a different authority model: probe at plan time, pin a baseline, serialize every drift decision, and preserve replayable package/receipt/checkpoint evidence. Continuously evolved destination schema is not CDF's pin contract.

### Binary sampling is rejected

The earlier 4,096-file/64-MiB sampling proposal leaves unprobed candidates without physical-schema hashes or verdicts and permits knowingly incomplete auto-pins. Hash-ranked sampling also lacks a bounded content hash to rank by. This contradicts the user's architectural requirement and P2's anti-convergence rule.

The recommended binary contract probes every matched metadata block. Scale uses deterministic order, cache reuse, bounded concurrency, per-file metadata ceilings, and an explicit failure when a configured total budget is exhausted—never silent sampling.

### Aggregate schema join

The aggregate join is the least upper bound induced by the ratified lossless widening relation:

- identical types join to themselves;
- if exactly one type widens losslessly to the other, the wider type wins;
- if neither direction has a ratified path, the field/file is incompatible;
- nested list/struct/map children apply the same rule recursively;
- a field missing from any otherwise compatible file becomes nullable and materializes a typed null array for that file;
- source field identity is the unnormalized source name; normalization runs after aggregation so cross-file collisions fail at plan time;
- candidates sort by canonical transport location; aggregate fields retain first appearance in that order.

This does not invent signed/unsigned, integer/float, timezone, decimal-scale, dictionary, extension-type, or other joins outside the current widening lattice.

### Metadata join

Reserved `cdf:*` metadata is regenerated from CDF authority. Non-reserved schema/field metadata appears on the aggregate only when identical across every candidate where the field exists. Conflicts remain in per-file discovery evidence with a named metadata-variance verdict; the implementation must not silently pick the first file or discard the conflict.

### Pin, effective schema, and drift

The recommended authority split is:

- `baseline_snapshot_hash`: immutable pinned contract baseline referenced by `cdf.lock`;
- `effective_schema_hash`: plan/package output after applying the current contract to the exhaustively probed set;
- `discovery_manifest_hash`: content address of candidates, probes, and verdicts.

First pin uses the exhaustive compatible aggregate. If the initial set contains an incompatible file, auto-pin fails with the complete report instead of silently omitting the file or choosing an arbitrary baseline.

After a pin exists, `evolve` may derive a plan-time effective schema admitting compatible additions/widenings as serialized contract events while leaving the pin unchanged. `freeze` keeps the effective schema equal to the baseline and quarantines deviations. Explicit `cdf schema pin` refreshes the baseline after showing the diff. Removing a file changes fresh manifest/effective authority but never deletes destination data or historical runtime state.

### Candidate identity

Each discovery-manifest entry contains canonical location, transport, size, modification time where present, transport identity plus strength, physical-schema hash, probe bytes, participation, and verdict.

For bounded local discovery, `(canonical relative path, size, mtime, physical-schema/footer hash)` is an observation identity, not whole-file cryptographic identity. Object-store checksums/ETags are strong only when the transport declares them strong; multipart/weak ETags are labeled weak. Weak evidence never replaces runtime exact/checkpoint identity.

### File quarantine and checkpoint advancement

A terminal file-level quarantine verdict marks that exact runtime file identity processed only after its quarantine package and destination receipt pass the normal gate. An unchanged bad file is not retried forever; a changed checksum/ETag/size identity is planned again. Admitted and quarantined positions are recorded independently of accepted segments, including all-quarantine runs.

## Recommended contract for ratification

1. Exhaustively probe every Parquet footer and Arrow IPC schema block; never auto-pin a sampled binary set.
2. Use one format-neutral candidate/probe/verdict/manifest aggregator now and plug row-oriented formats into it later.
3. Join only through equality or the ratified lossless lattice; missing compatible fields become nullable typed nulls; all other pairs are incompatible.
4. Preserve identical non-reserved metadata, record conflicts per file, and regenerate reserved CDF metadata.
5. Persist a content-addressed discovery-manifest sidecar and stamp baseline, effective-schema, manifest, and per-file decision evidence into plans/packages with backward-compatible serialization.
6. First pin requires an exhaustive compatible aggregate. Existing pins remain immutable until explicit refresh; `evolve` derives a verdict-bearing effective schema and `freeze` quarantines deviations.
7. Quarantined file identities advance only through the receipt/checkpoint gate; changed identities retry; removal never deletes destination data.
8. Discovery identities are strength-labeled bounded observations; runtime `FileManifest` identity remains incrementality authority.

## Ratification

The user ratified all eight points on 2026-07-09. The user questioned whether 128 MiB was sufficient for production scale; the resulting decision makes 64 MiB per file, 128 MiB total in flight, and 8 probes configurable, plan-recorded per-executor defaults rather than universal or cluster-wide limits. Exceeding a resolved limit fails with remediation and never falls back to sampling. Authority: `.10x/decisions/multi-file-discovery-aggregation-and-budget.md`.

## Conclusion

The durable seam is a resource-level discovery set with three distinct identities and total per-file verdicts. Existing single-file probes are reusable adapters. The current single-result snapshot, singular coercion plan, segment-derived positions, and first-partition preview assumptions are insufficient and must be replaced through bounded children after ratification.
