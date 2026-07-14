Status: active
Created: 2026-07-13
Updated: 2026-07-13

# Single-crossing expensive source boundary

## Context

Pinned and sampled discovery require every runtime partition to receive a total schema verdict. The first implementation obtained that verdict by running a separate current-file discovery pass before extraction. For a large remote JSON/CSV collection or a dynamic Python/Lua/WASM producer, that turns one logical run into repeated payload downloads or repeated user-code execution. It also makes explicit sampled coverage misleading because a pinned baseline causes the runtime front end to probe every candidate before extraction.

The user established the stronger invariant on 2026-07-13: during one run, an expensive source boundary is crossed at most once per partition, except for an explicit retry or replay. Schema admission is part of that stream, not a preliminary execution of the source.

## Decision

CDF MUST inventory file resources using metadata only: canonical location, size, generation/version, ETag/checksum, modification time, and identity strength. Inventory MUST NOT read payload bytes merely to validate format, compression, or schema.

For sequential and row-oriented sources, runtime schema observation MUST be fused with extraction. The decoder retains the first accounted window or batches, instantiates the compiled deferred-admission program from that physical observation, sends the retained data downstream, and continues consuming the same source stream. It MUST NOT discard sampled bytes or invoke/download the source again.

For formats with independently bounded schema metadata, such as Parquet footers and Arrow IPC schema blocks, a metadata probe MAY precede payload extraction. It is not a payload crossing: it MUST remain byte-bounded, MUST NOT read data pages, and MUST be cached by immutable content generation plus codec/contract identity. A full or high-coverage scan MUST still transfer payload bytes only once. Selective range execution may issue the plan-recorded coalesced ranges required for the selected decode units; it MUST NOT repeat a full generation.

An unchanged cached observation is reusable only when all of these match: source generation/checksum, format driver id and semantic version, canonical decoding options, normalization version, and pinned contract identity. The cache accelerates observation but never replaces generation preconditions on extraction.

Dynamic Python, Lua, and WASM producers follow the same rule. A cheap declared schema handshake may avoid runtime observation. Otherwise CDF starts the producer once, retains the first bounded batches for admission, and continues the same invocation.

The compiler records a total deferred-admission program containing every permitted outcome. Execution records the exact physical observation and selected verdict in package evidence. Unknown fields do not silently become typed destination columns during a run; they follow the compiled residual/quarantine policy until explicit promotion.

## Alternatives considered

Probe every current file before execution.

- Rejected. It re-crosses remote and dynamic source boundaries, defeats sampled coverage, and makes run cost proportional to both observation and extraction.

Trust a sampled pin without runtime reconciliation.

- Rejected. Sampling weakens advance knowledge only. Runtime verdicts remain total.

Infer and mutate the typed schema midway through a run.

- Rejected. It introduces schema epochs into one package/destination commit and silently mutates pinned authority. Residual capture plus explicit promotion preserves determinism.

Keep the first sampled window only in memory for later reread.

- Rejected. The retained window is already admitted data and must continue downstream under the ledger; it is not disposable probe output.

## Consequences

Source plans need a deferred schema-admission operation. Codec/source streams need a retained-prefix or retained-batch handoff. Observation cache entries need versioned identity keys and generation-safe invalidation. File inventory and ordinary planning become payload-free. Existing runtime-baseline logic that disables `sample_files` and probes all candidates must be deleted, not shimmed.

