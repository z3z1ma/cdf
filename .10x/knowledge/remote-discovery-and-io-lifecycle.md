Status: active
Created: 2026-07-26
Updated: 2026-07-26

# Remote discovery and I/O lifecycle

## Purpose

This record preserves the source lifecycle that emerged from P2/P3 field failures. It separates
schema selection, current-file admission, payload transfer, and content identity so future source
work does not reintroduce duplicate reads or single-file assumptions.

## One fixed schema before final execution

A final CDF execution plan always contains one fixed effective output schema. The compiler may need
bounded source I/O to discover it, but destination mutation and package production do not begin
until the schema is frozen and identity-bearing.

Cold, unpinned flow:

```text
metadata inventory
→ deterministically choose discovery coverage
→ perform exactly the configured discovery probes
→ aggregate/reconcile observations
→ freeze a hash-addressed schema snapshot
→ compile the final execution plan
→ execute against that fixed schema
```

The snapshot may be:

- persisted through `cdf schema pin` or first-run auto-pin; or
- run-local for explicit ephemeral discovery.

Even a run-local snapshot is pinned inside that run's plan and package identity. It cannot evolve
halfway through execution.

Pinned flow:

```text
metadata inventory
→ load the fixed pinned schema
→ compile total admission/coercion/residual/quarantine behavior
→ stream selected partitions
→ observe and reconcile physical schemas in the same extraction stream
```

A pinned run does not rediscover every current file before extraction. Runtime observation is
validation against the fixed program, not selection of a new output schema.

## Two independent discovery coverage axes

“Sampled” and “exhaustive” are meaningless unless both axes are stated:

| Axis | Examples |
|---|---|
| Candidate-file coverage | deterministic subset, every currently selected file |
| Within-file coverage | footer/schema metadata, bounded bytes, bounded records, full content |

Examples:

- Parquet can inspect every candidate footer without reading all data pages. This is exhaustive
  current-file physical-schema coverage through format metadata.
- One hundred huge JSON files can be sampled from every file with bounded prefixes/record windows.
  That is file-exhaustive and content-sampled.
- Stratified discovery can select ten of the hundred files and sample bounded records from each.
- A full-content scan is an explicit expensive mode, not what “all files” silently implies.

Discovery evidence must state:

- matched files and their stable logical identities;
- selected and unobserved files;
- coverage policy and deterministic selection seed/order;
- actual bytes/records read;
- metadata-only, file-exhaustive, content-exhaustive, or sampled classification;
- codec/options/normalizer/contract tuple;
- observation/generation identity and cache reuse.

Do not encode a test whose desired behavior is “a sampled pin pre-opens every runtime file.” That is
the superseded lifecycle.

## Admission after the pin

The compiled program must define every permitted runtime outcome before extraction:

- exact compatibility;
- lossless width widening;
- governed parse or lossy coercion;
- unknown fields or values preserved in the residual/variant column;
- isolatable incompatible records quarantined;
- framing/physical-schema failures quarantined at partition grain;
- strict control/contract violations fail before destination mutation where the contract requires.

Runtime may choose among these frozen verdicts based on the observed partition. It may never
silently widen the typed output schema in the middle of the run.

The plan needs the fixed output schema and total admission program. It does not need the exact
future physical schema of every incremental file.

## No hidden full pre-scan and no repeated full transfer

The useful invariant is:

> Inventory reads no payload. Discovery reads exactly its explicit coverage budget. Execution
> performs no separate current-schema pre-scan and transfers each extraction payload once. Any
> substantial same-command discovery payload is reused when generation identity permits.

A tiny sample prefix may be read again as the prefix of the later full stream when retention would
cost more machinery than the bounded duplication. A complete file or expensive decompressed spool
materialized during same-command discovery should be retained under the disk/memory ledger and
reused.

Use two distinct stores:

- **Observation cache:** small schema/footer/sample facts keyed by logical source identity,
  generation/content identity, codec version/options, normalizer, contract, and relevant discovery
  policy.
- **Payload spool:** accounted content-addressed bytes or decoded bootstrap batches whose material
  transfer is worth reusing.

Large payloads do not belong in an ordinary key-value observation cache.

## Remote Parquet execution policy

The transport/format planner chooses among:

### Bounded range discovery

Use footer and metadata ranges for schema discovery. Reuse the returned metadata in the execution
session when the same generation remains authoritative.

### Sequential streaming spool

Use for a full or high-coverage finite scan when the decoder needs a seekable object. One GET
streams through bounded RAM into an accounted local file while the decoder reads available
regions. This removes serialized request latency and overlaps transfer with decode.

The spool:

- reserves known object size against disk authority;
- is bound to the source generation/content identity;
- remains valid for the prepared execution session;
- is removed/released on every terminal path;
- preserves constant memory through bounded buffers;
- does not claim constant disk for arbitrary object sizes.

### Selective exact ranges

Use parallel, coalesced row-group/column-chunk ranges only when projection/predicate/statistics
demonstrate meaningful byte savings and the object has strong generation identity. Concurrency is
admitted by run CPU/network/memory authority and exposed as a knob; it is not a conservative hard
cap inherited by local files.

### Weakly versioned objects

If HTTP lacks a trustworthy ETag/version/checksum, independently joined ranges cannot prove one
object generation. Prefer one sequential response and derive content attestation from the complete
payload. Weak metadata is not a reason to reject an otherwise readable public file.

## Row-oriented and unbounded inputs

CSV, NDJSON, JSON streams, REST pages, and event feeds should decode directly from streamed,
possibly decompressed windows into bounded batches with backpressure. They must not be fully
buffered into a `Vec` or indefinitely growing spool.

Unbounded sources that require replay use:

- finite package epochs;
- a bounded rolling spool;
- checkpoint-driven retention;
- eviction only after the commit/receipt authority proves earlier bytes are no longer needed.

If a finite seekable object exceeds the disk budget and cannot use generation-bound selective
ranges or proven progressive eviction, fail cleanly with a typed resource error before exhausting
the host. Increasing disk usage without accounting is never a fallback.

## Logical identity, access location, and content attestation

Keep these distinct:

- **Logical source identity:** the resource/file identity used by project mapping and manifests.
- **Access location:** the current URL after redirects or provider-specific signed resolution.
- **Generation/content attestation:** ETag, version id, checksum, or full-stream hash proving bytes.

Redirect targets and signed URLs may be ephemeral. They should not replace the stable logical
identity. A redirect can add an egress host that must be allowed, but it does not by itself define
file identity.

HTTP robustness:

- `HEAD` is an optimization, not a universal protocol requirement.
- A `HEAD` 400/403/405 may fall back to a bounded `GET`/range probe when policy allows.
- Missing ETag/version/checksum may use same-response content attestation for sequential reads.
- Metadata from multiple requests is joined only under a strong generation precondition.
- Egress allowlists apply to every redirect/access host.
- Retry classification distinguishes provider throttling/transient status from permanent request
  errors and reports the actual fix.

## Prepared sessions own reuse and terminal evidence

An expensive source-boundary operation should yield a prepared, typed execution session rather
than leave unrelated caches and orchestration code to reconstruct ownership.

The session owns:

- resolved source identity/generation;
- discovery observations selected for reuse;
- retained sample windows or spool;
- transport/decoder handles;
- accounted memory/disk leases;
- actual I/O counters;
- cleanup and terminal publication.

The compiler may type-erase that payload behind the source runtime boundary. Generic orchestration
does not inspect format/source internals.

Terminal paths—success, quarantine, cancellation, retry, failure, panic containment—must release
leases and publish the correct final observation exactly once.

## Actual versus planned I/O

Never infer physical bytes transferred by reopening all planned tasks or summing object sizes.

Use:

- typed planning estimates for selected logical work;
- runtime `SourceIoMetrics` for actual requests/bytes/retries/cache/spool;
- package manifest sizes for package bytes;
- destination telemetry for bytes/rows committed.

A ranged Parquet footer read does not transfer the entire object merely because the observation
represents that object. A sequential content-attested spool does transfer it. Evidence must
preserve that distinction.

## Multi-file is the normal case

Discovery aggregation, schema reconciliation, partition planning, deterministic assembly,
manifest incrementality, preview, run, and replay must all operate over N files. A one-file path is
a degenerate N=1 case, never a separate implementation.

For very large N:

- inventory and task authority are streamed/content-addressed;
- metadata retention is budgeted and may externalize;
- small files may be coalesced into deterministic partitions;
- schemas reconcile under the resource contract;
- each file/partition can quarantine independently;
- no caller enumerates an external task authority merely to recreate a counter.

## Regression checklist

A source lifecycle change must test:

- cold discover → freeze → final plan → execute without another discovery pass;
- pinned run with an unseen compatible file;
- pinned run with a widen/coerce/residual/quarantine outcome;
- sampled file coverage independent of within-file record coverage;
- discovery payload reuse for an expensive/compressed seekable object;
- weak-metadata HTTP sequential attestation;
- redirect and egress handling;
- preview/run parity through the same resolution/codec/normalization path;
- multi-file manifest followed by no-op rerun;
- actual I/O accounting distinct from selected logical bytes;
- cancellation/failure cleanup of retained spools and leases.
