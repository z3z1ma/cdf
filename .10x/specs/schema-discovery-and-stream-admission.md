Status: active
Created: 2026-08-06
Updated: 2026-08-06
Supersedes: `.10x/specs/superseded/schema-discovery-and-stream-admission-lockfile.md`

# State-backed schema discovery and stream admission

## Purpose

This specification governs cold discovery, coverage, first-use freezing, active-schema admission,
observation caching, payload-spool reuse, and dynamic-producer bootstrap. It refines
`.10x/specs/data-onramp-schema-intelligence.md`,
`.10x/specs/schema-drift-dispositions.md`, and
`.10x/specs/residual-variant-capture.md`.

## Invariants

A package-producing command MUST freeze one logical output schema version and final native plan
before package or destination mutation. That schema remains immutable for the run. Physical
observations after final compilation select only a serialized total admission disposition and
cannot invent fields, coercions, migrations, or schema epochs.

Inventory MUST NOT read payload bytes for format confirmation, schema inference, compression
confirmation, or whole-file hashing. It may read bounded metadata and MUST label identity strength.

An active-authority run MUST NOT execute a current-schema pre-scan. It may reuse a strongly keyed
observation cache; a miss means in-stream observation during extraction, not an extra discovery
pass.

## First-use discovery and planning

For absent authority requiring discovery, preparation executes:

```text
metadata inventory
→ deterministic bounded discovery selection
→ observe selected candidates
→ aggregate/reconcile observations and hints
→ build one immutable proposed state version
→ compile final plan and total admission program
→ plan: discard/report, or compile/run: establish through state CAS
```

The returned proposal MUST feed final compilation directly. A writing command cannot establish the
head and then re-enter active preparation or rediscover. A plan proposal is memory/portable-plan
authority only and writes no state.

No destination preparation that can mutate, package segment creation, checkpoint proposal, run
ledger mutation, or producer side effect beyond the bounded discovery invocation may begin before
final compilation and required first-use state establishment.

## Independent coverage axes

Every discovery manifest records both axes.

File coverage:

- `all_files`: every matched candidate selected;
- `sampled_files`: explicit positive `sample_files = N` selects deterministically.

Within-file coverage:

- `format_metadata`;
- `bounded_content` with configured/observed byte and record limits;
- `full_content` only when every value was observed.

Sampling never activates implicitly due to count, cost, time, memory, or topology. The existing
`stratified-hash-v1` selector remains canonical: ordered canonical locations, first/last coverage,
balanced interior strata, SHA-256 score over version/resource/location/bounded identity, and
canonical tie breaks. Evidence records configuration, matched/selected/unobserved counts, scores,
strata, and bounded identities before concurrent probes start.

Parquet and Arrow IPC default to `all_files + format_metadata`. Row formats default to
`all_files + bounded_content` unless explicitly sampled. Reports never use unqualified
`exhaustive`; they name file, metadata, or content coverage accurately.

Each selected candidate records exact observed bytes/records and physical schema hash/verdict.
Unselected candidates record only bounded identity and `unobserved`, never placeholder schemas.

## Active-schema admission

With an active head, execution is:

```text
metadata inventory and incremental selection
→ load/verify exact state version and generation binding
→ load/compile final total admission program
→ open selected partitions
→ observe/reconcile while decoding the same stream
→ validate/package/deliver under the fixed version
```

The program binds logical version/generation, output schema, format driver/version/options,
normalizer, coercion allowances, typed drift dispositions, evidence/redaction policy, field roles,
and every permitted verdict. Execution cannot reparse or expand it.

The first physical window instantiates the program. Package evidence records exact observation and
chosen disposition. Outcomes are lossless admit/coerce, typed null, safe variant, safe quarantine,
or fail as governed by `.10x/specs/schema-drift-dispositions.md`. Unknown fields never become
destination columns.

Preview uses the identical inventory, active/proposed schema, interpretation, admission program,
and first-stream window as run, then stops at its bounded downstream limit.

## Observation cache and payload spool

A cache key contains:

```text
strong source generation/checksum
+ format driver id and semantic version
+ canonical format/transform options hash
+ normalization version
+ active schema version and admission-program identity
```

Weak identity, mismatch, corruption, or unsupported version misses safely. Cache telemetry is not
package authority; each attempted observation remains package evidence.

Large payloads use memory/disk-ledger-accounted spools with source, generation, transform, owner,
cleanup, and content hash. When same-command discovery fully downloads/decompresses a candidate,
extraction MUST consume that exact verified spool or retained decoded batches. It cannot repeat the
full source transfer. Small bounded unspooled prefix/footer/sample reads may repeat within the
recorded budget and are measured separately.

## Dynamic and unbounded producers

A declared schema handshake may freeze the plan before data. Otherwise one producer invocation
emits bounded retained/spooled bootstrap batches, waits at the plan/state-establishment barrier,
then continues through the exact compiled plan. It is not invoked once for discovery and again for
extraction. Unbounded sources retain one schema epoch until explicit controlled restart/replan.

## Errors and telemetry

Discovery failures name resource, candidate, both coverage axes, configured/observed bounds, and
the owning fix. Runtime drift names the active version, physical observation, violation class, and
compiled disposition rather than an inference stack.

Telemetry distinguishes metadata, discovery probe bytes, payload-spool bytes, extraction bytes,
spool reuse, cache hit/miss, and bounded duplicate probes. It also distinguishes accepted,
accepted-with-residual, quarantined, and failed outcomes.

## Scenarios

Given 100 remote JSON files and `sample_files = 10`, selection deterministically samples ten,
observes each only within bounds, records 90 unobserved candidates, and proposes one first-use
version without overclaiming content coverage.

Given that active version later sees a nullable mismatch, the typed field is null, exact original
value enters `_cdf_variant`, and the state head is unchanged.

Given cold transformed input requiring full decompression, one verified spool feeds discovery and
execution; the remote generation is transferred/decompressed once.

Given a dynamic producer without a handshake, one invocation supplies retained bootstrap batches,
waits for first-use state CAS, and continues without rerunning user code.

## Acceptance criteria

- First-use counters prove one discovery result feeds final planning and execution.
- Active-authority counters prove no pre-extraction schema probe.
- Coverage artifacts encode/validate both axes and current terminology.
- Inventory performs no whole-file read/hash; extraction reattests generation.
- Cache and spool tests cover identity, corruption, bounds, reuse, and cleanup.
- Preview/run share the admission front end and dynamic producers invoke once absent retry.
- Residual/quarantine/fail outcomes are serialized, replayable, and head-immutable.

## Explicit exclusions

- implicit promotion, hidden sampling, same-run schema epochs;
- weak cache identity as authority;
- unbounded payload retention;
- destination-specific admission semantics;
- lockfile or compatibility-reader behavior.
