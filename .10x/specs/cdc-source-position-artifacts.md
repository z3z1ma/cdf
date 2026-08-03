Status: active
Created: 2026-08-03
Updated: 2026-08-03

# CDC source-position artifacts

## Purpose

Replace CDF's stringly log position with typed, lossless, restart-safe PostgreSQL/MySQL committed
positions and a distinct opaque MongoDB change-stream resume-token position. Centralize the
position algebra used by checkpoint aggregation and frontier reachability, then replace every
pre-production embedding artifact coherently with no compatibility layer.

## Authority and ratification

The user ratified this contract on 2026-08-03:

- use protocol-specific committed PostgreSQL/MySQL position variants;
- use a separate typed MongoDB resume-token variant;
- preserve complete after-images/key-only deletes as the later CDC row contract;
- replace all artifacts outright because CDF is net-new and customer zero;
- add no compatibility readers, migrations, shims, transitional fields, or other debt;
- treat MongoDB as receipt-gated event-prefix segmentation, not source-transaction grouping.

Protocol evidence and limitations are recorded in
`.10x/research/2026-08-03-cdc-protocol-position-contract.md`.

## Scope

- replace `SourcePosition::Log(LogPosition)` with a typed committed-log enum;
- add a first-class MongoDB resume-token `SourcePosition` kind;
- define typed PostgreSQL/MySQL/MongoDB scope and coordinate structures;
- validate native ranges, identifiers, canonical encodings, hashes, and cross-field invariants;
- centralize same-scope, equivalence, reachability, ordered join, and slice-invariance semantics;
- route checkpoint/output aggregation and drain source-frontier comparison through that authority;
- replace every persisted/portable/declarative/package/checkpoint representation and fixture that
  embeds or renders a source position;
- fail old artifact versions directly and remove their old field shapes.

## Non-goals

- connecting to PostgreSQL replication, a MySQL binlog, or a MongoDB change stream;
- implementing transaction decode, `_cdf_op`, row materialization, `cdc_apply`, or destination
  mutation;
- implementing MySQL cross-primary failover/rebind;
- interpreting MongoDB resume-token internals or ordering arbitrary MongoDB tokens;
- choosing the PostgreSQL/MySQL maximum single-transaction byte policy;
- preserving any old JSON/TOML/SQLite/worker/package artifact.

## Canonical model

`SourcePositionKind` MUST add `ResumeToken`. `SourcePosition` MUST contain:

```rust
Log(CommittedLogPosition)
ResumeToken(ResumeTokenPosition)
```

The exact public Rust names MAY be adjusted mechanically for established crate naming conventions,
but the fields, type separation, and laws below MUST remain intact.

### PostgreSQL committed position

`CommittedLogPosition::PostgreSql` MUST carry:

- source-position version;
- scope:
  - `system_identifier`: nonempty canonical decimal text preserving the server-provided value;
  - `database_oid: u32`, greater than zero;
  - `slot`: valid nonempty control-free slot identity;
  - `output_plugin`: valid nonempty control-free plugin identity;
  - `semantics_sha256`: canonical hash of sorted publication names and every behavior-changing
    logical protocol option;
- `commit_lsn: u64`, greater than zero;
- `end_lsn: u64`, greater than or equal to `commit_lsn`;
- `xid: u32`, retained as transaction-boundary corroboration rather than an ordering axis.

`end_lsn` is restart and ordering authority. Persisted positions are constructible only from a
normal `Commit` or `Stream Commit`. There is no boundary boolean and no serializable in-progress,
abort, prepare, or rollback position.

### MySQL committed position

`CommittedLogPosition::MySql` MUST carry:

- source-position version;
- scope:
  - stable CDF deployment/source binding identity;
  - canonical active server UUID;
  - nonempty binlog basename;
  - `semantics_sha256` covering ROW format, FULL row image, GTID ON/consistency, and any other
    capture option that changes emitted history;
- native binlog filename;
- parsed numeric file sequence matching that filename and scope basename;
- terminal commit-event `end_log_position: u64`, at or beyond the first legal event position;
- canonical executed GTID set supporting tagged MySQL 8.4 GTIDs and disjoint ranges;
- canonical transaction GTID contained in that executed set.

Ordering within one scope is `(file_sequence, end_log_position)`. A successor MUST retain a GTID
set that is a superset of its predecessor. An active-server UUID change is not same-scope in this
version and requires a later explicit rebind contract.

### MongoDB resume token

`ResumeTokenPosition::MongoChangeStream` MUST carry:

- source-position version;
- scope:
  - stable CDF deployment/source binding identity;
  - watch level: cluster, database, or collection;
  - database/collection target where the watch level requires it;
  - canonical pipeline SHA-256;
  - canonical semantics-changing options SHA-256;
- the exact BSON token document bytes encoded as canonical base64;
- SHA-256 of the decoded BSON bytes;
- resume mode: `resume_after` or `start_after`;
- token source: event or post-batch.

The kernel MUST decode the base64, validate the hash, and reject empty/malformed payloads without
parsing undocumented resume-token fields. Mongo token equality requires exact scope, mode, and
bytes. Mongo tokens have no generic less-than/greater-than relation and cannot be arbitrary
`SourceFrontier` targets.

At runtime the Mongo source will accumulate ordered events into segments/packages, retain the
terminal event/post-batch token as the proposed frontier, and advance it only after the exact
destination receipt. That later runtime behavior requires no transaction grouping and does not
change the opacity laws here.

## Position algebra

One kernel authority MUST implement and test:

- `validate`: structural/version/native/canonical correctness;
- `same_scope`: whether comparison or advancement is meaningful;
- `equivalent`: exact restart-authority equality;
- `reaches`: monotone target comparison when supported;
- `join`: non-regressing aggregation of input and observed positions;
- `is_batch_slice_invariant`: whether canonical slicing preserves the settlement unit.

Required behavior:

- PostgreSQL same-scope reachability compares `end_lsn`; joins select the greatest committed end
  LSN and reject conflicting evidence for one coordinate.
- MySQL same-scope reachability compares file sequence/end position and verifies GTID-set
  non-regression; joins select the greatest legal coordinate.
- MongoDB reachability supports exact equality only. A source-provided terminal token may be used
  as the frontier for an explicitly ordered retained prefix, but generic unordered aggregation MUST
  reject divergent opaque tokens.
- mixed kinds, protocols, scopes, malformed coordinates, regressions, and conflicting evidence MUST
  fail before package/checkpoint publication.
- `aggregate_resource_output_position`, closed-position aggregation, drain termination, and any
  duplicate worker/frontier matcher MUST delegate to this authority rather than independently
  pattern-matching CDC variants.
- committed log positions are slice-invariant only while all slices remain one settlement unit.
  Mongo event-prefix positions MUST NOT be copied onto independently committable earlier prefixes.

## Artifact replacement

This is a single schema replacement with no backward compatibility:

- `SOURCE_POSITION_VERSION` becomes 2 and old `log/offset/sequence` JSON is removed;
- checkpoint state/store schema identities are bumped because they persist source-position JSON;
- portable source-position/checkpoint/task/result identities that embed the changed representation
  are bumped or otherwise replaced coherently so stale workers fail before execution;
- package manifest/state-preimage/processed-observation/quarantine/late-data identities that embed
  positions are replaced and their hashes change canonically;
- declarative source-position declarations and generated-schema identity are replaced; no legacy
  log declaration remains accepted;
- system-SQL, inspect/status/human rendering, JSON output, examples, and goldens render only the new
  typed forms;
- SQLite schema checks and row decoding accept only the new checkpoint/state versions;
- all current fixtures are regenerated/updated; none is retained to prove legacy compatibility.

Every old version MUST fail closed with a direct “current artifacts required; regenerate/recreate”
diagnostic. There MUST be no migration code because there are no customers or production artifacts
to preserve.

## Failure behavior

- authored malformed positions fail with `Contract` before execution;
- malformed/tampered persisted positions fail with typed artifact/data provenance before mutation;
- scope mismatch or regression fails before package finalization/checkpoint publication;
- unsupported Mongo ordering/frontier requests fail with direct remediation;
- stale artifact/worker versions fail before source contact or destination mutation;
- validation never logs raw token bytes beyond already-authorized artifact rendering.

## Acceptance scenarios

1. PostgreSQL positions round-trip the full unsigned LSN range and join monotonically by end LSN.
2. MySQL positions parse canonical file sequence and tagged/disjoint GTID sets, reject GTID
   regression, and join monotonically within one active-server lineage.
3. Mongo BSON bytes round-trip exactly through canonical JSON, tampering is detected, invalidate
   `start_after` remains distinct from `resume_after`, and arbitrary ordering is rejected.
4. Mixed kinds/scopes/protocols and conflicting evidence fail before checkpoint construction.
5. checkpoint aggregation and drain frontier reachability exercise the same position algebra.
6. old log declarations and all version-1 embedding artifacts fail; no legacy reader or migration
   exists in production source.
7. package, checkpoint, portable worker, declarative, system-SQL, inspect/status, and fixture
   representations agree on the new canonical forms and versions.
8. focused kernel/state/runtime/package/declarative/CLI/conformance tests cover their actual changed
   boundaries; one batched validation and one adversarial review are sufficient for closure.

## References

- `.10x/research/2026-08-03-cdc-protocol-position-contract.md`
- `.10x/specs/cdc-log-source-foundation.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/stream-epochs-watermarks.md`
- `.10x/decisions/kernel-owned-stream-epoch-policy.md`
- `.10x/knowledge/type-policy-authority.md`
- `.10x/knowledge/net-new-no-compatibility-policy.md`
