Status: active
Created: 2026-08-03
Updated: 2026-08-03

# Package-native keyed delete effects

## Context

CDF packages currently carry one homogeneous family of complete logical rows. `merge` interprets
those rows as keyed upserts, while the draft CDC design proposed retaining a row-level `_cdf_op`
column and representing deletes as full-schema rows with only key fields populated. That shape is
not a sound handoff contract:

- a delete identifies a logical row by key and does not possess values for every non-key field;
- forcing key-only deletes through the complete output schema invents nulls for non-nullable data;
- retaining `_cdf_op` in ordinary destination rows leaks execution control into the resource
  schema;
- delete facts are useful outside log CDC, including optional deletion feeds exposed by SaaS and
  API sources;
- hard delete, soft delete, and intentional non-application are destination/resource semantics,
  not source semantics;
- append-only event history and current-state synchronization are different products and should
  not be overloaded into one disposition.

CDF already has the necessary exact typed-key encoding, package-global spillable deduplication,
package identity, receipt, replay, and checkpoint gates. The missing boundary is a package-native
logical effect model that those mechanisms can govern.

## Decision

CDF packages distinguish ordinary row content from keyed-change content. `append` and `replace`
consume ordinary rows. `merge` and `cdc_apply` consume keyed changes containing two explicit,
manifest-typed effect families:

- `upsert`: one complete output row whose declared key identifies the destination row;
- `delete`: exactly the declared, mechanically derived key tuple.

Effect kind is identity-bearing package authority. It is carried by the manifest, segment
identity, state delta, destination commit request, staged-ingress acknowledgement, receipt, and
verification path. It is never inferred from a pathname or from nullable data fields.

A keyed-change package is a final keyed state transition, not an event log. Package construction
must leave at most one effect for every exact typed key:

- ordered CDC uses last-change-wins under source-protocol order;
- an ordinary merge without authoritative duplicate ordering fails on duplicate keys;
- an ordinary merge may use explicit `first` or `last` only when its compiled input order is
  authoritative for that choice.

After winner selection, canonical package ordering is versioned and deterministic rather than
source-event order. Every surviving effect is grouped into its typed physical family and ordered
by CDF's exact encoded key. Append-only consumers that require every intermediate source event
must model that history as an ordinary append resource instead of using `cdc_apply`.

Delete capture and delete application are separate compiled choices:

- a source declares whether deletion capture is unsupported, optional, or inherent and records
  the selected capture behavior in source-plan identity and coverage evidence;
- every delete-capable `merge` or `cdc_apply` binding explicitly selects `ignore`, `hard`, or
  `soft`; there is no implicit application default;
- captured deletes remain package content even under `ignore`, preserving the package as the
  truthful handoff artifact.

Package deletes are equality-by-declared-key only. Arbitrary predicates, ranges, source storage
positions, and destination-native expressions are not package delete effects.

`hard` removes matching target rows. `soft` is initially Boolean-marker-only: the destination
binding names one destination-owned, non-null Boolean marker; delete preserves every other field
and marks an existing row `true`; an absent row remains absent; every later upsert writes the
complete row and forces the marker to `false`. Automatic deletion timestamps and sparse tombstone
insertion are excluded until their time/value authority is separately ratified. Deleting an absent
key is a successful idempotent no-op for both hard and soft application.

The logical package never encodes hard versus soft. The selected application policy is carried by
the identity-bearing destination commit plan and lowered by a capability-proven destination
implementation.

Receipts distinguish exact package intent from destination-observed outcomes. They always record
and acknowledge exact upsert/delete effect counts and typed effect segments. Inserted, updated,
hard-deleted, soft-deleted, and missing-key outcomes are reported only where the destination can
prove them truthfully and economically; ignored delete count is exact under `ignore`.

## Alternatives considered

### Persist `_cdf_op` beside every complete output row

This preserves one Arrow schema and direct source event order. It is rejected because key-only
deletes have no truthful values for non-key fields, nullable placeholders contradict the output
contract, and a control column leaks into destination data. Keeping an internal operation signal
through decode/validation is still permitted; package construction must lower it into typed
effects.

### Store an Arrow union envelope containing operation, key, and after-image

A dense union can represent heterogeneous row shapes without invented nulls and can retain every
event in one physical stream. It is rejected for the current-state handoff because it makes every
destination decode a nested event protocol, blocks straightforward native bulk loading, retains
intermediate events that do not affect final state, and pushes unsupported union handling into
otherwise primitive destination schemas.

### Preserve an ordered event log in `cdc_apply` packages

This is valuable for audit/event destinations and truthfully mirrors source history. It is
rejected as the meaning of `cdc_apply`: destination current-state synchronization needs the final
effect per key, and retaining all intermediate events prevents independent high-throughput bulk
upsert/delete lowering. The use case remains available through an explicit append resource.

### Encode deletes as destination predicates or native statements

This can compact large ranges and exploit destination-specific engines. It is rejected because
predicate meaning, null equality, collation, schema, and execution safety vary across destinations;
the package would cease to be portable or independently verifiable. A future separately typed
predicate-delete capability would require its own cross-destination contract.

### Let each source or destination define its own tombstone schema

This minimizes central artifact changes. It is rejected because packages are the handoff point:
source-specific tombstones and destination-specific delete files would create leaky abstractions,
duplicate dedup/replay logic, and make the same deletion fact change meaning across routes.

### Default to hard delete or silently ignore deletes

Either choice invents a destructive or completeness semantic. It is rejected. Delete application
must be explicit for every delete-capable merge/CDC binding.

## Consequences

The package manifest, segment/state identities, destination commit request, staged-ingress
identity, receipts, verification, archive/export, inspection, and golden fixtures require one
clean current-format replacement. CDF is customer zero; no optional legacy fields, compatibility
readers, or migrations are admitted.

`merge` gains one package-native effect model that can later accept deletion feeds from Salesforce
and other sources without source/destination coupling. CDC reuses it while retaining distinct
source-position, transaction-boundary, and protocol-order authority.

Destinations may implement hard and soft application with transactions, staging tables, joins,
native merge engines, tombstones, copy-on-write, or another physical mechanism only when their
sheet and receipt verification prove the selected logical result. Physical tombstones do not
silently turn logical hard deletion into logical soft deletion.

Package construction owns exact cross-effect key resolution before finalization. Destinations
retain duplicate-key checks as a safety fence but do not choose winners. The existing spillable
typed-key implementation is the intended execution substrate, extended to compare upserts and
deletes in one winner domain.

This decision deliberately precedes package availability/partiality and the finalized-only versus
staged-ingress streaming design. Both ingress modes must consume the same keyed-effect contract.
