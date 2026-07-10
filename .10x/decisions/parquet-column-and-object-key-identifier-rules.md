Status: active
Created: 2026-07-10
Updated: 2026-07-10

# Parquet column and object-key identifier rules

## Context

The Parquet object-store destination sheet historically placed `object-key-component-v1` in `DestinationSheet.identifier_rules`. That value describes storage object-key components, not Arrow/Parquet column identifiers. The shared column-policy adapter correctly rejected it, leaving ordinary Parquet promotion unable to project residual paths into correction-sidecar columns.

P2 D-14 and `VISION.md` section 7.4 require source names to be preserved and destination column identifiers to be derived automatically through `namecase-v1` plus destination rules. Parquet also needs storage-key rules, but overloading one field makes extension unsafe: a future destination adapter cannot tell whether a rule governs tabular columns, schemas/tables, or object paths.

The user ratified the split on 2026-07-10. This decision completes the explicit Parquet exclusion recorded by `.10x/tickets/done/2026-07-09-p2-ws-c3-live-destination-normalization-duckdb-postgres.md`.

## Decision

`DestinationSheet.identifier_rules` is column-identifier authority only. Every tabular destination MUST provide a column policy there; code that interprets the field as an object/storage path rule is invalid.

Parquet columns use `namecase-v1`, with no destination-specific maximum length and the normal `namecase-v1` output character policy. Source identifiers remain in `cdf:source_name`; normalized collisions remain plan-time errors. This policy governs ordinary Parquet commits, correction-sidecar promoted fields, future versioned rematerialization, preview, package output schemas, and destination validation uniformly.

Object-store path components use a distinct typed optional `ObjectKeyRules` capability. Its first policy is `object-key-component-v1`. It MUST NOT be passed to the column `IdentifierPolicy` adapter. The Parquet destination sheet/protocol evidence carries both authorities separately: column rules in `identifier_rules`, object-key rules in `object_key_rules` (or the equivalent explicitly typed protocol-capability position). Serialization MUST preserve backward compatibility by making the new object-key position optional for destinations that do not address objects.

Adding or modifying a destination therefore requires explicit declaration of each namespace it uses. A relational destination usually declares only column rules; an object-store destination may additionally declare object-key rules. Destination code dispatches on typed rule positions and versions, never on destination names.

## Alternatives considered

Treat `object-key-component-v1` as a column normalizer.

- Rejected because it erases the namespace distinction and would let storage-path semantics leak into Arrow schemas.

Leave Parquet columns unnormalized or preserve arbitrary source names.

- Rejected because it violates D-14, makes Parquet behavior diverge from package/preview normalization, and reintroduces manual mappings/collisions.

Add a Parquet-only promotion rename helper.

- Rejected because column policy belongs in destination capabilities and must apply to ordinary writes, corrections, and future destinations uniformly.

Use one generic identifier rule with a `kind` switch.

- Rejected because a destination can have multiple namespaces simultaneously; separate typed positions are clearer and prevent accidental cross-use.

## Consequences

The kernel destination-sheet vocabulary gains a backward-compatible optional object-key rule type. Parquet moves `object-key-component-v1` into that position and declares `namecase-v1` for columns. Sheet hashes and dependent golden identities change intentionally.

Shared planning can now normalize Parquet correction-sidecar fields without a destination branch. Existing tests that feed object-key rules into the column adapter remain valid fail-closed regressions.

Conformance must prove ordinary Parquet output and correction sidecars use the same normalized column identity while object naming remains byte-stable under `object-key-component-v1`.
