Status: active
Created: 2026-08-03
Updated: 2026-08-03

# Destination introspection, package scope, and current replay policy

## Context

`.10x/decisions/superseded/destination-introspection-package-and-cli-policy.md` established three
sound boundaries: destination introspection is standard safety evidence rather than semantic
inference; `parquet://` names a destination root/prefix; and one package represents one resource
transition inside a possibly multi-resource run. It also required Postgres run/replay callers to
select `merge_dedup` after package construction.

The later package-global dedup implementation already made package construction the winner
authority. The ratified package-native keyed-effect decision now requires one final effect per key
across upserts and deletes before package finalization. Allowing a destination or replay caller to
choose `first`/`last` would change package meaning after hashing and make replay non-deterministic.

## Decision

Destination introspection remains standard behavior wherever a destination can support it. It
SHOULD be used for safety checks, drift detection, migration/load planning, receipt verification,
capability validation, and actionable failures. It MUST NOT infer missing write semantics such as
target identity, disposition, key authority, effect-reduction policy, delete-application policy,
resource identity, or checkpoint semantics.

`parquet://<root>` remains the CLI/project URI spelling for a local filesystem Parquet destination
root or prefix. It names an object tree, not one file. Relative roots resolve beneath the selected
project root; absolute roots are permitted. Empty roots and nested non-filesystem URI values are
invalid.

A package represents one resource transition. A run is an orchestration envelope that may contain
one or many resource transitions and therefore one or many packages.

All merge/keyed-effect winner selection occurs before package finalization under the resource
contract and `.10x/specs/package-keyed-delete-effects.md`. Ordinary unordered merge duplicates
default to `fail`; explicit first/last requires compiled authoritative input order; CDC uses
protocol-ordered last-change-wins. The recorded finalized effect set and reduction evidence are
package identity.

No destination-specific `merge_dedup` setting or CLI `--merge-dedup` input exists in the current
format. Destinations reject duplicate finalized effect keys as corrupted input; they do not select
winners. Verified replay consumes the package's final effects and recorded delete-application
policy without accepting a caller override.

Replay target selection remains explicit where the command/destination contract requires it. A
supplied target MUST match the package's recorded destination-commit target. Destination
introspection MAY verify compatibility but MUST NOT change target, disposition, keys, effect
reduction, or delete application.

## Alternatives considered

### Retain destination `merge_dedup = "fail"` only as a safety option

Rejected. Duplicate finalized package effects are corruption, not policy. An optional setting
suggests other winner modes remain legal after package finalization and duplicates safety logic
already required at the package and destination boundaries.

### Permit replay to select only the same recorded value

Rejected. Requiring a caller to restate identity already present in a verified package adds a
failure surface without authority. Exact package verification is the stronger check.

### Infer package policy from the live target

Rejected. Mutable destination state may prove compatibility, not decide source/resource meaning.

### Make destination introspection opt-in

Rejected. Introspection remains ordinary safety and verification behavior wherever capability
permits it.

## Consequences

This decision supersedes
`.10x/decisions/superseded/destination-introspection-package-and-cli-policy.md`. Its introspection,
Parquet URI, and resource-scoped package conclusions are retained; its destination/CLI merge-dedup
inputs are removed under the current-format-only policy.

Project models, CLI grammar, replay/recovery inputs, docs, tests, and fixtures will be replaced with
the current package-owned authority when the package keyed-effect implementation executes. No
ignored compatibility flag or parser alias is permitted.
