Status: recorded
Created: 2026-08-03
Updated: 2026-08-03

# C1 semantic-boundary error ownership

## Observation

The exact seven-file behavior-owning scope contains six `Internal` construction sites and one
test-only `ErrorKind::Internal` assertion. All six constructors retain framework ownership only
for impossible CDF-owned state: validated descriptor serialization, invalid static built-ins,
compiled authority bypass, concrete Arrow array contradiction, or private package-order
contradiction.

Authored semantic reference, definition, parameter, Arrow-compatibility, and mapping failures are
`Contract`. Unapproved source-observed semantic metadata and finalized package semantic
contradictions are `Data`. No foreign adapter error is flattened or reclassified by this tranche.

## Procedure

The frozen file manifest is
`.10x/evidence/.storage/2026-08-03-c1-error-ownership-files.txt`. The per-site result is
`.10x/evidence/.storage/2026-08-03-c1-error-ownership-ledger.tsv`.

Reproduce the exact `Internal` scan from the repository root:

```sh
xargs rg -n -- \
  'CdfError::internal|CdfError::new\(ErrorKind::Internal|ErrorKind::Internal' \
  < .10x/evidence/.storage/2026-08-03-c1-error-ownership-files.txt
```

Arithmetic: seven scoped files; three site-bearing files; seven syntactic matches; six error
construction sites plus one test assertion. The other four scoped files contain no `Internal`
constructor or direct-kind match.

## What it supports or challenges

This supports C1 acceptance criterion 4: the same semantic invalidity is classified by the
authority that introduced it, and only a compiler/runtime invariant breach becomes `Internal`.
It also supports the no-compatibility policy because no fallback reader or legacy resolution path
appears in the scope.

## Limits

The audit freezes the seven constructor-owning Rust files changed at the semantic boundary. The
engine residual consumer adds no semantic error constructor: it now propagates the registry's
source-observed `Data` result instead of suppressing it. The audit does not reclassify unrelated
constructors in that large orchestration module, migrated fixtures, CLI tests, or unchanged adapter
modules. Line numbers identify this checkpoint and may move in later commits; the frozen manifest
and reproduction pattern remain authoritative.
