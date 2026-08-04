Status: active
Created: 2026-08-03
Updated: 2026-08-03

# SQL resource envelope and profile boundary

## Context

CDF needs explicit SQL-shaped resource files without embedding operational configuration,
credentials, or runtime DataFusion plans. A pure `SELECT` plus companion metadata fragments one
resource across files, while a generic option map inside SQL recreates TOML with weaker typing.

## Decision

SQL resources live at `resources/**/*.cdf.sql`. Each file contains exactly one CDF-owned typed
`CREATE RESOURCE ... AS SELECT ...` statement and declares its canonical resource id explicitly.
CDF parses the envelope; DataFusion parses/analyzes only the `SELECT`; CDF lowers the accepted
subset into native versioned IR.

Typed envelope clauses own source profile/relation, logical destination target, disposition,
primary/merge keys, cursor, contract/trust, and execution facts. Unknown, duplicate,
contradictory, or out-of-order clauses fail. There is no generic `WITH` map, metadata header,
companion declaration, filename-derived resource id, embedded connection URI, or credential.

Named connection profiles, driver options, policy, and secret references remain typed `cdf.toml`
authority validated through driver schemas. The selected environment owns the destination
connection; SQL may name only the logical target. Existing declarative resources remain a peer
front-end until separately removed.

## Alternatives considered

### Standard SELECT plus companion TOML/YAML

Rejected. It creates synchronization and publication problems, weakens the “one explicit resource
file” mental model, and makes source locations for cross-file errors worse.

### SQL metadata comments or headers

Rejected. Comments are poor typed configuration authority and invite parsers that disagree about
which text is semantic.

### Generic `WITH (key = value)` options

Rejected. Arbitrary maps conceal clause types, allow adapter configuration to leak into SQL, and
recreate the current configuration spike inside a less suitable syntax.

### Runtime DataFusion plans

Rejected. DataFusion is compile-time analysis authority only. Runtime identity and execution remain
native CDF artifacts.

## Consequences

The SQL compiler needs one small envelope parser, exact source locations, a narrow native IR, and a
profile-aware project model. Filename moves do not silently rename resources. Declarative and SQL
origins differ in the manifest while equivalent lowered execution identities can match.
