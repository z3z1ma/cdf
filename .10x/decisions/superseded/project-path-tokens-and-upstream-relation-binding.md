Status: superseded
Created: 2026-08-04
Updated: 2026-08-04
Superseded-By: `.10x/decisions/project-path-tokens-and-upstream-relation-binding.md`
Completes: the focused D1.5/D3 checkpoints left by `.10x/decisions/filesystem-source-resource-and-configuration-authority.md`

# Project path tokens and `upstream(...)` relation binding

## Context

`.10x/decisions/filesystem-source-resource-and-configuration-authority.md` established the
filesystem and root configuration as source/resource authority, but deliberately left three
user-visible choices open: the exact path-token grammar, whether a configured source may exist
without resources, and how one SQL resource names its driver-owned relation.

The live source makes the relation problem broader than database table naming:

- PostgreSQL and SQLite require a table selector;
- ClickHouse requires a table and may require a stable key;
- REST requires a path and records selector and may require parameters/pagination;
- files require a glob and may require format/compression/discovery settings;
- Iceberg and Glue require structured namespace/table selectors and may require snapshot,
  partition, or format details.

A separate `USING public.orders` clause would privilege database-shaped sources and then require a
second extension surface for the other adapters. A fixed query alias such as `FROM source` would
hide rather than express the resource-specific relation arguments. The current driver boundary
already owns one closed `resource` option schema independently from its shared `source` option
schema, so that schema is the smallest existing authority capable of typing every relation shape.

The kernel's `ResourceId` accepts any non-empty string and destination name normalization is a
separate destination concern. Neither is suitable as project filesystem-token authority.

On 2026-08-04 the user confirmed all three recommendations exactly. CDF is net new and customer
zero, so this decision creates only the current grammar and no compatibility behavior.

## Decision

### Exact filesystem token grammar

Both the immediate source directory and the resource file stem MUST match:

```text
[a-z][a-z0-9_]{0,127}
```

Equivalently, each token is 1 through 128 ASCII bytes, begins with a lowercase ASCII letter, and
then contains only lowercase ASCII letters, digits, or underscores.

The compiler strips exactly one terminal `.cdf.sql` suffix before validating the resource token.
It preserves each accepted token byte-for-byte and forms exactly `<source>.<resource>`. It MUST NOT
case-fold, transliterate, normalize Unicode, substitute punctuation, call destination identifier
normalization, or accept quoted filesystem identities. Uppercase, hyphenated, Unicode, leading-
digit, leading-underscore, empty, and overlength tokens fail with the exact path and required
grammar. The leading-letter rule leaves underscore-prefixed `_cdf` names to CDF system authority.

Project path tokens receive dedicated types at the project/compiler boundary. Tightening the
general-purpose kernel `ResourceId` or reusing `SourceDriverId` is rejected because both represent
different domains and already carry broader internal values.

### Configured sources are never inactive

Every `[sources.<source>]` entry MUST join to an exact `sources/<source>/` directory containing at
least one valid regular `<resource>.cdf.sql` file. A configured source with a missing directory,
an empty directory, or no valid resource file is a blocking `Contract` diagnostic. There is no
`disabled`, `inactive`, `preconfigured`, warning-only, or environment-dependent escape hatch.

An otherwise empty project MAY contain no configured sources and no resources. Once source
configuration is authored, it represents active project authority and must have a resource.
`cdf add`/generation publishes a new source configuration and its first explicit resource files in
one crash-safe transaction; it never parks dead configuration for a later run.

### The query owns one typed `upstream(...)` relation

There is no separate relation clause and no compiler-provided `source`/`input` table alias. The
relational body MUST contain exactly one base table reference whose exact function name is
`upstream`:

```sql
CREATE RESOURCE
TARGET warehouse.issues
DISPOSITION MERGE
MERGE KEY (id)
CURSOR updated_at
TRUST GOVERNED
AS
SELECT id, state, updated_at
FROM upstream(table => 'public.issues')
WHERE state <> 'spam';
```

The path-bound source is resolved before the SQL relation. Its selected driver defines the closed
compile-time `upstream(...)` signature from its resource authority. Arguments use the named
`name => value` form shown above. Unknown, repeated, missing, or source-level arguments fail at
their exact source location; argument order is not semantic. Arguments lower through the ordinary
driver resource-option/`SourceCompileRequest` boundary rather than creating a second connector
configuration authority.

The complete literal/collection/tagged-variant grammar for structured resource arguments remains a
focused D3 checkpoint. It MUST be data-only, express the already-supported REST/files/Iceberg/Glue
resource shapes, validate through the selected driver's closed resource schema, and MUST NOT
become a generic top-level option bag, secret surface, or arbitrary row-expression/function
evaluator. The manifest separately retains authored SQL identity and canonical upstream
relation/resource-option identity.

### Ratified core envelope spelling

The CDF-owned envelope begins with bare `CREATE RESOURCE`; neither token is followed by an id. The
confirmed core form and order is exactly the example above: `TARGET`, `DISPOSITION`, conditional
`MERGE KEY`, optional `CURSOR`, `TRUST`, then `AS <SELECT body>`. A clause appears at most once;
contradictory, missing required, or out-of-order core clauses fail rather than normalize silently.

The exact placement/value grammar for additional already-named policy clauses such as
`PRIMARY KEY`, `CONTRACT`, and `EXECUTION`, plus semantic annotations, remains a focused D3
checkpoint. It cannot add a SQL resource/source id, add a separate relation clause, or alter
path/source/`upstream(...)` authority.

## Alternatives considered

### Permit kebab-case, Unicode, or case normalization

Rejected. Those forms require quoting/case/collision policies across filesystems, TOML, SQL, CLI
selectors, artifacts, and destinations. Silent normalization would create a second identity and
reuse of destination normalization would leak a downstream constraint into project authority.

### Allow inactive configured sources

Rejected. An inactive state adds lifecycle/default semantics for dead configuration and makes a
missing directory ambiguous with intentional staging. Atomic `cdf add` publication handles the
legitimate source-plus-first-resources workflow without another state.

### Put the relation in a separate `USING` clause

Rejected. A scalar relation token fits relational databases but not the structured selectors
already required by REST, files, Iceberg, and Glue. Adding driver-specific envelope clauses would
make the CDF parser an open connector grammar.

### Use a generic `WITH (...)` map

Rejected. It would recreate the spike-era resource option map in SQL and blur source-level versus
resource-level authority. `upstream(...)` is a table-valued relation with a closed driver-selected
signature, not an untyped metadata bag.

### Infer the relation from the resource filename

Rejected. `orders` need not select `public.orders`, a REST path, an Iceberg snapshot, or a file
glob. The path owns CDF identity; `upstream(...)` owns upstream relation identity.

## Consequences

- Source/resource paths are predictable unquoted snake-case identities with no collision
  normalizer or destination leakage.
- Every configured source contributes at least one resource to compilation and manifest output.
- The relational query visibly contains its one upstream relation while source configuration
  remains written exactly once in `cdf.toml`.
- Existing driver resource schemas remain the relation-type authority; no parallel connector
  grammar registry or single-implementation abstraction is introduced.
- D1.5 can validate project paths/configuration and D3 can parse/lower one common relation form
  across every source type.
- The spike-era resource maps/declarations and all compatibility readers remain excluded.
