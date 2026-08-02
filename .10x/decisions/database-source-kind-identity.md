Status: active
Created: 2026-08-02
Updated: 2026-08-02

# Database source kinds are concrete driver identities

## Context

`SourceRegistry` uses `SourceDriverDescriptor.kinds` as a unique configuration and construction
selector. Registration intentionally rejects duplicate kinds. The shipped Postgres adapter had
claimed the broad kind `sql`, and the initial SQLite specification repeated that value. Catalog
enrollment then failed because two drivers cannot truthfully own one selector.

The user ratified a unique SQLite kind and explicitly required Postgres to stop owning `sql` in the
same tranche on 2026-08-02.

## Decision

First-party database source kinds MUST be concrete source-family identities, normally identical to
their driver ids:

- Postgres: `postgres`
- SQLite: `sqlite`
- ClickHouse: `clickhouse`
- MongoDB: `mongodb`

`sql`, `document`, and similar broad categories are capabilities or descriptive metadata, not
registry selectors. A driver MUST return its concrete kind from `cdf add`, discovery metadata, and
compiled source identity. Project examples, fixtures, generated schemas, catalog artifacts, and
lock evidence MUST use that same concrete kind.

Because CDF is pre-production, the obsolete Postgres `sql` selector is removed rather than retained
as an alias. Generic/synthetic tests may still use the string `sql` when intentionally testing an
arbitrary kind; only Postgres identity-bearing uses change.

## Alternatives Considered

### Let Postgres continue to own `sql`

Rejected. It prevents another SQL-speaking adapter from registering and confuses a protocol
capability with a concrete construction authority.

### Dispatch one `sql` kind by a second `dialect` field

Rejected. This would change registry lookup, schema composition, `cdf add` ownership, ambiguity
errors, plan identity, and third-party extension rules for no current requirement. It would also
make the kind cease to be a complete selector.

### Retain `sql` as a Postgres compatibility alias

Rejected. The active pre-production current-only policy forbids compatibility branches for an
unshipped selector.

## Consequences

Each database adapter can register independently without generic orchestration changes. Source
configuration and generated artifacts become explicit and unambiguous. Existing Postgres examples
and tests must move from `kind = "sql"` to `kind = "postgres"`; pre-production artifacts using the
old value are intentionally not supported.
