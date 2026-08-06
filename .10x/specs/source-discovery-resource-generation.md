Status: active
Created: 2026-08-04
Updated: 2026-08-05

# Source discovery and resource generation

## Purpose

This specification defines discovery as an adapter-owned, read-only product lifecycle with two
explicit scopes: configured-source catalog discovery and authored-resource schema discovery. It
also defines an explicit generation mode that turns selected source candidates into thin CDF SQL
resources without pinning or compiling them.

## Identity model

CDF MUST keep distinct:

- configured source name from `[sources.<name>]`;
- driver-owned upstream relation identity such as table, collection, object/prefix, endpoint, or
  other selectable source entity;
- path-derived authored CDF resource id.

Reports and artifacts name all applicable identities. A CDF namespace MUST NOT be inferred to be a
configured source, even when generation defaults them to the same token.

## Source discovery

```text
cdf discover source <configured-source> [relation-selector...]
cdf discover source <configured-source> [relation-selector...] --out discovery.json
cdf discover source <configured-source> [relation-selector...] --generate [--namespace <name>]
```

The selected source driver optionally exposes a bounded catalog-discovery capability. It owns:

- authentication, pagination, rate/egress behavior, and cancellation;
- canonical upstream relation ids and safe display labels;
- stable generation/evidence when the protocol provides it;
- relation kind/capabilities and bounded schema summary when available;
- complete canonical driver-owned `upstream(...)` arguments for a generated thin resource;
- a deterministic suggested CDF resource token or an explicit “manual naming required” result.

The generic runtime owns budgets, redaction, cancellation, result accounting, deterministic
ordering, relation-selector filtering, artifact schema, renderer, and conformance laws. A driver
without source-catalog discovery fails with a precise unsupported-capability diagnostic; generic
code does not imitate it.

Relation selectors use the same exact/glob operators as CDF resource selectors but match canonical
adapter-emitted upstream relation ids. Reports label this identity space. Every positive selector
must match. Without a selector, discovery may render a bounded first page plus continuation and
truncation evidence. Generation requires a complete deterministic matched set; a truncated result
must be narrowed or explicitly continued before writing.

## Authored-resource discovery

```text
cdf discover resource <resource-selector...>
cdf discover resource <resource-selector...> --out discovery.json
```

This command expands CDF resource selectors, compiles only those resource bindings, and invokes the
existing driver-owned `SourceDiscoverySession` for bounded schema observation. It renders selected
candidates, both coverage axes, observed schema, source generation/evidence, budgets, and warnings.

It writes no schema snapshot, lock, compiled resource artifact, project manifest/index, package,
state, destination, receipt, or checkpoint. `--out` writes only the explicitly requested canonical
discovery artifact. Plan may reuse the same lifecycle in memory; run/compile may use it to establish
first-use authority under their own write contracts.

## Thin-resource generation

`--generate` is explicit local project mutation attached only to source discovery. For every
matched candidate with a valid proposal, it creates:

```text
cdf/<namespace>/<resource-token>.cdf.sql
```

The default namespace is the configured source name. `--namespace <name>` replaces that default
with an exact valid namespace token; it does not rename or reconfigure the source.

Generated SQL exposes the discovered top-level field inventory so the file is immediately useful
for projection editing and per-field expressions:

```sql
SELECT
  "order_id",
  "updated_at"
FROM upstream(source => '<configured-source>', <driver-owned relation arguments>);
```

Discovery-order top-level field names are safely quoted and MUST NOT be expanded into nested-field
projections. `SELECT *` is the explicit fallback only when the adapter cannot supply any schema
summary; the report identifies that fallback. No destination URI, credentials, secret
values/references, explicit resource id, compatibility metadata, or speculative policies are
generated. Path authority supplies the CDF resource id. First plan/run supplies applicable
defaults through ordinary compilation.

The adapter proposes a resource token because it owns relation structure. The generic layer
validates the strict token, path uniqueness, and deterministic candidate-to-path mapping. When two
candidates propose the same target or a safe token cannot be produced, both remain visible but
require narrower selection or a future explicit one-candidate naming option; generic code does not
silently normalize or suffix names.

Targets are create-or-verify:

- absent target: create;
- existing byte-identical generated query: unchanged;
- existing nonidentical file, directory, symlink, or escaped path: conflict and never overwrite.

Nonconflicting candidates are transactionally published and retained even when other candidates
conflict; the aggregate exits nonzero and reports every created/unchanged/conflicted candidate.
Generation uses the guarded multi-file project publication authority and exact candidate/source
generation preconditions so catalog drift between introspection and write cannot change the set.

Generation does not write `cdf.lock`, schemas, compiled artifacts, or the project index and does
not contact a destination. `--namespace` without `--generate` is a usage error.

## Relationship to `cdf add`

Discovery generation owns many-resource authoring from an already configured source. `cdf add`
remains only for bootstrapping one explicit external location/source proposal that is not yet
represented by `[sources.<name>]`. It MUST NOT grow a second configured-source catalog enumeration
or batch-resource generation path.

## Reports and artifacts

Source/resource discovery constructs one typed report used by human and JSON output. `--out` adds
an artifact effect. `--generate` adds ordered candidate-to-path effects. Redaction happens before
both paths. The terminal renderer preserves the established outcome-first CLI language and clearly
separates candidates, evidence, and effects.

The discovery artifact is versioned, canonical, bounded, secret-redacted, and contains configured
source identity/hash, adapter/version/schema, identity-space label, authored selectors, canonical
candidate/observation evidence, budgets/coverage/truncation, generation facts, and content hash. It
is evidence/proposal, never execution or lock authority.

## Acceptance scenarios

1. Read-only source discovery lists adapter-owned relations and writes nothing.
2. A glob selects several relations; generation creates thin files under the source-named
   namespace, then plan selectors discover those new path-derived resources.
3. `--namespace raw` changes only output paths; every generated `upstream(source => ...)` retains
   the configured source identity.
4. One existing conflicting file does not overwrite or block independent candidates; output names
   every created/unchanged/conflicted result and exits nonzero.
5. A paginated/truncated catalog cannot generate an implicitly partial matched set.
6. Resource discovery over two resources invokes only their drivers and produces schema evidence
   without pin/lock/project writes.
7. A source requiring secrets proves raw values absent from terminal, JSON, artifact, generated
   SQL, errors, and debug output.

## Acceptance criteria

- A new source adapter adds catalog discovery through its own crate/registry entry without generic
  source-id matches.
- Shared conformance covers bounds, pagination, cancellation, selectors, generation drift,
  redaction, unsupported capability, and candidate naming/collision behavior.
- Files, at least one relational catalog, and one non-tabular source prove truthful distinct
  discovery shapes.
- Guarded publication failpoints prove create-or-verify safety and forward recovery.
- CLI report-authority/parity tests cover read-only, artifact, generated, unchanged, partial
  conflict, and unsupported outcomes.
- `schema discover` is absent; no compatibility alias or second generation path remains.

## Explicit exclusions

- inferring keys, cursors, dispositions, contracts, or destination policy from weak evidence;
- generic table/collection/object identity;
- source configuration or secret generation;
- overwrite/force behavior;
- compile/pin/promotion during discovery;
- treating discovery artifacts as execution authority.

## Ratification status

The user confirmed the two discovery scopes, read-only default, explicit thin-resource generation,
namespace override, and useful partial generation success on 2026-08-04. On 2026-08-05 the user
superseded star-first generation: generated resources make a best effort to enumerate discovered
top-level fields, with `SELECT *` only when schema is unavailable.
