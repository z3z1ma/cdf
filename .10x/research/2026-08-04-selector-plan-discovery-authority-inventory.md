Status: done
Created: 2026-08-04
Updated: 2026-08-04

# Selector, portable-plan, and discovery authority inventory

## Question

How should CDF add multi-resource selectors, a stored plan that can execute on another machine
after thin revalidation, and adapter-owned source/resource discovery with optional thin-resource
generation without degrading the existing terminal plan experience or collapsing source identity?

## Sources and methods

- Inspected current `plan`/`run` argument and report construction in `cdf-cli-core` and `cdf-cli`.
- Inspected `CompiledSourcePlan`, driver-owned portable-plan validation, portable partition task
  capsules, worker artifact references, source generation/checkpoint bindings, and host admission.
- Inspected `SourceDriver::discovery_session`, shared discovery observations, `SourceAddPlanner`,
  source extension contracts, catalog-task source lifecycle, and current add/discover CLI behavior.
- Applied `.agents/skills/audit-cli-report-authority/SKILL.md` and read
  `.10x/knowledge/cli-report-authority.md` because the proposal adds a success artifact while
  preserving JSON/human report authority.
- Did not mutate product code, the sandbox, a source, or a destination.

## Findings

### Current plan output is a report, not execution authority

`ScanPlanReport` is a typed serializable success report and already drives both JSON and the
renderer-owned human document. It contains useful schema, fetch, pushdown, destination, DDL,
guarantee, state-advancement, explain, operator, scheduler, and package facts. It does not contain
the complete source/resource/validation/normalization/state/destination authority needed to execute
without replanning.

The report and a portable plan artifact are distinct contracts. `--json` selects machine rendering
of the command report. `--out <path>` should explicitly persist a separately versioned execution
artifact while the same existing report continues to render to the terminal and records the
artifact path/hash as an effect.

### Most low-level portability checks already exist

`CompiledSourcePlan` is serializable and hash-bound. `SourceDriver::validate_portable_plan` lets
each adapter fail closed on coordinator-local or otherwise nonportable physical plans. Portable
partition tasks already bind:

- CDF/artifact/Arrow/relational-engine/normalizer compatibility;
- source driver/version/option-schema/options/physical/semantic identities;
- project/resource/plan/partition/segment authority;
- output schema, validation, normalization, expression, operator, segmentation, and extent
  artifacts;
- checkpoint state, source generation, host budgets/capabilities, secret references, and typed
  content-store references.

Workers reconstruct and revalidate these facts without recompilation. The missing layer is a
versioned top-level resource-set plan that also binds selection, destination targets/capability
sheets, checkpoint heads, cross-resource execution policy, and artifact references.

### Thin preflight must validate, never repair

A stored plan is useful only when run refuses to reinterpret it. Preflight must validate artifact
hash/version/security, resource/project input hashes, registry compatibility, referenced artifacts,
destination configuration/sheet, exact checkpoint heads, source generations through adapter-owned
metadata checks, and host requirements before any package/destination/state mutation.

Preflight cannot refresh, rediscover, substitute partitions, accept schema drift, update a lock, or
recompile. Any semantic difference requires a new `cdf plan ... --out ...`. Runtime scheduling may
tighten nonidentity concurrency/memory/I/O ceilings while preserving the selected logical work and
canonical order.

There should be no arbitrary universal TTL. A source adapter either proves exact generation
revalidation, supplies a typed `not_after` bound when its protocol requires one, or rejects portable
plan export for that resource.

### Lock authority is the schema fence, not a discovery cache

The product-facing purpose of a resource lock entry is to freeze the governed output schema and
the semantic/compiler facts that make that schema meaningful. Planning from a locked resource must
not treat a fresh source observation as permission to add columns or derive destination migration.

“Intersection” is directionally correct but too lossy as a literal set operation. The compiled
reconciliation must preserve the locked output schema while classifying observations:

- observed locked field with compatible type: admit;
- observed extra field: drift/residual evidence under locked policy, never a new output column;
- missing locked field: preserve the locked field and apply its nullable/required/control verdict,
  never silently remove it;
- type difference: apply only already locked coercion/admission behavior or fail/quarantine.

Only explicit schema promotion replaces the lock baseline and can authorize new destination DDL.
First-use plan may propose/freeze a baseline without writing; direct run or run-from-plan commits
that exact candidate before execution.

### A resource selector can expand without compiling unrelated files

Path-derived resource ids allow exact selectors to resolve directly and glob selectors to expand
by enumerating only accepted resource paths. Expansion does not require parsing SQL, validating
source options, resolving secrets, or contacting sources. Exact and glob selectors can therefore
preserve the resource-first boundary.

The selection artifact should retain both the authored selectors and the canonical sorted resolved
resource ids. Exact/glob union, repeated `--exclude`, duplicate elimination, and zero-match errors
are sufficient; a general boolean query language is unnecessary.

### Multi-resource execution has two distinct failure barriers

Preparation/preflight is no-side-effect work and should complete for the whole explicitly selected
set before any destination mutation. If any member fails preparation, no member executes and no
portable plan file is published.

After execution begins, no cross-resource transaction exists. Independent resources may already
have durable packages, receipts, or checkpoints when another fails. Continuing untouched
independent resources and returning an aggregate nonzero result is more honest and useful than
pretending fail-fast restores atomicity. Shared checkpoint/destination scopes still obey their
existing serialization and receipt gates.

### Existing discovery is resource-plan scoped, not source-catalog scoped

`SourceDriver::discovery_session` receives a compiled resource plan and owns candidate schema
observations. That is the correct substrate for explicit discovery over authored CDF resources.

`SourceAddPlanner` can propose one add from a supplied location but does not enumerate a configured
source's catalog. Source-level discovery needs an optional adapter capability that emits bounded,
redacted canonical upstream-relation candidates, complete driver-owned `upstream(...)` arguments,
and safe resource-name proposals. Generic code must not guess database tables, object prefixes,
API endpoints, cursors, keys, or source-specific identity.

### Configured source, upstream relation, and CDF resource must stay distinct

For example:

```text
configured source: warehouse
upstream relation: analytics.customers
CDF resource:      marts.customer_dimension
```

The CLI should therefore use explicit nouns:

- `cdf discover source warehouse ...` enumerates/filter upstream relations;
- `cdf discover resource 'marts.*'` observes authored CDF resources.

Both may share a glob syntax, but selectors operate in different identity spaces and reports must
label them. `source.resource` shorthand would incorrectly imply that a CDF resource namespace is
its configured source.

### Discovery can safely generate thin resources when writing is explicit

Read-only source discovery can render candidate-to-path proposals. `--generate` can explicitly
publish matched candidates as thin current SQL:

```sql
SELECT *
FROM upstream(source => 'warehouse', relation => 'analytics.customers');
```

The adapter owns the canonical relation argument shape and suggested resource token. The generic
layer owns namespace selection, token/path validation, exact create-or-verify behavior,
transactional publication, collision reporting, and the typed success report. Generation does not
pin, compile, or contact a destination.

## Conclusions

The requested experience is compatible with existing architectural seams when the top-level plan
is an immutable orchestration artifact over existing portable source/task authorities, rather than
a serialized display report. Discovery similarly becomes coherent when source catalog enumeration
and authored-resource schema observation are explicit sibling scopes.

The terminal plan document should not be redesigned for artifact export. It remains the human
interface; artifact creation is an additional typed effect with its own schema and consumer.

## Limits

This inventory does not ratify selector glob/exclusion grammar, aggregate run failure policy,
portable-plan portability requirements, preflight all-or-none behavior, discovery command spelling,
or partial-success generation semantics. No external adapter catalog was contacted.
