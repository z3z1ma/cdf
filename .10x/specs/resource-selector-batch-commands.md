Status: active
Created: 2026-08-04
Updated: 2026-08-04

# Resource selectors and multi-resource commands

## Purpose

This specification defines deterministic resource-set selection for plan, run, compile, validate,
and resource discovery. It extends the resource-first model from one resource to an explicit set
without restoring whole-project coupling.

## Selector grammar

Commands accept one or more positive positional selectors:

```text
cdf plan local.events 'fineweb.*'
cdf run 'warehouse.*' --exclude warehouse.experimental
```

A selector is either:

- an exact canonical resource id; or
- a glob over the complete canonical resource id using `*`, `?`, and bracket character classes.

`*` does not receive source-specific meaning. Resource ids contain no path separator, so `**` is
not a distinct operator. Shell-sensitive selectors SHOULD be quoted in help/examples.

Repeated `--exclude <glob>` applies after the positive union. Exclusions do not introduce
resources. Duplicate matches are removed. The resolved resource set is sorted by canonical
resource id and is independent of argument, filesystem, or hash-map order.

Every positive selector MUST match at least one current path-derived resource. An exact miss gets
nearest-id suggestions; a glob miss names that glob. Exclusion matching zero is permitted. An empty
final set is a usage error.

Plan and run require at least one selector unless run receives `--plan`. An explicit `'*'` means
all resources; omission never accidentally contacts or executes the whole project.

## Expansion boundary

Exact ids resolve directly to `cdf/<namespace>/<resource>.cdf.sql`. Glob expansion enumerates and
validates only path shape/token identity under `cdf/`. It MUST NOT parse resource SQL, validate
unselected driver options, resolve unselected secrets, or contact any source.

After expansion, each selected resource is independently parsed and prepared through the
resource-first boundary. A malformed unselected file cannot block the set.

The typed selection record contains authored positive/exclusion selectors plus the canonical
resolved resource ids. Portable plans retain both for audit; execution uses only the exact resolved
ids and hashes.

## Multi-resource plan

`cdf plan SELECTOR...` prepares every selected resource without writes and renders:

- one aggregate selection/readiness summary;
- the existing plan document facts for each resource in canonical order;
- all preparation failures, each owned by its resource.

Plan MUST attempt every selected resource. If any resource fails, the command exits nonzero. When
`--out` is requested, no portable plan artifact is published unless every selected resource
prepares successfully; an incomplete artifact must not silently narrow the requested run.

## Multi-resource run

`cdf run SELECTOR...` MUST finish preparation and preflight for the entire selected set before any
package, destination, receipt, checkpoint, or run-ledger mutation. Any preparation failure prevents
execution of the complete set.

After the barrier, resources execute as independent runs under shared process CPU/memory/I/O/source
rate/destination limits. Existing checkpoint-scope and destination serialization remains
authoritative. A runtime failure in one resource does not cancel untouched independent resources.
The command records every terminal resource result and exits nonzero if any failed.

CDF does not claim cross-resource atomicity. Successful receipts/checkpoints remain durable and are
reported as such. The default `continue_independent` failure policy is identity-bearing in a
portable resource-set plan; no `--fail-fast` variant ships in the first implementation.

## Multi-resource compile and validate

Selected compile/validate use the same selector expansion. Unscoped compile/validate retain the
parent specification's explicit whole-project behavior. Compile may publish independently
successful resource authority; validate is no-write. Both render deterministic aggregate results.

## Output authority

Each command constructs one typed aggregate report containing ordered per-resource reports and
failures. Human rendering preserves the established plan/run visual language and progressive
disclosure. JSON serializes the same report. Selector expansion and aggregate status are new typed
fields; per-resource report facts are not reconstructed in the renderer.

## Acceptance scenarios

1. Exact plus overlapping globs resolve once in canonical order.
2. A typo glob fails instead of silently planning nothing.
3. A malformed unselected SQL file does not block a selected glob that excludes it.
4. Two selected resources prepare successfully, then one execution fails; the other completes and
   the aggregate exits nonzero with exact durable effects.
5. One selected resource fails preparation; neither resource reaches destination mutation and
   `--out` publishes no plan artifact.
6. Resource order permutations produce identical selection and portable plan identity.

## Acceptance criteria

- Parser/help/completion tests cover exact, glob, repeated exclude, quoting examples, duplicate,
  miss, and empty-set behavior.
- Counters prove expansion does not parse/contact unselected resources.
- Aggregate plan/run report tests preserve JSON/human/redaction authority.
- Multi-run tests prove the preparation barrier, continued independent execution, correct exit
  status, and durable effect reporting.
- No source/resource namespace inference or compatibility selector remains.

## Explicit exclusions

- cross-resource SQL dependencies or transactionality;
- tag/metadata/boolean selector languages;
- implicit all-resource plan/run;
- fail-fast execution policy in the first implementation.

## Ratification status

The user confirmed the selector grammar and batch law on 2026-08-04.
