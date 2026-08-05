Status: done
Created: 2026-08-04
Updated: 2026-08-04

# Resource preparation ergonomics inventory

## Question

Why do `cdf compile`, `cdf compile --refresh`, `cdf schema pin`, `cdf plan`, and
`cdf run` currently produce overlapping, project-wide, and misleading behavior, and what is the
smallest coherent current-only command model that supports working on one resource at a time?

## Sources and methods

- Inspected the active compilation, schema-discovery, project CLI, resource-planning, SQL-authoring,
  project-layout, and publication/recovery records.
- Inspected CLI grammar and dispatch in `crates/cdf-cli-core/src/args.rs` and
  `crates/cdf-cli/src/{compile_command,schema_command,project_command,context}.rs`.
- Traced query resource inventory and compilation through
  `crates/cdf-project/src/{project_inputs,query_compiler,manifest,lockfile}.rs`.
- Compared those authorities with the user-observed failure chain in the customer-zero sandbox at
  `/Users/alexanderbut/code_projects/cdf_sandbox` without mutating the sandbox.
- Inspected the sandbox project/resource layout only; no source, destination, package, lock, schema,
  manifest, or state operation was run.

## Findings

### Five user-facing operations currently share three hidden activities

| Surface | External source observation | Local authority writes | Effective scope |
|---|---:|---:|---|
| `cdf compile` | forbidden | manifest | whole project |
| `cdf compile --refresh` | yes | schema artifacts, lock, manifest | whole project |
| `cdf schema pin <resource>` | yes | schema artifacts, lock | selected resource after whole-project load |
| `cdf plan <resource>` | conditional discovery unless `--no-pin` | conditional schema artifacts and lock | selected plan after whole-project load |
| `cdf run <resource>` | conditional first-use discovery | conditional schema artifacts and lock, then run artifacts/destination | selected run after whole-project load |

The CLI vocabulary does not expose those actual boundaries. “Compile” sometimes means offline
manifest publication and sometimes live schema refresh. “Plan” is not reliably read-only. “Pin” is
a second persistent preparation entry point. Every resource command first loads and compiles the
whole project, so selection occurs too late to isolate credentials, invalid files, missing local
data, or source availability.

### The whole-project behavior is implementation, not a current SQL dependency requirement

`compile_query_project_resources` inventories every accepted resource, compiles each one, and only
then lets callers select a resource. The D3 SQL grammar currently rejects joins and cross-resource
references and admits exactly one `upstream(...)` relation. Therefore the dependency closure for a
selected resource is exactly that resource, its configured source, shared project/environment
policy, semantic references it actually uses, and the selected destination capability sheet.

Parsing or resolving unrelated resource files, source credentials, and source payloads is not
required to plan or run the selected resource. Whole-project validation remains useful for
`cdf validate` and an unscoped full-project compile.

### Error remediation is appended by error kind instead of failure authority

`with_compile_remediation` appends `cdf compile --refresh` to every `Contract` or `Data` error that
does not already contain that string. This causes an unknown configured source, missing local file,
invalid SQL, corrupt artifact, or unrelated project contract failure to recommend refresh even
when refresh cannot fix it. The outer CLI error catalog then adds generic project/validate help,
burying the actionable boundary further.

Remediation must be owned by the narrow failure producer. A compiler wrapper cannot infer that
refresh is correct from the broad error kind.

### The sandbox's terminal Internal error is deterministic manifest validation drift

`ProjectManifest` intentionally retains authored SQL text. `validate_security` converts the
manifest to JSON and rejects every string containing any Unicode control character. Parsed JSON
strings containing normal SQL line feeds, carriage returns, or tabs therefore fail with
`manifest string exceeds bounds or contains control characters`. The active manifest contract
requires exact authored SQL bytes/hash and makes this an implementation bug, not bad sandbox data.

The repair must continue rejecting unsafe controls while admitting the explicit textual whitespace
that authored SQL and diagnostics are allowed to contain. It must not weaken secret/path checks.

### `run` can reach execution with internally inconsistent relational authority

The observed `relational expression input schema differs from its compiled authority` means the
selected runtime resource can be hydrated with a pinned schema that no longer matches the
relational plan compiled earlier in the command path. Preparation and execution are separate
passes over mutable authority. A selected-resource preparation result must feed plan/run directly;
the command must not reconstruct one half of the resource after final relational compilation.

This finding identifies the violated boundary from the diagnostic and code path. A focused
reproduction is still required before naming the exact function-level defect.

## Recommended product model

The resource, not the project invocation, should be the unit of compilation, failure, locking,
publication, and recovery. Commands should express user intent; schema discovery, relational
compilation, cache validation, and manifest assembly should be internal phases shared by those
commands.

1. `cdf plan RESOURCE` answers “what would happen?” It may perform bounded read-only observation
   when local authority is absent, but it never writes. There is no `--no-pin` because planning
   cannot pin.
2. `cdf run RESOURCE` answers “do it.” It prepares exactly that resource, persists first-use local
   authority if needed, and executes the exact prepared result. Users do not have to compile or pin
   first.
3. `cdf compile [RESOURCE] [--locked]` is an optional explicit build/cache/CI operation, not a
   prerequisite in the author loop. With a resource it prepares only that resource. Without one it
   attempts every resource independently, reports every failure, and retains successful results.
   `--locked` forbids creation/change of committed authority while still allowing deterministic
   compilation from what is already locked. There is no `--refresh`.
4. First observation establishes a schema baseline. Existing schema evolution is never silently
   accepted by compile or run: it is admitted under the frozen program or surfaced through
   `cdf schema diff` and explicitly accepted through `cdf schema promote`. Remove `schema pin` and
   `schema discover`; keep `show`, `diff`, and `promote` as inspection/evolution intents.
5. `cdf validate [RESOURCE]` validates one resource when selected and the whole project when not.
   Whole-project validate/compile aggregate resource results instead of fail-fast masking later
   failures.
6. `cdf sql` always mounts system history and every valid compiled resource. A stale/broken resource
   appears as scoped status/diagnostic data and cannot be consumed as current execution authority;
   it does not make unrelated observability disappear.
7. Every failure producer supplies one primary stable code, location, plain-language cause, and
   authority-specific fix. Delete nested duplicate codes and broad error-kind-based
   refresh/validate suggestions.

## Artifact consequence

The single latest whole-project manifest is the architectural source of the all-or-nothing user
experience. Marking the same monolith “partial” merely makes one selected compile erase the useful
compiled view of another. Resource independence is a named requirement, so resource-addressed
artifacts are justified rather than speculative abstraction.

The recommended current-only artifact model is:

- `cdf.lock` is a map of independently usable resource commitments. Each entry binds its own
  compiler/dependency/normalizer/source/schema/semantic/destination facts; absence or staleness of
  one entry does not invalidate another.
- `.cdf/compiled/<resource>@<hash>.json` is the immutable complete compiled artifact for one
  resource. It contains the resource/input/schema/plan/semantic/lineage/diagnostic facts currently
  crowded into the project manifest.
- `.cdf/manifest.json` becomes a small generated project index over resource artifacts and
  resource statuses, not a second compilation authority. It identifies current, stale, failed, and
  absent resources without serving stale plan bytes as executable truth.
- Selected compile/run publishes one resource artifact and CAS-updates its lock/index entry under
  the existing guarded transaction. Unrelated resource bytes remain untouched.
- Whole-project compile attempts resources independently and publishes each successful resource.
  It exits nonzero with one aggregate report if any fail, but successful work remains usable and a
  retry naturally targets only failures.
- `cdf sql` mounts valid resource artifacts plus index status/diagnostics. Package/checkpoint/system
  tables remain queryable even when no resource artifact is current.

This is more change than adding a target argument, but less product complexity: one unit of
authority matches one unit of user work and one unit of failure.

## Proposed delivery slices

1. Repair the manifest text validator and replace generic/nested remediation with authority-owned
   diagnostics, using the sandbox failure chain as a golden journey.
2. Add direct selected-resource inventory/compilation and make plan strictly no-write.
3. Route run through one selected-resource preparation result and prove schema/plan identity cannot
   split before execution.
4. Cut lock and compiled artifacts to resource authority, replace the monolithic manifest with an
   index, and make whole-project compile/validate aggregate rather than fail fast.
5. Delete `compile --refresh`, `schema pin`, `schema discover`, and `plan --no-pin`; regenerate the
   public surface and rewrite onboarding around plan/run rather than preparation commands.

Each slice requires one bounded ticket, focused behavioral tests, affected-package checks/Clippy,
and an independent red-team review. No compatibility aliases or hidden legacy grammar are needed.

## Conclusions

The main usability defect is the wrong unit of authority: commands that name one resource still
construct, validate, publish, and invalidate one project-wide compiled world. Late selection,
overlapping verbs, fail-fast errors, stale SQL observability, and credential coupling all follow.

Make resources independently buildable and diagnosable. Then the obvious author loop is simply
`plan` and `run`; compile becomes optional tooling, schema commands mean evolution rather than
bootstrapping, and whole-project operations become useful aggregate reports rather than brittle
gates.

## Limits

This inventory did not mutate the product or sandbox and did not reproduce external source I/O.
It does not ratify first-use run writes, command removal, aggregate partial success, resource-
addressed lock/artifact semantics, or stale-resource observability. Those user-visible choices
remain blockers for implementation.
