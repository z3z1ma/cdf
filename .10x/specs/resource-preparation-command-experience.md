Status: draft
Created: 2026-08-04
Updated: 2026-08-04

# Resource-first command experience

## Focused refinements

The user ratified the three core choices in this specification on 2026-08-04: the plan/run author
loop and command removals, independent resource compilation authority, and aggregate whole-project
behavior with useful partial compile success.

The following focused draft specifications refine newly requested surfaces before this parent
contract becomes active:

- `.10x/specs/resource-selector-batch-commands.md` — exact/glob resource-set selection and
  multi-resource preparation/execution behavior;
- `.10x/specs/portable-plan-artifact.md` — `plan --out` and `run --plan` authority;
- `.10x/specs/source-discovery-resource-generation.md` — adapter-owned source/resource discovery
  and explicit thin-resource generation.

Where this parent uses singular `RESOURCE`, the selector specification expands that operation into
an ordered set of independently authoritative resources. The existing human plan renderer remains
the primary terminal experience; a portable plan artifact is a separate output, not a replacement
for the command report.

## Purpose

CDF commands MUST match user intent, not expose compiler phases. The current author loop is:

```text
cdf plan <resource>   # understand it, no writes
cdf run <resource>    # do it; prepare what is missing
```

Everything else exists for a distinct secondary intent: explicit CI/build preparation, project
validation, schema evolution, or inspection. A user MUST NOT have to understand the ordering among
compile, refresh, discover, and pin to run one resource.

This specification makes the resource the unit of compilation authority, failure, persistence,
and recovery. When activated it supersedes conflicting whole-project manifest, offline/refresh,
auto-pin plan, explicit pin/discover, and fail-fast behavior in the active compilation, discovery,
and CLI specs. Fixed-schema execution, explicit schema promotion, crash-safe publication,
source-read-only planning, secret redaction, and no hidden destination mutation remain unchanged.

## Product principles

### Intent over phases

The user chooses `plan`, `run`, `compile`, `validate`, `schema`, or `inspect`. Inventory, discovery,
schema freezing, SQL analysis, native lowering, artifact validation, and publication are shared
internal phases. No error asks the user to invoke an internal phase that the current intent can
safely perform itself.

### Selection before work

A command naming one resource MUST select that resource before inventory, parsing, driver
validation, secret resolution, source observation, destination planning, compilation, or artifact
loading. Unselected resources cannot block or slow selected work.

### Derived state self-heals; semantic change is explicit

Missing/stale generated compilation artifacts are rebuilt by a writing intent. First-use schema
observation may establish a baseline. A later change to governed output schema is never silently
accepted; `schema promote` owns that change.

### The lock is the output-schema fence

The primary product purpose of each resource entry in `cdf.lock` is to freeze the governed output
schema and the semantic/compiler bindings that make it executable. Discovery is evidence against
that authority, not a replacement for it. No ordinary plan, compile, or run may add a discovered
column to locked output or destination DDL.

### One failure, one owner, one fix

The primary error code belongs to the narrow boundary that can explain the failure. Outer layers
add structured context, not a second error code or generic command advice.

### Whole-project work is a report, not a hostage situation

Unscoped compile/validate attempts every independent resource, retains useful successes where the
command is allowed to write, reports every failure, and exits nonzero when any resource failed.

## Basic journeys

### First local resource

```text
cdf plan local.events
cdf run local.events
```

Plan may inspect the selected local files to build an in-memory plan and writes nothing. Run
establishes first-use schema/compiled authority, then executes exactly that prepared plan. No
compile, refresh, discover, or pin prerequisite exists.

### One usable resource in a large project

```text
cdf plan fineweb.documents
cdf run fineweb.documents
```

Only `cdf/fineweb/documents.cdf.sql`, `[sources.fineweb]`, selected environment policy, reachable
semantics, and the selected destination capability are relevant. Missing credentials, bad SQL,
missing files, or unavailable hosts for any other resource are not read or reported.

### Edit SQL and retry

Editing the selected SQL invalidates only that resource's generated compiled artifact. Plan
recompiles it in memory. Run or selected compile publishes its replacement. Unrelated compiled
resources remain current.

### Source schema drift

Run uses the fixed pinned schema and compiled admission program. Compatible drift follows an
already compiled verdict; governed output evolution is shown by `cdf schema diff <resource>` and
accepted only by `cdf schema promote <resource>`. Compile never doubles as silent promotion.

### Whole-project CI

```text
cdf compile --locked
```

Every resource is attempted independently from committed authority. The report lists all ready and
failed resources in one run. `--locked` forbids lock/schema commitment changes. The command exits
nonzero when any resource cannot compile, while diagnostics retain exact resource ownership.

### Query observability during breakage

`cdf sql` always mounts package/checkpoint/system history and the compilation index. Valid resource
artifacts remain queryable. A stale or failed resource appears as status and diagnostics; its stale
compiled plan is not treated as current execution authority.

## Command contract

### `cdf plan RESOURCE`

`cdf plan` answers “what would happen?” for exactly one resource.

- It MUST never write project, lock, schema, compilation, package, destination, state, receipt, or
  checkpoint data.
- It MUST use current local authority when sufficient.
- When first-use authority is absent, it MAY perform bounded read-only observation and compile the
  result in memory.
- It MUST render source selection/I/O, fixed input/output schema, transforms, destination changes,
  delivery guarantee, state advancement, and any local authority that run would establish.
- Its prepared result is discarded after rendering.

`--no-pin` MUST be deleted because plan cannot pin. No compatibility parser or hidden alias remains.

### `cdf run RESOURCE [--locked]`

`cdf run` answers “do it” for exactly one resource.

- It MUST call the same selected-resource preparation seam as plan.
- With missing first-use authority, ordinary run MAY observe and atomically establish the selected
  schema/lock/compiled authority before package or destination mutation.
- With existing authority, run MUST NOT perform a current-schema pre-scan; in-stream observation
  selects only an already compiled admission verdict.
- `--locked` MUST fail before package/destination mutation if the run would create or change
  committed schema/lock authority. Source execution itself remains allowed; locked does not mean
  network-offline.
- The prepared resource, schema, relational plan, and admission program MUST pass directly into
  execution. No partial hydration or second compilation pass is permitted.

### `cdf compile [RESOURCE] [--locked]`

`cdf compile` is optional explicit preparation for CI, cache warming, and artifact inspection. It
is not a prerequisite for plan or run.

- With `RESOURCE`, it MUST prepare and publish only that resource.
- Without `RESOURCE`, it MUST attempt every current resource independently, publish every
  successful result allowed by the lock policy, report every failure, and exit nonzero if any
  failed.
- Ordinary compile MAY establish missing first-use authority but MUST NOT silently promote an
  existing governed schema.
- `--locked` MUST forbid any committed lock/schema change and fail the affected resource when local
  authority is insufficient.
- Compile MUST NOT mutate a destination, package, state/checkpoint ledger, receipt, or run ledger.

`--refresh` MUST be deleted. Re-running compile already rebuilds stale generated compilation state;
`schema diff/promote` owns governed external schema change. No compatibility parser remains.

### `cdf validate [RESOURCE]`

- With `RESOURCE`, validate MUST inspect only that resource's authored/configured contract and
  report its readiness.
- Without `RESOURCE`, validate MUST attempt every resource and return one aggregate report.
- Validation MUST NOT publish compilation/schema/lock authority or mutate external state.
- Deep/live validation remains explicit where current source/destination contact is required and
  MUST retain per-resource isolation in its report.

### `cdf schema`

The current-only schema surface is:

- `cdf schema show RESOURCE`: show the governed baseline and its provenance without source I/O;
- `cdf schema diff RESOURCE`: observe current source schema and show drift without writes;
- `cdf schema promote RESOURCE`: plan or explicitly execute governed output-schema evolution.

`schema pin` and `schema discover` MUST be deleted. Plan already shows first-use schema and source
evidence; run/compile establish the first baseline; diff/promotion own later change.

### `cdf sql`

`cdf sql` MUST remain no-write and MUST NOT compile or contact a source/destination. It MUST mount:

- system/package/checkpoint tables regardless of compilation status;
- the project compilation index and resource status/diagnostics;
- complete compiled facts only from individually verified current resource artifacts.

A stale/missing/corrupt resource artifact invalidates that resource's compiled tables, not the
entire SQL session or unrelated system/resource tables.

## Selected-resource boundary

For a selected command, CDF MUST:

1. locate and structurally parse `cdf.toml` and the selected environment;
2. resolve `cdf/<namespace>/<resource>.cdf.sql` directly from the exact resource id;
3. parse only that resource file and select its explicit configured source;
4. validate only the selected source's driver/resource options and reachable semantic references;
5. resolve only secrets needed by the selected operation;
6. load/observe/compile only the selected source/resource/destination plan.

It MUST NOT inventory, parse, compile, resolve secrets for, or contact another resource or source.
The current SQL grammar forbids cross-resource references, so the dependency closure is exactly one
resource. A future cross-resource grammar must supersede this rule before implementation.

Root TOML syntax errors and selected-environment errors remain shared blockers because selected
authority cannot be located safely without them. An invalid unselected resource file is not a
selected blocker.

## Resource compilation authority

### Lockfile

`cdf.lock` MUST be a map of independently usable current resource commitments. Each resource entry
MUST bind the exact compiler/dependency tuple, normalizer, source configuration/driver, schema,
semantic references, destination capability/mapping, contract, and native plan identities it
requires.

- Missing/stale authority for resource A MUST NOT invalidate resource B.
- Updating A MUST preserve B byte-for-byte when B's own bound inputs remain current.
- If a shared input changes, only entries whose recorded binding includes that input become stale.
- A selected update MUST delete an invalid selected entry rather than translate or preserve it.
- No legacy whole-project exact-resource-set validator or compatibility representation remains.

### Immutable resource artifacts

Each successful preparation publishes a complete immutable resource artifact at a content-
addressed path such as:

```text
.cdf/compiled/<resource>@<compiled-hash>.json
```

The artifact MUST contain the resource's authored input identity, configured source binding,
schemas, native source/relational/contract/semantic/destination plans, lineage, diagnostics, and
all typed hashes needed to verify it without contacting an external system. It MUST use the
existing content-addressed sidecar and project transaction authorities rather than inventing a
publisher.

### Project compilation index

`.cdf/manifest.json` becomes a bounded generated index, not a monolithic compiled project graph.
For every path-derived resource it records:

- resource id and authored path;
- status: `current`, `stale`, `failed`, or `absent`;
- current artifact reference/hash when status is `current`;
- exact input/staleness facts;
- last failure's stable safe diagnostic when available;
- no secret values or stale plan bytes.

The index permits project status and SQL observability without making one broken resource global
authority. Execution MUST dereference only a `current` artifact whose inputs and lock binding
verify.

### Publication

Selected preparation MUST publish in this order under the project mutation guard:

1. create-or-verify the immutable resource artifact;
2. publish the updated index;
3. publish `cdf.lock` last when the command changes committed authority.

Every update uses exact prior/new expectations. An unrelated concurrent edit is preserved and
fails closed. Whole-project compile may commit successful resources independently so later
failures do not erase completed work. Reports identify exactly which resources were published.

## Schema lifecycle

First-use observation freezes a baseline before execution and may commit it during run/compile.
After a baseline exists:

- ordinary plan/compile use the locked output schema and rebuild only derived plan artifacts;
- source contact MAY observe current physical schema for inventory/planning evidence, but that
  observation is reconciled against the lock and cannot widen output or destination DDL;
- ordinary run observes physical schemas in-stream under the same fixed admission program;
- an observed extra field is explicit drift/residual evidence and never a typed output field;
- an absent locked field remains in locked output and follows its compiled nullable, required,
  control, residual, or quarantine verdict rather than disappearing through set intersection;
- a changed type follows only an already compiled coercion/admission verdict or fails/quarantines;
- compatible/widen/residual/quarantine outcomes do not rewrite the baseline;
- a change requiring a new governed output schema is reported, not accepted or migrated;
- `schema diff` observes and explains the candidate change;
- `schema promote` is the only authority-changing schema-evolution command and the only path that
  can authorize destination migration for a new output schema.

A no-write first-use plan MAY propose and freeze a candidate baseline in memory. A portable plan
MAY carry that exact candidate as proposed first-use authority. Direct run or run-from-plan MUST
atomically commit the exact candidate lock entry before package/destination mutation; if another
authority appeared or changed meanwhile, preflight fails and nothing executes.

This removes “refresh” as an overloaded synonym for compile, discovery, pinning, drift acceptance,
and retry.

## Diagnostics and recovery

Human errors MUST render one primary code and a compact causal structure:

```text
error[CDF-SOURCE-UNKNOWN]: cannot prepare fineweb.documents
  --> cdf/fineweb/documents.cdf.sql:2:6
  configured source "fineweb" does not exist

fix: add [sources.fineweb] to cdf.toml
```

JSON errors MUST carry the same primary code, resource/source/path/span, safe cause chain, and
structured remediation. Human rendering MUST be derived from that report.

Outer command/project layers MUST NOT prepend a second bracketed code or append advice based only
on `Contract`, `Data`, `Auth`, or another broad kind.

| Failure authority | Required fix |
|---|---|
| selected source binding | exact file/span/source and `cdf.toml` correction |
| missing first-use authority under `--locked` | rerun the same intent without `--locked`, or explicitly compile that resource |
| source auth/egress | selected source plus safe credential/allowlist field and denied host |
| empty file match | resource, resolved root, glob, and data/config correction |
| SQL/contract | exact authored file/span and rule |
| schema drift requiring evolution | `cdf schema diff <resource>`, then explicit promote decision |
| corrupt generated artifact | exact artifact and safe resource rebuild path |
| internal invariant | stable Internal code and report/capture guidance; never refresh/validate folklore |

Whole-project compile/validate MUST render a success/failure summary and all independently useful
failures, ordered by resource id. It MUST NOT hide later failures behind the first one.

## Manifest text safety

Resource artifact validation MUST admit authored horizontal tab, line feed, and carriage return.
It MUST continue rejecting every other C0/C1 control character, strings beyond the byte bound,
secret values in secret-governed fields, and forbidden host-path material. Multiline SQL must
round-trip with its exact authored hash.

## Acceptance scenarios

1. Given seven resources and credentials for only `fineweb.documents`, selected plan/run/compile for
   it never parse, resolve, or contact the other six and succeed when its own inputs are valid.
2. Given no lock/artifact, plan performs bounded selected observation, renders schema and intended
   first-use writes, and leaves every filesystem byte unchanged.
3. Given the same state, run publishes the selected baseline/artifact/index/lock before package
   mutation and executes that exact prepared object with one discovery pass.
4. Given valid resource A and missing-file resource B, unscoped compile publishes A, records B's
   scoped failure, reports both, and exits nonzero; a later `cdf run A` remains usable.
5. Given a syntax error in unselected B, `cdf plan A` succeeds while unscoped validate reports B.
6. Given an edit to A, A becomes stale while B remains current; SQL exposes B's compiled facts and
   A's stale status without serving A's old plan as current.
7. Given package history and no current compiled artifacts, `cdf sql` still queries system/package
   tables and compilation status.
8. Given an existing schema baseline and physical compatible drift, run uses a compiled verdict and
   does not rewrite the baseline. Given governed output drift, run fails before destination
   mutation with schema diff/promote guidance.
9. Given multiline SQL, selected compile publishes and reloads it successfully. Given a forbidden
   control character or secret value, artifact validation still fails closed.
10. Given several project failures, aggregate output shows one stable primary diagnostic per
    resource and never recommends refresh/validate unless that exact action owns the fix.

## Acceptance criteria

- Selected command counter tests prove no inventory, parse, secret resolution, source I/O, or
  destination planning for unrelated resources.
- Plan cold/pinned filesystem snapshots prove zero writes.
- Run first-use counters prove one observation feeds final plan and execution without partial
  rehydration or duplicate discovery.
- Lock/resource-artifact/index tests prove independent invalidation, exact CAS preservation, and
  crash-safe publication.
- Whole-project compile/validate tests prove deterministic aggregate results and useful partial
  compile success.
- SQL tests prove system-table availability and resource-isolated stale/corrupt behavior.
- Error tests prove one primary code, structured parity, exact remediation, redaction, and absence
  of generic compile/refresh/validate decoration.
- CLI grammar/generated artifacts contain no `compile --refresh`, `schema pin`, `schema discover`,
  or `plan --no-pin` surface.
- No legacy alias, shim, fallback reader, migrated field, retired fixture, or rejection-only
  compatibility test remains.

## Explicit exclusions

- cross-resource SQL dependencies;
- silent schema promotion;
- destination mutation from plan/compile/validate/sql;
- background/watch compilation;
- serving stale compiled facts as execution authority;
- compatibility with removed command spellings, monolithic manifest schema, or whole-project lock
  exact-set semantics;
- weakening fixed-schema admission, secret redaction, path fencing, or publication recovery.

## Ratification status

The parent model is user-ratified. Activation remains blocked only on the execution-relevant
choices named by the three focused draft specifications above. No product implementation begins
until those refinements are confirmed.
