Status: active
Created: 2026-08-06
Updated: 2026-08-06
Supersedes: `.10x/specs/superseded/resource-preparation-command-experience-lockfile.md`

# State-backed resource preparation and command experience

## Purpose

CDF commands MUST match user intent while freezing one exact logical schema before effects. The
ordinary loop remains:

```text
cdf plan <selector>...   # understand; no authority write
cdf run <selector>...    # prepare missing authority, then execute
```

This specification preserves resource-first selection, independent compilation, aggregate
partial reporting, beautiful terminal planning, static validation, and one execution verb. It
replaces filesystem lock authority with the state model in
`.10x/decisions/state-backed-schema-authority.md`.

Focused refinements remain:

- `.10x/specs/resource-selector-batch-commands.md` for exact/glob selection and barriers;
- `.10x/specs/portable-plan-artifact.md` for export and execution;
- `.10x/specs/source-discovery-resource-generation.md` for discovery/generation;
- `.10x/specs/schema-drift-dispositions.md` for total admission outcomes;
- `.10x/specs/schema-promotion-corrections.md` for explicit evolution.

## Authority model

Every project MUST declare a stable `[project].id`. For each selected environment and resource,
CDF resolves exactly one schema-authority key inside the configured state authority domain:

```text
project id + environment + resource id
```

The store supplies the domain identity. An active head binds a monotonically increasing generation
to one immutable canonical Arrow schema version. Absence is a first-use state, not permission for
ongoing evolution.

Project files own authored intent. State owns active logical schema and promotion history. Derived
compiled artifacts own compiler/source/semantic/contract/native plan facts. Target-installation
state owns the mapping from logical version to destination-specific physical installation. No one
record combines these authorities.

## Command laws

### Selection before work

A selected command MUST resolve its exact/glob resource set before resource parsing, secret
resolution, source I/O, destination planning, compilation, or state reads beyond locating the
selected environment. Unselected resources cannot block, slow, invalidate, or appear in the
report. Root configuration and selected-environment errors remain shared blockers.

### `cdf plan <selector>... [--out PATH]`

Plan is no-authority-write:

- It MUST NOT mutate project files, schema state, target-installation state, compiled caches,
  packages, destinations, run ledger, receipts, or checkpoints.
- With active authority, it MUST compile against that exact generation/schema hash. Bounded source
  observation may produce drift evidence but cannot widen output or destination DDL.
- With absent authority, it MAY perform bounded discovery and compile one complete proposed
  immutable version plus the total admission program that run would establish.
- It MUST render schema authority status, generation/hash when active, proposal fields when absent,
  observation strength, drift dispositions, destination installation, DDL, and run effects.
- `--out` may create only the explicit portable artifact and MUST preserve the terminal report.

### `cdf run <selector>... [--locked]`

Run uses the same selected preparation result as plan:

1. Prepare every selected resource completely, including proposed first-use versions, active-head
   preconditions, compiled admission programs, source generations, destination bindings, and
   checkpoint heads.
2. If any resource fails preparation, execute none.
3. Atomically establish the complete set of absent first-use heads through state CAS.
4. If any relevant head changed or another proposal won, execute none.
5. Execute the exact prepared objects without rediscovery, rehydration, or recompilation.

`--locked` MUST require every selected active head and MUST fail before effects when one is absent.
It does not mean offline: extraction may contact the selected source after preparation.

Once a head exists, ordinary run MUST NOT change it. Physical observations select only compiled
lossless-coercion, variant, quarantine, or fail dispositions. Discovered columns never become
typed output or destination migrations.

Independent resources may continue after the shared preparation barrier when one later execution
fails, under the existing `continue_independent` law.

### `cdf compile [selector...] [--locked]`

Compile is optional explicit preparation for CI, cache warming, and artifact inspection:

- With selectors, it touches only their authority and artifacts.
- Without selectors, it attempts every resource independently, retains permitted successes,
  reports all failures canonically, and exits nonzero if any failed.
- Ordinary compile MAY establish missing first-use heads and MUST publish the derived artifact that
  was compiled against the exact resulting generation.
- `--locked` requires existing active heads and cannot establish them.
- Compile cannot advance an established head or mutate a destination, package, run ledger,
  receipt, or checkpoint.

### `cdf validate [selector...]`

Validate remains static and offline. It MUST NOT open state, resolve secrets or environment
variables, enumerate source data, contact drivers, or inspect destinations. It validates authored
project/config/resource grammar, project-id shape, source bindings/options, secret-reference
syntax, and locally present derived artifact structure. Missing state/cache is reported as skipped
operational/generated status, not project invalidity.

### Schema and operational commands

The schema surface is:

```text
cdf schema show RESOURCE
cdf schema diff RESOURCE
cdf schema promote RESOURCE [--type /field=TYPE ...] [--execute]
```

Show reads active state authority without source I/O. Diff performs bounded observation and writes
nothing. Promote is the only established-head transition.

Doctor owns scoped operational readiness for state/source/destination/runtime. Add and discovery
generation author resources but do not establish schema authority or compile. `cdf sql` remains
no-write and exposes state/package/checkpoint history plus verified last-compiled cache facts,
clearly labelling cached schema authority as last-known when state was not contacted.

`cdf inspect lock`, `cdf contract freeze`, and lock-backed `cdf contract test` do not exist.
`cdf contract show` remains policy rendering.

## Derived compilation authority

Each current compiled resource artifact MUST bind:

- project id, environment, resource id, and state authority-domain id;
- exact schema-head generation and schema hash, or the complete first-use proposal during planning;
- exact authored resource/configured-source/semantic inputs;
- compiler, dependency, normalizer, source driver, contract/admission, native relational, and
  destination mapping identities;
- safe diagnostics and no secret values.

Immutable artifacts remain content-addressed under `.cdf/compiled/`. `.cdf/manifest.json` is a
bounded generated current/stale/failed/absent index and is not runtime schema authority. Publishing
derived files uses generic guarded project-file transactions, but no project file is a final public
schema commit point.

An edit invalidates only artifacts whose bound inputs changed. Updating resource A cannot modify
resource B's state head or artifact bytes. Whole-project compile may commit each successful
resource independently because compile has no external execution barrier.

## First-use and concurrency scenarios

Given absent authority, plan performs bounded observation, renders one proposed baseline and
leaves project/state/destination bytes unchanged.

Given that exact proposal, run prepares every selected resource, establishes the proposal set in
one state transaction, publishes derived caches, and executes without a second discovery pass.

Given two different concurrent first-use proposals for one key, exactly one transaction wins; the
loser reports the relevant head conflict and performs no external effect.

Given an identical repeated establishment, state returns the existing matching head idempotently.

Given active authority for A and a promotion of unrelated B, A's compiled artifact and portable
plan remain valid.

Given active authority and new source columns, plan reports drift and no migration; run follows the
compiled disposition and the head remains byte-identical.

Given an existing destination whose physical schema disagrees with recorded installation, plan/run
fail scoped destination preflight. Ordinary source planning does not repair it.

## Diagnostics and reports

Human and JSON output MUST derive from one typed redacted report. Schema facts distinguish:

- state authority domain and key;
- `absent|active|promoting` status;
- generation/schema version;
- `not_checked|bounded|catalog_exact|stream` observation;
- drift class and compiled disposition;
- destination installation status/version;
- actual state/cache/execution effects.

Errors name the selected resource, exact failing boundary, expected and observed generation/hash
when safe, and the command that owns repair. They never recommend creating or refreshing a
lockfile.

## Acceptance criteria

- Selected-command counters prove no work against unrelated resources or environments.
- Plan snapshots prove zero project/state/runtime writes with both absent and active authority.
- Multi-resource run tests prove all-or-none first-use establishment before effects.
- Concurrent proposal tests prove exact-one-winner and identical idempotency.
- Compile tests prove selected isolation, aggregate partial success, and `--locked` absence failure.
- Active-schema drift tests prove no head or destination-schema mutation.
- Derived artifact/index tests prove exact per-resource state bindings and independent staleness.
- Static validate tests prove no state, secret, source, destination, or environment-variable access.
- CLI/help/report tests prove current state terminology and removal of lockfile command surfaces.

## Explicit exclusions

- automatic promotion or same-run schema epochs;
- Postgres state implementation;
- destination schema as canonical authority;
- global database revision preconditions;
- compatibility readers, dual file/state authority, inferred project ids, or lockfile diagnostics;
- schema export/import, background compilation, or implicit destination repair.

## Ratification status

The user ratified the state-backed authority identity, first-use compile/run behavior, `--locked`
meaning, and complete current-only lockfile deletion on 2026-08-06.
