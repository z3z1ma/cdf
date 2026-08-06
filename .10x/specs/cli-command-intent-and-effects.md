Status: active
Created: 2026-08-04
Updated: 2026-08-04

# CLI command intent, scope, and effects

## Purpose

Every CDF command must have one user intent, one identity space, and an explicit effect ceiling.
Preparation phases are reusable internals, not a workflow the user must manually sequence. This
specification applies the resource-first model to validate, doctor, add, package/run recovery,
preview, backfill, status, and inspect so the redesigned plan/run surface does not leave neighboring
commands coupled to whole-project loading or generic remediation.

## Command law

Before doing expensive or fallible work, every command MUST resolve its identity space and scope:

- resource commands use `.10x/specs/resource-selector-batch-commands.md`;
- source commands name configured-source or adapter-emitted upstream-relation identity explicitly;
- package commands name package authority;
- recovery commands name run/package/checkpoint authority;
- destination/runtime commands name that operational scope.

A command MUST NOT inventory, parse, resolve credentials for, contact, or reject an unrelated
identity. Shared root configuration syntax may block locating scope; an unrelated resource, source,
secret, data file, package, run, or destination cannot.

Every command report MUST declare its highest effect class and actual effects:

1. `static`: authored/local structural reads only;
2. `observe`: bounded source/destination/runtime/state reads, no authority mutation;
3. `author`: explicit local project/source configuration writes;
4. `execute`: package/destination/receipt/checkpoint/run effects;
5. `recover`: effects derived only from durable package/run/checkpoint authority.

The classes are ceilings, not claims that every possible read/write occurred. One typed redacted
report drives human and JSON rendering. Errors name the selected identity, failing boundary, and an
action that actually owns the repair; outer layers do not append generic validate/compile advice.

## Static validate

`cdf validate [RESOURCE_SELECTOR...]` has a `static` ceiling and implements
`.10x/decisions/static-validation-operational-readiness-boundary.md`.

It validates:

- `cdf.toml` syntax, closed keys, selected-environment structure, destination URI syntax, and secret
  reference syntax without resolution;
- `cdf/<namespace>/<resource>.cdf.sql` path/token placement, UTF-8/size bounds, SQL syntax and
  envelope, configured-source binding, and reachable local semantic references;
- registered driver identity and closed source/resource option schemas through pure schema
  validation only;
- parse/hash/security integrity of locally present lock, compiled resource artifacts, and project
  index, while treating absent generated authority as status.

It reports project path plus counts for environments, configured sources, authored/selected/valid/
invalid resources, warnings/errors, and current/stale/missing generated authority. It attempts all
selected resources, orders diagnostics canonically, and exits nonzero only for static invalidity.
It does not claim operational readiness.

## Scoped doctor

Doctor owns readiness that varies by host, credentials, network, source generation, destination,
or state. Its current-only scopes are:

```text
cdf doctor                         # local runtime only; no remote contact
cdf doctor runtime
cdf doctor resource <selector>...
cdf doctor source <configured-source>...
cdf doctor destination
cdf doctor all
```

`resource` uses the shared selector contract and checks only the credentials, egress, source
health, destination readiness, runtime capability, and ledger/checkpoint relationships reachable
from that set. `source` checks only the named configured sources. `destination` checks the selected
environment destination and ledger/mirror relationship. `runtime` checks local facilities such as
memory/spill, Python, DuckDB ICU, and required host capabilities. `all` is the only implicit
whole-project operational probe. Bare doctor equals runtime so a harmless installation check never
surprises the user with network or credential access.

Every doctor report names probes attempted/skipped/passed/warned/failed and actual external
authorities contacted. A missing secret is a scoped operational/auth failure, not an invalid
project. Doctor writes nothing and never compiles, pins, promotes, packages, or repairs.

## Add and generated authoring

`cdf add <resource-id> <location> [--source <configured-source>] [--option <key=value>] [--dry-run]`
bootstraps exactly one explicit external location not yet represented by a configured source. It
has an `author` ceiling, with bounded `observe` work permitted only against that location.

- Resource path identity, configured source identity, and upstream relation identity remain
  distinct. When `--source` is omitted for a new source, the resource namespace is the displayed
  proposed source name; the proposal is recorded, not inferred later. `--source` overrides it.
- Add selects the proposed resource/source before project inventory and cannot be blocked by an
  unrelated resource or credential.
- The driver owns location probing and canonical source/relation options. Generic code owns bounds,
  redaction, conflicts, guarded publication, and reporting.
- Successful add writes only the required `[sources.<name>]` entry, explicit secret reference/private
  secret state allowed by existing policy, and one thin resource whose projection enumerates the
  discovered top-level columns for immediate editing. `SELECT *` is permitted only as an explicit
  reported fallback when the bounded location probe cannot return schema fields. Add does not pin
  the observed schema, compile, write `cdf.lock`, or publish a project index.
- `--dry-run` runs the same proposal/preflight and writes nothing. It reports exact proposed effects.
- The next action is `cdf plan <resource-id>`, never compile/refresh/pin folklore.

Batch authoring from an existing source belongs only to `cdf discover source ... --generate`.
Direct database DSNs retain the owner-only private-secret decision in
`.10x/decisions/cdf-add-dsn-secret-persistence.md`.

## One execution verb, explicit input authority

CDF has one top-level execution intent: `run`. Resource preparation, portable-plan consumption,
package delivery, and interrupted-run recovery differ by input authority, not by user-facing verb:

```text
cdf run <resource-selector>...
cdf run --plan <plan.json>
cdf run --package <package> --to <destination>
cdf run --resume [<run-id>]
```

The four input modes are mutually exclusive. The typed report names `resource_set`, `portable_plan`,
`package`, or `interrupted_run` as its input authority and renders the same established preflight,
effects, and proof language. There are no top-level `cdf replay` or `cdf resume` commands and no
compatibility aliases.

`cdf run --package` creates a new run from exact package authority. Selection and preflight load
only the package, destination binding, required runtime capability, and optional explicit state
target. It MUST NOT load project resources, compile SQL, contact a source, rediscover schema,
re-evaluate a contract, or recommend compile/validate. It previews package/destination effects,
receipt/checkpoint conditions, and duplicate behavior before mutation, then reports exact durable
effects.

`cdf run --resume` continues existing run authority from run-ledger/package/receipt/checkpoint
facts. With an explicit run id it selects exactly that run. Without one, it proceeds only when
exactly one recoverable interrupted run exists; zero is a clean no-work result and multiple is an
ambiguity report listing exact ids without mutating anything. A finalized package makes source and
compiler access forbidden. A pre-package interrupted run may re-enter preparation/extraction only
for the exact recorded resource/source under the existing crash matrix; unrelated project failures
cannot block it.

Backfill and other resource execution commands use resource selectors and the same all-selected
preparation barrier as run. Preview is bounded `observe`; status and inspect remain read-only over
their named authority. None may hide an effectful repair behind a read-only command name.

## Acceptance scenarios

1. Validate succeeds identically with required environment variables unset and with networking
   denied, while reporting a syntactically valid secret reference and operational checks skipped.
2. Validate over one selected resource does not parse an invalid unselected resource and aggregate
   validate reports both valid and invalid counts without source I/O.
3. Bare doctor checks only local runtime; resource doctor for one resource resolves/contacts only
   its reachable authorities; `doctor all` is the sole whole-project probe.
4. Add creates one thin resource with an explicit discovered top-level projection and source
   proposal without lock/compiled artifacts, then points to plan. An unrelated invalid resource or
   missing secret does not block it; a star projection is an explicit schema-unavailable fallback.
5. `run --package` succeeds when authored project SQL is broken and source credentials are absent,
   because package authority is sufficient.
6. `run --resume` after package finalization performs zero source/compiler calls and follows
   durable receipt/checkpoint facts; a missing id with multiple candidates makes no choice.
7. Human and JSON reports agree on scope, effect ceiling, actual effects, skipped checks, and safe
   remediation for every command family.

## Acceptance criteria

- Counter/fault tests prove each command's negative I/O and mutation boundaries.
- CLI grammar/help/generated artifacts describe scopes and effects without legacy aliases.
- Add/doctor/validate reports and every run input mode use one typed report each with
  JSON/human/redaction parity.
- Static validate, scoped doctor, add dry/write, run-from-package, and run-resume pass focused
  integration tests with unrelated broken resources and unavailable credentials.
- `validate --deep`, generic compile/refresh/pin advice, fat generated resource envelopes, and
  whole-project preload paths are absent.

## Explicit exclusions

- automatic repair by validate or doctor;
- source/destination contact from validate;
- hidden schema pin/compile from add or discovery generation;
- package execution from mutable project SQL instead of package authority;
- compatibility aliases or parsers for removed command forms;
- a generic selector language spanning resources, configured sources, packages, runs, and
  destinations.

## Ratification status

The user confirmed the proposed doctor/add surfaces and the single-verb `run --package` /
`run --resume` replacement on 2026-08-04. The static validate boundary was independently ratified
and active first.
