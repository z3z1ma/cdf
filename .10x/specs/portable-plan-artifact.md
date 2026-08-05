Status: draft
Created: 2026-08-04
Updated: 2026-08-04

# Portable plan artifact and execution

## Purpose

This specification defines a durable, portable, secret-redacted resource-set execution plan that
can be produced on one machine and executed on another after thin fail-closed preflight. It does
not replace the existing human plan report.

## CLI contract

```text
cdf plan <selector>... --out plan.json
cdf run --plan plan.json
```

`--out <path>` adds an artifact write to an otherwise no-project-write plan command. Plan MUST
still render its existing terminal document. Under `--json`, the command serializes the same typed
success report, including the redacted artifact path/hash/byte count; it does not emit the portable
artifact on stdout or replace the report shape with raw plan bytes.

The artifact path is explicit. An existing nonidentical file MUST NOT be overwritten. An existing
byte-identical artifact reports `unchanged`. Artifact creation is the only allowed plan write.

`cdf run --plan <path>` conflicts with resource selectors and every semantic plan-shaping option:
destination override, projection, filter, limit, order, segmentation identity, schema update, or
another plan path. It MAY accept presentation/progress options and runtime scheduling knobs that
only tighten nonidentity host ceilings.

The flag is `--plan`, not `--plan-json`: JSON is the first versioned encoding, while the CLI noun
names the semantic artifact rather than permanently coupling its consumer to an encoding suffix.

## Artifact model

`plan.json` MUST be a versioned, canonical, bounded, content-hashed, deny-unknown-fields document.
It binds:

- plan schema version and top-level semantic hash;
- authored selectors and canonical ordered resource ids;
- project/environment/compiler/dependency/normalizer identities;
- exact current resource artifact/input/lock/schema/semantic/contract/native relational/operator
  identities;
- source driver/version/option-schema/redacted-option/physical-plan/source-semantic identities;
- exact scan/partition/task-set membership, pushdown, output schema, validation, normalization,
  segmentation, extent, lineage, and source generation preconditions;
- destination configuration hash, canonical driver/sheet/mapping/target/disposition/DDL/guarantee
  authority;
- pipeline/checkpoint scope and exact input checkpoint heads;
- execution failure policy and required host capabilities;
- typed content-addressed references for large sub-artifacts;
- adapter-supplied `not_after` only when its protocol has real temporal validity.

For each resource the artifact labels schema authority as either:

- `locked`: binds the exact existing `cdf.lock` resource entry and governed output schema; or
- `proposed_first_use`: embeds the exact candidate baseline produced by bounded discovery and
  proves that no resource lock entry existed when planning.

In both cases the artifact's output schema and destination DDL are frozen. A later observation
cannot add fields or migrations.

It MUST NOT contain secret values, resolved credentials, signed URLs, bearer tokens, open handles,
absolute coordinator paths, SQLite connections, destination sessions, runtime handles, or host
placement. Secret references and redacted endpoints are allowed under existing policy.

The top-level artifact reuses existing portable source/partition/execution values and typed hashes.
It MUST NOT serialize `ScanPlanReport`, DataFusion plans, debug output, trait objects, or a second
generic representation of driver-owned plans.

## Portability

Every selected source driver and destination binding MUST explicitly validate portable authority
at artifact creation. A plan with coordinator-local paths, inaccessible local content-store
references, weak uncheckable generation, unsupported driver portability, or unresolved required
external artifacts fails `--out` with a resource-owned diagnostic. Terminal-only plan may still
succeed when the plan is useful locally.

Large task/compiled artifacts MAY be referenced rather than inlined only when the reference names a
content-addressed store available to the runner and carries byte count, hash, and provider
generation. A host-local `.cdf/` reference is not cross-machine portability. The first
implementation does not invent an archive/bundle or silently copy payloads.

## Run preflight

`cdf run --plan` MUST validate the entire plan before source payload reads, package creation,
destination preparation/mutation, run-ledger mutation, receipt, or checkpoint work:

1. artifact version, canonical bytes/hash, bounds, controls, redaction, and typed references;
2. installed CDF/Arrow/relational-engine/normalizer and source/destination registry compatibility;
3. current project/environment and every bound authored/configuration/lock/resource artifact hash;
4. availability and exact identity of every referenced plan/task artifact;
5. exact checkpoint/state heads and destination configuration/capability sheet;
6. adapter-owned bounded metadata revalidation of each source generation and optional `not_after`;
7. execution host capabilities and runtime ceilings.

Preflight MUST NOT compile, refresh, discover a replacement, change selection, update schema/lock,
substitute a source generation/partition/task, migrate destination policy, or mutate any authority.
Any difference fails with exact re-plan guidance. All resources pass before any executes.

For `locked` resources, preflight requires the exact lock entry and reconciles any fresh source
schema evidence against that locked output: extra fields remain drift/residual evidence, missing
fields retain locked verdicts, and changed types use only compiled admission rules. It MUST NOT
derive output fields or destination migrations from the observation.

For `proposed_first_use` resources, preflight requires that the resource lock entry is still absent
and that the observed generation/schema matches the embedded candidate exactly. Run atomically
commits that candidate lock entry and compiled resource artifact before package/destination
mutation. A concurrent or different baseline fails the complete resource-set preflight; it is not
merged.

Secret values may rotate behind the same reference. Preflight proves reference resolution and
authorization without placing the value in plan identity. Egress policy applies on the runner.

There is no arbitrary global TTL. Exact revalidation owns validity; adapters unable to prove it
must reject portable export or provide a real protocol-derived temporal bound.

## Execution

After preflight, run executes the exact native compiled resource plans and canonical resource set.
Runtime scheduler resolution may tighten jobs, memory, disk, connection, or blocking-lane ceilings
without changing logical membership/order, pushdown, schema, package identity, or destination
semantics.

Package/run evidence binds the portable plan hash. Replay continues to consume package authority,
not the original plan file. The runner does not rewrite the plan.

## Report authority and terminal UX

The existing plan report remains the sole input to human and command JSON rendering. `--out` adds a
typed optional artifact effect with path, hash, bytes, resource count, and `created|unchanged`
status. Renderer modules decide placement using established Effects/Proof vocabulary; command code
does not build layout.

Run-from-plan likewise constructs one typed aggregate run report that names the consumed plan hash
and preflight result while preserving established per-resource progress/outcome rendering.
Sensitive fields are redacted before both JSON and human paths.

## Acceptance scenarios

1. Plan without `--out` produces byte-equivalent existing terminal/JSON facts and no artifact.
2. Plan with `--out` renders the same resource plan plus one artifact effect; its file verifies and
   an identical rerun reports unchanged.
3. A plan produced on host A runs on compatible host B with the same project/environment, shared
   artifacts, source generations, destination sheet, and checkpoint heads without recompilation.
4. Changed SQL/config/lock/schema/source generation/destination sheet/checkpoint head or registry
   version fails preflight before every mutation.
5. A local-only source may render a terminal plan but fails portable export precisely.
6. Runtime jobs/memory tightening changes telemetry only, not plan/package identity.
7. A secret-bearing destination/source proves the raw secret absent from artifact, command JSON,
   human output, errors, and debug formatting.
8. A locked resource observes an added source column; terminal/plan evidence reports drift while
   output schema and destination DDL remain byte-identical and no migration is proposed.
9. A first-use plan embeds a candidate baseline; run commits exactly it before execution, while a
   concurrent lock entry makes preflight fail without mutation.

## Acceptance criteria

- Canonical artifact round-trip/tamper/unknown-version/bounds/redaction tests pass.
- Existing portable source/worker validators are reused and extended, not copied.
- Preflight counter/failpoint tests prove no mutation and no replanning at every boundary.
- Cross-host fixture proves one stored plan can execute under compatible injected services.
- CLI report-authority tests preserve terminal snapshots and JSON shape apart from the intentional
  optional artifact/preflight fields.
- Generated help/completions/man/docs clearly distinguish `--json`, `--out`, and `run --plan`.
- No compatibility consumer for older/nonportable plan shapes ships.

## Explicit exclusions

- embedding data payloads or credentials in the plan;
- remote scheduler/RPC/placement;
- automatic replan, drift acceptance, or plan repair;
- generic TTL unrelated to adapter protocol;
- plan archive/bundle format in the first implementation;
- replacing terminal plan output with artifact JSON.

## Ratification blocker

Confirm `plan --out plan.json` plus `run --plan plan.json`, preservation of the existing terminal
report, strict cross-machine portability at export, and whole-plan no-repair preflight before any
selected resource executes.
