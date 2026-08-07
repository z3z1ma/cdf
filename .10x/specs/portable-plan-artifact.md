Status: active
Created: 2026-08-06
Updated: 2026-08-06
Supersedes: `.10x/specs/superseded/portable-plan-artifact-lockfile.md`

# Portable plan artifact with per-resource state preconditions

## Purpose

This specification defines a canonical, bounded, secret-redacted resource-set plan produced on one
machine and executed on another after thin whole-plan preflight. It preserves the existing terminal
plan document; artifact output is orthogonal.

## CLI contract

```text
cdf plan <selector>... --out plan.json
cdf run --plan plan.json
```

`--out` creates or verifies only the explicit artifact. Existing nonidentical bytes are never
overwritten; identical bytes report `unchanged`. Terminal and command JSON rendering remain driven
by the ordinary typed plan report with one additive artifact effect.

Run-from-plan conflicts with selectors and every semantic plan-shaping option. Presentation,
progress, and runtime knobs may only tighten nonidentity ceilings.

## Artifact authority

The deny-unknown-fields, versioned, canonical, content-hashed artifact binds:

- project id/name, environment, compiler/dependency/normalizer and exact authored input hashes;
- exact canonical ordered resource selection;
- compiled source/relational/contract/admission/output schema plans;
- source driver/options/generations/partitions/task sets and portability attestations;
- destination configuration, target, sheet/mapping/capabilities, DDL, disposition, and guarantee;
- input checkpoint heads, pipeline scope, failure policy, and host requirements;
- one per-resource schema authority precondition.

The schema precondition is either:

```text
absent:
  authority domain
  exact project/environment/resource key
  complete proposed immutable schema version

exact:
  authority domain
  exact project/environment/resource key
  head generation
  schema hash
```

There is no global state revision or embedded project lock. Unrelated resource/environment changes
cannot invalidate the artifact.

The artifact contains no resolved secret, signed URL, bearer token, open handle, absolute
coordinator path, database connection, destination session, or runtime placement. Existing typed
portable source/task values are reused; no DataFusion plan, CLI report, debug string, or second
generic driver representation is serialized.

## Portability

Every source/destination validates portable authority at artifact creation. Coordinator-local
paths, weak uncheckable generations, unsupported portability, or unavailable referenced artifacts
fail `--out` for the owning resource while terminal-only planning may remain useful.

Large content-addressed artifacts may be referenced only when the runner can access the same store
and the reference carries exact bytes/hash/provider generation. The initial implementation does not
invent a payload bundle.

A plan with existing schema authority runs only against the same authority-domain id. Another
local SQLite store is not silently equivalent. Shared Postgres portability is future work; an
explicit future schema import/deployment surface would be required to cross authority domains.

## Whole-plan preflight

Before source payload, package, destination, run-ledger, receipt, checkpoint, or schema-authority
mutation, run MUST validate every selected resource:

1. artifact version/hash/canonical bytes/bounds/redaction/typed references;
2. installed binary/registry/compiler/normalizer compatibility;
3. exact project id, environment, authored/configuration and compiled input hashes;
4. each relevant schema authority-domain/key precondition only;
5. exact source generation and adapter portability attestation;
6. destination binding/capability/installation and checkpoint heads;
7. host capabilities and runtime ceilings.

Preflight cannot compile, repair, discover a replacement, reinterpret policy, change selection,
accept drift, migrate a destination, or mutate authority.

For exact active authority, fresh observation is evidence against the bound output and compiled
dispositions; it cannot create fields or DDL. For absent authority, observed generation/schema must
still match the embedded complete proposal. After every resource passes, run atomically establishes
all absent proposals through one state transaction before effects. A race/conflict executes none.

Secret values may rotate behind stable references; runner authorization and egress are checked
without entering plan identity. There is no arbitrary global TTL.

## Execution and reporting

Execution uses the exact native plans. Scheduler resolution may tighten jobs/memory/disk/
connection ceilings without changing membership, pushdown, schema, package identity, or semantics.
Run/package evidence binds the portable plan hash; package replay does not need the plan.

Plan and run reports expose state authority domain/key, precondition, schema generation/hash or
first-use proposal, drift disposition, preflight status, and actual state effects through one typed
redacted JSON/human authority.

## Scenarios

An absent-authority plan writes only `plan.json`; run on a compatible host establishes the exact
proposal set before effects.

A changed relevant head, source generation, project input, destination sheet, checkpoint head, or
host contract blocks all execution. Promotion of unrelated resource B does not block planned A.

An added source field against exact active authority is drift evidence, produces no migration, and
cannot alter the embedded output schema.

A local-only source may render a terminal plan but fails portable export precisely.

## Acceptance criteria

- Canonical round-trip/tamper/version/bounds/redaction tests pass.
- Per-resource absent/exact precondition tests reject relevant changes and ignore unrelated ones.
- Whole-plan counter/failpoint tests prove zero effects before every resource passes.
- Cross-host fixture executes one stored plan under compatible injected services.
- Report tests preserve terminal UX and JSON/human/redaction parity.
- Generated help/docs distinguish `--json`, `--out`, and `run --plan`.
- No reader for the superseded lock-bound plan shape ships.

## Explicit exclusions

- payload/credential embedding, remote scheduling, automatic replan, plan repair, global TTL;
- cross-authority-domain equivalence, schema export/import, or Postgres implementation;
- replacing terminal plan output with artifact JSON.

## Ratification status

The user ratified per-resource state preconditions, cross-machine thin preflight, and preservation
of terminal plan output on 2026-08-06.
