Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-09-p2-ws-a10d-effective-schema-runtime-evidence.md
Verdict: pass

# P2 A10d effective schema runtime adversarial review

## Target

Adversarial review of immutable-baseline/effective-schema planning, runtime reconciliation, package evidence, destination consumption, replay, and source/destination extension cost under `.10x/knowledge/source-destination-extension-invariant.md`.

## Assumptions tested

- Runtime evolution is proven against a prior immutable baseline, not merely an aggregate first pin.
- Resource authority is plan-level rather than copied into partitions.
- The engine does not assume one file equals one partition or that every source has a path.
- Physical-schema memory grows with unique schemas rather than file count.
- The kernel does not duplicate the contract widening/coercion lattice.
- Empty inputs and early limits preserve honest evidence cardinality.
- Generic CLI and executor code do not branch on Parquet/Arrow IPC.
- Arrow fingerprints are structural and unambiguous for nested schemas and metadata.
- New public authority types have a deliberate extension policy.

## Findings

- Significant, repaired: the first implementation copied resource-level schema authority into every partition to avoid extending `EnginePlan`. Authority now appears once in typed plan evidence; partition metadata contains only an observation binding plus redundant physical hash.
- Significant, repaired: an early kernel model introduced string verdict/widening concepts parallel to `cdf-contract`. It was removed; the engine serializes and validates the exact existing `SchemaCoercionPlan` per observation.
- Significant, repaired: physical schemas were initially represented per file. The runtime now catalogs them once by verified structural hash.
- Significant, repaired: generic planner/executor code read partition metadata named `path`, and evidence types were file-specific. The seam now uses source-neutral observation identities; the file adapter performs the location mapping.
- Significant, repaired: generic CLI code branched on local Parquet and Arrow IPC to decide runtime observation. That dispatch now lives behind the project discovery/compiler boundary, and a regression proves non-observable sources remain source-free.
- Significant, repaired: the original end-to-end test pinned the fully evolved three-file set, so it did not prove immutable-baseline evolution. The regression now pins one file first, adds compatible drift second, and compares exact lock/snapshot bytes and references before plan/run.
- Significant, repaired: the canonical Arrow hash inherited delimiter concatenation and `DataType::to_string()`, omitting nested child metadata. It now uses recursive length-prefixed encoding with adversarial collision and nested-identity tests.
- Significant, repaired: new public evidence structs were exhaustive construction traps. New authority types are non-exhaustive with validated constructors/accessors; the two existing public aggregate structs receive documented one-time pre-1.0 migrations.
- Significant, repaired: empty files produced no batch from which execution could attest schema evidence. Readers now emit one typed zero-row evidence batch; output rows remain zero.
- Significant, repaired: attempted-file equality initially assumed all planned partitions run, which fails under limits, and per-file artifact identity implied one partition per file. Evidence now keys attempted observations and supports repeated partitions plus early termination.
- Pass: destination planning consumes the effective schema and effective snapshot hash through the generic resource trait; baseline authority remains unchanged.
- Pass: package identity includes plan authority and per-attempt coercion sidecars; verification detects tampering and replay needs no source contact.
- Pass: legacy plans omit the optional evidence field safely; workspace, golden, docs, lint, supply-chain, and scoped semver gates are green apart from the two recorded migrations.

## Verdict

Pass. All material authority, scalability, source-neutrality, hashing, cardinality, and public-extension findings were repaired before closure.

## Residual risk

The current product adapter that creates runtime-effective observations is exhaustive local binary discovery. The kernel/engine seam is not file-specific, but each future source archetype must supply a deterministic bounded observation identity and pass conformance before claiming this behavior. Incompatible observations and all-quarantine checkpoint semantics remain explicitly owned by A10e rather than being weakened here.
