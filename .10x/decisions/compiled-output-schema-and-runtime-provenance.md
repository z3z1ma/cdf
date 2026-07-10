Status: active
Created: 2026-07-10
Updated: 2026-07-10

# Compiled output schema is distinct from contract and runtime provenance authority

## Context

Multi-input execution has three schema identities that cannot safely be collapsed: the immutable baseline/effective contract identity, the exact observed physical schema and coercion evidence for each input, and the Arrow schema emitted after projection, normalization, and framework residual columns. A destination must consume the last of these. Treating a physical file schema or a contract snapshot hash as the destination schema creates competing truths, breaks when files differ, and makes zero-row plans unverifiable.

Legacy serialized `EnginePlan` artifacts also predate explicit schema authority. Silently synthesizing authority while deserializing them would turn missing evidence into trusted evidence.

## Decision

`EnginePlan` carries two separate optional serialized authorities emitted by every current planner:

- `EngineSchemaAuthority` owns baseline and effective contract hashes.
- `EngineOutputSchema` owns the exact recursive Arrow schema after projection, normalization, and `_cdf_variant` materialization.

Package-producing execution and destination preflight MUST require and recompute both authorities. Legacy plans MAY deserialize with either authority absent for inspection, but MUST fail before source, package, state, or destination mutation until replanned. No sentinel or inferred trusted hash is permitted.

Readers preserve observed physical provenance through trusted batch metadata and exact coercion evidence. The engine validates that evidence before rebinding emitted arrays to the compiled output schema. Runtime-only physical metadata that cannot be stable across a multi-input resource does not become destination schema authority; its observed and constrained types remain in manifest-bound coercion artifacts.

The framework residual field is system-owned only when one contract-level classifier verifies the exact closed shape: `_cdf_variant`, nullable UTF-8, `cdf:semantic=json`, and `cdf:variant_encoding=residual-json-v1`. Destinations MUST consume that classifier rather than duplicating literals or granting the `_cdf_` namespace generally.

Current conformance and live-run fixtures MUST obtain plans from the real planner. Hand-authored current-plan JSON is prohibited because it creates a second construction path that drifts whenever authority fields evolve.

## Alternatives considered

- Use the effective snapshot hash as the emitted Arrow-schema hash: rejected because normalization, projection, and framework columns change the actual destination schema.
- Preserve physical metadata on the destination schema: rejected because one resource can have several valid physical schemas and one destination schema.
- Let destinations recompute output schema independently: rejected because it duplicates compiler semantics and increases destination extension cost.
- Trust missing legacy authority by deriving a sentinel or source hash: rejected because deserialization would manufacture execution authority.
- Special-case `_cdf_variant` in each destination: rejected because it duplicates a versioned framework contract and permits drift.
- Keep hand-authored plan fixtures: rejected because tests would certify stale plans rather than the production compiler.

## Consequences

- Zero-row plans and packages still have exact destination schema authority.
- Physical observations remain evidence without becoming a competing destination truth.
- Adding a destination requires consuming the plan schema and the shared framework-field classifier, not reproducing normalization or residual rules.
- Adding a source requires emitting neutral candidates and trusted physical/coercion evidence, not changing generic execution.
- Legacy plan inspection remains possible, while execution fails closed with a precise replan requirement.
- Plan/package hashes change when the actual emitted schema or evidence changes; golden fixtures must be regenerated through the canonical planner workflow.
