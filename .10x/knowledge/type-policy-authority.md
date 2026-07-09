Status: active
Created: 2026-07-09
Updated: 2026-07-09

# Type-policy authority belongs to compiled identity

## Rule

Type coercion and lossy-mapping allowances are semantic contract authority. A runtime dependency, connector constructor, trust preset, test hook, or process-local default MUST NOT grant that authority independently of the compiled resource.

An allowance is executable only when its user-visible configuration surface is ratified, compiled into the plan, serialized into package identity/evidence, and rendered as the corresponding total coercion verdict. Until such a surface exists, `coerce_types` and `allow_lossy_mapping` remain false unless an already-governing active specification and compiled record explicitly say otherwise.

## Why

The B7 REST closure review found that a runtime-only `with_type_policy` builder could authorize parse or lossy casts after compilation. The resulting rows and verdicts could differ without changing the plan or package identity, violating plan-first execution, deterministic replay, and the P2 anti-convergence rule.

## Application

- Source adapters may observe physical types and execute a compiled coercion plan; they may not invent policy.
- Shared readers should accept either the already-compiled plan or a strict policy derived from compiled input, not a mutable runtime allowance.
- Tests for strict defaults should prove unauthorized parse/lossy cases cannot emit `CoercedByPolicy` or `LossyAllowed` evidence.
- Adding a new TOML/CLI policy surface requires a specification/decision update, JSON Schema coverage, plan/package identity coverage, command rendering, and conformance.

Width-preserving automatic widenings remain governed by the active schema-intelligence specification and are not an implicit parse/lossy allowance.
