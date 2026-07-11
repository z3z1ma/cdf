Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-c1-scheduler-admission-contract.md

# Engine plans carry canonical source schedules

## What was observed

The CLI retains the neutral compiled source plan alongside each resolved queryable resource. After ordinary engine planning, it compiles and binds the canonical partition schedule into `EnginePlan` and `ExplainData`. Driver/version/physical-plan/partition authority therefore reaches the serialized plan before execution rather than being reconstructed from completion order.

Inspection remains no-contact and does not resolve secrets. Executable first-party declarative sources require a source driver plan and injected execution services; missing authority fails before runtime construction.

Effective jobs and host/memory ceilings are resolved after source and destination selection and rendered as runtime evidence. They are intentionally not added to `EnginePlan`, `ExplainData`, package trace, or any hash input, so the same plan remains portable across a laptop, constrained container, and future embedded/distributed host.

## Procedure

- keyless append file validate/plan/preview/run product scenario — passed with schedule binding.
- strict Clippy for engine and CLI all targets — passed.

## What this supports

Canonical ordinals and scheduler declarations are now observable plan evidence for live first-party execution, ready for C2/A5 to consume without changing package semantics based on scheduling.

## Limits

Foreign producer capability plans remain open under SX1/C3. Production fan-out and jobs-invariance artifacts remain C2/C4 scope.
