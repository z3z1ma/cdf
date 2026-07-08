Status: active
Created: 2026-07-08
Updated: 2026-07-08

# Python front-door product surface

## Purpose and scope

This specification governs how Tier 2 `python://` resources appear through the CDF product surface: project resource resolution, `cdf plan`, `cdf preview`, `cdf run`, interpreter remediation, deterministic cross-interpreter behavior, and the dlt shim graduation path.

It derives from `VISION.md` Chapters 9, 18, 20, 21, and 23; decisions D-23 and D-25; `.10x/specs/resource-authoring-planning-batches.md`; `.10x/specs/project-cli-observability-security.md`; `.10x/specs/run-orchestration-ledger.md`; `.10x/tickets/done/2026-07-05-python-sdk-bridge.md`; `.10x/tickets/done/2026-07-05-dlt-shim-preview.md`; and `.10x/tickets/done/2026-07-06-python-doctor-interpreter-probe.md`.

## Behavior

`cdf.toml` resources whose `source` is `python://...` MUST resolve through the same project resource lookup path as declarative file, REST, and SQL resources. Product commands MUST use the configured `[python].interpreter` rather than an ambient interpreter unless a later decision ratifies an explicit fallback.

Python remains an authoring and interchange tier. The Python bridge MAY execute trusted resource code to produce Arrow-compatible batches, but package building, destination commits, receipts, checkpoints, replay, resume, and downstream execution MUST remain Rust-owned.

`cdf doctor` remains the remediation authority for interpreter health. `plan`, `preview`, and `run` MUST fail closed with a remedial message that points to doctor when a Python resource lacks a configured interpreter or the configured interpreter fails the fixed probe. Doctor probes MUST NOT import or execute project Python resource code.

`cdf plan` for a Python resource MUST report the resource descriptor, schema, projected partitions, pushdown honesty, delivery guarantee, and destination planning preview without committing packages, destinations, checkpoints, or run-ledger state. Planning MAY execute resource description code only through a constrained Python bridge path that is necessary to discover descriptor/schema metadata. If metadata discovery would require unconstrained row production, planning MUST fail closed with a remediation note instead of fabricating schema or capability claims.

`cdf preview` for a Python resource MUST sample the first deterministic planned partition and the first emitted batch without creating package, destination, checkpoint, or run-ledger state. Preview MUST route data through the PyO3/Arrow bridge and MUST preserve redaction boundaries for secrets, logs, stderr, and structured errors.

`cdf run` for a Python resource MUST feed the emitted Arrow batches into the general run spine. A successful run MUST produce the same package, destination commit, receipt verification, checkpoint gating, run-ledger transitions, replay inputs, and resume-compatible artifacts as a non-Python resource with equivalent Arrow batches.

When deterministic inputs are fixed, GIL and free-threaded Python interpreter builds MUST produce identical package hashes and observable product reports. Free-threaded builds MAY add parallelism where the bridge declares it safe, but parallelism MUST NOT change package identity, checkpoint positions, receipts, or JSON output fields.

The dlt shim MUST graduate only behind evidence from a real dlt resource/source integration or an explicit GA gap list. Divergences from dlt behavior MUST be documented as compatibility data and MUST NOT delegate destination writes to dlt.

## Interfaces

`python://` resource URIs MUST identify the Python module/file and callable in a stable, project-relative form. Resolution MUST reject ambiguous, missing, non-callable, or path-escaping resource targets.

Product reports MAY add Python-specific structured details, but existing JSON field names are additive-only. Human output MUST use the active renderer once the renderer workstream is available.

Interpreter errors MUST be classified under the shared CLI error taxonomy and MUST redact secrets and environment values that could contain secrets.

## Acceptance criteria

- `cdf plan my.python_resource` resolves the project resource, reports honest planning facts, and writes no package, destination, checkpoint, or run-ledger state.
- `cdf preview my.python_resource` samples one batch through the Python bridge and writes no package, destination, checkpoint, or run-ledger state.
- `cdf run my.python_resource` completes through the general run spine with package creation, destination commit, trait-level receipt verification, checkpoint gating, run-ledger transitions, and replay/resume-compatible artifacts.
- Interpreter-missing and interpreter-unhealthy paths point to `cdf doctor` and do not execute project resource code during doctor probing.
- Deterministic GIL and free-threaded runs have identical package hashes where the same input and bridge semantics are declared deterministic.
- dlt GA work has either real dlt-source integration evidence or a recorded gap list with owners.

## Explicit exclusions

This spec does not make Python the execution substrate. It does not introduce dynamic Rust plugins, WASM resource execution, subprocess authoring changes, a scheduler, or dlt destination delegation. It does not weaken the existing doctor no-resource-code-execution guarantee.
