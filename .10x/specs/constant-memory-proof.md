Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Constant-memory proof protocol

## Purpose and scope

This specification governs process-tree budget resolution/enforcement, headroom calibration, allocation classification, stress generation/workloads, metrics, failure semantics, diagnostics, and closure audit.

## Budget resolution

Resolution MUST occur before task admission and include requested/default process budget, effective cgroup/container/host limit, headroom policy version/components, managed pool, discovery/other subcaps, child budgets, and spill disk. The managed pool MUST be strictly below the process budget.

F1 MUST calibrate exact headroom/default-resolution values from named host/runtime/library tuples before A2 implementation. Values MUST include thread stacks/runtime idle, allocator/native baseline and load peaks, parser/compression/database client/DataFusion behavior, and foreign-runtime modes. Unknown native growth cannot be admitted as fixed headroom.

## Enforcement and observation

Linux enforced tests MUST create/verify a cgroup v2 scope when supported, set calibrated limits, run CDF as a child, and collect memory peak/events plus process-tree RSS high-water. Missing permission/provider produces an explicit unavailable cell, never a false pass. At least one release host class must enforce the law.

The runner MUST record managed current/peak by owner, process/child RSS high-water, cgroup current/peak/events, mapped/file-backed observations where available, spill bytes, and measurement method/resolution. Metrics are not interchangeable.

Child adapters MUST inherit the aggregate authority and have declared admission budgets. A child exceeding its sub-budget fails/cancels the run cleanly where the platform permits; signal/OOM termination is reported as internal/resource failure, never success.

## Allocation classification and audit

Production data-bearing queues/APIs cannot carry naked buffers. Static and runtime audit MUST enumerate source/format/engine/contract/package/transport/destination/interop allocation sites and classify them. Native calls declare peak scratch/internal threads or run inside conservative admitted envelopes falsified by stress.

Metadata cardinality is part of memory. Plans, listings, manifests, observations, verdict/provenance, lineage, events, acknowledgements, and progress use bounded aggregates or append/spill-backed sinks. “Small per item” is not an exemption.

## Stress workloads

Generators MUST be deterministic, chunked, and measured separately. Dataset recipes record logical/physical bytes, rows/files/segments/schema/key distributions, compression ratio, expected spill, and semantic assertions.

The mandatory matrix includes the decision's workload classes. Each case asserts semantic completion/package verification, configured budget resolution, high-water ceiling, expected spill/cleanup, no cgroup OOM/kill, and stable throughput after warmup. Geometric-size cases demonstrate no input-size slope beyond bounded noise.

The too-small case MUST fail before an impossible single working set allocation and render minimum legal budget plus tuning/remediation. Spill-too-small/disk-full cases fail separately without OOM or orphaned files.

## Product diagnostics

`cdf doctor` MUST show effective process/container/default authority and spill disk readiness. `cdf run --explain-memory` and final panels show budget, managed pool, native headroom, peak RSS/process-tree where available, largest owners, waits/flushes/spill, effective jobs, and unavailable measurement caveats. Secret/path redaction applies.

## Conformance and closure

Cancellation/error/panic/OOM-simulation tests MUST release managed leases and clean/hand off spill/staging. Repeated runs detect leaks. Budget/host changes cannot alter package identity above legal minimum.

WS-F cannot close while a production whole-input/package/listing/metadata collection or unclassified native/child allocation remains. A generated owner matrix maps every identified materialization to implementation evidence or a measured bounded no-action rationale.

## Explicit exclusions

This spec does not use RSS for allocation admission, count spill disk as memory, guarantee OS page-cache behavior, preselect an allocator, or make unsupported host enforcement appear portable.
