Status: active
Created: 2026-07-10
Updated: 2026-07-10

# Performance lab and terabyte-scale envelope

## Purpose and scope

This specification governs P3 benchmark datasets, measurement protocol, phase telemetry, roofline comparisons, regression gates, constant-memory stress evidence, and the generated performance-envelope document. Runtime semantics remain governed by `.10x/specs/architecture-layering-runtime.md`; package, receipt, checkpoint, and destination semantics remain governed by their focused active specifications.

## Dataset catalog

The lab MUST describe datasets as deterministic generation or acquisition specifications. Large generated or downloaded data MUST NOT be committed.

The catalog MUST include:

- full-year NYC TLC Parquet, with a recorded-fixture substitute for ordinary CI;
- TPC-H SF10 and SF100 in Parquet and CSV;
- deterministic wide, nested, malformed, and schema-varying JSON/NDJSON;
- a generated 100 GB constant-memory stress input;
- small/startup fixtures that expose fixed overhead separately from throughput.

Every catalog entry MUST record logical rows, encoded bytes where known, schema, generator/source version, seed or immutable content identity, and licensing/provenance.

## Measurement protocol

The macro runner MUST record wall time, CPU time where available, rows, logical bytes, physical bytes, peak RSS, spill bytes, and phase durations/bytes. Warm and cold I/O modes MUST be distinct and MUST NOT be averaged together.

Reference runners MUST include raw arrow-rs readers for applicable formats, raw sequential device read/write, memcpy bandwidth, DuckDB native read/COPY paths, and Polars scans where the dependency or external binary can be exercised without contaminating the CDF build. Every comparison MUST carry a bias label describing semantic work omitted or added by either side.

The event spine MUST expose duration and byte facts sufficient to break down decode, validation/normalization, segment encode, persistence/hash, destination write, finalize/receipt, and checkpoint gate. Timing and rendering MUST remain outside deterministic artifact identity.

Criterion microbenchmarks SHOULD isolate validation kernels, hashing, Arrow/foreign interop, encoding, and other repeatable CPU kernels. Macro results MUST NOT be inferred from microbenchmarks alone.

## Baselines and regressions

Before any P3 runtime or decoder optimization lands, WS-L MUST record the current full baseline on a named host class. Missing scenarios MUST be recorded as failed/unavailable cells, not omitted.

Comparable CI results use median-of-N. A regression greater than 10% against the current baseline for the same host class and mode MUST fail. High variance, changed hardware, changed reference version, or missing cache-state control MUST make the comparison inconclusive rather than green.

Baseline changes require evidence naming the code/dependency/environment change, old and new distributions, and whether the movement is expected. A baseline MUST NOT be reset merely to clear a failure.

## Memory law

Peak runtime memory MUST be a function of the resolved memory budget and bounded fixed overhead, not input size. The slow tier MUST generate and process 100 GB under a 2 GiB budget, assert the ceiling with a documented measurement method, observe spill when planned, and complete successfully. A separate below-minimum test MUST fail with a `Data`-class error and concrete remediation before the operating system kills the process.

## Envelope artifact

The lab MUST generate a human-readable envelope document containing host descriptions, target and observed tables, roofline ratios, absolute rates, overhead percentage, peak memory, known bias, and profiles. README performance claims MUST link to this artifact and MUST NOT exceed its evidence.

## Acceptance scenarios

Given an unchanged host class and benchmark fixture, when the macro suite runs N times, then the report records the median and dispersion and compares only like-for-like warm/cold modes.

Given a raw reference that omits packages, validation, receipts, or checkpoint work, when its result appears beside CDF, then the report names that bias and computes CDF evidence overhead explicitly.

Given the generated 100 GB stress input and a 2 GiB budget, when the stress suite runs, then peak RSS remains within the ratified ceiling, spill is observable, and completion does not depend on input size.

Given a performance regression above 10%, when CI evaluates the same host class, then the gate fails without rewriting the baseline.

## Explicit exclusions

This spec does not authorize weakening deterministic artifacts, changing SHA-256, bypassing the commit gate, adding an ephemeral execution path, or publishing unlabeled marketing benchmarks. It does not itself authorize runtime, decoder, destination, or dependency changes.
