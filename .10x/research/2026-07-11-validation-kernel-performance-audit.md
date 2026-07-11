Status: done
Created: 2026-07-11
Updated: 2026-07-11

# Validation kernel performance audit

## Question

Does the current contract evaluator support the P3 ≥1 GB/s/core type/null/domain/range target, and where should vectorized validation live without leaking DataFusion semantics into the contract?

## Sources and methods

Inspected contract program/compiler/evaluator, engine execution/predicate/variant paths, benchmark harness/spec, dependency boundaries, and target envelope. Traced rule preparation, row iteration, value extraction, outcome construction, quarantine/residual use, and package dedup separation.

## Findings

`evaluate_record_batch` prepares each rule, then loops every row for every rule. Predicate evaluation calls per-row `is_null` and typed/lexical value conversion; domain/range decisions and row outcomes are assembled scalarly. Engine residual predicates and variant/residual construction also contain row loops. The only existing large test is correctness/timing-adjacent and does not establish a roofline or regression gate.

The semantic rule set is naturally vectorizable. Nullability uses Arrow validity bitmaps. Numeric/temporal/decimal range rules use typed comparison kernels. Domain rules use typed dictionary/hash membership or Arrow kernels. Rule masks combine by bitmap operations into total accepted/quarantine/residual/drop/error verdict masks. Type/schema validation occurs once per batch. Detailed row evidence and residual JSON need only touch violating/captured rows.

Putting semantics solely in DataFusion physical expressions would violate the engine-free contract boundary and weaken a lightweight reference path. `cdf-contract` should compile an executor-neutral vector kernel plan using Arrow arrays/compute primitives; `cdf-engine` fuses/adapts it into the graph. A simple scalar evaluator remains a test oracle for generated differential cases, not a silent production fallback for supported hot rules.

Benchmarking can be gamed by all-valid narrow integers or counting uninspected batch bytes. The envelope must report bytes actually inspected and rows/rules, with numeric null/range, string domain, decimals/timestamps, mixed TLC-width, nested/variant boundaries, and 0/1/100% violation densities at 8k–64k rows. Evidence serialization is measured separately from predicate kernels.

## Conclusion

Create a dedicated P3 validation-kernel workstream. Compile typed vector kernels and bitmap verdict algebra in `cdf-contract`, integrate zero-copy masks/selects into the fused graph, retain scalar differential/properties, and enforce the ≥1 GB/s/core target across a declared matrix.

## Limits

Exact Arrow compute dependency selection and fusion strategy require WS-L baseline/microbench evidence. Regex/custom code/domain structures beyond the current closed rule set need separate kernel tickets rather than scalar fallback disguised as completion.
