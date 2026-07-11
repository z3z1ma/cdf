Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Vectorized contract validation

## Purpose and scope

This specification governs validation-plan binding, vector rule kernels, bitmap verdict algebra, selected-row evidence, engine integration, scalar differential testing, memory/determinism, performance measurement, and extension rules.

## Plan binding and kernels

The serialized `ValidationProgram` remains semantic authority. Runtime binding MUST verify schema/hash/types once and produce a typed vector plan without changing program identity. Null/domain/range/type rules MUST support the full declared Arrow types for which the semantic compiler admits them; unsupported combinations fail during plan/deep validation with exact rule/field/type/remediation.

Kernels MUST preserve null, NaN, signed zero, decimal scale/precision, timezone/unit, collation/byte, dictionary, nested, and domain comparison semantics established by the program. Optimized and scalar oracle outcomes MUST be identical, including rule ids, precedence, total verdict, counts, row order, and selected evidence.

## Bitmap verdict algebra and output

Each rule produces or contributes to a bitmap/selection representation. Combination follows canonical serialized rule order and the total verdict lattice. All-pass paths retain aggregates/masks and MUST NOT allocate one object/string/value per row. Accepted/quarantine/residual/drop selections are ownership-aware Arrow filters/takes or equivalent vector operations under the memory ledger.

Detailed values, JSON residuals, and row-level evidence are generated only for rows whose disposition requires them. High-failure cases remain bounded and spill evidence through the ordinary sink. Reusing scans/bindings across rules MUST NOT erase per-rule attribution.

## Integration and fallback

The neutral evaluator MUST NOT depend on DataFusion. Engine fusion may consume masks to combine validation with projection/filter/normalization where measurement proves benefit, but the scalar oracle and standalone contract tests remain engine-free. No supported native rule silently falls back to per-row production evaluation.

Custom Python/WASM/subprocess rules use declared foreign execution/performance classes and return ordinary verdict masks/outcomes; they are excluded from native-kernel throughput claims. New built-in rules require semantic oracle cases, vector implementation or explicit non-vectorized status, cost/memory declaration, and benchmark/conformance cells.

## Correctness and performance matrix

Property/fuzz tests generate Arrow arrays/programs and compare vector versus scalar outcome for every admitted type/rule/disposition, including chunk/dictionary/slice/null/NaN/boundaries and mixed rules. Jobs/batch-size changes cannot alter verdict/package identity.

The lab MUST report bytes actually inspected, rows, rules, cycles/instruction, branches/misses, allocations, bitmap/evidence bytes, and throughput for numeric null/range, string domain, decimal/timestamp, mixed TLC-width, nested/variant boundaries, and 0/1/100% violation density at 8k/16k/64k rows. Kernel-only, selected-row materialization, and end-to-end validation phases are separate.

Program close requires ≥1 GB/s/core for the ratified 64k hot-rule matrix on the reference host class or a focused target supersession backed by roofline evidence. It also requires no correctness/evidence overhead loophole: bytes not inspected cannot inflate throughput.

## Explicit exclusions

No general expression JIT, GPU, unsafe SIMD, regex/custom semantic invention, or weakening of verdict/evidence semantics is specified here.
