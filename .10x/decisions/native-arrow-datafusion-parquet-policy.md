Status: active
Created: 2026-07-06
Updated: 2026-07-06

# Native Arrow/DataFusion Parquet policy

## Context

CDF's book and decision register make Arrow and DataFusion load-bearing architecture. Parquet is not the canonical package data format, but it is the archive, interchange, file-source, and analytical tier. The first implemented Parquet source, Parquet destination, and package archive writer used DuckDB's bundled Parquet support to avoid introducing `RUSTSEC-2024-0436` through arrow-rs `parquet -> paste`.

Research in `.10x/research/2026-07-06-native-parquet-paste-risk.md` found:

- Latest `parquet 59.0.0` still depends unconditionally on `paste 1.0`.
- Latest `datafusion 54.0.0` still routes Parquet support through arrow-rs `parquet`.
- `RUSTSEC-2024-0436` is an informational unmaintained advisory with no patched version, not a known exploit, memory-safety, remote-code-execution, or data-corruption advisory.
- Current `deny.toml` has no advisory ignores because `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md` intentionally preserved strict advisory checks.

On 2026-07-06 the user explicitly ratified `.10x/tickets/done/2026-07-06-native-arrow-parquet-policy.md`, including the option to accept this one advisory temporarily because DataFusion is critical to the design.

## Decision

CDF will replace the DuckDB-backed Parquet workaround with native Arrow/DataFusion Parquet implementations.

CDF will accept a narrow, time-boxed exception for `RUSTSEC-2024-0436` only for `paste 1.0.15` introduced through the native arrow-rs/DataFusion Parquet dependency path.

The exception is not a broad permission to ignore unmaintained advisories. It must be represented in `deny.toml` and evidence records so advisory scanners still prove that no other advisory is ignored.

The implementation will proceed through bounded tickets:

- `.10x/tickets/2026-07-06-rustsec-paste-parquet-exception.md`
- `.10x/tickets/2026-07-06-native-parquet-file-source.md`
- `.10x/tickets/2026-07-06-native-parquet-writer-archive.md`

## Alternatives considered

Keep DuckDB-backed Parquet until upstream removes `paste`.

This keeps advisory gates clean, but it leaves a foreign FFI/backend shim in the exact data format path where CDF's architecture wants Arrow/DataFusion-native behavior. It also preserves Arrow-major bridge complexity and makes DataFusion less central than the book intends.

Use the latest native packages without an exception.

This is not currently available. Latest arrow-rs `parquet` still depends on `paste`, and DataFusion Parquet still routes through that crate.

Carry a fork or patch of arrow-rs Parquet that removes `paste`.

This may become necessary if the exception blocks a release or compliance requirement, but it is not the first choice. Carrying a fork of Apache Arrow Rust's Parquet implementation is a larger supply-chain and maintenance obligation than accepting this informational advisory with a precise owner and revisit trigger.

Ignore the advisory broadly.

Rejected. Broad ignores make future supply-chain findings harder to trust and violate the spirit of the existing policy gate.

## Consequences

The current DuckDB-backed Parquet surfaces are temporary technical debt, not the target architecture.

`cargo audit`, `cargo deny`, `osv-scanner`, `cargo vet`, Semgrep, CodeQL, source-only gitleaks, and direct unsafe scanning remain required quality gates. Evidence for native Parquet tickets must show the only accepted advisory is the ratified `paste` advisory on the approved dependency path.

The exception must be revisited at every Arrow/DataFusion dependency-pin review and no later than the next CDF minor dependency tuple decision under D-28. It must be removed when arrow-rs/DataFusion removes `paste`, when a maintained upstream replacement lands, or when a stronger supply-chain requirement forbids the exception.

If another advisory appears on the native Parquet path, this decision does not authorize accepting it.
