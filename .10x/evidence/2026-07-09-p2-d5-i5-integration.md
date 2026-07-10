Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-d5-binary-format-autodetection.md, .10x/tickets/done/2026-07-09-p2-ws-i5-recorded-http-request-capture-race.md

# P2 D5 and I5 integration evidence

## What was observed

D5 makes the two unambiguous binary file formats inferable without a `format` declaration: a resource glob ending in `.parquet` or `.arrow` compiles to one concrete format decision while retaining whether the choice was explicit or inferred. Local resolution confirms extension plus head/footer magic for every matched file before partitions are emitted. The format decision and its extension/magic evidence are recorded on partitions and revalidated by the common preview/run open path. Direct HTTPS Parquet confirmation uses exactly two bounded six-byte range reads. Unsupported HTTPS Arrow is rejected at the shared plan/deep-validation partition seam before transport contact, and mismatch diagnostics are command-neutral while naming the resource, file, declared/inferred/resolved format, extension signal, magic signal, and remediation.

I5 repairs only the deterministic S5 recorded-HTTP fixture. Header capture now retries `WouldBlock` and `Interrupted` until a complete header, EOF, the 8192-byte cap, or a one-second deadline. The fixture restores blocking response I/O with a one-second write timeout, stores contextual worker failures for assertion-time reporting, and never panics from `Drop`/join. The S5 scenario still asserts exactly four authenticated requests and its existing schema snapshot, normalization, package, receipt, checkpoint, cursor, and secret-redaction evidence.

## Procedure

- D5's focused and affected verification passed 329/329 after reviewer repair. The suite covers glob-first inference, every-match local confirmation, local Arrow lifecycle, local Parquet package execution, no-write mismatch diagnostics, bounded HTTPS Parquet confirmation, and pre-contact HTTPS Arrow rejection.
- I5's lifecycle plus S5/S7 verification passed 6/6; post-repair S5 repetition passed 10/10. The fixture regressions include split headers, deterministic `WouldBlock`, EOF/cap/deadline errors, a four-MiB delayed-read response, and non-panicking failure teardown.
- Final parallel `cargo nextest run --workspace --all-targets --no-fail-fast` passed 809/809, including S5 and the 100-run golden/live stability tests.
- Workspace formatting, `git diff --check`, all-target check, warnings-denied workspace Clippy, warnings-denied documentation, and doc tests passed.
- `cargo deny check`, `cargo audit`, `cargo vet --locked`, `cargo machete --with-metadata`, and workspace semver checks against `HEAD` passed. Audit reported only the existing allowed `RUSTSEC-2024-0436` unmaintained-`paste` warning; OSV reported that same ratified advisory and no additional finding.
- Semgrep `p/rust` scanned the touched Rust source with zero findings. Scoped source scans over the five touched crates and `.10x/` found no secrets.
- `cargo llvm-cov --workspace --all-targets --summary-only` passed. Totals: 82.92% regions, 78.72% functions, and 84.37% lines.

## What this supports or challenges

This supports every D5 and I5 acceptance criterion. Binary inference is attached to the resource/glob plan and validates all local matches; it is not a one-file decoder shortcut. The final full-workspace pass demonstrates that the S5 parallel flake is closed without weakening its assertions.

The evidence also supports the architectural boundary raised during this tranche: D5 improves format resolution for multi-file resources, but it does not claim that schema discovery/pinning is multi-file complete. That larger resource-level aggregation contract remains owned and blocked explicitly by A10 rather than being hidden behind the binary-format work.

## Limits

D5 intentionally does not infer text formats, aliases such as `.ipc`/`.feather`, compressed Parquet/IPC, arbitrary HTTP listings, or remote Arrow IPC. I5 is test-harness-only. Multi-file schema aggregation, pinned discovery-manifest identity, file-level schema verdicts, remote enumeration, and full S1-S4/S6/S8 closure remain active P2 work.
