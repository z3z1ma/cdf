Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-h3-adhoc-parquet-run.md, .10x/tickets/done/2026-07-09-p2-ws-a9-local-arrow-ipc-discover-run.md

# P2 H3 and A9 integration evidence

## What was observed

H3 adds evidence-preserving ad-hoc execution for one local or stable HTTP(S) Parquet file. The synthesized resource has a stable canonical-location-derived id, persists only safe `.cdf/adhoc/` paths, pins through the existing discovery lifecycle, and rejoins the ordinary plan/package/destination-receipt/checkpoint/run-ledger spine. Local source paths are staged under a hashed path; URL userinfo, signed material, unsupported schemes, invalid secret-bearing local paths, and preconfigured synthetic-id collisions fail before ad-hoc or runtime mutation. A destination failure leaves an integrity-verifiable `Loading` package and terminal `RunFailed` evidence without receipt or checkpoint advancement; a fresh-package retry reuses the resource identity and succeeds. Same-package ad-hoc resume is explicitly excluded because the current resume command cannot preserve H3's explicit destination selection.

A9 adds deterministic local single-file Arrow IPC file-framing discovery, pin/show/diff/no-pin/auto-pin lifecycle, and package-producing execution. Generic discovery selects candidates through contained glob traversal plus bounded compression magic inspection, without runtime manifest SHA calculation. The seekable schema probe measures actual logical bytes read and the generic CLI path stays below half of a one-megabyte payload. Preview and run use the same pinned-schema reconciliation reader; runtime still computes and commits the exact full SHA-backed `FileManifest`. Unsupported remote, compressed, multi-file, malformed, truncated, and stream-framed discovery cases fail without writes. Package integrity, DuckDB receipt verification, pinned hash agreement, physical provenance, coercion evidence, manifest identity, and checkpoint equality were inspected directly.

## Procedure

- H3 focused tests passed 6/6; `cdf add` regressions passed 4/4; migrated command-family error precedence passed 1/1.
- A9 focused Arrow IPC CLI tests passed 5/5; format discovery tests passed 4/4; the full affected library set passed 467/467 before final integration.
- Final `cdf-cli` nextest passed 240/240.
- Final all-feature workspace check, warnings-denied Clippy, formatting, and diff checks passed.
- Workspace doc tests and documentation generation passed.
- `cargo deny check` passed with the repository's existing duplicate-version warnings; `cargo audit` passed with only the existing allowed `RUSTSEC-2024-0436` `paste` maintenance warning; semver checks passed against `origin/main`.
- `cargo llvm-cov --workspace --all-features --locked --summary-only` passed. Totals: 81.86% regions, 78.00% functions, and 83.35% lines.
- Gitleaks v8.18.4-compatible binary scanning of every changed and untracked file completed with no leaks.
- Two full parallel workspace nextest runs each executed 792 tests and passed 791; only the standalone-green S5 recorded-HTTP request assertion failed. An immediate isolated S5 rerun passed 1/1, and coverage's sequential full test execution also passed S5. Inspection proved the fixture treats accepted-socket `WouldBlock` as EOF and can capture a partial header under parallel load. This unrelated integration-harness defect was closed by `.10x/tickets/done/2026-07-09-p2-ws-i5-recorded-http-request-capture-race.md`; it did not exercise H3 or A9 code.

## What this supports or challenges

This supports every acceptance criterion of H3's bounded ad-hoc Parquet slice and A9's bounded local Arrow IPC file-framing slice. It also demonstrates that the initial adversarial blockers—H3 secret/id boundaries and A9's hidden full-file discovery read—were repaired with product-path regressions rather than asserted away.

The evidence challenges any claim that the complete workspace parallel suite is currently green: it is 791/792 until I5 repairs the S5 fixture. H3/A9 closure relies on green focused, affected-crate, CLI, sequential coverage, and independent review evidence, not on misreporting that known unrelated failure.

## Limits

H3 excludes broader source kinds, credentials, HTTP glob enumeration, and same-package ad-hoc resume. A9 excludes remote/stream/compressed/multi-file IPC, text sampling, Hints, and format auto-detection. A10 separately owns the same generic-dispatch boundedness regression discovered in local Parquet. D5 owns binary extension-plus-magic auto-detection after A9.
