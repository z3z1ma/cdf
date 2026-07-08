Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws2b-clap-parser-foundation.md, .10x/decisions/cli-command-grammar-and-parser.md, .10x/specs/project-cli-observability-security.md

# P1 product WS2B clap parser foundation evidence

## What was observed

WS2B replaced the `cdf-cli` hand-rolled parser foundation with a clap v4 builder parser while preserving the existing dispatcher-facing argument structs and command enum flow.

Observed implementation facts:

- `crates/cdf-cli/Cargo.toml` now declares `clap` 4.6.1 with the already-locked builder/help/usage/error-context feature set.
- `Cargo.lock` changed only to add the existing locked `clap` package edge to `cdf-cli`; no clap package version or transitive package version changed.
- `crates/cdf-cli/src/args.rs` now builds a clap command tree and converts `ArgMatches` into the existing `Cli`, `Command`, `ScanArgs`, `RunArgs`, state, replay, backfill, package, inspect, contract, and SQL argument shapes.
- The explicit compatibility pre-pass for `--json`, `--project`, and `--env` remains in `Cli::parse`, so those globals are accepted anywhere in argv before clap parses the command-specific surface.
- Help is now parser-generated. `cdf <command> --help`, nested help such as `cdf state rewind --help`, and `cdf help state rewind` are handled at the parser layer.
- Unknown command errors continue to flow through `InvocationResult::from_error`, preserving exit 2 and the existing JSON error envelope shape.
- Command modules still receive the same semantic argument structs; WS2B did not add `--to`, no-arg resource defaults, minted package/checkpoint ids, `--scope key=value`, bare resume behavior, completions, man pages, or renderer changes.

Focused parser tests added or updated:

- root clap help lists the root command surface;
- `cdf plan --help` and `cdf state rewind --help` render subcommand help;
- `cdf help state rewind` renders nested help;
- `--json` after a subcommand help request still returns a success JSON envelope;
- `--project`, `--env`, and `--json` after `validate` are still honored by the global compatibility pre-pass.

## Procedure

Build, test, and quality commands run:

- `cargo check -p cdf-cli --locked`
- `cargo test -p cdf-cli parser_ --locked`
- `cargo test -p cdf-cli help_lists_required_command_surface --locked`
- `cargo test -p cdf-cli unknown_command_returns_usage_exit_code --locked`
- `cargo test -p cdf-cli --locked`
- `cargo fmt --all`
- `cargo fmt --all -- --check`
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`
- Direct unsafe scan:
  `rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|Unpin|Send|Sync" crates/cdf-cli/src/args.rs crates/cdf-cli/src/commands.rs crates/cdf-cli/src/lib.rs crates/cdf-cli/src/tests.rs`
- Source-only Gitleaks over touched files and final WS2B records from a temporary directory:
  `gitleaks dir --no-banner --redact --report-format json --report-path /tmp/firn-ws2b-gitleaks.json /tmp/firn-ws2b-gitleaks.*`
- Focused jscpd over touched parser implementation files:
  `jscpd --format rust,toml --min-lines 8 --min-tokens 80 --reporters console,json --output /tmp/firn-ws2b-jscpd-parser-only --exit-code 0 crates/cdf-cli/Cargo.toml crates/cdf-cli/src/args.rs crates/cdf-cli/src/commands.rs`
- Broader focused jscpd over touched Rust/TOML parser and test files:
  `jscpd --format rust,toml,markdown --min-lines 8 --min-tokens 80 --reporters console,json --output /tmp/firn-ws2b-jscpd-final --exit-code 0 crates/cdf-cli/Cargo.toml crates/cdf-cli/src/args.rs crates/cdf-cli/src/commands.rs crates/cdf-cli/src/tests.rs <WS2B records>`
- Record-focused jscpd attempts:
  `jscpd --format markdown --min-lines 8 --min-tokens 80 --reporters console,json --output /tmp/firn-ws2b-jscpd-records --exit-code 0 <temporary copied records>` and the same command with `.markdown` extensions.

## Results

- `cargo check -p cdf-cli --locked`: passed.
- Focused parser tests: passed, 5 tests.
- Root help compatibility sentinel: passed, 1 test.
- Unknown command JSON/exit sentinel: passed, 1 test.
- `cargo test -p cdf-cli --locked`: passed, 138 library tests, 1 integration test, and 0 doctests.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- Direct unsafe scan: no matches in touched Rust files.
- Source-only Gitleaks: passed with no leaks.
- Parser implementation jscpd: passed with 3 files analyzed, 0 clones, 0 duplicated lines.
- Broader Rust/TOML touched-file jscpd: completed with 4 files analyzed, 25 clones, 345 duplicated lines, 4.46% duplicated lines, and `newClones = 0`. The duplicate signal is in the existing large CLI test/source surface, not in the parser implementation scan.
- Markdown record jscpd: attempted against copied `.md` files and copied `.markdown` files, but this installed jscpd build reported 0 Markdown files analyzed in both cases. Record duplicate detection therefore has no useful tool signal; the limitation is recorded rather than treated as a pass.

Parent verification after worker completion:

- Parent inspected the implementation diff and tightened one generic parser usage message in `optional_path_arg` from "package root" to "path" so non-package paths do not inherit package wording.
- `cargo test -p cdf-cli parser_ --locked`: passed, 5 tests.
- `cargo test -p cdf-cli unknown_command_returns_usage_exit_code --locked`: passed, 1 test.
- `cargo test -p cdf-cli help_lists_required_command_surface --locked`: passed, 1 test.
- `cargo test -p cdf-cli --locked`: passed, 138 library tests, 1 integration test, and 0 doctests.
- `cargo check -p cdf-cli --locked`: passed.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- Direct unsafe/FFI scan over touched Rust files: no matches.
- `semgrep --config auto --quiet --error` over touched Rust files: passed with no findings.
- Source-only Gitleaks over touched code and WS2B records copied to `/tmp`: passed with no leaks.
- Parser implementation jscpd rerun after parent wording repair: 3 files analyzed, 0 clones, 0 duplicated lines.
- Broader jscpd over touched Rust/TOML/record inputs: 4 source files analyzed, 25 clones, 345 duplicated lines, 4.46% duplicated lines. Markdown records were not analyzed by the installed jscpd build; the clone signal remains in existing Rust test/source regions.
- `rust-code-analysis-cli` metrics were captured for `crates/cdf-cli/src/args.rs` and `crates/cdf-cli/src/commands.rs` under `target/quality/reports/ws2b-rust-code-analysis-*.json`.
- `scc` over touched parser/test files reported Rust/TOML size and complexity: 4 files, 7,737 total lines, 2,339 code lines, total complexity 53.
- `cargo tree -p cdf-cli --locked -i clap@4.6.1`: passed and showed `clap v4.6.1 -> cdf-cli`.
- `cargo audit --deny warnings`: failed only on already-ratified `RUSTSEC-2024-0436` for `paste 1.0.15`; `cargo audit --ignore RUSTSEC-2024-0436` passed.
- `cargo deny check advisories`: passed.
- `cargo deny check`: passed; duplicate Arrow-major warnings are the already-recorded DuckDB private-driver residual.
- `cargo vet --locked`: passed.
- `osv-scanner --lockfile Cargo.lock`: nonzero only for already-ratified `RUSTSEC-2024-0436` on `paste 1.0.15`, with no fixed version.
- `tools/codeql-rust-quality.sh`: refreshed `target/quality/codeql-db-rust` because Rust source, manifest, or lockfile content changed; analysis completed through the reusable database path. `jq '[.runs[].results[]?] | length' target/quality/reports/codeql-rust-current.sarif` returned `0`. Extractor warnings were the known macro-heavy CodeQL Rust extractor profile recorded in `.10x/knowledge/quality-gate-execution.md`.
- `git diff --check`: passed.
- `rg -n "[Kk]iller[ _-]?[Dd]emo" . --hidden`: found no matches.

## What this supports

This supports closing WS2B. The parser foundation is now clap-backed, command dispatch compatibility is preserved, global `--json`/`--project`/`--env` compatibility remains intact, unknown command JSON error compatibility is retained, and per-subcommand help exists at the parser layer.

## Limits

This evidence does not claim WS2C product grammar semantics or WS2D generated artifacts. `--to` aliases, minted run/package/checkpoint identifiers, environment-derived destination defaults, human `--scope key=value`, no-arg resume draining, generated shell completions, man pages, styled help snapshots, and renderer changes remain owned by their separate tickets.
