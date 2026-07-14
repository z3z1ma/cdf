Status: done
Created: 2026-07-08
Updated: 2026-07-13
Parent: .10x/tickets/done/2026-07-08-p1-product-ws2-command-grammar-redesign.md
Depends-On: .10x/decisions/superseded/cli-command-grammar-and-parser.md

# P1 product WS2B: Clap parser foundation

## Scope

Replace the hand-rolled parser foundation in `cdf-cli` with a clap v4 derive parser while preserving current command behavior and JSON/exit compatibility.

Owns `crates/cdf-cli/Cargo.toml`, `crates/cdf-cli/src/args.rs`, `crates/cdf-cli/src/lib.rs`, and parser-focused tests.

## Acceptance criteria

- `cdf-cli` uses clap v4 derive or an equivalent clap builder surface ratified by `.10x/decisions/superseded/cli-command-grammar-and-parser.md`.
- Existing `Command`/args shapes or their replacements still drive the current command dispatcher without broad command-module rewrites.
- `--json`, `--project`, and `--env` compatibility is preserved, including current support for `--json` anywhere in argv.
- Unknown command usage errors retain exit 2 and JSON error envelope compatibility.
- Per-subcommand `--help` exists at the parser layer, even if final styled help snapshots are owned by a later child.
- Existing parser/CLI tests pass or are intentionally updated only for parser-foundation behavior ratified by the decision.

## Evidence expectations

Focused parser tests, `cargo test -p cdf-cli --locked` for parser/help cases, `cargo fmt --all --check`, focused clippy, source-only Gitleaks, direct unsafe scan, and focused `jscpd` over parser files.

## Explicit exclusions

No product grammar semantic changes such as default-minted run ids, `plan` destination defaults, no-arg `resume`, or `--scope key=value`; WS2C owns those. No generated completions/man pages; WS2D owns them. No renderer migration.

## Progress and notes

- 2026-07-08: Replaced the hand-rolled parser in `crates/cdf-cli/src/args.rs` with a clap v4 builder command tree that converts back into the existing dispatcher-facing args structs.
- 2026-07-08: Preserved the global compatibility pre-pass for `--json`, `--project`, and `--env` anywhere in argv, added parser-layer subcommand help, and kept WS2C semantic grammar changes out of scope.
- 2026-07-08: Added focused parser/help tests for nested help, `cdf help <path>`, JSON help envelopes with `--json` after the subcommand, and globals after `validate`.
- 2026-07-08: Closure evidence recorded in `.10x/evidence/2026-07-08-p1-product-ws2b-clap-parser-foundation.md`; adversarial review recorded in `.10x/reviews/2026-07-08-p1-product-ws2b-clap-parser-foundation-review.md`.
- 2026-07-08: Parent review tightened one generic path-argument error message and reran focused/full parser quality gates, including jscpd, complexity metrics, supply-chain scanners, and reusable CodeQL. Parent-observed evidence is appended to `.10x/evidence/2026-07-08-p1-product-ws2b-clap-parser-foundation.md`.

## Blockers

None.
