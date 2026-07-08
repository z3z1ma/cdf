Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws3-rendering-system-design-language.md
Depends-On: .10x/decisions/cli-design-language-and-renderer.md

# P1 product WS3B: Renderer foundation

## Scope

Introduce the dedicated `cdf-cli` renderer boundary without migrating every command.

Owns renderer modules, renderer dependencies, `CommandOutput` integration hooks, display mode detection, redaction boundary primitives, and foundational tests.

## Acceptance criteria

- `crates/cdf-cli/src/render/**` or equivalent exists with non-monolithic modules.
- TTY/headless, width, `NO_COLOR`, `CLICOLOR_FORCE`, and `--no-color` are represented in one renderer configuration boundary.
- The renderer exposes primitives for status lines, key-value panels, tables, section rules, humanized units, and next-command affordances.
- ASCII fallback is testable.
- JSON mode bypasses human rendering.
- Foundational snapshot tests cover rich and headless/static output for representative primitives.

## Evidence expectations

Renderer unit tests, snapshots, redaction tests, `cargo test -p cdf-cli --locked` for renderer tests, fmt/clippy, source-only Gitleaks, direct unsafe scan, and focused `jscpd`.

## Explicit exclusions

No command-family migration except minimal wiring needed to compile. No live progress; WS5 owns progress. No parser grammar changes.

## Blockers

None.
