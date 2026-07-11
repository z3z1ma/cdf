Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/done/2026-07-08-p1-product-ws3-rendering-system-design-language.md
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

No command-family migration except minimal wiring needed to compile. No live progress; WS5 owns progress. No parser grammar changes beyond minimal `--no-color` display flag plumbing.

## Blockers

None.

## Progress and notes

- 2026-07-08: Worker inspected the owning ticket, renderer decision, CLI/security and live-progress specs, `QUALITY.md`, `cdf-cli` output/parser/library surfaces, current tests, and dirty worktree state before editing. Existing dirty WASM records are unrelated and remain untouched.
- 2026-07-08: Added `crates/cdf-cli/src/render/**`, `CommandOutput` rendered-human hooks, global `--no-color` parser plumbing, and foundational renderer snapshot-style tests without migrating command families.
- 2026-07-08: Closure evidence recorded in `.10x/evidence/2026-07-08-p1-product-ws3b-renderer-foundation.md`; adversarial review recorded in `.10x/reviews/2026-07-08-p1-product-ws3b-renderer-foundation-review.md`.
