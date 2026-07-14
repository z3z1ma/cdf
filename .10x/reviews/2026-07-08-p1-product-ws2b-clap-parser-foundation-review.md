Status: recorded
Created: 2026-07-08
Updated: 2026-07-13
Target: .10x/tickets/done/2026-07-08-p1-product-ws2b-clap-parser-foundation.md
Verdict: pass

# P1 product WS2B clap parser foundation review

## Target

Implementation and closure evidence for `.10x/tickets/done/2026-07-08-p1-product-ws2b-clap-parser-foundation.md`.

## Assumptions tested

- Clap adoption must not force broad command-module rewrites.
- `--json`, `--project`, and `--env` must remain accepted anywhere in argv, including after subcommands.
- Unknown command errors must remain usage failures with exit 2 and the existing JSON envelope.
- Per-subcommand help must be parser-generated, not a static top-level help fallback.
- Existing command semantic validation must remain in the current command modules for WS2C, not sneak into WS2B.
- The dependency change must not resolve new crate versions under the lockfile.

## Findings

No blocking findings.

The implementation uses clap v4's builder surface instead of derive. This was allowed by the WS2B ticket and `.10x/decisions/superseded/cli-command-grammar-and-parser.md` as an equivalent clap surface. It also avoids adding `clap_derive` and proc-macro transitive dependencies in this foundation slice.

`Cargo.lock` was updated even though the user write-scope named `crates/cdf-cli/Cargo.toml` rather than the lockfile. This was necessary for `--locked` Cargo checks after adding the direct `cdf-cli` dependency edge. The lockfile diff is limited to adding `clap` to the local `cdf-cli` dependency list; no package version changed.

`crates/cdf-cli/src/commands.rs` changed only to carry parser-generated help text through the existing `Command::Help` dispatch arm. This is not a command-family semantic rewrite and is required for user-visible per-subcommand help.

Residual risk: clap diagnostics change the human text inside usage errors, including unknown command messages. The stable machine contract is preserved by tests: JSON errors still use the existing envelope and exit 2. Future WS2D help snapshots should pin the final styled text once generated artifacts are in scope.

Residual risk: some compatibility duplicate-argument ordering from the hand parser cannot be reproduced exactly once clap has normalized matches. The conversion layer preserves the meaningful compatibility contract and rejects conflicting positional/flag resource values instead of silently choosing by argv order. Existing parser and full CLI tests pass.

Residual risk: the installed jscpd command did not analyze Markdown records even when records were copied outside `.10x` and given `.md` or `.markdown` extensions. Parser implementation files reported 0 clones; the broader Rust/TOML slice reported existing duplication with `newClones = 0`.

Parent verification addendum: parent inspection found one non-blocking wording issue in a shared path-argument helper, where an error message said "package root" for every optional path argument. The implementation was tightened to say "path" before parent verification. Focused parser tests, full `cdf-cli` tests, check, fmt, clippy, Semgrep, source-only Gitleaks, jscpd, rust-code-analysis, scc, cargo-deny, cargo-audit with the ratified paste ignore, cargo-vet, OSV residual inspection, and reusable CodeQL all completed as recorded in `.10x/evidence/2026-07-08-p1-product-ws2b-clap-parser-foundation.md`.

## Verdict

Pass. WS2B satisfies the parser foundation acceptance criteria: clap v4 is now the parser foundation, dispatcher-facing structs remain compatible, global compatibility pre-pass is preserved, unknown command JSON/exit behavior is stable, per-subcommand help exists at the parser layer, and no WS2C product grammar semantics were implemented.

## Residual risk

WS2C must still implement the shorter product grammar semantics and aliases. WS2D must still generate completions, man pages, and help snapshots from the clap command definition.
