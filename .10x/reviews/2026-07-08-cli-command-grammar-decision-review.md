Status: recorded
Created: 2026-07-08
Updated: 2026-07-13
Target: .10x/decisions/superseded/cli-command-grammar-and-parser.md
Verdict: pass

# CLI command grammar decision review

## Target

Review of `.10x/decisions/superseded/cli-command-grammar-and-parser.md` and WS2A closure readiness.

## Findings

- Pass: the decision directly addresses the P1 parser mandate by selecting clap v4 derive, completions, and man-page generation.
- Pass: the decision preserves the stable JSON envelope and exit-code taxonomy instead of treating parser migration as permission to break scripts.
- Pass: global `--json` compatibility is explicitly called out as a migration risk because current parsing accepts it anywhere in argv.
- Pass: every command family required by `.10x/specs/project-cli-observability-security.md` appears in the command table.
- Pass: compatibility aliases are explicit, including `--target`, `--resource`, legacy state flags, and Postgres replay safety flags.
- Pass: implementation has been split into disjoint child scopes: parser foundation, product grammar semantics, and generated artifacts/help.
- Minor, accepted: the decision does not fully specify future package-id registry resolution for package commands. It intentionally leaves those forms as directory-compatible until registry support exists, which is safer than inventing package registry semantics in a parser decision.

## Verdict

Pass. WS2A can close as a decision slice; implementation belongs to WS2B, WS2C, and WS2D.

## Residual risk

The highest migration risk is preserving the current anywhere-in-argv `--json` compatibility under clap. WS2B owns proving that with tests.
