Status: active
Created: 2026-07-13
Updated: 2026-07-13

# CLI exposes one current command grammar

## Context

The original clap migration deliberately retained aliases and caller-selected artifact identities while command families moved off the hand-rolled parser. CDF now has no installed script population, and `.10x/decisions/pre-production-current-format-only.md` requires deprecated CLI forms and parser prepasses to be deleted. The compatibility table in `.10x/decisions/superseded/cli-command-grammar-and-parser.md` therefore no longer describes the shipped product.

## Decision

`crates/cdf-cli/src/args.rs` is the sole executable grammar authority. The generated help, man pages, completions, and `docs/commands/` reference MUST be derived from that same clap command tree and MUST pass freshness checks.

CDF MUST expose only canonical current forms. Resource and other primary nouns are positional. `--to` selects a destination URI; `--target` exists only on commands where it names a destination-local table/object target. CDF-owned run, package, checkpoint, rewind-marker, and ordinary pipeline identities are minted or derived by CDF and MUST NOT be accepted from ordinary command callers. Global terminal/project/environment/machine-output options remain clap-global and may appear at supported nested positions without a parser prepass.

The current command tree is the generated `docs/commands/README.md` inventory, not a duplicated hand-maintained table in this decision. Adding, removing, or renaming a command changes product behavior and requires its owning spec/ticket plus regenerated artifacts. In particular, `cdf state migrate` does not ship until a supported predecessor state schema exists under `.10x/decisions/state-current-schema-package-receipt-recovery.md`.

Advanced current inputs are not compatibility aliases merely because a simpler human form exists. Examples include `--scope-json` for typed/non-string scope keys and destination-local `--target`/dedup controls where the destination protocol requires them. Their implementation and docs MUST describe the distinct current capability they express.

The stable machine contract remains structured success/error JSON with meaningful exit codes and redaction before serialization. Removing a deprecated invocation MUST NOT weaken JSON truth, error codes, secret handling, or the help/suggestion experience for canonical commands.

## Alternatives considered

- Retain aliases until 1.0. Rejected because there is no installed base and aliases keep duplicate parsing, tests, help, and semantic precedence alive.
- Keep a hand-maintained exhaustive grammar table in this decision. Rejected because it would compete with the clap tree and generated artifacts; freshness checks provide one executable source of truth.
- Delete advanced forms such as `--scope-json` because they look older. Rejected where they express a distinct current capability that the ergonomic shorthand cannot represent.
- Make generated docs the parser input. Rejected because clap already owns parsing, validation, suggestions, help, completions, and man generation without another grammar DSL.

## Consequences

The CLI surface is smaller, generated artifacts cannot advertise deleted commands, and adding a command requires one clap edit plus regeneration rather than alias choreography. Development scripts using pre-production forms must update. A future compatibility promise requires a new decision defining which canonical forms become stable and for how long.
