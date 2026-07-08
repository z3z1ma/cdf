Status: active
Created: 2026-07-08
Updated: 2026-07-08

# CLI command grammar and parser

## Context

P1 product experience requires `cdf` to stop fighting operators. Current source inspection shows the CLI parser is hand-rolled in `crates/cdf-cli/src/args.rs`, static global help is in `crates/cdf-cli/src/commands.rs`, and command modules still enforce several runtime-only grammar requirements. The current grammar is functional but inconsistent:

- `cdf run` requires user-minted package and checkpoint identifiers even though the system owns those identities.
- `plan` and `preview` use positional resources, while `run` also accepts `--resource`.
- `plan`/`explain` require `--target` even when the project environment can resolve a destination.
- `state show/history` require `--scope-json`; P1 requires human `--scope key=value` input.
- `resume` requires a run id today; P1 requires bare `cdf resume` to scan and drain interrupted work.
- `--json`, `--project`, and `--env` are stripped globally from anywhere in argv before parsing; scripts may rely on this.

The stable machine contract lives in `crates/cdf-cli/src/output.rs`: success is `{ ok: true, command, result }`, errors are `{ ok: false, error: { kind, message, exit_code, not_supported } }`, and exit codes are already meaningful.

P1 requires a mature parser with per-subcommand help, typo suggestions, shell completions, man pages, and styled help.

## Decision

Use `clap` v4 derive for `cdf-cli` command parsing, with `clap_complete` for shell completions and `clap_mangen` for man pages when those generated artifacts are implemented.

The CLI grammar is:

| Command | Primary human form | Compatibility forms retained during migration |
|---|---|---|
| help | `cdf help [command...]`; `cdf <command> --help` | `cdf`, `cdf --help`, `cdf -h` |
| version | `cdf --version` | `cdf version`, `cdf -V` |
| init | `cdf init [dir] [--name name] [--force]` | current flags retained |
| validate | `cdf validate` | current form retained |
| plan | `cdf plan [resource] [--to dest]` | `--resource`, `--target` accepted as aliases; scan flags retained |
| explain | `cdf explain [resource] [--to dest]` | `--resource`, `--target` accepted as aliases; scan flags retained |
| run | `cdf run [resource] [--to dest]` | legacy `--resource`, `--pipeline`, `--target`, `--package-id`, `--checkpoint-id` accepted for scripts |
| preview | `cdf preview <resource> [--limit n]` | `--resource` and existing scan flags retained where direct-stream preview can honor them |
| sql | `cdf sql <query...>` | existing query-join behavior retained |
| inspect project | `cdf inspect project` | current form retained |
| inspect resources | `cdf inspect resources` | current form retained |
| inspect resource | `cdf inspect resource <id>` | current form retained |
| inspect lock | `cdf inspect lock` | current form retained |
| inspect destinations | `cdf inspect destinations` | `destination` alias retained |
| inspect package | `cdf inspect package <dir-or-id>` | current directory form retained; package id resolution may land when registry support exists |
| inspect run | `cdf inspect run <run-id>` | current form retained |
| diff schema | `cdf diff schema` | current form retained until richer diff args are specified |
| contract freeze | `cdf contract freeze [contract]` | `--contract` alias retained |
| contract show | `cdf contract show [trust]` | `--trust` alias retained |
| contract test | `cdf contract test [contract]` | `--contract` alias retained |
| state show | `cdf state show [resource] [--scope key=value]...` | legacy `--pipeline`, `--resource`, and `--scope-json` retained for scripts |
| state history | `cdf state history [resource] [--scope key=value]...` | legacy `--pipeline`, `--resource`, and `--scope-json` retained for scripts |
| state rewind | `cdf state rewind [resource] --to checkpoint [--scope key=value]...` | `--target-checkpoint` retained; marker checkpoint is minted by default; explicit `--marker-checkpoint` retained for scripts |
| state migrate | `cdf state migrate` | current form retained |
| state recover | `cdf state recover --package dir --to dest` | current `--receipt`, `--target`, `--merge-dedup` retained |
| resume | `cdf resume [run-id]` | `--run` and `--run-id` retained; no-arg resume scans the ledger |
| replay package | `cdf replay package <pkg-or-dir> [--to dest]` | current `--target` and `--merge-dedup` retained for Postgres |
| backfill | `cdf backfill <resource> --from cursor --to cursor [--target target|--to dest]` | `--resource`, current `--target`, `--execute`, and `--slice-size` retained |
| package ls | `cdf package ls [dir]` | current form retained |
| package gc | `cdf package gc [dir]` | current form retained |
| package verify | `cdf package verify <dir-or-id>` | current directory form retained; package id resolution may land when registry support exists |
| package archive | `cdf package archive <dir-or-id> [--format parquet] [--force]` | current directory form retained |
| doctor | `cdf doctor` | current form retained |
| status | `cdf status` | current form retained |

Grammar principles:

- Primary nouns are positional.
- `--to` names destinations everywhere. `--target` remains a compatibility alias only where it currently means destination target/table rather than destination URI.
- `--env` and `--project` remain global.
- `--json` remains accepted anywhere in argv through an explicit compatibility pre-pass or clap global flag behavior that is proven by tests.
- `--scope key=value` is the human form; `--scope-json` remains a script compatibility escape hatch.
- Identifiers owned by CDF, including package id, checkpoint id, run id, and state-rewind marker checkpoint, are minted by default.
- Destructive or ambiguous commands require confirmation in human mode and accept `--yes`; JSON/script modes may require explicit flags rather than prompts.

Resolution precedence is: explicit flag, then environment variable, then project/environment config, then minted or derived default. The first environment variables ratified for the parser are `CDF_ENV`, `CDF_PROJECT`, `CDF_TARGET`, `NO_COLOR`, and `CLICOLOR_FORCE`; additional variables require a later decision or ticket-local ratification.

## Alternatives considered

- Keep the hand-rolled parser and add missing features manually. Rejected because P1 requires subcommand help, typo suggestions, completions, man pages, and consistent grammar; hand-rolling all of that would spend complexity on plumbing rather than product behavior.
- Use `bpaf`, `argh`, or a bespoke parser crate. Rejected for this codebase because `clap` is already present in the workspace lockfile through `cdf-benchmarks`, already has supply-chain posture in `supply-chain/config.toml`, and directly supports the required generated artifacts.
- Switch immediately to breaking-only grammar with no aliases. Rejected because the JSON/script contract is sacred and current tests cover script-oriented forms.

## Consequences

Parser migration must be staged. First land clap data structures and compatibility tests, then move command semantics to the shorter forms, then generate completions/man pages and help snapshots.

The decision does not make unsupported product behavior magically available. Where a short form depends on lower-layer defaults or resolution that does not exist yet, the parser must accept the form and the command layer must fail with the structured not-supported/error experience until the lower layer lands.

Every parser migration child must prove the JSON envelopes and exit-code taxonomy remain stable.
