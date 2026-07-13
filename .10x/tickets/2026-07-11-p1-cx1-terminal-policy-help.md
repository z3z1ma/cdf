Status: blocked
Created: 2026-07-11
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-11-p1-ws9-cli-experience-excellence.md
Depends-On: .10x/specs/cli-interaction-excellence.md

# P1 CX1: terminal policy, channels, and help

## Scope

Implement centralized quiet/verbose, color, progress, pager, Unicode, terminal-width, and stdout/stderr policy. Complete short/long help descriptions and keep generated artifacts derived from clap authority.

## Acceptance criteria

- Global policy parses exactly, including conflicts and compatibility aliases.
- Terminal size/display width and TTY/headless channel behavior follow the spec.
- Short and long help are useful and generated artifacts remain fresh.
- Focused snapshots cover TTY, redirection, widths, Unicode/ASCII, color, and pager eligibility.

## Exclusions

No command-family visual migration or live progress redesign; CX2/CX3 own those.

## Blockers

- Pager process authority is unratified. The active terminal decision specifies `auto|never` eligibility but does not select a cross-platform pager executable/discovery mechanism, environment contract, subprocess arguments, failure/fallback behavior, or ownership of process-handoff tests. Review proved that parsing a detached boolean was worse than no option, so the dead `--pager` surface and configuration were removed. CX1 cannot claim the pager portion of its scope until that process contract is ratified; the other five review findings do not depend on it.

## Evidence expectations

Parser tests, TTY/headless channel snapshots, width/display tests, help/man/completion freshness, redaction checks, and focused CLI quality commands.

## References

- `.10x/decisions/cli-progressive-disclosure-terminal-contract.md`
- `.10x/decisions/cli-command-grammar-and-parser.md`

## Journal

- 2026-07-12: Inspected the complete ticket, governing spec and decisions, prior clap/help artifact evidence, renderer evidence, progress evidence, and the initially clean worktree before editing. Scoped implementation to `cdf-cli`, generated CLI artifacts, the direct dependency edges required for terminal size/display width, and this ticket.
- 2026-07-12: Added one terminal-policy model for quiet/verbose, color, progress, pager, Unicode, stdout/stderr TTY state, terminal dimensions, `COLUMNS` fallback, `NO_COLOR`, and `CLICOLOR_FORCE`. The parser compatibility pre-pass accepts the global policy anywhere, supports repeated `-v`, preserves `--no-color`, and rejects quiet/verbose, color-alias, and invalid-enum combinations with copyable corrections.
- 2026-07-12: Routed renderer detection through independent stdout/stderr channel facts, routed success progress to stderr, made progress creation honor JSON/progress/verbosity policy, measured table content with Unicode display width, and kept redirected output ANSI-free with ASCII truncation when Unicode is disabled.
- 2026-07-12: Added concise clap command/option descriptions plus root long-help environment/default guidance and examples. Regenerated committed help, man, and bash/zsh/fish/PowerShell completion artifacts from the clap authority.
- 2026-07-12: Stopped further commands at orchestrator direction after shared build contention and a centrally terminated broader release installation. No commit or push was made.
- 2026-07-12: Re-read and repaired all six significant review findings. Five are implemented: narrow grids stack with wrapped full values, quiet creates no progress sink, Unicode auto requires a UTF-8-capable locale/non-dumb terminal, the global pre-pass stops at `--`, and help/man/completion/docs generation now shares complete clap authority without placeholder descriptions. The sixth, pager execution, lacks product authority; removed its dead parser/configuration surface and recorded the blocker rather than inventing a process contract.
- 2026-07-12: `run --jobs` was already concurrently present in the shared clap authority from the P3 C2 slice. CX1 did not add, remove, or semantically review that option. Regeneration necessarily propagated the current authority into `cdf-run` help/man/completion/docs artifacts; those `--jobs` artifact lines are C2-derived cross-slice content and are not CX1 acceptance evidence.
- 2026-07-12: Repaired the two re-review gaps without expanding pager or command-family scope. Grid rendering now selects stacked/wrapped records whenever the natural display width would force any cell truncation, including one- and two-column tables at 40 columns; grid mode therefore never uses ellipsis as the only access path to a value. Clap now owns terminal enum value sets and quiet/verbose plus color-alias conflicts in addition to the compatibility pre-pass, so generated completions enumerate `auto|always|never` and inherited man pages retain nonblank policy descriptions.

## Evidence

- Acceptance: global policy parses exactly, including conflicts and compatibility aliases. Observation: `cargo test -p cdf-cli --locked parser_` completed with 8 passed, 0 failed, 272 filtered library tests; the binary and `doctor_env` targets contained 0 matching tests. This includes the new global-policy-anywhere and exact-conflict-correction cases plus existing global JSON/project/env/help compatibility. Limit: this focused filter does not prove every command-family semantic path.
- Acceptance: terminal size/display width and TTY/headless channel behavior follow the spec. Observation: `cargo check -p cdf-cli --offline` completed successfully after updating the lockfile with already-locked `crossterm 0.28.1` and `unicode-width 0.2.2` direct edges; a subsequent `cargo check -p cdf-cli --locked` completed successfully. Source-level cases were added for terminal/COLUMNS/fallback precedence including 40/80/160 columns, pager eligibility, redirected versus TTY color, `NO_COLOR` explicit override, Unicode/ASCII glyph policy, and Unicode display width. Limit: the focused `cargo test -p cdf-cli --lib --locked terminal` run was aborted before producing test results; these new terminal/render assertions are compiled but not recorded as executed evidence.
- Acceptance: short/long help is useful and generated artifacts remain derived from clap authority. Observation: `cargo run -p cdf-cli --locked --features cli-artifacts --bin cdf-generate-cli-artifacts -- --out-dir crates/cdf-cli/generated` completed and printed `generated CLI artifacts in crates/cdf-cli/generated`. Limit: the post-generation `--check` freshness command and feature-gated freshness test were not run before command execution was stopped. Generated diffs therefore require integrator inspection and a freshness check.
- Acceptance: focused snapshots cover TTY, redirection, widths, Unicode/ASCII, color, and pager eligibility. Observation: deterministic source-level snapshot/assertion cases exist for these dimensions, and the pre-existing headless/rich/redaction snapshots remain in place. Limit: the newly added terminal/render tests did not complete an execution run, no real PTY recording was made, and pager coverage proves eligibility only; no external pager process handoff was added or tested.
- Channel and isolation observation: success progress rendering now uses stderr configuration and primary documents use stdout configuration; JSON still bypasses human rendering/progress. Limit: no focused end-to-end execution command was completed after this channel split, so command-family snapshot updates may still be required by integration.
- Quality limit: `cargo fmt --all` completed during implementation and the focused CLI checks above ran. No final `cargo fmt --check`, clippy, full `cdf-cli` suite, workspace check/test, secret scan, or broad quality gate was run; broad checks remain with the orchestrator as delegated.
- Repair verification: `cargo test -p cdf-cli --lib --locked cx1_` compiled the focused slice and emitted `10 passed; 0 failed; 274 filtered out; finished in 0.00s`. The cases cover 40-column five-field stacked output with no width overflow/value truncation, Unicode display width, ASCII/UTF-8 auto capability, TTY/redirection color policy, quiet/no-progress sink behavior, width precedence, exact parser conflicts, policy placement, `--` SQL payload preservation, and distinct complete short/long help. Limit: the cargo runner buffered output for more than 15 seconds at 0% CPU; on orchestrator-directed termination it emitted the already-complete passing result. This proves the test assertions completed but not clean test-runner process shutdown in the shared environment.
- Artifact generation and freshness: the focused generator completed for `crates/cdf-cli/generated`; the already-built repository-native generator then completed `--docs-dir docs --docs-only`, `--out-dir crates/cdf-cli/generated --check`, and `--docs-dir docs --docs-only --check`. Both artifact trees reported fresh. A focused scan found no `Command option`, `Command value`, or `--pager` in generated CLI artifacts or command docs. `cdf-run.1` contains inherited color/progress/Unicode globals, and root generated help contains progress, Unicode, environment guidance, and examples.
- Scoped quality: `git diff --check -- Cargo.lock crates/cdf-cli docs/commands .10x/tickets/2026-07-11-p1-cx1-terminal-policy-help.md` passed. No release install, workspace build/test, E3/C2 mutation, commit, or push was performed.
- Second repair verification: the focused test binary ran 12 `cx1_` tests with 12 passed, 0 failed, 274 filtered in 0.01s. New cases prove one- and two-column 40-column tables stack before truncation and reconstruct every full value without `~`/`…`; direct clap parsing proves exact terminal enum candidates and quiet/verbose plus `--no-color`/`--color` conflicts are owned by the clap graph.
- Generated-surface verification: the native generator refreshed CLI artifacts and command docs, then both CLI and docs `--check` modes reported fresh. `cdf-run.1` contains nonblank color/progress/Unicode descriptions, bash completion uses `compgen -W "auto always never"` for all three policy values, and focused scans found no placeholder help, pager residue, or scoped whitespace error. The feature-gated semantic test `cx1_generated_help_and_man_pages_are_complete_and_share_global_authority` passed 1/1 after its focused `--no-run` build.

## Review

Review date: 2026-07-12

Findings:

- **significant — narrow rendering does not implement the governing stacked-layout contract and can exceed the detected width.** `Table::render` always emits a boxed grid. `table_widths` refuses to shrink any column below eight cells, so a five-column table has a minimum rendered width of 56 cells (`5 * 8` content plus 16 cells of framing) even when the terminal is 40 columns. Progress uses a five-column table, and no stacked-record primitive or width-triggered fallback exists. The added display-width test covers only a two-column table at 20 columns, so it cannot falsify this case. This contradicts the spec's 40-column scenario, its requirement to switch narrow output to stacked records, and its requirement to preserve access to truncated values.
- **significant — quiet mode still emits successful progress narration.** `TerminalPolicy::progress_enabled` ignores `Verbosity::Quiet`, creates a progress sink for `-q`, and `DisplayVerbosity::Quiet` retains terminal milestones. `HumanOutput::RenderedWithProgress` then renders that milestone to stderr. The decision says quiet suppresses progress and non-primary success narration; retaining the terminal progress milestone violates both clauses. No parser/render test exercises an actual quiet success result.
- **significant — pager policy is parsed but has no observable effect.** `pager_eligible` is dead code guarded with `allow(dead_code)`; no output path classifies bounded read-only commands, consults terminal rows, hands redacted output to a pager, or tests process handoff. Thus `--pager auto` and `--pager never` behave identically. The unit test proves only a detached boolean and cannot support centralized pager behavior or the required redaction-before-handoff boundary.
- **significant — help/reference artifacts remain incomplete and inconsistent with the clap authority.** Fifteen generated help pages still use placeholder descriptions such as `Command option` or `Command value` for material operands/options (`--pipeline`, `--trust`, `--merge-dedup`, checkpoint identifiers, and others), contrary to the operator-facing-description requirement. Root short help intentionally hides progress, pager, and Unicode, but root `render_help` uses `write_help`, so `cdf help`, `cdf --help`, and `generated/help/cdf.txt` also omit the promised complete long-help flags, environment/default guidance, and examples. The generated subcommand man pages omit all global terminal flags because `command_at_path` renders a detached subcommand, while help and completions include them. Finally, `docs/commands/*.md`, which declare themselves generated from clap, were not regenerated and still show the old `Continuous Data Framework CLI`/`--no-color`-only grammar.
- **significant — Unicode `auto` does not cover ASCII-only terminals.** `rich_glyphs` equates `auto` with `DisplayMode::Tty`; terminal detection contains no locale/capability signal. A TTY with an ASCII/C locale therefore receives Unicode glyphs. The only new ASCII assertion uses explicit `--unicode never`, so it does not cover the spec's permanent ASCII-only-terminal conformance case.
- **significant — the global pre-pass steals policy-looking SQL/query arguments even after the option terminator.** The scan never tracks `--` and removes `--color`, `--progress`, `--pager`, `--unicode`, `-q`, and `-v` from every argv position before clap parses the trailing SQL query. For example, `cdf sql -- --color` fails because the pre-pass treats `--color` as a missing global value instead of query text. This regresses the command decision's `cdf sql <query...>` and existing trailing/hyphen-valued query grammar; the new parser test proves global flags after a normal resource but not delimiter or free-form-query boundaries.

Artifact-diff assessment: the completion expansion is large but predominantly explainable by clap replicating six global flags across every command path. It also propagates the concurrently present `run --jobs` grammar into help/man/completions; that is cross-slice churn rather than a CX1 terminal-policy change, though it is consistent with the current shared clap authority. No unexplained command additions/removals were observed. Freshness itself remains unproved because the recorded generation was not followed by `--check`.

Verdict: **fail**. The stdout/stderr split and JSON-success isolation are structurally improved, but the findings above directly contradict terminal, help, parser, and artifact acceptance criteria; this is not closure-ready.

Residual Risk: no focused test was run during review because the contradictions are source/artifact-observable and the ticket already states the only relevant new test run was interrupted. Actual PTY behavior, redirected stderr snapshots, headless determinism, Windows behavior, artifact freshness, and end-to-end redaction across progress/pager boundaries therefore remain unevidenced in addition to the concrete failures above.

Repair status: **re-review requested**. Findings 1, 2, 4, 5, and 6 have focused passing evidence above. Finding 3 was not papered over: the dead pager surface was removed and the missing process authority is now the sole named blocker. Review should verify the five repairs and judge the blocker boundary; closure is not requested while pager semantics remain unratified.

Second repair status: **re-review requested**. The two 2026-07-12 re-review findings now have permanent focused tests and fresh generated artifacts. Pager remains the only blocker and has no parser, help, completion, docs, or runtime residue.

### Re-review — 2026-07-12

Prior-finding disposition:

- The 40-column five-field case now selects stacked records, wraps values to the available display width, and retains the complete values. The threshold arithmetic and focused assertion cover the original 56-cell minimum-grid counterexample.
- Quiet now makes `TerminalPolicy::progress_enabled` false, so `human_progress_sink` returns `None`; it no longer retains or renders a terminal progress milestone.
- The counterfeit pager surface is fully removed from parser policy, terminal state, generated help/man/completions, command docs, and implementation references. The ticket remains `blocked` and names the still-unratified executable discovery, environment, subprocess, fallback, security, and test ownership semantics. This is an honest blocker boundary, not acceptance evidence.
- Help snapshots and command docs are regenerated from clap, distinguish short root help from complete long root help, include environment/examples, and contain none of the prior `Command option`/`Command value` placeholders. Subcommand man pages now inherit the current global flags.
- Unicode `auto` now requires both a TTY and a UTF-8-capable, non-`dumb` environment; explicit `always`/`never` remain authoritative. This closes the prior ASCII/C-locale failure at the modeled boundary.
- The global compatibility scan now stops at `--`; the remaining argv, including policy-looking SQL tokens, passes intact to the trailing SQL grammar.

Findings:

- **significant — grid-mode truncation still does not preserve access to complete values.** Stacking is selected only when `minimum_grid_width(column_count) > terminal_width`. At 40 columns, a one- or two-column table remains a grid; `table_widths` then shrinks long cells and `truncate` irreversibly replaces the tail with `~`/`…`. No adjacent detail, continuation, or alternate human path exposes the omitted value, and the existing two-column test explicitly accepts truncation. The repair closes the original five-column overflow but not the governing requirement that tables preserve access to truncated values.
- **significant — generated man pages and completions are present and fresh but not complete representations of terminal-policy grammar.** Every generated man page lists inherited `--progress <WHEN>` and `--unicode <WHEN>` with blank descriptions because those global arguments use `hide_short_help(true)` and the man generator receives no visible help text for them; `cdf-run.1` and `cdf.1` demonstrate the gap. Shell completions also treat the policy values as filesystem paths (`compgen -f`) because the clap arguments do not declare their `auto|always|never` value sets. The exact enums and conflicts live only in the manual pre-pass, so clap-derived artifacts cannot expose or complete the exact grammar despite the freshness checks. Help snapshots/docs do contain prose descriptions, but that does not make the man/completion artifacts complete.

Generated-churn assessment: the large completion/man/help/docs diff is mechanically attributable to inherited global flags, completed descriptions, and regeneration of every clap command path. `run --jobs` already exists in `HEAD` clap authority; CX1 adds only its description and necessarily refreshes the derived `cdf-run` artifacts. It is therefore identifiable C2-derived authority, not an unexplained CX1 grammar mutation. No unrelated command addition/removal or pager residue was found.

Verdict: **fail** for acceptance closure. The six original implementation directions are substantially repaired and the pager gap is correctly blocked, but the two significant artifact/value-access findings still contradict the active spec and the ticket's help/generated-artifact criteria. The ticket should remain blocked; resolving pager authority alone would not make it done.

Residual Risk: the re-review was source/artifact based and did not repeat the executor's recorded focused test or freshness commands. Real PTY behavior, redirected stderr, Windows locale/console capability, grapheme-cluster wrapping, and end-to-end redaction before any future pager handoff remain bounded but unproven. The UTF-8 locale heuristic is conservative on platforms that support Unicode without setting the inspected locale variables.

### Final re-review — 2026-07-12

Findings: **none** in the repaired, implemented CX1 surface.

Disposition:

- `grid_would_truncate` compares the complete natural display-cell width, including framing, with the configured width before grid rendering. Any one-, two-, or multi-column table that would shrink a header or value now selects stacked records; stacked labels and values wrap by display width without ellipsis. If the grid path is selected, its natural cells plus framing already fit, so `table_widths` and `truncate` cannot discard content. The focused one-, two-, and five-column 40-column cases exercise the former counterexamples and reconstruct full values.
- Clap now owns `auto|always|never` through each policy argument's `value_parser`, owns quiet/verbose and color/alias conflicts through `conflicts_with`, and retains the compatibility pre-pass's exact user-facing corrections. Generated man pages include nonblank inherited color/progress/Unicode descriptions and possible values. Bash, zsh, fish, and PowerShell artifacts each expose the three policy candidates for every generated command path rather than filesystem completion.
- Prior repairs remain intact: quiet prevents progress-sink construction; Unicode `auto` requires a TTY plus modeled UTF-8/non-`dumb` capability; `--` terminates global compatibility scanning and preserves SQL payload tokens; root short/long help and command docs are complete and placeholder-free; stdout/stderr and JSON isolation wiring is unchanged.
- Pager remains absent from runtime policy, parser, help, man pages, completions, and command docs. The ticket's sole blocker accurately records the unratified process-discovery, environment, subprocess, fallback, security, and test-ownership semantics. No dead pager capability remains to imply support.
- Generated churn is attributable to the clap graph: global policy propagation, enum candidates/descriptions, complete command descriptions, and regeneration of all help/man/completion/docs paths. `run --jobs` is present in `HEAD` authority, so its generated lines are identifiable C2-derived content rather than an unexplained CX1 grammar mutation. No unrelated command addition/removal was observed.

Verdict: **pass** for the repaired and implemented CX1 surface. The two prior significant findings are closed, and no earlier repair regressed. This pass does not authorize ticket closure: CX1 correctly remains `blocked` because the governing pager requirement cannot be implemented until its semantic/process contract is ratified.

Residual Risk: this final review inspected source, generated artifacts, focused tests, and journaled evidence without repeating the executor's focused runs or freshness checks. Real PTY/redirection behavior, Windows locale/console capability, grapheme-cluster wrapping, and future redaction-before-pager-handoff remain outside the observations. Those limits do not challenge the repaired findings, but pager ratification and implementation will require a new adversarial review before closure.

## Retrospective

- What worked: reusing the existing clap authority, artifact generator, renderer boundary, and progress verbosity model kept policy centralized and avoided command-specific flag branches or compatibility shims.
- What surprised: the existing renderer measured characters rather than terminal display cells, detected only stdout TTY state even for progress, and combined successful progress with primary stdout despite the governing channel contract.
- Costly friction: shared-target rebuilds and concurrent lower-crate edits made nominally focused test invocations expensive; chaining multiple cargo test filters amplified that cost and obscured which later phase had started. Future verification should use one `--lib` invocation per required filter, record its result immediately, and run generation freshness separately.
- Five whys: focused verification did not close because the terminal/render test process was interrupted; it was still running because cargo was rebuilding shared dependencies; those dependencies changed concurrently; the shared target serializes/build-invalidates across executors; therefore orchestration should schedule CLI link-heavy verification after lower-crate executors quiesce or give executors isolated target directories.
- Durable follow-up: no new product bug outside CX1 was discovered. The remaining work is closure work already owned here: execute the terminal/render cases, inspect generated semantics, prove `--check` freshness, reconcile affected snapshots, and commission adversarial review.
- Repair lesson: a parsed policy with no consumer is counterfeit capability. Cross-platform subprocess behavior needs authority for discovery, arguments, failure/fallback, and security boundaries before its flag exists. Removing the dead pager surface made the remaining blocker honest and kept pressure from turning an unspecified semantic default into product behavior.
- Repair lesson: generated authority must be checked as a graph, not one directory. Clap source, help snapshots, man pages, completions, and `docs/commands` now regenerate/check together; subcommand man generation must build the root command first so inherited globals are present.
- Repair lesson: free-form trailing grammars require a hard pre-pass boundary. `--` now ends global compatibility scanning, preserving policy-looking SQL tokens exactly instead of interpreting payload as CLI control.
- Second repair lesson: fitting framing is weaker than fitting data. A grid is eligible only when every natural cell fits; otherwise stacked wrapping is the lossless representation, regardless of column count.
- Second repair lesson: a manual compatibility scan may transport global flags, but enum candidates and conflicts must also exist in clap so help, man pages, and shell completion remain executable descriptions of the same grammar.
