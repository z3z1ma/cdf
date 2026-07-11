Status: done
Created: 2026-07-11
Updated: 2026-07-11

# Rust CLI experience study

## Question

Which interaction patterns make exemplary Rust command-line products feel fast, legible, trustworthy, and composable, and where does CDF's current P1 renderer fall short?

## Sources and methods

Inspected the current CDF parser, renderer, output routing, progress sink, error catalog, renderer tests, and the recorded Chapter 23 terminal session. Compared them with primary project documentation and repositories:

- uv CLI/reference/help and official examples: <https://docs.astral.sh/uv/reference/cli/>, <https://docs.astral.sh/uv/getting-started/help/>, <https://docs.astral.sh/uv/>
- Jujutsu CLI/configuration: <https://docs.jj-vcs.dev/latest/cli-reference/>, <https://docs.jj-vcs.dev/latest/config/>
- ripgrep guide/FAQ: <https://github.com/BurntSushi/ripgrep/blob/master/GUIDE.md>, <https://github.com/BurntSushi/ripgrep/blob/master/FAQ.md>
- bat README: <https://github.com/sharkdp/bat>
- Cargo terminal configuration: <https://doc.rust-lang.org/cargo/reference/config.html#term>

The comparison focused on default information density, output channels, TTY adaptation, verbosity, help, paging, width/color/Unicode policy, errors, progress, machine composition, and rendering cost. It did not rank projects by popularity or copy their branding.

## Findings from exemplary tools

uv's signature is a small aligned activity vocabulary (`Resolved`, `Prepared`, `Installed`) with counts and elapsed time. It makes the default narrative scannable, supports `-q`/repeated quiet, `-v`, no-progress, and condensed versus long help. The lesson is not the exact verbs; it is that a command's primary state transitions form the visual skeleton and details are subordinate.

Jujutsu distinguishes primary command output from incidental operation messages: `--quiet` suppresses the latter while warnings/errors remain. It automatically pages suitable output, exposes explicit color/pager controls, and uses templates for dense domain views instead of hard-coding one presentation. The durable lesson is channel/importance separation and operator-controlled depth.

ripgrep makes environment-aware defaults central to usefulness: TTY-sensitive color, plain pipe output, condensed `-h` versus long `--help`, automatic filtering, explicit debug escape hatches, and maximum-column controls. It preserves Unix composition rather than decorating redirected output.

bat auto-pages only interactive long output, exits the pager for one-screen results, and becomes plain content when piped. Its style components are optional rather than an indivisible ornamental frame. Cargo separately controls quiet/verbose, color, Unicode, hyperlinks, and progress with `auto` defaults.

Across the tools, beauty comes from restraint, fast feedback, semantic alignment, and context adaptation—not more borders, colors, or panels.

## CDF source findings

CDF already has valuable foundations: one `RenderDocument` path, structured JSON envelopes, a redaction boundary, deterministic headless output, typed remediation, bounded nonblocking progress ingestion, TTY/headless tests, and semantic humanization.

The current default presentation is too heavy:

- every major command starts with an 80-column rule;
- boxed tables are the only tabular primitive and truncate evidence without a detail affordance;
- run/replay output prepends a complete milestone table and then repeats the same facts in summary panels;
- every key-value field is visually equal, so package hashes compete with the outcome;
- terminal width comes only from `COLUMNS` or 80 and cell sizing counts characters rather than display columns;
- `CLICOLOR_FORCE` is detected but does not enable color in headless output;
- Unicode capability has no independent policy;
- no global quiet/verbose/progress/pager contract is wired, despite dormant progress verbosity types;
- success/progress routing is assembled as one final human string rather than primary stdout plus incidental/progress stderr;
- help descriptions are mostly empty, making the broad grammar undiscoverable;
- human rendering is snapshot-tested but not budgeted as a hot-path consumer under P3.

The recorded Chapter 23 replay demonstrates the problem: an eight-row boxed milestone history precedes a second full-width rule and five evidence panels. It is informative but slow to parse, repetitive, and hostile to repeated daily use.

## Conclusions

CDF should preserve its evidence richness while changing the default lens. The default human contract should be a compact aligned activity stream and one outcome summary; verbose mode and `inspect` expose proof detail. Progress/warnings belong on stderr, primary results on stdout, and machine modes remain decoration-free. Stable long inspection/help/diff output may auto-page; execution progress never does.

The renderer needs terminal-native width/Unicode handling, borderless relational views, semantic emphasis, true quiet/verbose controls, and rate-limited in-place progress. These are product contracts, not scattered formatting tweaks, and are owned by the WS9 graph.

## Limits

This study did not run a user study, test screen readers, benchmark terminal emulators, or select new dependencies. The implementation tickets must validate accessibility/plain-text equivalence, Windows terminals, snapshot stability, and rendering overhead before closure.
