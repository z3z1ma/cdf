Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws6a-docs-topology-quickstart.md, .10x/specs/docs-onboarding-surface.md

# P1 product WS6A docs topology and quickstart evidence

## What was observed

WS6A created the initial in-repository documentation topology:

- `docs/README.md`
- `docs/quickstart.md`
- `docs/architecture.md`
- `docs/commands/README.md`
- `docs/errors/README.md`
- `docs/operators/README.md`
- `docs/operators/recovery.md`
- `docs/operators/replay.md`
- `docs/operators/backfill.md`
- `docs/operators/doctor-status-cron.md`
- `docs/operators/release-install.md`
- `docs/operators/troubleshooting.md`

The docs satisfy the topology from `.10x/specs/docs-onboarding-surface.md` while preserving the WS6A boundary:

- `docs/commands/` and `docs/errors/` are placeholder index pages that explicitly delegate generated command reference and generated error catalog work to WS6B.
- The quickstart uses current CLI behavior for build, init, validate, plan, run, system SQL, package/state inspection, contract freeze/test, and replay from a clean replay ledger.
- Crash/resume and drift quarantine are documented through the current conformance-owned MVP fixture rather than a public crash flag or hand-built example project.
- Runnable REST/Postgres example projects are linked to WS6C instead of implemented here.
- `cdf init` README scaffold work is linked to WS6D instead of changing `cdf init`.

## Procedure and results

Required context was read before editing:

- `VISION.md` Chapters 18-23, including Chapter 23 MVP.
- `QUALITY.md`.
- `.10x/specs/docs-onboarding-surface.md`.
- `.10x/specs/project-cli-observability-security.md`.
- `.10x/specs/conformance-governance-roadmap.md`.
- `.10x/tickets/done/2026-07-08-p1-product-ws6a-docs-topology-quickstart.md`.
- Current CLI help behavior for quoted commands, including `run`, `plan`, `replay package`, `state recover`, `backfill`, `doctor`, `status`, and `package verify`.

CLI and quickstart checks:

- `cargo build -p cdf-cli --locked`: passed.
- Temp-project quickstart transcript with `cdf init`, seeded `data/events.ndjson`, `validate`, `plan local.events --target local_events`, explicit `run`, two `cdf sql` system-history queries, `inspect package`, `state history`, `contract freeze`, `contract test`, clean-ledger `replay package`, replay `state history`, and `cdf version`: passed.
- A simplified parser smoke after concurrent CLI changes also passed for `plan local.events`, `run local.events`, `state history local.events`, and clean-ledger `replay package`.
- `cargo test -p cdf-conformance mvp_acceptance_demo --locked`: passed; 1 test. This backs the quickstart section that points crash/resume and drift-quarantine proof to the conformance fixture.

Static docs checks:

- Local Markdown file and checked-anchor validator over `docs/**/*.md`: passed.
- `git diff --check -- docs`: passed.
- Focused `rg` sweep over `docs` and the WS6A ticket for placeholder/marketing/forbidden-demo phrase sentinels: passed with no output.
- Help sanity for documented command families (`cdf help run`, `cdf help plan`, `cdf help replay package`, `cdf help state recover`, `cdf help backfill`, plus earlier doctor/status/package checks): passed.

Parent verification after the WS2C CLI grammar commit:

- `cargo build -p cdf-cli --locked`: passed.
- `cargo test -p cdf-conformance mvp_acceptance_demo --locked`: passed; 1 test.
- Destination-file-aware Markdown file and anchor validator over `docs/**/*.md`: passed, including links into `VISION.md` and `.10x/specs/**`.
- Repository forbidden demo-phrase scan excluding `target/`: no matches.
- `cargo fmt --all -- --check`: passed.
- Scoped `git diff --check` over modified WS6A records: passed.

Unavailable local docs tools:

- `markdownlint`, `markdownlint-cli2`, `lychee`, and `mdbook` were not installed. No tools were installed for this ticket.

Quality note:

- During worker execution, `cargo fmt --all -- --check` was initially blocked by concurrent dirty `crates/cdf-cli/**` changes outside WS6A scope. After the WS2C CLI grammar slice landed, parent verification reran `cargo fmt --all -- --check` successfully.

## What this supports

This supports closing WS6A:

- The docs topology exists.
- The quickstart names prerequisites and runs from a clean checkout plus temporary local project state.
- Implemented command snippets were verified against the current CLI.
- Pending generated references, error catalog, runnable examples, and init README scaffold are explicitly linked to their owning tickets.
- Architecture and operator guides link to the book and active specs instead of redefining behavior.

## Limits

This evidence does not prove generated command reference freshness, generated error catalog freshness, runnable examples, docs-site generation, installer behavior, or `cdf init` README behavior. Those remain owned by WS6B, WS6C, WS6D, and WS8 tickets.
