Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-product-ws6d-init-readme-scaffold.md
Verdict: pass

# P1 product WS6D init README scaffold review

## Target

Review of the WS6D implementation in:

- `crates/cdf-project/src/scaffold.rs`.
- `crates/cdf-project/src/tests.rs`.
- `crates/cdf-cli/src/tests.rs`.
- `docs/quickstart.md`.

Evidence: `.10x/evidence/2026-07-08-p1-product-ws6d-init-readme-scaffold.md`.

## Findings

- Pass: README behavior belongs to `cdf-project::write_local_project_scaffold`,
  preserving the existing CLI/project boundary. `cdf-cli` output code was not
  changed because the existing report fields already express created, skipped,
  and replaced paths.
- Pass: unforced overwrite protection now includes `README.md`, so an existing
  README blocks init before scaffold writes occur.
- Pass: force replacement treats `README.md` as scaffold-owned while preserving
  unrelated runtime/user files such as `.cdf/state.db`, `cdf.lock`, and data
  inputs.
- Pass: README content is static and does not include project names, local
  roots, secrets, `.cdf/`, package paths, checkpoint state, destination files,
  or machine-specific assumptions.
- Pass: README command mentions are limited to implemented parser commands and
  local scaffold resource syntax. The richer onboarding path remains delegated
  to `docs/quickstart.md`.
- Pass: parent smoke generated a fresh project, read the emitted README, seeded
  `data/events.ndjson`, and successfully ran the README's validate, plan, and
  run command sequence.
- Pass: JSON compatibility is preserved at the envelope/schema level. The
  existing `created`, `skipped`, and `replaced` arrays now include `README.md`
  when that file is created or replaced.
- Pass: the quickstart update is limited to the now-false init output/note and
  does not broaden docs content.
- Pass: parent quality verification covered fmt, focused and full cdf-cli tests,
  clippy, unsafe scan, Semgrep, scoped gitleaks, jscpd, complexity metrics,
  supply-chain gates, repository forbidden-phrase scan, and CodeQL.

## Residual risk

The older active scaffold-default decision lists the initial scaffold-owned
paths before WS6D and does not enumerate README. This is not treated as a
closure blocker because the same decision explicitly allows later scaffold
expansion, and the active docs onboarding spec plus WS6D ticket specifically
authorize the README addition.

The broad `jscpd` scan over the whole touched CLI test module reports existing
duplication outside the edited init-test range. The scaffold/docs-focused scan
passes with 0 clones, and the broad duplication is not caused or worsened by
WS6D.

The README still necessarily points to repository docs rather than embedding a
full tutorial in every scaffold. That is intentional per the docs onboarding
spec, and the emitted README commands were smoke-tested independently.

## Verdict

Pass. Evidence supports the acceptance criteria, and no review finding blocks
closing WS6D.
