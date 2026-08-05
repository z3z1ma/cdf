Status: active
Created: 2026-08-04
Updated: 2026-08-04
Parent: `.10x/tickets/2026-08-04-resource-first-cli-experience-program.md`

# U0 manifest text and diagnostic ownership

## Scope

Repair the two immediately reproduced correctness failures without coupling them to the later
resource-artifact redesign:

- admit authored horizontal tab, line feed, and carriage return in manifest strings so multiline
  CDF SQL can compile, publish, and reload;
- retain bounds, secret, host-path, and all other C0/C1 control-character rejection;
- delete the blanket compile-layer remediation decorator that appends `cdf compile --refresh` to
  broad Contract/Data/Auth failures, preserving the originating typed diagnostic and remediation;
- add focused regression coverage at manifest and CLI error-report boundaries.

The error-ownership audit scope is exactly `crates/cdf-project/src/manifest.rs` and
`crates/cdf-cli/src/compile_command.rs`. Record a reproducible constructor inventory and per-site
classification for touched Internal/Contract paths under `.10x/evidence/.storage/` as required by
the project audit skill.

## Non-goals

- removing compile refresh grammar or changing compile preparation semantics;
- resource-sharded lock/compiled artifacts/project index;
- static validate, selectors, schema lifecycle, portable plans, or discovery;
- reclassifying unrelated Internal constructions or changing the stable CLI taxonomy;
- weakening secret/path/bounds validation.

## Acceptance criteria

1. A manifest containing ordinary multiline SQL with tabs/newlines/carriage returns validates,
   round-trips canonical serialization, and preserves the exact authored input hash.
2. Every other C0/C1 control character remains rejected at the manifest boundary; length, secret,
   and host-path fences retain focused protection.
3. Compile returns the originating code/kind/message/context/remediation for representative
   Contract, Data, Auth, and Internal failures and never adds generic refresh advice based only on
   broad kind.
4. Human and JSON diagnostics derive from the same structured error facts and remain redacted.
5. The exact two-file error-construction inventory, classification ledger, focused tests, affected
   checks, strict affected Clippy, formatting, and `git diff --check` are recorded honestly.
6. One independent frozen-diff subagent review returns pass or all significant/critical findings
   are fixed and re-reviewed before closure.

## References

- `.10x/specs/resource-preparation-command-experience.md`
- `.10x/specs/project-compilation-manifest.md`
- `.10x/knowledge/error-ownership-taxonomy.md`
- `.agents/skills/audit-error-ownership/SKILL.md`
- `.10x/knowledge/cli-report-authority.md`
- `.agents/skills/audit-cli-report-authority/SKILL.md`

## Assumptions

- User-ratified: multiline SQL is valid authored input; generic refresh/validate folklore is not
  acceptable remediation.
- Record-backed: generated manifest corruption remains CDF-owned Internal authority; this repair
  changes which whitespace is valid, not ownership of a truly corrupt private artifact.
- Record-backed: the existing typed `CdfError`/CLI report boundary is retained rather than replaced.

## Journal

- 2026-08-04: Opened from the exact sandbox reproduction and source inspection. The current
  validator rejects every Unicode control, including SQL line feeds, while compile command wraps
  all errors in a broad-kind remediation helper. No product code changed in this shaping turn.
- 2026-08-04: Execution began after the user confirmed the final command-intent contract. Re-read
  the governing manifest/error/report authorities and fixed the audit scope to manifest validation
  plus compile command error propagation before touching product code.

## Blockers

None. The ticket is executable from its active references.

## Evidence

Pending execution.

## Review

Pending independent review.

## Retrospective

Pending execution.
