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
- 2026-08-04: The first focused test invocation mistakenly combined a short-name filter with
  `--exact` and selected zero tests. Re-running the non-exact short-name filter reproduced the
  sandbox failure: the multiline manifest fixture failed Internal at
  `manifest string exceeds bounds or contains control characters`.
- 2026-08-04: Restricted the security exception to the typed `authored_sql` field and only HT/LF/CR;
  all other strings remain control-free. Added exhaustive C0/C1 rejection under both compiler and
  artifact authority, retained length/secret/host-path fences, and proved exact authored SQL/hash
  round-trip. Compile now directly returns the selected offline/refresh result; the broad
  Contract/Data message mutator is deleted.
- 2026-08-04: The broader SQL command slice exposed two dormant query-first fixture drifts once
  manifest compilation could proceed: a `.cdf.sql` test still contained the deleted declarative
  envelope, and an unpinned file query expected zero discovery. Updated only those test inputs/
  expectations to current D3 behavior; no product behavior changed.
- 2026-08-04: The supplied sandbox's ordinary offline compile stopped earlier at existing missing
  finalized authority for `fineweb_local.documents`. The same worktree binary then completed
  `compile --refresh` for all seven resources and published manifest hash
  `sha256:23547530b0fd91f04fa03c397dd3ac070e880aa1a2825aba8e1f23d7a53fd173`, proving the exact
  previously failing multiline publication path no longer reaches Internal.
- 2026-08-04: Independent frozen-diff review rejected the first repair: matching any JSON key named
  `authored_sql` gave adapter-owned nested data the whitespace exception, and compile-path evidence
  did not directly exercise Data/Auth preservation. Replaced the key-name check with an explicit
  typed-manifest location walk that admits HT/LF/CR only at
  `resources[*].origin.authored_sql`. Added a nested adapter-key collision regression, a CRLF/tab
  typed-manifest round-trip with an independently calculated SHA-256, and real offline Data plus
  refresh Auth CLI diagnostics. No serializer, adapter payload, or generic string gained the
  exception.

## Blockers

None. The ticket is executable from its active references.

## Evidence

- AC 1-2: The original focused command
  `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-project --lib
  manifest_identity_is_stable_and_excludes_generation_time -- --nocapture` failed before the fix
  with the reproduced Internal error. After the reviewed repair, `security_tests` passed 3/3,
  `multiline_authored_sql_round_trips_with_exact_content_authority` passed 1/1, and the original
  identity test passed 1/1. The security tests exhaust every C0/C1 code point except the three
  admitted typed-authored-SQL whitespace characters and verify Compiler→Internal, Artifact→Data,
  the adapter-owned `authored_sql` collision, non-SQL whitespace rejection, size, secret, and
  absolute-path fences. The round-trip fixture contains HT, CRLF, and LF and compares both manifest
  authorities to an independently calculated SHA-256 of the exact authored bytes.
- AC 1-4: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-project --lib manifest -- --nocapture`
  passed 30/30 focused manifest/discovery/runtime tests. `DUCKDB_DOWNLOAD_LIB=1 cargo test -p
  cdf-cli --lib tests::sql:: -- --nocapture` passed 11/11 after the two stale test fixtures were
  made current. The missing-lock test proves the same original message/code/kind appears in JSON
  and human output, no generic refresh suffix appears, and neither lock nor manifest is written.
  A second CLI test proves an offline manifest-path failure remains Data and a denied refresh HTTP
  observation remains Auth, with neither message receiving generic refresh decoration.
- AC 3-4: Compile has no command-layer error transformer after choosing offline versus refresh; the
  originating `CdfError -> CliError` conversion owns code, kind, remediation, and redaction.
  `cargo test -p cdf-cli-core --lib output::tests -- --nocapture` passed 2/2 for Internal mapping and
  secret-bearing JSON/headless/TTY parity. A subsequent `error_catalog` filter selected zero tests
  and is explicitly not evidence.
- AC 5: The durable file manifest and classification ledger are
  `.10x/evidence/.storage/2026-08-04-u0-error-construction-files.txt` and
  `.10x/evidence/.storage/2026-08-04-u0-error-construction-ledger.tsv`. Reproduce with
  `git ls-files -- crates/cdf-cli/src/compile_command.rs crates/cdf-project/src/manifest.rs | sort`
  and `rg -n -- 'CdfError::internal|ManifestErrorAuthority::Compiler => CdfError::internal'
  crates/cdf-cli/src/compile_command.rs crates/cdf-project/src/manifest.rs`. A supplemental
  `rg -n -- 'ErrorKind::Internal'` over the same files finds four test assertions and no direct
  constructor. The final production inventory has two files, two site-bearing files, fourteen
  Internal construction sites (four CLI, ten manifest), zero unclassified Internal constructors,
  plus the two compile-owned Contract endpoints affected by deletion of the decorator.
- AC 5: `DUCKDB_DOWNLOAD_LIB=1 cargo check -p cdf-project -p cdf-cli --all-targets` and strict
  affected-package Clippy with `-D warnings` passed after the reviewed repair. The explicit
  cognitive-complexity diagnostic completed and reported only pre-existing dependent/test
  functions; no changed production function crossed the threshold. `cargo fmt --all -- --check`
  and `git diff --check` passed.
- Sandbox limit/evidence: offline compile did not reach manifest publication because its monolithic
  current authority still blocks on `fineweb_local.documents`; this is parent U2/U3 scope, not a U0
  regression. Refresh succeeded for seven resources in 4.5 seconds. It intentionally updated the
  sandbox lock/schema/manifest authority under the command's existing contract.
- Graph limit: `graphify-out/graph.json` exists but the `graphify` executable is unavailable in this
  environment, so no graph freshness claim is made.

## Review

The first frozen-diff review verdict was `fail`: one significant structural security bypass and one
minor diagnostic-evidence gap. Both findings are repaired and recorded above. A second independent
frozen-diff verdict is pending.

## Retrospective

- Artifact security cannot treat every string as a token. Free-form authored content needs a
  structural field-owned character policy, while diagnostic/configuration and adapter-owned
  strings keep the stricter fence. A matching key name is not typed authority.
- A broad error-kind remediation mutator erases ownership even when it preserves the numeric kind.
  Command dispatch should pass typed failures through; the originating boundary supplies the fix.
- Removing an early Internal stop-line can reveal stale tests that never reached their assertions.
  Running the complete affected command family once was useful; fixing current-only fixtures was
  smaller and safer than weakening the new regression.
- A filtered test command is evidence only when its output reports the expected nonzero selection.
  The mistaken `--exact` invocation cost one short rerun and is recorded to prevent false evidence.
