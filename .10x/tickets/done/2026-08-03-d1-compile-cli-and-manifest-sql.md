Status: done
Created: 2026-08-03
Updated: 2026-08-03
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`
Depends-On: `.10x/tickets/done/2026-08-03-d1-project-compilation-manifest-core.md`

# D1 compile CLI and manifest SQL

## Scope

Add `cdf compile`/`cdf compile --refresh`, minimal stable project/environment location loading,
manifest verification, and the seven ratified read-only manifest tables in the existing `cdf sql`
SQLite catalog. Update scaffold/docs/generated CLI artifacts and focused command reports.

## Non-goals

- replacing SQLite with DataFusion;
- SQL resource authoring or native scalar IR;
- implicit compile/refresh/recovery from `cdf sql` or another read-only command;
- destination/state/package/checkpoint mutation;
- manifest history or remote serving.

## Acceptance criteria

1. CLI grammar, help, generated inventory, JSON reports, and human reports expose only `cdf compile`
   and `cdf compile --refresh` with clear offline/external-observation behavior.
2. Offline compile performs no network contact and publishes only a lock-matching manifest;
   missing/stale locked authority fails with exact `--refresh` remediation.
3. Refresh contacts only source-side read authorities required to update observations and publishes
   manifest plus changed lock atomically with `cdf.lock` last. It never mutates a destination,
   execution state, package, receipt, or checkpoint.
4. `cdf sql` loads project location/environment and a verified matching manifest without compiling
   declarative/SQL resource files or constructing source/destination registries.
5. `manifest_project`, `manifest_inputs`, `manifest_resources`, `manifest_fields`,
   `manifest_semantics`, `manifest_lineage`, and `manifest_diagnostics` expose stable columns and
   canonical JSON for nested facts.
6. Missing, stale, tampered, wrong-environment, or pending-publication manifests fail read-only with
   exact remediation and no filesystem change.
7. Existing checkpoint/package SQL tables and read-only/mutating-keyword/file-shape protections
   remain intact.
8. Scaffold ignores `.cdf/` while continuing to commit `cdf.lock`; examples/docs reflect current
   behavior.
9. Focused CLI/project tests prove no recompile/source contact, deterministic query rows, JSON/human
   parity, publication recovery separation, and error ownership.
10. Formatting, generated-artifact checks, `git diff --check`, focused tests/checks, and targeted
    strict Clippy pass without a whole-workspace suite.

## References

- `.10x/specs/project-compilation-manifest.md`
- `.10x/decisions/superseded/project-manifest-path-compile-and-query-policy.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/skills/audit-project-file-publication/SKILL.md`
- `.10x/skills/audit-cli-report-authority/SKILL.md`

## Assumptions

- Command spelling, manifest path, table names, read-only behavior, publication policy, and latest-
  generation retention are user-ratified.

## Journal

- 2026-08-03: Opened after D1 ratification. No product code changed in this shaping turn.
- 2026-08-03: Activated after manifest core commit `1acf7cb2` reached `main`. Re-read the CLI
  report-authority skill and publication authority; command success will use one typed report for
  JSON and renderer-owned human output.
- 2026-08-03: Added registry-free dispatch for `cdf compile` and `cdf sql`. Offline compilation
  hydrates only locked local schema artifacts, validates exact lock/project/resource/semantic
  authority, and reasserts the exact lock while publishing the manifest as the transaction commit
  target. Refresh is the only path that recovers an interrupted project publication, constructs
  source execution services, observes refreshable sources, and publishes schema sidecars,
  manifest, and `cdf.lock` in one transaction with the lock last.
- 2026-08-03: Added a minimal verified-manifest context for SQL. It stable-reads and verifies
  `cdf.toml`/`cdf.lock`/`.cdf/manifest.json`, then mounts the seven ratified manifest tables plus
  the six existing package/checkpoint tables. Changing a declarative resource file after compile
  does not trigger recompilation; tampered manifest content fails without any filesystem change.
- 2026-08-03: The scaffold now writes `.gitignore` with only `.cdf/`, preserving `cdf.lock` as
  committed authority. Generated help/man/completion/command-reference artifacts and operator docs
  describe offline versus refresh behavior.
- 2026-08-03: Clarified the earlier no-recompile observation: a changed declarative input never
  triggers implicit compilation, but must make manifest-backed SQL fail stale. The final red-team
  review correctly found that the loader verified the manifest's recorded input identities without
  re-reading those inputs, so the first implementation could serve an obsolete compiled view.
- 2026-08-03: The same review found two further authority violations: destination URI aliases were
  treated as lockfile ids instead of resolving through the built-in composition root, and the
  offline manifest publisher delegated to the generic mutating publisher which could forward-
  recover a pending transaction that appeared after initial load. The review verdict was `fail`
  with three significant findings and no critical findings.
- 2026-08-03: Closed those exact findings. Stable manifest load now securely re-reads and hashes
  every project-relative authored input twice around the generation/public-file stability sample;
  manifest compilation receives the canonical destination id resolved from built-in scheme
  registrations; and manifest-only offline publication uses a fail-closed pending-marker policy
  under the existing mutation guard. Explicit refresh remains the recovery path.

## Blockers

None after the manifest-core dependency closes.

## Evidence

- `cargo check -p cdf-cli-core -p cdf-project -p cdf-cli --all-targets` passed. This proves the
  changed compiler/project/CLI targets and tests typecheck; it does not prove runtime behavior.
- `cargo clippy -p cdf-cli-core -p cdf-project -p cdf-cli --all-targets -- -D warnings` passed.
  This proves the changed targets pass their strict lint wall; it is not a behavioral proof.
- Focused nextest selection ran 18 tests across CLI core/project/CLI with 605 skipped: all 18
  passed. It covered deterministic refresh→offline→repeat publication, missing-lock remediation,
  verified manifest SQL rows/canonical nested JSON, authored-input no-recompile behavior, tamper
  rejection/no-write behavior, existing package/checkpoint tables, read-only SQL protections,
  scaffold ignore policy, root help, and system-SQL error ownership. This intentionally did not run
  a whole workspace suite.
- `compile_refresh_observes_only_refreshable_sources_and_publishes_schema_authority` passed alone.
  It proves a local discovery resource is observed once, schema sidecars/lock/manifest are
  published, and destination/state/package/checkpoint files remain absent.
- Both generated CLI artifact check commands passed, followed by `git diff --check`. This proves
  generated help/man/completion/command/error references are fresh and the diff has no whitespace
  errors.
- GitHub Actions `Fast Quality` completed successfully for D1 core commit `1acf7cb2`; the CLI
  integration commit is not yet pushed at this evidence point.
- `graphify update .` could not run because the `graphify` executable is unavailable in this
  environment; no graph freshness claim is made.
- Red-team closure selection
  `test(/fail_closed_publication_never_recovers|sql_mounts_manifest_tables_then_rejects_stale|compile_binds_destination_uri_aliases/)`
  passed 3/3 with 576 tests skipped. It proves each named finding at its behavioral boundary:
  pending publication is byte-for-byte unchanged by offline publication, changed authored input is
  rejected without a write, and `postgresql`, `clickhouses`, and `parquet` bind to the canonical
  `postgres`, `clickhouse`, and `parquet_object_store` lock identities.
- Final `cargo clippy -p cdf-builtin-drivers -p cdf-project -p cdf-cli --all-targets -- -D warnings`
  and `git diff --check` passed. This is targeted static/format evidence, not a whole-suite claim.
- GitHub Actions `Fast Quality` completed successfully for CLI integration commit `19aafa0c`.

## Review

The independent lane-boundary reviewer returned `fail` with three significant findings: stale
authored inputs could remain queryable, aliased destination schemes did not bind to canonical lock
ids, and offline manifest publication could recover a transaction created after initial load.
Each finding is resolved by a direct production fence and a focused regression test. Final
reconciled verdict: `pass`. Residual risk is limited to host-level races outside the existing
stable-generation/double-read and mutation-guard model; no compatibility behavior was introduced.

## Retrospective

- A hash recorded in a manifest proves compilation identity only when readers re-observe the
  corresponding authored authority. Verification must join stored identity to current bytes.
- URI schemes are routing aliases; canonical driver ids belong to the composition root and must be
  supplied to artifact compilation rather than reconstructed from URI text.
- Reusing a transactional publisher is insufficient when the caller has stricter side-effect
  semantics. Recovery policy is part of the publication API, not an incidental implementation
  detail.
