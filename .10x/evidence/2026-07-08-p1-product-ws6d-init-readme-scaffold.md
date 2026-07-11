Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws6d-init-readme-scaffold.md, .10x/specs/docs-onboarding-surface.md

# P1 product WS6D init README scaffold evidence

## What was observed

`cdf init` now scaffolds `README.md` as a `cdf-project` owned scaffold file.
The README is static: it links users to `docs/quickstart.md`, names only
supported first commands (`cdf validate`, `cdf plan local.events --target
local_events`, and `cdf run --resource local.events --pipeline local.events
--target local_events`), and does not interpolate the project name, target root,
secrets, `.cdf/`, packages, checkpoint state, or destination paths.

The quickstart init transcript was updated narrowly to include `README.md` in
the created-path list and to remove the obsolete note that `cdf init` does not
create a README.

## Procedure

Inspected governing records and implementation before editing:

- `.10x/tickets/done/2026-07-08-p1-product-ws6d-init-readme-scaffold.md`.
- `.10x/specs/docs-onboarding-surface.md`.
- `.10x/tickets/done/2026-07-08-p1-product-ws6a-docs-topology-quickstart.md`.
- `.10x/decisions/cdf-init-local-scaffold-defaults.md`.
- `docs/quickstart.md`.
- `crates/cdf-project/src/scaffold.rs`.
- `crates/cdf-cli/src/project_command.rs`.
- `crates/cdf-cli/src/tests.rs`.

Changed:

- `crates/cdf-project/src/scaffold.rs`: added static `README_SCAFFOLD`, writes
  `README.md`, and includes it in unforced overwrite preflight.
- `crates/cdf-project/src/tests.rs`: proves the scaffold report includes
  `README.md`, the file exists, links `docs/quickstart.md`, names supported
  commands, avoids `secret://`, and avoids the local root path.
- `crates/cdf-cli/src/tests.rs`: proves fresh init JSON includes `README.md`,
  existing no-force init refuses and preserves an existing README, and `--force`
  replaces an existing README while preserving unrelated runtime/user files.
- `docs/quickstart.md`: updates only the init output/note that became false.

Direct JSON proof from `target/debug/cdf --json init "$TMP/readme-json-proof"
--name readme_json_proof` exited 0 and kept the existing JSON envelope and
fields:

```json
{
  "ok": true,
  "command": "init",
  "result": {
    "project_name": "readme_json_proof",
    "created": ["cdf.toml", "README.md", "resources", "resources/files.toml", "data"],
    "skipped": [],
    "replaced": [],
    "force": false
  }
}
```

Verification commands:

- `cargo fmt --all -- --check`: passed.
- `cargo test -p cdf-cli init_ --locked`: passed, 4 tests.
- `cargo test -p cdf-project local_project_scaffold_writes_valid_project_without_runtime_artifacts --locked`: passed, 1 test.
- `cargo test -p cdf-cli --locked`: passed, 142 unit tests plus 1 integration test.
- `cargo clippy -p cdf-cli -p cdf-project --all-targets --locked -- -D warnings`: passed.
- `rg -n "unsafe" crates/cdf-project/src/scaffold.rs crates/cdf-project/src/tests.rs crates/cdf-cli/src/tests.rs`: no matches.
- `gitleaks detect --no-git --source "$TMP_TOUCHED_COPY" --redact --no-banner --log-level warn`: passed with no findings over touched source/docs/records in the final scoped copy.
- `jscpd --min-lines 8 --min-tokens 80 --reporters console --threshold 0 --no-colors --no-tips crates/cdf-project/src/scaffold.rs crates/cdf-project/src/tests.rs docs/quickstart.md`: passed with 0 clones.
- `scc --by-file crates/cdf-project/src/scaffold.rs crates/cdf-project/src/tests.rs crates/cdf-cli/src/tests.rs`: reported total Rust complexity 9; `scaffold.rs` complexity 6, `cdf-project/src/tests.rs` complexity 0, and `cdf-cli/src/tests.rs` complexity 3.
- `git diff --check`: passed.

Parent verification repeated and extended the closure-relevant checks:

- `cargo fmt --all -- --check`: passed.
- `cargo test -p cdf-cli init_ --locked`: passed, 4 tests.
- `cargo test -p cdf-project local_project_scaffold_writes_valid_project_without_runtime_artifacts --locked`: passed, 1 test.
- `cargo test -p cdf-cli --locked`: passed, 142 unit tests plus 1 integration test plus doc tests.
- `cargo clippy -p cdf-cli -p cdf-project --all-targets --locked -- -D warnings`: passed.
- Direct unsafe-token scan over the touched Rust files: no matches.
- Temp generated-project smoke: `cdf init` emitted `README.md`; the README contained `docs/quickstart.md`, `cdf validate`, `cdf plan local.events --target local_events`, and `cdf run --resource local.events --pipeline local.events --target local_events`; it did not contain local paths, `secret://`, `.cdf/`, state, destination, checkpoint, or package paths; after seeding `data/events.ndjson`, `cdf validate`, `cdf plan`, and the README's `cdf run` command succeeded.
- `rust-code-analysis-cli -m` over the touched Rust files: passed, JSON metrics written under `target/quality/reports/ws6d-rust-code-analysis`.
- Parent `jscpd` over touched Rust/docs: passed under the existing duplicate budget, with 24 clones, 337 duplicated lines, and 4.05 percent total duplication in `target/quality/reports/ws6d-jscpd-parent/jscpd-report.json`; clone ranges are in pre-existing portions of `crates/cdf-cli/src/tests.rs`.
- `semgrep scan --config p/rust --error` over the touched Rust files: passed with 0 findings.
- Scoped gitleaks over touched source, docs, and WS6D records: passed with no findings. A broader repo gitleaks walk was interrupted because it started traversing local build artifacts; this evidence relies on the scoped source/docs/record scan that matches the ticket write set.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`: passed, with only the already-ratified `paste` advisory ignored.
- `cargo deny --locked check advisories licenses sources`: passed.
- `cargo vet --locked --no-minimize-exemptions`: passed.
- `osv-scanner scan source --lockfile Cargo.lock`: returned only the already-ratified `RUSTSEC-2024-0436`.
- `cargo machete`: passed with no unused dependencies.
- `scc --by-file` over touched Rust recorded `Rust files=3 code=2177 complexity=9`.
- Repository forbidden demo-phrase scan excluding `target/`: no matches.
- `tools/codeql-rust-quality.sh`: passed using the reusable database path `target/quality/codeql-db-rust`; the helper refreshed the database because the Rust source fingerprint changed, then produced `target/quality/reports/codeql-rust-current.sarif` with 0 results.

Additional duplication scan:

- `jscpd --min-lines 8 --min-tokens 80 --reporters console --threshold 0 --no-colors --no-tips crates/cdf-project/src/scaffold.rs crates/cdf-project/src/tests.rs crates/cdf-cli/src/tests.rs docs/quickstart.md`: reported 24 clones, 337 duplicated lines, 4.05% total duplication. The reported clone ranges were in pre-existing portions of `crates/cdf-cli/src/tests.rs` outside the WS6D-edited init-test block, so no WS6D follow-up was opened.

## What this supports or challenges

Supports all WS6D acceptance criteria:

- Fresh `cdf init` creates `README.md`.
- Existing README content is preserved without `--force`.
- `--force` replaces an existing README and reports it through the existing
  `replaced` JSON array.
- README content points to `docs/quickstart.md`, names only implemented parser
  commands, and avoids secrets, runtime state, absolute local paths, and
  machine-specific assumptions.
- Existing JSON shape and exit-code behavior remain compatible; only the
  scaffold path values change to include the new file.

## Limits

This evidence does not prove generated command reference freshness, generated
error catalog freshness, runnable examples, external docs publishing, or the
future renderer/live-progress user experience. Those are outside WS6D. CodeQL
still reports extractor macro warnings that are covered by
`.10x/knowledge/quality-gate-execution.md`; SARIF security/query results were
empty.
