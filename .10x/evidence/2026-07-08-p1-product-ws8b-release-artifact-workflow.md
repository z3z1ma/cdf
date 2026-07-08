Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws8b-release-artifact-workflow.md, .10x/specs/versioning-lts-release-policy.md, .10x/knowledge/datafusion-cratesio-arrow59-tripwire.md

# WS8B release artifact workflow evidence

## What was observed

WS8B added:

- `.github/workflows/release-artifacts.yml`
- `LICENSE`
- `tools/verify-release-metadata.sh`
- `tools/package-release-artifact.sh`
- `tools/verify-release-artifacts.sh`
- `tools/test-release-artifacts.sh`
- `tools/write-reproducible-targz.py`

The release workflow has a metadata job, a five-target build matrix, a bundle verification job, and a GitHub prerelease upload job. The target matrix covers the active policy's initial mainstream targets:

- `x86_64-unknown-linux-gnu` on `ubuntu-latest`
- `aarch64-unknown-linux-gnu` on `ubuntu-24.04-arm`
- `x86_64-apple-darwin` on `macos-15-intel`
- `aarch64-apple-darwin` on `macos-15`
- `x86_64-pc-windows-msvc` on `windows-latest`

No mainstream target is silently omitted. The workflow relies on current hosted runner labels accepted by local `actionlint`; actual hosted-runner execution remains future evidence.

`tools/verify-release-metadata.sh` fails closed when the requested version does not match `[workspace.package].version`, the changelog lacks a dated section for the version, `LICENSE` is absent, the workspace license is not Apache-2.0, or the DataFusion git pin is active while any crate manifest lacks `publish = false`.

`tools/package-release-artifact.sh` creates `cdf-<version>-<target>.tar.gz` with:

- `bin/cdf` or `bin/cdf.exe`
- `LICENSE`
- `CHANGELOG-excerpt.md`
- `release-metadata.txt`
- `generated/ARTIFACTS.txt`
- generated completions/man pages when supplied under `target/generated/completions` or `target/generated/man`

It writes the archive through `tools/write-reproducible-targz.py`, which orders paths deterministically and normalizes tar owner/group names, ids, modes, and mtimes while writing gzip output with timestamp zero. It writes an adjacent `.sha256` file and verifies the archive digest immediately. `tools/verify-release-artifacts.sh` verifies every expected archive, checksum, binary path, license, changelog excerpt, release metadata, and generated-artifact inventory.

The workflow does not call `cargo publish` or `cargo login`. A self-safe grep guard in the metadata job checks the workflow and tool scripts for publication commands.

## Procedure and results

- `bash -n tools/verify-release-metadata.sh tools/package-release-artifact.sh tools/verify-release-artifacts.sh tools/test-release-artifacts.sh`: passed.
- `ruby -e 'require "yaml"; ARGV.each { |f| YAML.load_file(f); puts "ok #{f}" }' .github/workflows/release-artifacts.yml`: passed.
- `"$HOME/go/bin/actionlint" .github/workflows/*.yml`: passed after replacing stale macOS labels with current hosted labels.
- `brew install shellcheck`: installed ShellCheck 0.11.0 because the tool was missing and this ticket adds shell scripts.
- `shellcheck tools/verify-release-metadata.sh tools/package-release-artifact.sh tools/verify-release-artifacts.sh tools/test-release-artifacts.sh tools/install-cdf.sh tools/test-install-cdf.sh`: passed.
- `python3 -m py_compile tools/write-reproducible-targz.py`: passed.
- `tools/verify-release-metadata.sh 0.1.0 --write-changelog-excerpt target/quality/reports/ws8b-changelog-excerpt.md`: passed with `release metadata ok for 0.1.0`.
- `tools/test-release-artifacts.sh`: passed. It proved successful package/verify, deterministic archive bytes and SHA-256 across two packages from identical staged inputs, generated completion/man inclusion when present, absent generated artifact inventory when missing, checksum mismatch failure, and inconsistent metadata failure.
- `publish_pattern='cargo publi[s]h|cargo logi[n]'; if grep -RE "$publish_pattern" .github/workflows tools; then exit 1; fi; echo 'crates.io publication disabled guard passed'`: passed.
- `cargo build -p cdf-cli --release --locked`: passed before later unrelated CLI edits appeared. It emitted three pre-existing unused-import warnings in `crates/cdf-cli/src/contract_command.rs`, `crates/cdf-cli/src/package_command.rs`, and `crates/cdf-cli/src/project_command.rs`; WS8B did not touch `crates/cdf-cli/src/**`.
- `target/release/cdf version`: passed with `cdf 0.1.0`.
- `tools/package-release-artifact.sh --version 0.1.0 --target aarch64-apple-darwin --binary target/release/cdf --out-dir target/quality/ws8b-local-dist && tools/verify-release-artifacts.sh 0.1.0 target/quality/ws8b-local-dist aarch64-apple-darwin`: passed.
- Local checksum produced: `a657a54da373395e62c501ed35fbadf19c47b91e56ef23d0cbd6bf8f12620a3f  cdf-0.1.0-aarch64-apple-darwin.tar.gz`.
- Archive listing showed `LICENSE`, `CHANGELOG-excerpt.md`, `release-metadata.txt`, `bin/cdf`, and `generated/ARTIFACTS.txt`.
- Extracted `release-metadata.txt` recorded `crates_io_publication: disabled while the DataFusion git pin is active`.
- Extracted `generated/ARTIFACTS.txt` recorded that completions and man pages are not included until WS2D supplies them.
- `tools/install-cdf.sh --version 0.1.0 --target aarch64-apple-darwin --base-url target/quality/ws8b-local-dist --prefix target/quality/ws8b-install-smoke && target/quality/ws8b-install-smoke/bin/cdf version`: passed with `Installed cdf 0.1.0 ...` and `cdf 0.1.0`.
- `gitleaks detect --no-git --source target/quality/ws8b-gitleaks-source --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-ws8b-release-source-final.json`: passed with no leaks.
- `jscpd .github/workflows/release-artifacts.yml tools/verify-release-metadata.sh tools/package-release-artifact.sh tools/verify-release-artifacts.sh tools/test-release-artifacts.sh LICENSE .10x/evidence/2026-07-08-p1-product-ws3d-recovery-state-backfill-rendering.md --reporters console,json --output target/quality/reports/jscpd-ws8b-release-final --ignore "**/target/**,**/.git/**,**/reports/**" --min-lines 12 --min-tokens 80 --threshold 10 --no-colors`: passed with 0 clones and 0.00% duplication.
- `scc --by-file --format json .github/workflows/release-artifacts.yml tools/verify-release-metadata.sh tools/package-release-artifact.sh tools/verify-release-artifacts.sh tools/test-release-artifacts.sh > target/quality/reports/scc-ws8b-release.json && scc --by-file ...`: passed. Shell total complexity was 92 across four scripts; the highest file was `tools/package-release-artifact.sh` at 40.
- `semgrep scan --config p/secrets --error --json --output target/quality/reports/semgrep-ws8b-secrets.json --exclude target .github/workflows/release-artifacts.yml tools/verify-release-metadata.sh tools/package-release-artifact.sh tools/verify-release-artifacts.sh tools/test-release-artifacts.sh`: passed with 0 findings.
- `cargo metadata --format-version 1 --locked > target/quality/reports/cargo-metadata-ws8b-release.json`: passed.
- `cargo deny --locked check advisories licenses sources`: passed.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`: passed.
- `cargo vet --locked --no-minimize-exemptions`: passed with `Vetting Succeeded (452 exempted)`.
- `osv-scanner scan source --lockfile Cargo.lock --format json > target/quality/reports/osv-ws8b-release.json`: exited nonzero because it reported the already-ratified `RUSTSEC-2024-0436` paste advisory.
- `jq -e '([(.results // [])[] | .packages[]?.vulnerabilities[]?.id] | unique) == ["RUSTSEC-2024-0436"]' target/quality/reports/osv-ws8b-release.json`: passed with `true`.
- `rg -n -i "killer[ _-]?demo" .10x docs crates VISION.md QUALITY.md README.md CHANGELOG.md tools .github --glob '!target/**' --glob '!**/target/**'`: exited 1 with no matches.
- `git diff --check -- .github/workflows/release-artifacts.yml tools/verify-release-metadata.sh tools/package-release-artifact.sh tools/verify-release-artifacts.sh tools/test-release-artifacts.sh LICENSE .10x/evidence/2026-07-08-p1-product-ws3d-recovery-state-backfill-rendering.md`: passed.

## Reproducibility blocker repair

An external review found a blocking WS8B issue: the initial `tools/package-release-artifact.sh` used ordinary `tar -czf`, which produced checksummed archives but did not prove reproducibility because tar entry order, owner/group metadata, mtimes, and gzip header timestamps could vary.

The repair replaced ordinary tar/gzip invocation with `tools/write-reproducible-targz.py`, using only Python standard-library modules available in CI after `actions/setup-python`. The writer:

- walks staged paths in sorted order;
- writes tar entries in USTAR format;
- normalizes uid/gid to `0`;
- normalizes uname/gname to empty strings;
- normalizes mtime to `0`;
- normalizes directory modes to `0755`, executable files to `0755`, and non-executable files to `0644`;
- writes gzip output with `mtime=0` and an empty gzip filename field.

The release workflow now installs Python in the build matrix before packaging.

Rerun checks after the repair:

- `bash -n tools/verify-release-metadata.sh tools/package-release-artifact.sh tools/verify-release-artifacts.sh tools/test-release-artifacts.sh`: passed.
- `python3 -m py_compile tools/write-reproducible-targz.py`: passed.
- `tools/test-release-artifacts.sh`: passed, including `ok reproducible package hash is stable for identical staged inputs`.
- `shellcheck tools/verify-release-metadata.sh tools/package-release-artifact.sh tools/verify-release-artifacts.sh tools/test-release-artifacts.sh tools/install-cdf.sh tools/test-install-cdf.sh`: passed.
- `"$HOME/go/bin/actionlint" .github/workflows/release-artifacts.yml`: passed.
- `ruby -e 'require "yaml"; ARGV.each { |f| YAML.load_file(f); puts "ok #{f}" }' .github/workflows/release-artifacts.yml`: passed.
- `tools/verify-release-metadata.sh 0.1.0 --write-changelog-excerpt target/quality/reports/ws8b-changelog-excerpt-rerun.md`: passed.
- `publish_pattern='cargo publi[s]h|cargo logi[n]'; if grep -RE "$publish_pattern" .github/workflows tools; then exit 1; fi; echo 'crates.io publication disabled guard passed'`: passed.
- `tools/package-release-artifact.sh --version 0.1.0 --target aarch64-apple-darwin --binary target/release/cdf --out-dir target/quality/ws8b-local-dist-rerun && tools/verify-release-artifacts.sh 0.1.0 target/quality/ws8b-local-dist-rerun aarch64-apple-darwin`: passed.
- Rerun local checksum produced: `d345025c46596b45523034a43e56bac815880eeac5b580969fee8daff2a88991  cdf-0.1.0-aarch64-apple-darwin.tar.gz`.
- Rerun archive listing showed deterministic sorted order: root, `CHANGELOG-excerpt.md`, `LICENSE`, `bin/`, `bin/cdf`, `generated/`, `generated/ARTIFACTS.txt`, `release-metadata.txt`.
- Rerun extracted `release-metadata.txt` still recorded `crates_io_publication: disabled while the DataFusion git pin is active`.
- Rerun extracted `generated/ARTIFACTS.txt` still recorded that completions and man pages are not included until WS2D supplies them.
- `tools/install-cdf.sh --version 0.1.0 --target aarch64-apple-darwin --base-url target/quality/ws8b-local-dist-rerun --prefix target/quality/ws8b-install-smoke-rerun && target/quality/ws8b-install-smoke-rerun/bin/cdf version`: passed with `cdf 0.1.0`.
- `gitleaks detect --no-git --source target/quality/ws8b-gitleaks-repro --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-ws8b-repro.json`: passed with no leaks.
- `jscpd .github/workflows/release-artifacts.yml tools/verify-release-metadata.sh tools/package-release-artifact.sh tools/verify-release-artifacts.sh tools/test-release-artifacts.sh tools/write-reproducible-targz.py LICENSE .10x/tickets/done/2026-07-08-p1-product-ws8b-release-artifact-workflow.md .10x/evidence/2026-07-08-p1-product-ws8b-release-artifact-workflow.md .10x/reviews/2026-07-08-p1-product-ws8b-release-artifact-workflow-review.md .10x/tickets/2026-07-08-p1-product-ws8-release-engineering.md .10x/evidence/2026-07-08-p1-product-ws3d-recovery-state-backfill-rendering.md --reporters console,json --output target/quality/reports/jscpd-ws8b-repro --ignore "**/target/**,**/.git/**,**/reports/**" --min-lines 12 --min-tokens 80 --threshold 10 --no-colors`: passed with 0 clones and 0.00% duplication across Bash, Python, and YAML.
- `scc --by-file --format json .github/workflows/release-artifacts.yml tools/verify-release-metadata.sh tools/package-release-artifact.sh tools/verify-release-artifacts.sh tools/test-release-artifacts.sh tools/write-reproducible-targz.py > target/quality/reports/scc-ws8b-repro.json && scc --by-file ...`: passed. Shell total complexity was 97; Python writer complexity was 16.
- `semgrep scan --config p/secrets --error --json --output target/quality/reports/semgrep-ws8b-repro-secrets.json --exclude target .github/workflows/release-artifacts.yml tools/verify-release-metadata.sh tools/package-release-artifact.sh tools/verify-release-artifacts.sh tools/test-release-artifacts.sh tools/write-reproducible-targz.py`: passed with 0 findings.
- `cargo metadata --format-version 1 --locked > target/quality/reports/cargo-metadata-ws8b-repro.json`: passed.
- `cargo deny --locked check advisories licenses sources`: passed.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`: passed.
- `cargo vet --locked --no-minimize-exemptions`: passed with `Vetting Succeeded (452 exempted)`.
- `osv-scanner scan source --lockfile Cargo.lock --format json > target/quality/reports/osv-ws8b-repro.json`: exited nonzero because it reported the already-ratified `RUSTSEC-2024-0436` paste advisory.
- `jq -e '([(.results // [])[] | .packages[]?.vulnerabilities[]?.id] | unique) == ["RUSTSEC-2024-0436"]' target/quality/reports/osv-ws8b-repro.json`: passed with `true`.
- `rg -n -i "killer[ _-]?demo" .10x docs crates VISION.md QUALITY.md README.md CHANGELOG.md tools .github --glob '!target/**' --glob '!**/target/**'`: exited 1 with no matches.
- `git diff --check -- .github/workflows/release-artifacts.yml LICENSE tools/verify-release-metadata.sh tools/package-release-artifact.sh tools/verify-release-artifacts.sh tools/test-release-artifacts.sh tools/write-reproducible-targz.py .10x/tickets/done/2026-07-08-p1-product-ws8b-release-artifact-workflow.md .10x/evidence/2026-07-08-p1-product-ws8b-release-artifact-workflow.md .10x/reviews/2026-07-08-p1-product-ws8b-release-artifact-workflow-review.md .10x/tickets/2026-07-08-p1-product-ws8-release-engineering.md`: passed.

## Parent review reruns

Parent review reran these checks after the reproducibility repair:

- `bash -n tools/verify-release-metadata.sh tools/package-release-artifact.sh tools/verify-release-artifacts.sh tools/test-release-artifacts.sh`: passed.
- `python3 -m py_compile tools/write-reproducible-targz.py`: passed.
- YAML parse for `.github/workflows/release-artifacts.yml`: passed.
- `actionlint .github/workflows/release-artifacts.yml`: passed.
- `shellcheck tools/verify-release-metadata.sh tools/package-release-artifact.sh tools/verify-release-artifacts.sh tools/test-release-artifacts.sh tools/install-cdf.sh tools/test-install-cdf.sh`: passed.
- `tools/verify-release-metadata.sh 0.1.0 --write-changelog-excerpt target/quality/reports/ws8b-parent-changelog-excerpt.md`: passed.
- `tools/test-release-artifacts.sh`: passed, including the byte-identical reproducible package proof.
- Crates.io publication guard grep across `.github/workflows` and `tools`: passed.
- `jscpd` over the WS8B workflow/scripts/license/records: passed with 0 clones and 0.00% duplication.
- `semgrep scan --config p/secrets` over the WS8B workflow/scripts: passed with 0 findings.
- `scc` over the WS8B workflow/scripts: 6 files, 811 lines, 689 code lines, total complexity 113.
- Scoped Gitleaks over `target/quality/ws8b-gitleaks-parent-scope`: passed with no leaks.
- `cargo metadata --format-version 1 --locked`, `cargo deny --locked check advisories licenses sources`, `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`, and `cargo vet --locked --no-minimize-exemptions`: passed.
- `osv-scanner scan source --lockfile Cargo.lock --format json`: reported only the already-ratified `RUSTSEC-2024-0436` advisory.
- Parent host package/verify/install smoke using existing `target/release/cdf`: passed with `cdf 0.1.0`.

## Limited or blocked checks

- CodeQL was not run for WS8B. This slice changed workflow, shell scripts, license text, and records; it did not change Rust source, manifests, or lockfiles. Per the user instruction, no disposable CodeQL database was created.
- `cargo fmt --all -- --check` was attempted after WS8B checks. It failed parsing `crates/cdf-cli/src/sql_command.rs` with `unexpected closing delimiter`) due to unrelated dirty CLI work outside WS8B's write scope. WS8B did not touch `crates/cdf-cli/src/**`; the earlier release build succeeded before those unrelated edits appeared.
- The GitHub Actions release workflow has not been executed remotely. Local actionlint, YAML parsing, shell lint, metadata validation, host build/package, checksum verification, installer smoke, and security/duplication/supply-chain checks are the available local equivalent.

## What this supports

This supports closing WS8B: the release workflow and reusable scripts produce fail-closed reproducible checksummed binary archives, preserve the no-crates.io-publication boundary while DataFusion is git-pinned, include license and changelog excerpts, conditionally package generated CLI artifacts, and provide local smoke evidence for packaging and installer consumption.

## Limits

Actual hosted builds for the five target runners and actual GitHub prerelease upload remain future CI evidence. WS2D now owns the generator at `.10x/tickets/done/2026-07-08-p1-product-ws2d-completions-manpages-help.md`; WS8B only packages those artifacts when present and records their absence when not present.
