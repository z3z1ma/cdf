Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws8c-changelog-installer-channel.md, .10x/specs/versioning-lts-release-policy.md

# WS8C changelog and installer channel evidence

## What was observed

WS8C added `CHANGELOG.md`, `tools/install-cdf.sh`, and `tools/test-install-cdf.sh`.

`CHANGELOG.md` keeps an `Unreleased` section and a dated `[0.1.0] - 2026-07-08` section. The headings use the active changelog policy's allowed section vocabulary.

`tools/install-cdf.sh` installs artifacts named `cdf-<version>-<target>.tar.gz` with adjacent `cdf-<version>-<target>.tar.gz.sha256` checksum files by default. It supports `--version`, `--prefix`, `--base-url`, `--artifact`, `--checksum`, `--target`, and `--dry-run`, plus matching `CDF_INSTALL_*` environment overrides. The default release URL is `https://github.com/z3z1ma/cdf/releases/download/v<version>/`.

The installer supports the first Darwin/Linux shell targets: `aarch64-apple-darwin`, `x86_64-apple-darwin`, `aarch64-unknown-linux-gnu`, and `x86_64-unknown-linux-gnu`. Unsupported targets fail closed before download or install. The installer does not invoke `sudo` or attempt privilege escalation.

The install path is `<prefix>/bin/cdf`. The installer fetches the checksum file, parses a 64-hex SHA-256 digest, fetches the artifact, compares the artifact digest, extracts into a temporary directory, verifies the contained `cdf` binary can print `cdf version`, then creates the prefix bin directory and replaces the target binary. Dry-run prints the resolved version, target, artifact, checksum, prefix, and install path without downloading or writing.

`tools/test-install-cdf.sh` builds local temporary tarball fixtures and exercises the installer without project or release-service mutation.

Follow-up install channels are explicitly deferred: brew, cargo-install, OS package-manager feeds, signed updates, and auto-update channels remain out of scope unless a later active child ticket scopes one of them. WS8C implements only the shell installer.

## Procedure

- `bash -n tools/install-cdf.sh`
- `bash -n tools/test-install-cdf.sh`
- `tools/test-install-cdf.sh`
- `command -v shellcheck`
- `git diff --check`
- `rg -n "[Kk]iller[ _-]?[Dd]emo" . --hidden -g '!target/**' -g '!**/target/**' -g '!.git/**'`
- `tools/install-cdf.sh --dry-run --prefix /tmp/cdf-dry-run-probe`
- `gitleaks detect --no-git --source CHANGELOG.md --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-ws8c-changelog-final.json`
- `gitleaks detect --no-git --source tools/install-cdf.sh --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-ws8c-install-cdf-final.json`
- `gitleaks detect --no-git --source tools/test-install-cdf.sh --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-ws8c-test-install-cdf-final.json`
- `gitleaks detect --no-git --source .10x/tickets/done/2026-07-08-p1-product-ws8c-changelog-installer-channel.md --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-ws8c-final-done-ticket.json`
- `gitleaks detect --no-git --source .10x/evidence/2026-07-08-p1-product-ws8c-changelog-installer-channel.md --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-ws8c-final-evidence.json`
- `gitleaks detect --no-git --source .10x/reviews/2026-07-08-p1-product-ws8c-changelog-installer-channel-review.md --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-ws8c-final-review.json`
- `npx --yes jscpd@5 CHANGELOG.md tools/install-cdf.sh tools/test-install-cdf.sh .10x/tickets/done/2026-07-08-p1-product-ws8c-changelog-installer-channel.md .10x/evidence/2026-07-08-p1-product-ws8c-changelog-installer-channel.md .10x/reviews/2026-07-08-p1-product-ws8c-changelog-installer-channel-review.md --reporters console --output target/quality/reports/jscpd-ws8c-final-records --ignore "**/target/**,**/.git/**,**/reports/**" --min-lines 12 --min-tokens 80 --threshold 10 --no-colors`
- Parent review verified `git remote -v` points at `https://github.com/z3z1ma/cdf.git`, justifying the default release URL.
- Parent review replaced the installer artifact binary lookup with `find ... -print -quit` to avoid `head`/SIGPIPE interaction under `set -o pipefail`.
- Parent reran `bash -n tools/install-cdf.sh tools/test-install-cdf.sh && tools/test-install-cdf.sh`.
- Parent reran `tools/install-cdf.sh --dry-run --prefix /tmp/cdf-parent-dry-run-probe`.
- Parent reran `git diff --check` over the WS8C touched files.
- Parent reran a forbidden-phrase scan over `.10x`, docs, crates, root docs, changelog, and tools.
- Parent reran Gitleaks over copied touched source and WS8C records; report path `target/quality/reports/gitleaks-p1-ws8c-parent.json`.
- Parent reran `jscpd --format bash,markdown` over touched source and WS8C records; report path `target/quality/reports/jscpd-p1-ws8c-parent/jscpd-report.json`.

## Results

- Shell syntax checks passed for both scripts.
- Installer smoke tests passed:

```text
ok success install verifies checksum and prints version
ok dry-run leaves prefix untouched
ok default release URL follows requested version
ok checksum mismatch fails before install
ok missing checksum fails before install
ok failed artifact download fails before install
ok unsupported target fails before install
installer smoke tests passed
```

- `shellcheck` was not installed locally, so ShellCheck did not run.
- `git diff --check` passed.
- The forbidden-phrase scan exited 1 with no matches.
- Native dry-run on this machine exited 0 and resolved `aarch64-apple-darwin` to the default GitHub release artifact and checksum URLs without writing.
- Scoped Gitleaks scans over the changelog, installer, test script, done ticket, evidence record, and review record each passed with no leaks.
- jscpd over the changelog, installer, test script, done ticket, evidence record, and review record exited 0. jscpd analyzed the two Bash files, found 0 clones, and reported 0.00% duplication. Markdown files were not analyzed by the selected jscpd detectors.
- Parent smoke tests passed after the `find -print -quit` robustness fix.
- Parent native dry-run passed and resolved the default artifact URL to `https://github.com/z3z1ma/cdf/releases/download/v0.1.0/cdf-0.1.0-aarch64-apple-darwin.tar.gz` on the local machine without writing.
- Parent `git diff --check` passed.
- Parent forbidden-phrase scan found no matches.
- Parent Gitleaks scan over copied touched source and records passed with no leaks.
- Parent jscpd scan analyzed the two Bash files, found 0 clones, and reported 0.00% duplicated lines. Markdown records were passed to the command but not analyzed by the selected jscpd detectors.

## What this supports or challenges

This supports closing WS8C: the changelog follows the active convention, the first install channel is a checksum-verifying shell installer with dry-run and user-selected prefix support, and local smoke tests prove the required fail-closed paths before the target binary is written.

## Limits

The smoke tests use local tarball fixtures rather than real GitHub release artifacts because WS8B owns the release workflow and artifact publication. Remote download behavior is covered only by the installer code path and command-shape review, not by a live network release. ShellCheck was unavailable locally. No Rust or Cargo checks were run because WS8C did not change Rust source, manifests, or lockfiles; unrelated dirty Rust and Cargo files are owned by other active workers.
