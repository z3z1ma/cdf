Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-product-ws8c-changelog-installer-channel.md
Verdict: pass

# WS8C changelog and installer channel review

## Target

Review `CHANGELOG.md`, `tools/install-cdf.sh`, `tools/test-install-cdf.sh`, and WS8C closure evidence against `.10x/specs/versioning-lts-release-policy.md`.

## Findings

No blocking findings.

## Assumptions tested

- Changelog convention: `CHANGELOG.md` contains `Unreleased` plus a dated `[0.1.0] - 2026-07-08` section and uses headings allowed by the active release policy.
- Install target support: the installer supports the Darwin/Linux shell targets it can reasonably install from a POSIX shell and rejects unsupported targets before download or installation.
- Checksum-before-mutation: checksum retrieval, parsing, artifact download, digest comparison, extraction, and version probing all occur before creating the install directory or writing `<prefix>/bin/cdf`.
- Dry-run behavior: dry-run prints the resolved artifact, checksum, prefix, target, and install path, then exits before download or write.
- User-selected prefix: `--prefix` and `CDF_INSTALL_PREFIX` choose the install prefix; the script defaults to `$HOME/.local` and never invokes `sudo`.
- Failure behavior: local fixture smoke proves checksum mismatch, missing checksum, failed artifact source, and unsupported target all fail without writing `<prefix>/bin/cdf`.
- Version output: the installer captures `cdf version` from the extracted binary before install and prints it in the final install message.
- Scope control: no release workflow, signing infrastructure, brew tap, crates.io publication, package-manager feed, CLI code, `crates/cdf-cli/**`, or `crates/cdf-project/**` files were changed by WS8C.
- Parent review: the default GitHub release URL matches the configured `origin` remote, and the installer now uses `find ... -print -quit` for binary discovery to avoid `head`/SIGPIPE behavior under `set -o pipefail`.

## Residual risk

Actual remote artifact availability and release workflow naming remain future WS8B evidence. The shell installer intentionally does not promise a Windows shell path even though WS8B may produce a Windows binary artifact. ShellCheck was not available locally; Bash syntax and smoke tests covered the scripts with the available tooling.

## Verdict

Pass. WS8C satisfies the changelog and first shell-installer acceptance criteria, with follow-up channels explicitly deferred rather than silently implemented.
