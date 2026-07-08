Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-product-ws8b-release-artifact-workflow.md
Verdict: pass

# WS8B release artifact workflow review

## Target

Review `.github/workflows/release-artifacts.yml`, `LICENSE`, `tools/verify-release-metadata.sh`, `tools/package-release-artifact.sh`, `tools/verify-release-artifacts.sh`, `tools/test-release-artifacts.sh`, and WS8B evidence.

## Findings

No blocking findings.

Resolved blocking finding:

- Reproducibility: the first WS8B packager used ordinary tar/gzip archive creation, which made archive bytes susceptible to filesystem metadata and gzip timestamp drift. The repair switches archive writing to `tools/write-reproducible-targz.py`, normalizing path order, uid/gid, owner/group names, modes, tar mtimes, and gzip mtime. `tools/test-release-artifacts.sh` now proves two packages from identical staged inputs are byte-identical and have identical SHA-256 values.

## Assumptions tested

- Versioned prerelease workflow: the workflow accepts manual versions and tag-triggered versions, validates metadata first, builds artifacts, verifies checksums, and can upload a GitHub prerelease. It does not publish crates.
- Mainstream target coverage: all five targets from the active release policy are present in the build matrix. The ARM Linux target uses an ARM hosted runner rather than a silent skip.
- Fail-closed metadata: local smoke proves mismatched release version fails before packaging, and the metadata validator enforces license, changelog, workspace version, and DataFusion-git publication constraints.
- Fail-closed checksums: local smoke proves checksum mismatch is rejected by the artifact verifier, and the workflow verifies per-target checksums plus aggregate `SHA256SUMS`.
- Reproducibility: local smoke proves same staged inputs produce byte-identical `.tar.gz` archives and identical adjacent checksum values.
- Artifact contents: local package and archive inspection prove the binary, `LICENSE`, changelog excerpt, release metadata, and generated artifact inventory are inside the archive.
- Generated completions/man pages: the packager includes them when supplied and records the WS2D dependency when absent, so WS8B does not invent a generator.
- Installer compatibility: a locally packaged `aarch64-apple-darwin` artifact was installed by `tools/install-cdf.sh`, which verified the checksum before writing and produced a working `cdf 0.1.0` binary.
- Scope control: no `crates/cdf-cli/src/**`, parser/rendering/runtime/Python implementation, CLI generated-artifact generator, brew tap, signing key infrastructure, crates.io publication, or unrelated WASM records were changed by WS8B.
- Quality gates: actionlint, YAML parse, ShellCheck, Python compile, smoke tests, Gitleaks, jscpd, Semgrep secrets, scc metrics, cargo metadata, cargo-deny, cargo-audit with the ratified paste ignore, cargo-vet, and OSV residual classification were run.

- Parent review reran the release-script syntax checks, Python compile, YAML parse, actionlint, ShellCheck, metadata validation, reproducible artifact smoke, publication guard, scoped Gitleaks, jscpd, Semgrep secrets, scc metrics, cargo metadata, cargo-deny, cargo-audit, cargo-vet, OSV residual classification, and a host package/verify/install smoke using the existing release binary.

## Residual risk

Actual GitHub-hosted build and prerelease upload are not yet remote evidence. The release workflow uses current hosted runner labels accepted by `actionlint`, but final proof requires a workflow run.

The local worktree later contained unrelated dirty CLI edits with a syntax error in `crates/cdf-cli/src/sql_command.rs`, causing `cargo fmt --all -- --check` to fail after WS8B validation. This is not a WS8B implementation finding because WS8B did not touch Rust source and the local release build/package smoke completed before that unrelated parse error appeared.

OSV still reports `RUSTSEC-2024-0436`; the residual was classified as exactly the already-ratified paste advisory, matching the existing cargo-audit and cargo-deny posture.

## Verdict

Pass. WS8B satisfies the child ticket's acceptance criteria with local evidence and clear CI-only residual risk, including reproducible archive evidence. Parent closure still needs actual release workflow run evidence and WS2D-generated completions/man pages before claiming the full WS8/P1 release surface is complete.
