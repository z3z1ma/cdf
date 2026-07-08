Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-ci-gitleaks-install-module-path.md
Verdict: pass

# CI Gitleaks install module path review

## Target

The CI workflow change in:

- `.github/workflows/fast-quality.yml`
- `.github/workflows/slow-quality.yml`

## Findings

No blocking findings.

The change is the smallest fix for the observed CI failure: it updates only the Go module path used by `go install` and keeps `v8.18.4` pinned. The Gitleaks scan commands, reports, exit behavior, and quality workflow structure are unchanged.

Local verification installed and ran the binary from the corrected path. Source search confirms the old `github.com/gitleaks/gitleaks/v8` install path no longer appears in workflow install steps. The edited workflow files parse as YAML, and a scoped Gitleaks scan over the edited files and records passed.

## Residual risk

The full CI workflows were not rerun from GitHub Actions in this local pass. Other unrelated failures may still exist after this install step proceeds.

## Verdict

Pass. The patch directly addresses the reported module-path failure without widening scope.
