Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-ci-gitleaks-install-module-path.md, .github/workflows/fast-quality.yml, .github/workflows/slow-quality.yml

# CI Gitleaks install module path evidence

## What was observed

GitHub Actions failed while installing Gitleaks:

- Required path: `github.com/gitleaks/gitleaks/v8@v8.18.4`.
- Release module declares path: `github.com/zricethezav/gitleaks/v8`.

The fix updates both quality workflows to use the declared module path while keeping the pinned `v8.18.4` version.

## Procedure

- `gh auth status`: authenticated with `repo` and `workflow` scopes.
- `rg -n "gitleaks|zricethezav|github.com/gitleaks" .github tools QUALITY.md .10x`: found the failing install path in `.github/workflows/fast-quality.yml` and `.github/workflows/slow-quality.yml`.
- `tmpbin="$(mktemp -d)"; GOBIN="$tmpbin" go install github.com/zricethezav/gitleaks/v8@v8.18.4 && "$tmpbin/gitleaks" version`: pass; the binary installed and executed.
- `rg -n "github.com/gitleaks/gitleaks/v8" .github`: no matches after the patch.
- `rg -n "go install github.com/.*/gitleaks/v8@v8\\.18\\.4" .github`: found only the corrected `github.com/zricethezav/gitleaks/v8@v8.18.4` installs after the patch.
- `ruby -e 'require "yaml"; ARGV.each { |path| YAML.load_file(path); puts "ok #{path}" }' .github/workflows/fast-quality.yml .github/workflows/slow-quality.yml`: pass; both edited workflow files parse.
- Temp-copy Gitleaks scan over the edited workflows and related 10x records using the pinned binary's `gitleaks detect --no-git --no-banner --redact --source "$tmp"` form: pass; no leaks detected.

## What this supports or challenges

This supports the CI fix: the module path now matches the pinned release's declared Go module, so the install step should no longer fail for that reason.

## Limits

This evidence does not prove the full GitHub Actions workflows are green. It verifies the failing install command locally and preserves the existing pinned version and gate semantics.
