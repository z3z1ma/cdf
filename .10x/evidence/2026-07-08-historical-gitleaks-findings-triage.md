Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-historical-gitleaks-findings-triage.md, .10x/knowledge/historical-gitleaks-findings.md

# Historical Gitleaks findings triage evidence

## What was observed

Full-history Gitleaks reports exactly two known findings:

- `generic-api-key` at `src/cdf/core/project.py`, commit `7fd1eddf7e6ab65afb4b9c63556d49e95ff5d50e`, line 158, fingerprint `7fd1eddf7e6ab65afb4b9c63556d49e95ff5d50e:src/cdf/core/project.py:generic-api-key:158`.
- `generic-api-key` at `src/cdf/core/feature_flag/harness.py`, commit `3e93ea80bfad6cd7a905b4a15ea9c3f6adb01dd8`, line 47, fingerprint `3e93ea80bfad6cd7a905b4a15ea9c3f6adb01dd8:src/cdf/core/feature_flag/harness.py:generic-api-key:47`.

Both inspected historical contexts are Harness SDK-key field/schema declarations, not committed credential values or private-key material. Both historical paths are absent from the current tree.

Current tracked source scanned clean with Gitleaks.

## Procedure

History scan with redacted output:

```text
gitleaks git --redact --report-format json --report-path reports/ai-quality/gitleaks-historical-triage-git.json --no-banner --log-level error .
jq '[.[] | {RuleID, Description, File, Commit, StartLine, EndLine, Fingerprint, Secret, Match}]' reports/ai-quality/gitleaks-historical-triage-git.json
```

Historical context inspection used `git show <commit>:<path>` around the reported lines, with string literals and field values redacted before display. The inspected redacted shapes were field declarations for a Harness SDK-key setting:

```text
<harness-sdk-key-field>: pydantic.UUID4 = <field-declaration>
```

and:

```text
<harness-sdk-key-field>: pydantic.UUID4 = pydantic.Field(...)
```

Current-tree absence check:

```text
git ls-tree -r --name-only HEAD | rg '^src/cdf/core/(project.py|feature_flag/harness.py)$' || true
```

Current tracked-source scan:

```text
tmpdir=$(mktemp -d /tmp/cdf-gitleaks-current.XXXXXX)
git ls-files -z | rsync -a --files-from=- --from0 ./ "$tmpdir"/
gitleaks dir --redact --report-format json --report-path reports/ai-quality/gitleaks-historical-triage-current-source.json --no-banner --log-level error "$tmpdir"
rm -rf "$tmpdir"
jq 'length' reports/ai-quality/gitleaks-historical-triage-current-source.json
```

Final closure hygiene:

```text
git diff --check
rg -n 'tickets/2026-07-08-historical-gitleaks-findings-triage|still open under|remain open under' .10x QUALITY.md --glob '!.10x/evidence/2026-07-08-historical-gitleaks-findings-triage.md'
gitleaks dir --redact --report-format json --report-path reports/ai-quality/gitleaks-historical-triage-10x.json --no-banner --log-level error .10x
gitleaks dir --redact --report-format json --report-path reports/ai-quality/gitleaks-historical-triage-quality-md.json --no-banner --log-level error QUALITY.md
gitleaks dir --redact --report-format json --report-path reports/ai-quality/gitleaks-historical-triage-current-source-final.json --no-banner --log-level error <tracked-source-snapshot>
jscpd . --reporters json,console --output reports/ai-quality/jscpd --ignore "**/target/**,**/.git/**,**/reports/**"
```

An independent read-only explorer inspected the same two commits/paths and reported the same classification: both are false positives on Harness SDK-key schema/config declarations, and both paths are absent from HEAD.

## Results

- `gitleaks git`: exit 1 with exactly the two redacted findings above.
- `git ls-tree` current-tree absence check: no output for either historical path.
- Current tracked-source `gitleaks dir`: exit 0; report length `0`.
- Final `.10x` Gitleaks scan: exit 0; report length `0`.
- Final `QUALITY.md` Gitleaks scan: exit 0; report length `0`.
- Final tracked-source snapshot Gitleaks scan: exit 0; report length `0`.
- `git diff --check`: pass.
- Stale active-ticket reference scan, excluding this evidence record's quoted command: no matches.
- Forbidden demo wording scan: no matches.
- `jscpd`: exit 0; total duplicated lines 6309 / 108071 = 5.837828835%, Rust duplicated lines 5078 / 83401 = 6.088656011%, `newClones: 0`, `newDuplicatedLines: 0`.
- No evidence found that either historical value was a real credential, token, or private key.

## Decision supported

Treat the two exact historical findings as documented false-positive scanner noise. Preserve repository history. Do not rotate credentials and do not rewrite history based on these findings.

Future quality runs should keep source-only and staged-diff Gitleaks scans as hard gates. If full-history Gitleaks is run, the two exact fingerprints may be interpreted through `.10x/knowledge/historical-gitleaks-findings.md`; any additional finding remains a hard failure until triaged.

## Limits

This evidence covers only the two named historical findings. It does not prove every historical commit is secret-free.

No external Harness service was contacted. The conclusion is based on source inspection, redacted Gitleaks metadata, current-tree absence, and current-source scanning.
