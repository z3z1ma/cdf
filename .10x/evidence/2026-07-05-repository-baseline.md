Status: recorded
Created: 2026-07-05
Updated: 2026-07-05
Relates-To: .10x/decisions/firn-system-authority.md, .10x/research/2026-07-05-book-ingestion.md

# Repository baseline before firn implementation

## What was observed

On 2026-07-05, before creating `.10x/` records, `/Users/alexanderbut/code_projects/personal/firn` contained only `.git` and `firn-the-book-of-the-system.md` at the repository root. `git status --short` reported the book as untracked.

## Procedure

Commands run from the repository root:

```text
ls -la
wc -l firn-the-book-of-the-system.md
git status --short
find .10x -maxdepth 3 -type f
```

## What this supports or challenges

This supports treating the book as the only initial project authority and confirms there were no pre-existing active `.10x/` records to reconcile.

## Limits

This evidence records the initial repository state only. It does not validate implementation correctness.

