Status: active
Created: 2026-07-09
Updated: 2026-07-09

# Incremental commit discipline

The user expects verified work to be committed as it progresses rather than accumulated into one large uncommitted program diff.

- Commit each bounded executable child when its acceptance evidence and required review are complete.
- Tightly coupled children MAY share one integration commit when they were verified and reviewed as one coherent tranche.
- Do not mix unrelated active work into a closure commit.
- Do not commit a ticket as complete before its records, evidence, review, and parent references agree.
- If an active blocked/shaping record must accompany a completed tranche because it explains the next architectural boundary, include it explicitly and name it in the commit summary.
- After each commit, verify the remaining worktree so the next ticket starts from a legible boundary.
