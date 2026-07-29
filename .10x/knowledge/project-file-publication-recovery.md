Status: active
Created: 2026-07-28
Updated: 2026-07-28

# Project file publication recovery

Multi-file project publication uses `.cdf/project-files.transaction.json` as a private,
owner-readable generation authority. The marker contains hashes and relative paths, never project
or secret contents.

The durable boundary is one-way:

1. prepare and sync every new file and newly created parent;
2. publish and sync a `pending` marker;
3. install targets in order, with `cdf.lock` last;
4. sync installed parents;
5. replace the marker with `committed` at the same monotonically increasing generation.

Once `pending` is durable, forward recovery is the only terminal decision. Destructive rollback is
unsafe unless a separate durable abort protocol retains everything needed to finish that rollback
after another crash. An in-memory rollback can consume the prepared new bytes, strand a pending
journal, or overwrite a non-cooperating editor.

Recovery runs under the project mutation guard and accepts each target only when it equals the
journaled prior or new hash. Prior requires the verified managed temporary; new is idempotently
accepted and its matching temporary is cleaned. Any third value is unrelated authority and must be
preserved with a `Contract` failure.

Read-only commands do not recover. They observe a committed generation before and after loading
and fail closed on `pending`; otherwise plan, preview, inspect, and dry-run operations would mutate
project authority. A real non-dry-run `cdf add` is the explicit recovery entry point and then
performs the ordinary stable-generation load.

Error kinds follow ownership: malformed/missing private marker or temporary state is `Internal`;
host permissions, space, descriptors, and device failures are `Environment`; unrelated public
target authority is `Contract`.

Use `.10x/skills/audit-project-file-publication/SKILL.md` whenever a command publishes more than one
project authority file or changes project loading/recovery.
