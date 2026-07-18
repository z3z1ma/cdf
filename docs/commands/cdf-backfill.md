# `cdf backfill`

Generated from the CLI's clap definitions.

```text
Plan or execute a bounded cursor backfill

Usage: cdf backfill [OPTIONS] [RESOURCE]...

Arguments:
  [RESOURCE]...  Resource identifier

Options:
      --from <CURSOR>          Inclusive cursor lower bound
  -q, --quiet                  Suppress progress and non-primary success narration
      --to <CURSOR>            Destination URI or cursor upper bound, as shown in usage
  -v, --verbose...             Show evidence detail; repeat for diagnostics
      --color <WHEN>           Color policy: auto, always, or never [possible values: auto, always, never]
      --target <TARGET>        Destination target or table
      --execute                Apply the planned operation
      --progress <WHEN>        Progress policy: auto, always, or never [possible values: auto, always, never]
      --slice-size <N>         Rows per backfill slice
      --unicode <WHEN>         Unicode policy: auto, always, or never [possible values: auto, always, never]
      --memory-budget <BYTES>  Process memory budget, e.g. 4GiB or 512MiB
      --spill-budget <BYTES>   Spill/disk budget, e.g. 64GiB or 512MiB
  -h, --help                   Print help
```
