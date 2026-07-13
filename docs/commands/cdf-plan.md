# `cdf plan`

Generated from the CLI's clap definitions.

```text
Plan a resource run without executing it

Usage: cdf plan [OPTIONS] [RESOURCE]...

Arguments:
  [RESOURCE]...  Resource identifier

Options:
  -q, --quiet                        Suppress progress and non-primary success narration
      --select <FIELDS>              Comma-separated projected fields
      --filter <EXPR>                Filter expression; may be repeated
  -v, --verbose...                   Show evidence detail; repeat for diagnostics
      --color <WHEN>                 Color policy: auto, always, or never [possible values: auto, always, never]
      --limit <N>                    Maximum rows to read
      --order-by <FIELD[:asc|desc]>  Ordering field and optional direction
      --progress <WHEN>              Progress policy: auto, always, or never [possible values: auto, always, never]
      --to <DEST>                    Destination URI or cursor upper bound, as shown in usage
      --unicode <WHEN>               Unicode policy: auto, always, or never [possible values: auto, always, never]
      --no-pin                       Do not pin newly discovered schema
  -h, --help                         Print help
```
