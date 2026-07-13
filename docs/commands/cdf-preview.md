# `cdf preview`

Generated from the CLI's clap definitions.

```text
Read a bounded preview without committing data

Usage: cdf preview [OPTIONS] [RESOURCE]...

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
      --unicode <WHEN>               Unicode policy: auto, always, or never [possible values: auto, always, never]
  -h, --help                         Print help
```
