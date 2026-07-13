# `cdf explain`

Generated from the CLI's clap definitions.

```text
Explain resolution, capabilities, and execution choices

Usage: cdf explain [OPTIONS] [RESOURCE]...

Arguments:
  [RESOURCE]...  Resource identifier

Options:
  -q, --quiet                        Suppress progress and non-primary success narration
      --resource <RESOURCE>          Resource identifier (compatibility form)
      --select <FIELDS>              Comma-separated projected fields
  -v, --verbose...                   Show evidence detail; repeat for diagnostics
      --color <WHEN>                 Color policy: auto, always, or never [possible values: auto, always, never]
      --filter <EXPR>                Filter expression; may be repeated
      --limit <N>                    Maximum rows to read
      --no-color                     Compatibility alias for --color never
      --order-by <FIELD[:asc|desc]>  Ordering field and optional direction
      --progress <WHEN>              Progress policy: auto, always, or never [possible values: auto, always, never]
      --package-id <ID>              Explicit package identifier for script compatibility
      --unicode <WHEN>               Unicode policy: auto, always, or never [possible values: auto, always, never]
      --to <DEST>                    Destination URI or cursor upper bound, as shown in usage
      --target <TARGET>              Destination target/table compatibility option
      --no-pin                       Do not pin newly discovered schema
  -h, --help                         Print help
```
