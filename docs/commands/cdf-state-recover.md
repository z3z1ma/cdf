# `cdf state recover`

Generated from the CLI's clap definitions.

```text
Recover state from a committed package receipt

Usage: cdf state recover [OPTIONS]

Options:
      --package <DIR>         Package directory
  -q, --quiet                 Suppress progress and non-primary success narration
      --to <DEST>             Destination URI or cursor upper bound, as shown in usage
  -v, --verbose...            Show evidence detail; repeat for diagnostics
      --color <WHEN>          Color policy: auto, always, or never [possible values: auto, always, never]
      --receipt <ID>          Receipt identifier
      --no-color              Compatibility alias for --color never
      --target <TARGET>       Destination target/table compatibility option
      --merge-dedup <POLICY>  Merge deduplication policy
      --progress <WHEN>       Progress policy: auto, always, or never [possible values: auto, always, never]
      --unicode <WHEN>        Unicode policy: auto, always, or never [possible values: auto, always, never]
  -h, --help                  Print help
```
