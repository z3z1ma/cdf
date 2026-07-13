# `cdf resume`

Generated from the CLI's clap definitions.

```text
Resume interrupted work from the run ledger

Usage: cdf resume [OPTIONS] [RUN_ID]...

Arguments:
  [RUN_ID]...  Run identifier; omit to scan interrupted work

Options:
  -q, --quiet            Suppress progress and non-primary success narration
      --run <RUN_ID>     Run identifier compatibility option
  -v, --verbose...       Show evidence detail; repeat for diagnostics
      --color <WHEN>     Color policy: auto, always, or never [possible values: auto, always, never]
      --no-color         Compatibility alias for --color never
      --progress <WHEN>  Progress policy: auto, always, or never [possible values: auto, always, never]
      --unicode <WHEN>   Unicode policy: auto, always, or never [possible values: auto, always, never]
  -h, --help             Print help
```
