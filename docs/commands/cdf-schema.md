# `cdf schema`

Generated from the CLI's clap definitions.

```text
Discover, pin, compare, and promote schemas

Usage: cdf schema [OPTIONS] [COMMAND]

Commands:
  discover  Discover the current physical source schema
  pin       Pin a discovered schema into the project contract
  show      Show the selected durable record
  diff      Compare durable schemas
  promote   Plan or execute residual schema promotion

Options:
  -q, --quiet                  Suppress progress and non-primary success narration
  -v, --verbose...             Show evidence detail; repeat for diagnostics
      --color <WHEN>           Color policy: auto, always, or never [possible values: auto, always, never]
      --progress <WHEN>        Progress policy: auto, always, or never [possible values: auto, always, never]
      --unicode <WHEN>         Unicode policy: auto, always, or never [possible values: auto, always, never]
      --memory-budget <BYTES>  Process memory budget, e.g. 4GiB or 512MiB
      --spill-budget <BYTES>   Spill/disk budget, e.g. 64GiB or 512MiB
  -h, --help                   Print help
```
