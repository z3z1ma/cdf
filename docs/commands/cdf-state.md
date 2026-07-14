# `cdf state`

Generated from the CLI's clap definitions.

```text
Inspect and recover checkpoint state

Usage: cdf state [OPTIONS] [COMMAND]

Commands:
  show     Show the selected durable record
  history  Show checkpoint history
  rewind   Create a marker that rewinds checkpoint state
  recover  Recover state from a committed package receipt

Options:
  -q, --quiet            Suppress progress and non-primary success narration
  -v, --verbose...       Show evidence detail; repeat for diagnostics
      --color <WHEN>     Color policy: auto, always, or never [possible values: auto, always, never]
      --progress <WHEN>  Progress policy: auto, always, or never [possible values: auto, always, never]
      --unicode <WHEN>   Unicode policy: auto, always, or never [possible values: auto, always, never]
  -h, --help             Print help
```
