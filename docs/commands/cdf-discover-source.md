# `cdf discover source`

Generated from the CLI's clap definitions.

```text
Discover relations exposed by one configured source

Usage: cdf discover source [OPTIONS] [CONFIGURED_SOURCE] [RELATION_SELECTOR]...

Arguments:
  [CONFIGURED_SOURCE] [RELATION_SELECTOR]...  Identifiers or paths shown in usage

Options:
      --out <PATH>             Write the command's canonical artifact without replacing terminal output
  -q, --quiet                  Suppress progress and non-primary success narration
      --generate               Create or verify resource files for matched source relations
  -v, --verbose...             Show evidence detail; repeat for diagnostics
      --color <WHEN>           Color policy: auto, always, or never [possible values: auto, always, never]
      --namespace <NAMESPACE>  Generated resource namespace; defaults to the configured source
      --progress <WHEN>        Progress policy: auto, always, or never [possible values: auto, always, never]
      --unicode <WHEN>         Unicode policy: auto, always, or never [possible values: auto, always, never]
      --memory-budget <BYTES>  Process memory budget, e.g. 4GiB or 512MiB
      --spill-budget <BYTES>   Spill/disk budget, e.g. 64GiB or 512MiB
  -h, --help                   Print help
```
