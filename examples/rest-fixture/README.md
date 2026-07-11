# REST fixture example

This project loads a local JSON response into DuckDB without external network access or credentials.

From this directory, start the fixture server in one terminal:

```bash
python3 -m http.server 8765 --bind 127.0.0.1 --directory fixtures
```

Then run CDF from a second terminal (set `CDF` to the built binary when it is not on `PATH`):

```bash
cdf validate --deep
cdf plan api.events
cdf run api.events
cdf sql 'select * from package_files'
```

The example is intentionally public and needs no auth token. A real authenticated REST source must put the token behind a `secret://` reference; never place the token in resource TOML.
