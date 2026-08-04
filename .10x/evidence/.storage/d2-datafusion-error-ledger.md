# D2 DataFusion error-boundary ledger

Frozen scope: the two paths in `d2-datafusion-error-files.txt`.

Reproduce the site inventory from the repository root:

```sh
xargs rg -n 'map_err\((datafusion_planning_error|binding_error|execution_error)\)|classify_datafusion_error\(' \
  < .10x/evidence/.storage/d2-datafusion-error-files.txt
```

Classification:

- DataFusion analysis and coercion sites in `expression.rs`: typed `Plan`, `Configuration`,
  `SchemaError`, and `NotImplemented` roots are caller-owned `Contract`; host I/O and resource
  exhaustion are `Environment`; dependency invariants remain `Internal`.
- Physical-expression creation and type/nullability interrogation in `expression_execution.rs`:
  the same typed binding split applies, with `cdf compile` remediation added only to `Contract`.
- Physical-expression evaluation and scalar broadcasting in `expression_execution.rs`: typed
  `Execution` and `ArrowError` roots caused by admitted input values are `Data`; host I/O and
  resource exhaustion are `Environment`; dependency invariants remain `Internal`.
- Any typed `CdfError` embedded below a DataFusion wrapper keeps its original kind, retry delay,
  and primary message while the scalar phase adds context.

Inventory at closure: 14 `map_err` boundary sites plus three classifier/helper references across
two site-bearing files. The scalar-literal Arrow IPC encoder/decoder remains separately classified:
encoding impossible CDF-owned state is `Internal`; a malformed recorded literal is stale
`Contract` authority.
