# `catalog`

The catalog subpackage wraps a Polars table containing one row per SRA run. It
validates accessions, preserves an immutable audit trail, and converts run rows
into sample-sized `ProcessingUnit` objects. `DatasetBuilder.fetch_runs()` and
`load_runs()` return this class; metadata enrichment and every execution path
accept it.

## `RunCatalog`

Create one with `RunCatalog.from_csv(path)` or
`RunCatalog.from_records(records)`. The source must contain a `Run` column.

Public members:

- `frame` returns the underlying `polars.DataFrame`; `audit` returns operation
  messages as a tuple.
- `filter(predicate, description=None)` (also `where`) accepts a Polars
  expression such as `pl.col("size_MB") < 1000`, or a row callable returning a
  Boolean. `description` gives the audit entry a stable label.
- `transform(function, description=None)` calls `function(frame)`. The
  function must return a Polars `DataFrame` that still has `Run`. Use this for
  joins, normalization, or multi-column selection that does not fit `filter`.
- `select(*columns)` retains requested columns plus `Run`.
- `with_columns(*expressions)` applies Polars expressions.
- `replace_frame(frame, event)` explicitly replaces the table and records the
  supplied audit `event`.
- `deduplicate_runs()` coalesces compatible duplicate rows and raises when the
  same run has contradictory values.
- `normalized_entities()` returns run, experiment, SRA Sample, BioSample, and
  study table views where available.
- `processing_units(by="experiment")` returns shared
  [`ProcessingUnit`](../README.md) objects. `by` can be `run`, `experiment`,
  `sra_sample`, or `biosample`.

`validate_polars_runtime()` is used by catalog entry points to detect a mixed
or partially upgraded Polars installation before work begins.

See [Catalogs](../../../docs/Catalogs.md) for complete workflows.
