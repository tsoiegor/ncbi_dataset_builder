# Run catalogs

The `ncbi_dataset_builder.catalog` subpackage wraps an SRA RunInfo table in
`RunCatalog`. It keeps one row per run, preserves an immutable audit trail,
rejects contradictory duplicates, and converts rows into the
`ProcessingUnit` objects consumed by every execution system.

The package-level [README](../README.md) explains shared models. This page
covers `catalog/core.py` and the exports in `catalog/__init__.py`.

## Module map

| Module | Contents |
| --- | --- |
| `core.py` | `RunCatalog`, `GroupLevel`, and `validate_polars_runtime` |
| `__init__.py` | Public exports: `RunCatalog` and `validate_polars_runtime` |

## Required data

Every catalog must have a `Run` column. Other methods use standard RunInfo
columns when available:

| Purpose | Columns |
| --- | --- |
| Grouping | `Run`, `Experiment`, `SRA Sample`, `BioSample` |
| Species validation | `TaxID` or `species_taxid` or `taxid`; `ScientificName` or `scientific_name` |
| Size estimates | `bases`, `size_MB` |
| Unit metadata | `LibraryStrategy`, `LibraryLayout`, `Platform` |
| Normalized views | `SRA Study` and/or `BioProject` |

A missing grouping column raises when that grouping is requested. Missing
optional size or metadata values become zero or an empty tuple.

## `RunCatalog`

```python
RunCatalog(
    frame,
    *,
    audit=(),
)
```

| Argument | Meaning |
| --- | --- |
| `frame: polars.DataFrame` | Run-oriented table. It must retain `Run`. The object stores the frame rather than copying it. |
| `audit: Iterable[str]` | Prior human-readable operation messages, frozen as a tuple. |

Catalog operations return new `RunCatalog` objects. They do not mutate the
source catalog.

`GROUP_COLUMNS` is the class-level mapping from the four accepted `by` values
to RunInfo columns: `run -> Run`, `experiment -> Experiment`,
`sra_sample -> SRA Sample`, and `biosample -> BioSample`. Use
`processing_units(by=...)` rather than changing this mapping.

### Construction

#### `from_csv(path) -> RunCatalog`

Load a RunInfo CSV, infer the full schema, parse dates when possible, and treat
`""`, `NA`, `N/A`, `null`, and `None` as null values.
`path` accepts `str` or `Path`.

#### `from_records(records) -> RunCatalog`

Build a Polars frame from an iterable of row dictionaries. At least the
`Run` field must be present after frame construction.

Both constructors call `validate_polars_runtime()` first.

### Properties

| Property | Result |
| --- | --- |
| `frame` | Underlying `polars.DataFrame`. Use Polars for inspection; use catalog methods to preserve audit entries. |
| `audit` | Ordered immutable tuple of operation messages. Execution records retain this tuple. |

### `filter(predicate, *, description=None) -> RunCatalog`

`predicate` may be:

- a Polars expression, evaluated efficiently by Polars; or
- a Python callable receiving one named row dictionary and returning a Boolean.

`description` becomes the stable audit label. Without it, the expression
representation or callable name is used. `where` is an alias.

```python
import polars as pl

# Keep paired ATAC-seq runs below 100 GB.
selected = catalog.filter(
    (pl.col("LibraryLayout") == "PAIRED")
    & (pl.col("size_MB") < 100_000),
    description="paired runs below 100 GB",
)

print(selected.audit[-1])
```

The returned audit entry records the row count before and after filtering.

### `transform(function, *, description=None) -> RunCatalog`

Call `function(frame)` for an arbitrary table-level operation.

| Argument | Meaning |
| --- | --- |
| `function` | Callable that must return a Polars `DataFrame` containing `Run`. |
| `description` | Optional audit label; defaults to the callable’s qualified name. |

```python
def add_size_gb(frame: pl.DataFrame) -> pl.DataFrame:
    # Decimal GB matches the scheduler’s size model.
    return frame.with_columns(
        (pl.col("size_MB") / 1_000).alias("estimated_size_gb")
    )

catalog_with_size = catalog.transform(
    add_size_gb,
    description="add decimal-GB estimate",
)
```

Use this method for joins, multi-column normalization, or other operations that
do not fit a direct catalog helper. Returning another type raises `TypeError`;
dropping `Run` raises `ValueError`.

### Column operations

| Method | Arguments and result |
| --- | --- |
| `select(*columns)` | Retain requested names and always add `Run` if omitted. Missing names follow Polars error behavior. |
| `with_columns(*expressions)` | Apply Polars expressions and append an `added/updated columns` audit event. |
| `replace_frame(frame, *, event)` | Explicitly replace the frame and append the supplied audit message. The new frame must still contain `Run`. |

### `deduplicate_runs() -> RunCatalog`

Group by `Run` while preserving first-seen run order:

- exact or null-compatible duplicates are coalesced;
- a non-null value is retained when another duplicate row is null; and
- contradictory non-null values raise `CatalogConflictError` instead of
  silently discarding data.

The error reports up to ten conflicting run accessions and column names.
`DatasetBuilder.fetch_runs()` and `load_runs()` invoke this method
automatically.

### `normalized_entities() -> dict[str, DataFrame]`

Return the original run view plus deduplicated entity views whose accession
columns exist:

| Key | Deduplication column |
| --- | --- |
| `runs` | None; original frame |
| `experiments` | `Experiment` |
| `sra_samples` | `SRA Sample`, falling back to `Sample` |
| `biosamples` | `BioSample` |
| `studies` | `SRA Study`, falling back to `BioProject` |

These are table views, not `RunCatalog` objects.

### `processing_units(*, by="experiment") -> list[ProcessingUnit]`

`by` accepts `"run"`, `"experiment"`, `"sra_sample"`, or
`"biosample"`. A blank group value falls back to that row’s run accession.

For each group the method:

1. preserves run order;
2. rejects units spanning multiple taxonomy IDs or scientific names;
3. sums `bases` and converts `size_MB / 1000` to decimal GB;
4. retains linked experiment, SRA Sample, and BioSample accessions; and
5. adds library strategy, layout, platform, and per-run size metadata.

```python
units = selected.processing_units(by="experiment")

for unit in units:
    print(
        unit.unit_id,
        unit.run_accessions,
        unit.taxid,
        unit.total_size_gb,
    )
```

`DatasetBuilder.build()` and `submit_slurm()` call this method using the
configured or per-call grouping level.

## `validate_polars_runtime() -> str`

Check that the imported Polars package and its internal re-exports agree, then
return the version string. The targeted diagnostic catches a common notebook
failure after upgrading Polars in a still-running kernel or mixing
`polars` and `polars-lts-cpu`.

On inconsistency it raises `DependencyError` with restart and reinstall
guidance.

## Complete example

```python
from pathlib import Path

import polars as pl

from ncbi_dataset_builder import BuilderConfig, DatasetBuilder

builder = DatasetBuilder(
    BuilderConfig(
        workspace=Path("/data/ncbi-workspace"),
        email="researcher@example.org",
    )
)

# Offline source. A live builder.fetch_runs(...) produces the same API.
catalog = builder.load_runs(Path("runinfo.csv"))

# Every operation returns a new catalog and records what changed.
selected = (
    catalog
    .filter(
        pl.col("LibraryStrategy") == "ATAC-seq",
        description="ATAC-seq only",
    )
    .with_columns(
        pl.col("ScientificName").str.strip_chars(),
    )
    .deduplicate_runs()
)

print(selected.frame.shape)
print("\n".join(selected.audit))
```

Continue with the [execution API](../execution/README.md) after catalog
selection, or the user-facing [catalog guide](../../../docs/Catalogs.md) for
source-query examples.
