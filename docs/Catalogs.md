# Catalogs

`RunCatalog` is the boundary between discovery and processing. Every catalog
has one row per run and must retain the `Run` column.

## Load a file

```python
from pathlib import Path
from ncbi_dataset_builder import BuilderConfig, DatasetBuilder

builder = DatasetBuilder(BuilderConfig(workspace=Path("/data/workspace")))
catalog = builder.load_runs(Path("runinfo.csv"))
```

No NCBI credentials are needed to load an existing CSV.

## Fetch directly from NCBI

```python
builder = DatasetBuilder(
    BuilderConfig(
        workspace=Path("/data/workspace"),
        email="researcher@example.org",
        ncbi_api_key="0123456789abcdef0123456789abcdef01234567",  # fake example
    )
)
catalog = builder.fetch_runs(
    '"ATAC-seq"[Strategy] AND "Mus musculus"[Organism]'
)
```

`refresh=True` bypasses the cached query CSV.

## Transform with Polars

```python
import polars as pl

def normalize_catalog(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        pl.col("ScientificName").str.strip_chars(),
        (pl.col("size_MB") / 1_000).alias("estimated_size_gb"),
    ).filter(pl.col("LibraryLayout") == "PAIRED")

selected = catalog.transform(normalize_catalog, description="paired runs with normalized species")
```

The function receives the full `DataFrame` and must return a `DataFrame` with
`Run`. `filter`, `select`, `with_columns`, and `deduplicate_runs` return new
catalogs and append audit entries. `processing_units(by=...)` supports `run`,
`experiment`, `sra_sample`, or `biosample`; the builder default is experiment.

