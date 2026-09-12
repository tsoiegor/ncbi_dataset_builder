"""Apply an audited Polars transformation to a run catalog."""

from pathlib import Path

import polars as pl

from ncbi_dataset_builder import BuilderConfig, DatasetBuilder

builder = DatasetBuilder(BuilderConfig(workspace=Path("/data/ncbi-workspace")))
catalog = builder.load_runs(Path("runinfo.csv"))
selected = catalog.transform(
    lambda frame: frame.filter(pl.col("LibraryLayout") == "PAIRED").with_columns(
        (pl.col("size_MB") / 1_000).alias("estimated_size_gb")
    ),
    description="paired runs with size in GB",
)
print(selected.audit)

