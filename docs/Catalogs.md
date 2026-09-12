# Catalogs: selecting the data to process

`RunCatalog` is the boundary between discovery and execution. It wraps a
Polars `DataFrame`, requires one `Run` column, and keeps an immutable audit
trail of transformations.

Execution deduplicates the catalog and converts its rows into processing units.
Inspect this boundary carefully: grouping determines which runs are downloaded,
merged, processed, retried, and published together.

## Choose a catalog source

| Source | Method | Needs NCBI email? | Cache behavior |
| --- | --- | --- | --- |
| Existing RunInfo CSV | `builder.load_runs(path)` | No | Reads the supplied file |
| NCBI SRA query | `builder.fetch_runs(query, refresh=False)` | Yes | Caches CSV by query hash in `workspace/catalogs/` |
| GEO GSE/GSM accessions | `builder.fetch_geo_runs(accessions)` | Yes | Resolves linked SRA records through Entrez |
| Python records | `RunCatalog.from_records(records)` | No | In-memory construction |

## Required and useful columns

| Purpose | Column candidates | Required? |
| --- | --- | --- |
| Run identity | `Run` | Always required |
| Experiment grouping | `Experiment` | Required for `group_by="experiment"` |
| SRA Sample grouping | `SRA Sample` | Required for `group_by="sra_sample"` |
| BioSample grouping | `BioSample` | Required for `group_by="biosample"` |
| Taxonomy | `TaxID`, `species_taxid`, or `taxid` | Required before genome resolution |
| Species name | `ScientificName` or `scientific_name` | Strongly recommended |
| Raw-size estimate | `size_MB` | Recommended for storage admission |
| Base estimate | `bases` | Optional provenance/sizing |
| Unit metadata | `LibraryStrategy`, `LibraryLayout`, `Platform` | Optional but useful |
| Study views | `SRA Study`, `BioProject` | Optional |

Missing `size_MB` becomes zero in unit estimates. That can make storage
admission optimistic, so inspect size coverage before a large run.

## Load an existing CSV

```python
from pathlib import Path

from ncbi_dataset_builder import BuilderConfig, DatasetBuilder

builder = DatasetBuilder(
    BuilderConfig(
        workspace=Path("/data/ncbi-workspace"),
    )
)
catalog = builder.load_runs(Path("/data/catalogs/runinfo.csv"))
```

### Parameters

| Parameter | Meaning | Restriction |
| --- | --- | --- |
| `path: str \| Path` | Existing CSV read immediately | File must contain `Run` |

The loader:

- infers the full schema;
- attempts date parsing;
- treats empty string, `NA`, `N/A`, `null`, and `None` as null;
- validates the Polars runtime; and
- deduplicates runs through the high-level builder method.

## Fetch from NCBI

```python
builder = DatasetBuilder(
    BuilderConfig(
        workspace=Path("/data/ncbi-workspace"),
        email="researcher@example.org",
        ncbi_api_key=None,
    )
)

catalog = builder.fetch_runs(
    '"ATAC-seq"[Strategy] AND "Homo sapiens"[Organism]',
    refresh=False,
)
```

### Parameters

| Parameter | Default | Meaning | Restriction |
| --- | --- | --- | --- |
| `query: str` | Required | NCBI SRA Entrez expression | Email must be configured |
| `refresh: bool` | `False` | Bypass and replace matching cached catalog | Still uses normal NCBI pagination/rate controls |

The cache filename uses a short SHA-256 digest of the query, not the query text.
A cache hit is loaded and deduplicated. A refreshed result is written through a
temporary file and atomic replacement.

## Resolve GEO accessions

```python
catalog = builder.fetch_geo_runs(
    ["GSE123456", "GSM234567"],
)
```

| Parameter | Meaning | Restriction |
| --- | --- | --- |
| `accessions: list[str]` | GSE/GSM accessions resolved to linked SRA IDs | Email must be configured |

This produces the same `RunCatalog` interface. It does not automatically select
GEO supplementary FASTQ URLs; see the
[acquisition API](../src/ncbi_dataset_builder/acquisition/README.md) for that
provider.

## Inspect before filtering

```python
print(catalog.frame.shape)
print(catalog.frame.columns)
print(catalog.frame.select(
    "Run",
    "Experiment",
    "SRA Sample",
    "BioSample",
    "ScientificName",
    "TaxID",
    "LibraryLayout",
    "size_MB",
).head())
print(catalog.audit)
```

Check:

| Check | Why |
| --- | --- |
| Every row has `Run` | Fundamental identity |
| Grouping column is populated | Empty grouping values fall back to `Run` |
| Unit rows agree on taxid/species | Cross-species units are rejected |
| `size_MB` is numeric and populated | Queue storage estimates use it |
| Library strategy/layout match your processor | Prevents inappropriate processing |
| Duplicates are compatible | Contradictory duplicate values raise |

## `RunCatalog` constructor

```python
RunCatalog(
    frame,
    audit=(),
)
```

| Parameter | Default | Meaning |
| --- | --- | --- |
| `frame: polars.DataFrame` | Required | Run-oriented data containing `Run` |
| `audit: Iterable[str]` | Empty tuple | Prior operation messages |

Catalog operations return new objects; the original is unchanged.

## Filtering

### Polars expression

```python
import polars as pl

selected = catalog.filter(
    (pl.col("LibraryStrategy") == "ATAC-seq")
    & (pl.col("LibraryLayout") == "PAIRED")
    & (pl.col("size_MB") <= 100_000),
    description="paired ATAC runs no larger than 100 GB",
)
```

### Python callable

```python
selected = catalog.filter(
    lambda row: row.get("Platform") == "ILLUMINA",
    description="Illumina only",
)
```

### Parameters

| Parameter | Default | Meaning | Restriction |
| --- | --- | --- | --- |
| `predicate` | Required | Polars expression or row-dictionary callable | Other values raise `TypeError` |
| `description` | Expression/callable representation | Human-readable audit label | Supply stable text for reproducible logs |

`where` is an alias of `filter`. Prefer Polars expressions for large catalogs.

## Transforming the full table

```python
def add_estimated_gb(frame: pl.DataFrame) -> pl.DataFrame:
    # Decimal GB matches the queue’s conversion from size_MB.
    return frame.with_columns(
        (pl.col("size_MB") / 1_000).alias("estimated_size_gb")
    )

transformed = catalog.transform(
    add_estimated_gb,
    description="add decimal-GB estimate",
)
```

| Parameter | Meaning | Restriction |
| --- | --- | --- |
| `function` | Callable receiving the full Polars frame | Must return a Polars `DataFrame` |
| `description` | Optional audit label | Returned frame must retain `Run` |

## Other catalog operations

| Method | Parameters | Result/restriction |
| --- | --- | --- |
| `select(*columns)` | Column names | Always adds `Run` if omitted |
| `with_columns(*expressions)` | Polars expressions | Appends an `added/updated columns` audit event |
| `replace_frame(frame, *, event)` | New frame and required audit event | New frame must retain `Run` |
| `deduplicate_runs()` | None | Coalesces null-compatible duplicates; rejects contradictory values |
| `normalized_entities()` | None | Returns run/experiment/sample/study Polars views |
| `processing_units(*, by="experiment")` | One grouping level | Returns validated `ProcessingUnit` objects |

## Deduplication behavior

Rows with the same run accession are handled as follows:

| Duplicate condition | Result |
| --- | --- |
| All values identical | One row retained |
| One row has null and another a value | Non-null value coalesced |
| Same run has two different non-null values in any column | `CatalogConflictError` |

The error reports up to ten conflicting run accessions and their columns.

## Choosing `group_by`

| Value | RunInfo column | Unit contents | Choose when |
| --- | --- | --- | --- |
| `"run"` | `Run` | One run | Runs must remain independent |
| `"experiment"` | `Experiment` | All runs in one experiment | Default for assay processing and required for compact publishing |
| `"sra_sample"` | `SRA Sample` | Runs linked to one SRA Sample | Multiple experiments should deliberately merge |
| `"biosample"` | `BioSample` | Runs linked to one BioSample | Broad biological-sample aggregation is intended |

If the grouping value is null/empty for a row, that row falls back to its
`Run` accession. A completely missing grouping column raises.

### Processing-unit fields

| Field | How it is computed |
| --- | --- |
| `unit_id` | Group key or fallback run |
| Accession tuples | Distinct non-empty values in input order |
| `taxid` | Unique value across supported taxid columns |
| `scientific_name` | First non-empty supported species value after consistency check |
| `total_bases` | Sum of `bases`, with missing values treated as zero |
| `total_size_gb` | Sum of `size_MB / 1000`, with missing values treated as zero |
| Metadata | Unique strategy/layout/platform values and per-run size mapping |

If a unit contains more than one taxonomy ID or species name, processing-unit
creation raises rather than choosing one.

## Audit trail

```python
for event in selected.audit:
    print(event)
```

The execution record copies the final catalog audit. Use explicit
`description=` and `event=` strings so another user can understand why rows
were kept or changed.

## Safe first-run selection

1. Deduplicate.
2. Confirm grouping and taxonomy columns.
3. Filter to the intended assay/layout/species.
4. Inspect unit counts and sizes.
5. Select one or two representative units.
6. Run them through the complete execution path.
7. Scale only after validating outputs and storage estimates.

```python
units = selected.processing_units(by="experiment")
for unit in units[:10]:
    print(
        unit.unit_id,
        unit.scientific_name,
        unit.taxid,
        unit.run_accessions,
        unit.total_size_gb,
    )
```

## Common problems

| Error/symptom | Meaning | Action |
| --- | --- | --- |
| Missing `Run` | Invalid catalog boundary | Preserve/add the run accession column |
| Missing grouping column | Requested grouping cannot be formed | Select a supported column or change `group_by` |
| Cross-species unit error | Grouping combined contradictory metadata | Correct catalog rows or use a narrower grouping |
| Duplicate conflict | Same run has contradictory non-null data | Investigate source; do not arbitrarily keep first |
| Storage admits too much | Missing/zero `size_MB` | Repair size metadata or use more conservative storage controls |
| Polars inconsistency error | Mixed/stale Polars installation | Restart kernel and install exactly one Polars distribution |

## Related pages

- [Local execution](LocalExecution.md)
- [Choosing an execution system](ExecutionSystems.md)
- [Architecture](Architecture.md)
- [Catalog API reference](../src/ncbi_dataset_builder/catalog/README.md)
- [Metadata API reference](../src/ncbi_dataset_builder/metadata/README.md)
