# Data acquisition

The `ncbi_dataset_builder.acquisition` subpackage turns catalog processing
units into local FASTQs and resolves a validated reference genome for each
species. The high-level `DatasetBuilder` constructs SRA and genome services
automatically; these APIs matter when you need custom providers, GEO files,
assembly policy, or preflight checks.

The package-level [README](../README.md) defines `ProcessingUnit`,
`StagedFastq`, `FastqSet`, and `GenomeRef`.

## Module map

| Module | Contents |
| --- | --- |
| `fastq.py` | Provider protocols, resumable HTTP downloader, SRA Toolkit provider, and mapped GEO provider |
| `genomes.py` | NCBI assembly records, deterministic policy, download/cache manager, and custom references |
| `geo.py` | GEO-to-SRA resolution and supplementary-file discovery |
| `__init__.py` | Public subpackage export list |

## FASTQ provider contracts

### `FastqProvider`

`FastqProvider` is a typing `Protocol`, not a concrete base class. A
compatible provider implements:

```python
fetch(
    unit: ProcessingUnit,
    destination: Path,
    *,
    threads: int,
) -> FastqSet
```

| Argument | Meaning |
| --- | --- |
| `unit` | Catalog-derived runs and species metadata to materialize together. |
| `destination` | Package-managed FASTQ root, normally `workspace/fastq/`. |
| `threads` | Maximum CPU count available to provider tools. |

The result must have correct layout fields and non-empty files. The builder
calls `FastqSet.validate()` before processing.

### `StagedFastqProvider`

This protocol extends `FastqProvider` with two-phase streaming:

| Method | Arguments and result |
| --- | --- |
| `stage(unit, destination, *, threads)` | Download and validate bounded input; return `StagedFastq`. |
| `materialize(unit, staged, destination, *, threads)` | Convert staged data into a processor-ready `FastqSet`. |
| `fetch(unit, destination, *, threads)` | One-step compatibility method. |

`DatasetBuilder` detects `stage` and `materialize` at runtime. Separating
the phases lets a raw SRA download wait in the storage window while CPU-heavy
FASTQ conversion begins only when a processor can launch.

# SRA acquisition

## `SraToolkitProvider`

```python
SraToolkitProvider(
    *,
    runner=None,
    retries=3,
    prefetch_max_size="100G",
    prefetch_reset_after_failures=None,
    prefetch_retry_max_delay_seconds=300.0,
    progress=None,
)
```

| Argument | Meaning |
| --- | --- |
| `runner: CommandRunner | None` | External-command implementation; defaults to `CommandRunner()`. |
| `retries: int` | Positive retry count for conversion commands. It also sets the default prefetch-reset interval. |
| `prefetch_max_size: str` | SRA Toolkit size limit such as `"100G"`; `"u"` means unlimited. |
| `prefetch_reset_after_failures: int | None` | Failed prefetch attempts before removing incomplete accession-local staging. `None` becomes `retries + 1`. |
| `prefetch_retry_max_delay_seconds: float` | Non-negative ceiling for the backoff between prefetch attempts. |
| `progress: ProgressReporter | None` | Optional progress and cache-event sink. |

### Tool lifecycle

For every run accession the provider:

1. reuses a validated raw SRA archive when available;
2. otherwise runs `prefetch` and validates the archive with
   `vdb-validate`;
3. removes stale accession-local lock files after failed prefetch processes;
4. periodically resets incomplete staging after the configured number of
   failures;
5. runs `fasterq-dump --split-3`;
6. classifies `_1`, `_2`, and unpaired outputs;
7. compresses with `pigz` when available, otherwise gzip;
8. checksums per-run files;
9. merges runs in processing-unit order; and
10. publishes `fastq.manifest.json` only after validation.

Prefetch recovery retries indefinitely with a bounded delay. Conversion
commands use the finite `retries` setting.

### Cache behavior

| Cache | Reuse condition |
| --- | --- |
| Raw SRA | Expected archive and staging manifest exist; `vdb-validate` succeeds when prepared |
| Per-run FASTQ | Manifest run accession matches and every file checksum matches |
| Unit FASTQ | Unit run list matches and every merged file checksum matches |

A changed run list invalidates the unit manifest and stale merged FASTQs. Files
are published through partial paths or atomic JSON writes.

### Public methods

| Method | Behavior |
| --- | --- |
| `preflight()` | Require `prefetch`, `vdb-validate`, and `fasterq-dump`; return their version lines and optional `pigz`. |
| `fetch(unit, destination, *, threads)` | Stage and materialize in one call. |
| `stage(unit, destination, *, threads)` | Check the configured size limit, prefetch missing raw archives, and return exact cleanup roots. |
| `materialize(unit, staged, destination, *, threads)` | Convert or reuse per-run FASTQs, merge them, checksum files, and return `FastqSet`. |

`DatasetBuilder` creates this provider with
`BuilderConfig.prefetch_max_size` unless another provider is injected.

## `AtomicDownloader`

```python
AtomicDownloader(
    *,
    user_agent,
    retries=5,
    timeout_seconds=120.0,
    progress=None,
)
```

| Argument | Meaning |
| --- | --- |
| `user_agent: str` | Required HTTP caller identity. |
| `retries: int` | Retry count after transient download failures. |
| `timeout_seconds: float` | Per-attempt HTTP timeout. |
| `progress: ProgressReporter | None` | Optional reporting sink. |

### `download(url, destination, *, expected_sha256=None, expected_size_gb=None) -> Path`

The downloader:

- reuses a destination only when it satisfies requested validation;
- resumes from a partial file with an HTTP Range request when supported;
- restarts cleanly when the server ignores the range;
- validates optional checksum and approximate size; and
- atomically replaces the destination after the complete partial file passes.

`expected_size_gb` is decimal GB. `expected_sha256` is compared
case-insensitively.

# GEO acquisition

## `GeoFastqProvider`

```python
GeoFastqProvider(
    urls,
    *,
    downloader,
    progress=None,
)
```

| Argument | Meaning |
| --- | --- |
| `urls: dict[str, list[str]]` | Processing-unit ID to explicit supplementary FASTQ URLs. |
| `downloader: AtomicDownloader` | Required HTTP downloader. |
| `progress: ProgressReporter | None` | Optional reporting sink. |

Downloaded names are classified with common R1/R2 conventions. The provider
returns single, paired, or mixed `FastqLayout` accordingly.

| Method | Behavior |
| --- | --- |
| `fetch(unit, destination, *, threads)` | Download/reuse and classify mapped files. HTTP work is currently sequential; `threads` is accepted for protocol compatibility. |
| `stage(unit, destination, *, threads)` | Materialize GEO FASTQs immediately and wrap them in `StagedFastq.ready_fastq`. |
| `materialize(unit, staged, destination, *, threads)` | Return the already-ready FASTQ set after identity checks. |

The cache path includes a short hash of the configured URL list, so changing
the mapping creates a different root.

```python
from ncbi_dataset_builder import (
    AtomicDownloader,
    DatasetBuilder,
    GeoFastqProvider,
)

downloader = AtomicDownloader(
    user_agent="my-lab-dataset-builder/1.0",
)
provider = GeoFastqProvider(
    {
        "sample-A": [
            "https://example.org/sample-A_R1.fastq.gz",
            "https://example.org/sample-A_R2.fastq.gz",
        ]
    },
    downloader=downloader,
)

# The rest of DatasetBuilder scheduling is unchanged.
builder = DatasetBuilder(config, fastq_provider=provider)
```

## `GeoSupplementaryFile`

Frozen value:

```python
GeoSupplementaryFile(
    geo_accession,
    url,
    filename,
)
```

| Field | Meaning |
| --- | --- |
| `geo_accession: str` | Parent GSE or GSM accession. |
| `url: str` | Download URL declared in GEO MINiML. |
| `filename: str` | File name derived from the URL. |

## `GeoClient`

```python
GeoClient(
    entrez,
    sra,
)
```

| Argument | Meaning |
| --- | --- |
| `entrez: EntrezClient` | Used to follow GEO database links and retrieve MINiML archives. |
| `sra: SraClient` | Used to fetch linked RunInfo. |

| Method | Behavior |
| --- | --- |
| `resolve_to_sra(accessions)` | Resolve a list of GSE/GSM accessions to linked SRA IDs and return one deduplicated `RunCatalog`. |
| `discover_supplementary(series_accession)` | Parse one GSE MINiML archive and return `GeoSupplementaryFile` records. |

`DatasetBuilder.fetch_geo_runs()` is the usual high-level route to
`resolve_to_sra()`. Discovery does not download the supplementary files; map
selected URLs into `GeoFastqProvider` explicitly.

# Genome acquisition

## `GenomeCandidate`

Normalized frozen assembly value:

```python
GenomeCandidate(
    accession,
    taxid=None,
    scientific_name=None,
    source_database=None,
    assembly_status=None,
    refseq_category=None,
    assembly_level=None,
    release_date=None,
    contig_n50=None,
    scaffold_n50=None,
    total_length=None,
    atypical=False,
    warnings=(),
    raw={},
)
```

| Argument | Meaning |
| --- | --- |
| `accession: str` | Versioned assembly accession. |
| `taxid: int | None` | Assembly organism taxonomy ID. |
| `scientific_name: str | None` | Reported organism name. |
| `source_database: str | None` | RefSeq, GenBank, or another source. |
| `assembly_status: str | None` | Current/replaced/suppressed-style status. |
| `refseq_category: str | None` | Reference or representative category. |
| `assembly_level: str | None` | Contig, scaffold, chromosome, or complete genome. |
| `release_date: str | None` | Ranking tie-breaker. |
| `contig_n50`, `scaffold_n50` | Optional assembly continuity metrics. |
| `total_length: int | None` | Reported sequence length. |
| `atypical: bool` | NCBI atypical flag. |
| `warnings: tuple[str, ...]` | Status or quality warnings. |
| `raw: dict` | Original NCBI report retained for provenance; excluded from equality comparison. |

`from_report(report)` normalizes nested NCBI Datasets fields into this shape.

## `GenomeSelectionPolicy`

```python
GenomeSelectionPolicy(
    allow_atypical=False,
    minimum_assembly_level=None,
    prefer_reference=True,
    prefer_refseq=True,
)
```

| Argument | Meaning |
| --- | --- |
| `allow_atypical: bool` | Permit NCBI-atypical assemblies. |
| `minimum_assembly_level: str | None` | Optional minimum: `contig`, `scaffold`, `chromosome`, or `complete genome`. |
| `prefer_reference: bool` | Rank reference, then representative, before uncategorized assemblies. |
| `prefer_refseq: bool` | Prefer RefSeq to an otherwise equal candidate. |

`LEVELS` is the class-level ordering used for validation and ranking:
`contig < scaffold < chromosome < complete genome`. It is documented for
transparency, but callers should configure `minimum_assembly_level` instead of
mutating this constant.

### `select(candidates, *, taxid, pin=None) -> GenomeCandidate`

With `pin`, require the exact accession and matching taxonomy ID. A pin
bypasses the normal status/quality ranking.

Without a pin, reject missing accessions, wrong taxonomy IDs,
suppressed/replaced/withdrawn/anomalous status, disallowed atypical assemblies,
and levels below the minimum. Rank remaining candidates by:

1. reference/representative category when enabled;
2. RefSeq source when enabled;
3. assembly level;
4. scaffold N50, falling back to contig N50;
5. total length;
6. release date; and
7. accession for deterministic final ordering.

### `rationale(candidate) -> tuple[str, ...]`

Return human-readable taxonomy, status, category, source, level, N50, and
release-date facts stored in `GenomeRef.selection_rationale`.

## `GenomeManager`

```python
GenomeManager(
    root,
    *,
    runner=None,
    policy=None,
    progress=None,
)
```

| Argument | Meaning |
| --- | --- |
| `root: Path` | Genome cache containing FASTAs, archives, per-species locks, and `genomes.lock.json`. |
| `runner: CommandRunner | None` | External-command abstraction. |
| `policy: GenomeSelectionPolicy | None` | Selection policy; default policy when omitted. |
| `progress: ProgressReporter | None` | Optional reporting sink. |

| Method | Arguments and result |
| --- | --- |
| `preflight()` | Require NCBI Datasets CLI and return its version. |
| `candidates(taxid)` | Query assembly reports, recursively normalize them, deduplicate by accession, and return candidates. |
| `cache_inventory(requirements, *, description="Genome references")` | Validate unique `(taxid, pin)` requirements and return cached references keyed by that tuple. |
| `resolve(*, taxid, scientific_name=None, pin=None)` | Reuse a checksum-valid reference or select/download/register it; return `GenomeRef`. |
| `register_custom(*, taxid, scientific_name, accession, fasta)` | Check a non-empty local FASTA, hash it, and write it into the genome lockfile as source `custom`. |

The manager serializes concurrent resolution with species and lockfile locks.
A cached entry is reusable only when its FASTA exists, is non-empty, matches
the stored SHA-256, and satisfies an optional pin.

```python
from pathlib import Path

from ncbi_dataset_builder import GenomeManager

manager = GenomeManager(Path("/data/ncbi-workspace/work/genome_cache"))

# Register an assembly already managed by your lab.
custom = manager.register_custom(
    taxid=9606,
    scientific_name="Homo sapiens",
    accession="lab-GRCh38-v1",
    fasta=Path("/references/GRCh38.fa"),
)

# The same taxid now resolves through the validated lockfile entry.
resolved = manager.resolve(
    taxid=9606,
    scientific_name="Homo sapiens",
    pin="lab-GRCh38-v1",
)
assert resolved.sha256 == custom.sha256
```

For execution-time exact NCBI pins, pass
`genome_pins={9606: "GCF_000001405.40"}` to `build()` or
`submit_slurm()`.

## Internal helpers

`_from_manifest`, report-tree traversal, nested-field readers, and FASTQ
classification/merge helpers are implementation details. They are not exported
and may change without compatibility guarantees.
