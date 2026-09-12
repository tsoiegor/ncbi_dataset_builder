# `acquisition`

This subpackage turns remote SRA/GEO records into local FASTQs and resolves a
reference genome for each sample. `DatasetBuilder` constructs the default SRA
and genome services, while callers may inject compatible providers for other
data sources.

## FASTQ providers

`FastqProvider` is the minimal protocol. Its
`fetch(unit, destination, threads)` method accepts a
[`ProcessingUnit`](../README.md), a cache directory, and a positive CPU count;
it returns a `FastqSet` ready for a processor.

`StagedFastqProvider` adds two-phase streaming:

- `stage(unit, destination, threads)` downloads bounded provider-owned input
  and returns `StagedFastq` with exact cleanup roots.
- `materialize(unit, staged, destination, threads)` turns that staged input
  into a validated `FastqSet` immediately before processing.

`SraToolkitProvider(runner=None, retries=3, prefetch_max_size="100G",
prefetch_reset_after_failures=None, prefetch_retry_max_delay_seconds=300,
progress=None)` is the default implementation. The `runner` invokes
`prefetch`, `vdb-validate`, `fasterq-dump`, and optional `pigz`; retries control
conversion attempts and incomplete-download reset; `prefetch_max_size` accepts
values such as `"100G"` or `"u"`.
Its public methods are `preflight()` for external-tool versions, `fetch()` for
the one-step protocol, and `stage()`/`materialize()` for the streaming queue.
`DatasetBuilder` calls the two-phase methods automatically when present.

`GeoFastqProvider(urls, downloader=..., progress=None)` downloads explicitly
mapped GEO supplementary FASTQs. `urls` maps processing-unit IDs to URL lists;
`downloader` is the `AtomicDownloader` below. It exposes the same `fetch`,
`stage`, and `materialize` methods and is passed to
`DatasetBuilder(..., fastq_provider=provider)`.

`AtomicDownloader(user_agent=..., retries=5, timeout_seconds=120,
progress=None)` is shared by HTTP-based acquisition. `download(url,
destination, expected_sha256=None, expected_size_gb=None)` writes through a
temporary file, resumes partial content, retries transient failures, and
verifies optional size/checksum fields.

## GEO

`GeoSupplementaryFile(geo_accession, url, filename)` records the parent GEO
accession, declared URL, and derived filename. `GeoClient(entrez, sra)` uses
the metadata clients documented in
[`metadata`](../metadata/README.md):

- `resolve_to_sra(accessions)` accepts GSE/GSM strings and returns a
  [`RunCatalog`](../catalog/README.md).
- `discover_supplementary(series_accession)` returns downloadable files for one
  GSE series.

## Genomes

`GenomeCandidate(accession, taxid, scientific_name, source_database,
assembly_status, refseq_category, assembly_level, release_date, contig_n50,
scaffold_n50, total_length, atypical=False, warnings=(), raw={})` is a
normalized NCBI assembly record. Optional fields may be `None` when NCBI omits
them. `from_report(report)` parses an NCBI Datasets report mapping.

`GenomeSelectionPolicy(allow_atypical=False, minimum_assembly_level=None,
prefer_reference=True, prefer_refseq=True)` controls accepted assembly level
and deterministic RefSeq/reference ranking. `minimum_assembly_level` may be
`contig`, `scaffold`, `chromosome`, or `complete genome`.
`select(candidates, taxid=..., pin=None)` chooses one candidate or an exact
versioned assembly; `rationale(candidate)` explains the selected properties.
`BuilderConfig` holds this policy because genome identity is stable workspace
semantics.

`GenomeManager(root, runner=None, policy=None, progress=None)` manages
downloaded and custom genomes. `root` contains compressed FASTAs and
`genomes.lock.json`; `runner` is a
[`CommandRunner`](../support/README.md):

- `preflight()` checks NCBI Datasets CLI and decompression tools.
- `candidates(taxid)` lists normalized assemblies for an NCBI taxonomy ID.
- `cache_inventory(requirements, description=...)` reports reusable references
  for `(taxid, pin)` pairs.
- `resolve(taxid, scientific_name, pin=None)` selects/downloads a genome and
  returns [`GenomeRef`](../README.md).
- `register_custom(taxid, scientific_name, accession, fasta)` registers a
  non-empty local reference with source `custom` for subsequent `resolve()`
  calls.
