# Architecture and data model

## Design rules

1. Accessions are identities, not labels. Run, Experiment, SRA Sample, BioSample, Study/BioProject, and GEO accessions are stored separately.
2. A run catalog always has one row per Run. Identical duplicate RunInfo rows are coalesced; contradictory rows raise `CatalogConflictError`.
3. Remote metadata comes from structured NCBI E-utilities XML/CSV or NCBI Datasets JSON, never presentation HTML.
4. A completed stage is represented by an atomically published artifact or manifest. A filename alone is not completion.
5. Failed work keeps its inputs. Cleanup is not part of success detection.
6. Local and Slurm execution invoke the same task function and write the same state schema.

## Layers

| Layer | Main objects | Responsibility |
|---|---|---|
| Catalog | `RunCatalog` | RunInfo retrieval input, filtering, conflict detection, entity views, grouping, batch packing |
| Metadata | `EntrezClient`, `SraClient`, `BioSampleClient`, `MetadataBundle` | Accession resolution, throttled requests, SRA package XML, BioSample XML, normalized and raw metadata |
| GEO | `GeoClient`, `GeoFastqProvider` | GEO-to-SRA links, MINiML supplementary discovery, direct FASTQ retrieval |
| FASTQ | `SraToolkitProvider`, `AtomicDownloader`, `FastqSet` | Resumable SRA downloads, validation, fasterq conversion, layout modeling, checksums |
| Genomes | `GenomeManager`, `GenomeSelectionPolicy`, `GenomeRef` | Candidate reports, deterministic ranking, downloads, checksums, lockfile |
| Processing | `Processor`, `ProcessingResult`, `AtacSeqProcessor` | Assay-specific transformation behind a three-argument interface |
| Execution | `LocalExecutor`, `SlurmExecutor` | Bounded local threads or `sbatch` arrays |
| State | `TaskStateStore` | Atomic running/succeeded/failed records and explicit retries |
| Facade | `DatasetBuilder` | Compose the layers into user workflows |

## Entity flow

```text
Entrez query or GEO accession
        |
        v
SRA RunInfo (one row per Run)
        |
        +--> structured SRA/BioSample metadata
        |
        v
Processing units (Experiment by default)
        |
        +--> species TaxID --> genome candidate ranking --> locked GenomeRef
        |
        +--> ordered Run accessions --> validated SRA --> FastqSet
        |
        v
processor(FastqSet, GenomeRef, threads)
        |
        v
validated ProcessingResult + durable task state
```

## Why Experiment is the default unit

An SRA Experiment defines a library strategy, source, selection, layout, platform design, and a sample reference. A BioSample may have multiple experiments and assays. The old implementation grouped by SRA Sample and could therefore merge distinct experiments. The new default avoids that. Users can select `run`, `sra_sample`, or `biosample` explicitly.

Every unit is checked for a single species TaxID and scientific name. Mixed read layout is not discarded: `FastqLayout.MIXED` carries paired reads and split-3 orphan/single reads separately.

## Metadata representation

`MetadataBundle` writes:

```text
metadata/
  metadata.json
  packages.ndjson
  runs.ndjson
  experiments.ndjson
  sra_samples.ndjson
  studies.ndjson
  submissions.ndjson
  biosamples.ndjson
  sample_descriptions/
    SRS....json
```

Normalized fields make common analysis easy, and repeated BioSample keys become lists instead of silently overwriting one another. Package relations preserve the Study -> Experiment -> SRA Sample -> Run graph, while each per-sample document embeds all linked entities. Public accessions are resolved with ESearch before numeric UIDs are sent to EFetch; sending an SRS/SRX/SRR string directly to SRA EFetch returns HTTP 400. The full successful E-utilities response for every batch is cached under `metadata_cache/raw`. Set `include_raw=True` to additionally embed recursively represented XML trees in the returned bundle and NDJSON; this is off by default because Python object expansion of tens of thousands of XML packages is memory-intensive.

## Genome ranking

Candidate filtering requires the requested TaxID when the report provides one and rejects suppressed, replaced, withdrawn, anomalous, and—by default—atypical assemblies. Ranking is deterministic:

1. NCBI reference genome, then representative genome;
2. RefSeq before GenBank;
3. assembly level: complete genome, chromosome, scaffold, contig;
4. scaffold N50, falling back to contig N50;
5. total sequence length;
6. release date;
7. accession as the final stable tie-breaker.

The chosen report is reduced to `GenomeRef`, checksummed, and stored in `genomes/genomes.lock.json`. A user pin overrides ranking but must be present in the NCBI result. `register_custom` records an external genome with the same checksum/provenance contract.

## State and concurrency

State is namespaced by saved plan ID and task ID. Per-task JSON avoids a single SQLite writer bottleneck on shared cluster filesystems. Writes use a temporary file and `os.replace`; claims use exclusive lock files with stale-lock recovery. A generated plan is the resume token—regenerating a plan creates a new namespace intentionally.

Genome selection is protected per TaxID, while updates to the shared genome lockfile use a separate global lock. Local worker count is bounded by both `max_workers` and `total_threads // threads_per_task`.
