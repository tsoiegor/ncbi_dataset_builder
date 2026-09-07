# Migration from the original scripts

The original files remain under `src/multispeciesATACseq_processing/`, the original top-level `src/bam2bw.py` remains untouched, and the original README is in `docs/legacy/README.original.md`. They are reference material and are not imported by the new package.

## Mapping

| Original area | New area | Important behavior change |
|---|---|---|
| `SRAinfo.py` | `catalog.py`, `metadata.py`, `workflow.py` | One Run row, unrestricted filters, explicit grouping, structured metadata |
| `NCBI.py` | `metadata.py`, `geo.py`, `http.py` | Official XML/CSV endpoints, throttling, retry, timeout, raw-response cache |
| `DownloadSRA.py` | `fastq.py` | Checked return codes, native resume, validation, bounded execution, manifests |
| `GenomeUtils.py` / `GenomeCollection.py` | `genomes.py` | NCBI Datasets reports, deterministic selection, exact TaxID, versioned lockfile |
| `runDatasetGeneration.py` | `workflow.py`, `processing/atac.py` | Assay-independent processor contract, durable state, checked subprocess pipeline |
| `SLURM.py` | `execution.py`, `worker.py` | Actual `sbatch` arrays; no resource-prefix emulation with `srun` |
| experimental `Processor.py` / `Downloader.py` | typed protocols and `DatasetBuilder` | Removed incomplete cloud queue and credential-in-payload path |
| `QualityStatsCollection.py` | processor metrics in `ProcessingResult` | Incomplete class was not carried forward |

Legacy BioSample JSON may contain presentation markup such as `<span class="highlight">...` or encoded text such as `&amp;`. Run `ncbi-dataset sanitize-legacy-metadata PATH...` once to clean those snapshots. New metadata never parses the NCBI presentation page: `DatasetBuilder.fetch_metadata(["SRS..."])` resolves the accession and builds a complete sample document from official SRA Experiment Package and BioSample XML, including library and study fields that the old table parser omitted.

## Resolved defects and quirks

- Constructor-time full Polars-to-pandas copies are gone.
- Filtering is no longer hardcoded to bulk/scATAC, exact two-word species names, tumor labels, or fixed thresholds.
- Scientific names are not truncated to two words.
- Duplicate Run accessions are checked: compatible nulls are coalesced and conflicting fields fail visibly.
- Grouping does not assume all fields under an SRA Sample are identical. Experiment is the default and mixed species in any unit fail planning.
- BioSample fields no longer come from brittle HTML text replacement. Repeated XML attributes are retained as lists, so a later normalization loop cannot overwrite an earlier tissue value.
- Taxonomy-page scraping and one-request-per-row behavior are gone; RunInfo TaxID is the species key.
- Missing local RefSeq/GenBank assembly-summary files are no longer prerequisites.
- Genome ranking no longer admits unrelated assemblies using a large-size OR condition, silently changes species names, or confuses direct and species TaxIDs.
- Genome `recompute` logic and temporary-directory races are replaced by checksummed accession paths and locks.
- NCBI Datasets failures and invalid ZIP/FASTA outputs fail the task rather than being treated as success.
- Batch construction is deterministic, linearithmic after sorting, retains totals below the target, and puts oversized units in a one-item batch. It cannot return an empty batch forever or duplicate a boundary item.
- The misleading `top`/`bottom` ascending sort behavior is removed. Selection is expressed explicitly with Polars sort/head.
- `-1` no longer expands to one worker per Run. Local concurrency is always bounded.
- Download progress is represented by validated artifacts/manifests, not the directory entry's byte size.
- Every external command checks its return code. Network and command retries are bounded and preserve the last error.
- SRA `--split-3` output is modeled as paired plus orphan reads; basename-independent R1/R2 counting no longer decides layout.
- Multi-run inputs use stable RunInfo order and direct gzip-member concatenation without an unquoted `bash -c cat` glob.
- Bowtie2/samtools streaming checks every process in the pipeline. An upstream failure cannot be masked by the last command.
- Bowtie2 index reuse requires all six `.bt2` or all six `.bt2l` files, not any matching file.
- The failed-sample list/set type error is replaced by JSON task state.
- A BigWig or other processor failure cannot be logged and followed by successful cleanup. Validation happens before success; default cleanup is none.
- Private `ThreadPoolExecutor._shutdown` mutation and next-batch future reuse are gone.
- CLI options now affect the plan they describe; no hidden hardcoded sample-selection mode or always-forwarded exclusion flags remain.
- Slurm CPU/memory defaults cannot be undefined, swapped, or divided by zero. Array resources are explicit and validated.
- Batch ranges use generated array indices, avoiding inclusive-help/exclusive-slice ambiguity.
- Missing local `utils.py` and `bam_utils.py` are not dependencies of the new ATAC processor.
- Processor exceptions propagate to durable failure state instead of being swallowed into an exit code of zero.
- Mutable default lists/dictionaries from the old BigWig path are not used.
- Strand-specific deepTools filtering is configurable and opt-in; the built-in ATAC default is unstranded.

## Deliberate non-ports

- DNA Zoo/Wasabi fallback is not part of the core NCBI genome contract. Register a custom FASTA or implement a `GenomeManager` adapter if a non-NCBI source is required.
- The unfinished bucket/cloud queue is not retained. Object-store execution should be an executor/provider plugin with external secret management.
- Automatic destructive cleanup is not retained.
- Lexical tissue balancing and fixed per-taxon caps are not policy defaults. Express them as catalog operations in project code so they are reviewable.

## Migration sequence

1. Install the package and run tests/preflight.
2. Load the existing `data/multispeciesATACseq/SraRunInfo.csv` with `DatasetBuilder.load_runs`.
3. Translate each old fixed filter into a named Polars expression and inspect the audit trail.
4. Plan by Experiment first; compare counts against any historic SRA-Sample-grouped dataset.
5. Pin genome accession versions for benchmark reproduction.
6. Wrap the model dataset-generation function in the three-argument processor contract.
7. Run one accession locally, then one small Slurm array.
8. Compare output schema, read counts, assembly accession, tool versions, and model-ready examples before scaling.
