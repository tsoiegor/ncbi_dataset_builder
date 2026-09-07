# Operations, recovery, and cluster behavior

## Workspace layout

```text
workspace/
  catalogs/          cached complete RunInfo query results
  metadata_cache/    raw E-utilities responses
  metadata/          normalized NDJSON plus complete metadata JSON
  genomes/           accession-versioned FASTA, NCBI ZIP, lockfile, indexes
  fastq/             per-unit SRA cache, per-run FASTQ, merged FASTQ, manifests
  work/              plan/task-specific processor intermediates
  results/           processor outputs, namespaced by plan ID and task
  plans/             immutable saved plans
  state/tasks/       atomic per-plan/per-task state
  slurm/             generated sbatch scripts
  logs/slurm/        array stdout/stderr
```

The package does not automatically delete SRA, FASTQ, BAM, or failed-task intermediates. Storage policy is project-specific and cleanup should be a separate, audited operation after output checksums and downstream ingestion are complete.

## Preflight and smoke test

```bash
ncbi-dataset --workspace workspace preflight \
  --processor ncbi_dataset_builder.processing.atac:default_atac_processor
```

Then run one small accession through metadata, planning, download, and processing before a cluster-scale submission. `preflight` proves executables are visible; it cannot prove network access, reference compatibility, quota, scratch capacity, or Slurm policy.

## Network behavior

E-utilities calls use a user agent, timeout, exponential backoff with jitter, `Retry-After`, and bounded retries. Rate is 3 requests/second without an NCBI key and 10 with one. Large searches use Entrez History and paginated EFetch instead of constructing huge URL lists.

HTTP supplementary downloads write `filename.part`, use Range resume when supported, validate optional size/SHA-256, then atomically rename. NCBI SRA downloads delegate resume to `prefetch` and are accepted only after `vdb-validate`.

## Failure and resume semantics

Task states are `pending`, `running`, `succeeded`, and `failed`.

- `succeeded`: processor returned `ProcessingResult(success=True)` and every declared output is non-empty.
- `failed`: traceback is persisted; inputs remain available.
- `running`: another worker owns it. A second worker skips it instead of changing its state.
- `failed` tasks do not run again unless `--retry-failed` is used.
- `succeeded` tasks never run again for the same saved plan, even with `--retry-failed`.
- a plan is bound to its first processor identity; changing processors requires a new plan.

If a worker dies while marked running, the claim becomes reclaimable after seven days. For multi-day site jobs, keep the default; for a different cluster policy, expose a site-specific state-store wrapper rather than deleting lock/state files during active work.

Inspect state:

```bash
ncbi-dataset --workspace workspace status --plan workspace/plans/dataset.json
```

Retry locally:

```bash
ncbi-dataset --workspace workspace build --plan workspace/plans/dataset.json \
  --processor my_pipeline:process --retry-failed
```

Retry through a fresh array submission:

```bash
ncbi-dataset --workspace workspace submit-slurm --plan workspace/plans/dataset.json \
  --processor my_pipeline:process --retry-failed
```

All array indices are submitted; successful/running tasks exit as skipped. This keeps array indexing stable and auditable.

## Slurm details

The generated script requests one task per array index and includes CPU, memory, time, optional partition/account/QoS, `%A_%a` logs, and `set -euo pipefail`. It uses `sbatch --parsable`, not a collection of foreground `srun` calls.

Requirements:

- the saved plan and workspace are on storage visible to every node;
- the same `ncbi_dataset_builder` package and processor module are importable by the script's Python;
- `NCBI_API_KEY`, if used, is exported to jobs rather than written to a plan;
- cluster modules/containers are loaded before `submit_slurm`, or the generated script is extended with site setup lines;
- log directories exist at submission time (the API creates them).

Different resource requirements should be placed in separate plans/arrays. One Slurm array has one resource request.

Pass `batch_ids={3, 4}` in Python or repeat `--batch-id 3 --batch-id 4` in the CLI to build, submit, or inspect only selected deterministic batches. Slurm compresses their actual plan indices into an array expression, so worker indices still address the immutable full plan.

## Scale and performance notes

- Use Polars expressions rather than Python row predicates for large filters.
- Set `max_workers` and `total_threads`; the local executor will not intentionally oversubscribe CPU threads.
- Put SRA temporary data and FASTQ output on high-throughput scratch where possible.
- `fasterq-dump` requires substantial temporary space, often more than the final compressed FASTQ.
- `pigz` is optional but strongly recommended.
- Genome and Bowtie2 indexes are shared and lock-protected; do not place them on node-local storage unless each job receives a separate root.
- RunInfo `size_MB` is an estimate used only for packing. Storage monitoring must use actual filesystem usage.
