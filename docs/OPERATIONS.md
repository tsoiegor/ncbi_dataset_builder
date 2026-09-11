# Operations, recovery, and cluster behavior

## Workspace layout

```text
workspace/
  catalogs/          cached complete RunInfo query results
  metadata_cache/    read-through raw E-utilities responses and normalized bundles
  metadata/          normalized NDJSON plus complete metadata JSON
  genomes/           flat accession FASTA files, lockfile, and lazy indexes
  fastq/             bounded per-unit raw/conversion/final FASTQ workspace
  work/              plan/task-specific processor intermediates
  results/           processor outputs, namespaced by plan ID and task
  plans/             immutable saved plans
  state/tasks/       atomic per-plan/per-task state
  state/batches/     atomic per-plan/per-batch lifecycle and storage manifests
  slurm/             generated sbatch scripts
  logs/slurm/        coordinator stdout/stderr
  logs/units/        one combined log per plan/batch/unit
  dataset/           optional compact published dataset
```

The relevant storage-heavy directories expand as follows:

```text
genomes/
  GCF_000001405.40.fasta.gz
  genomes.lock.json
  indexes/GCF_000001405.40/GCF_000001405.40.*.bt2

fastq/SRX123/                    # deleted after successful processing by default
  .raw/SRR123/SRR123.sra         # transient: removed after FASTQ validation
  .runs/SRR123/*.fastq.gz        # transient: removed after unit merge
  SRX123_1.fastq.gz
  SRX123_2.fastq.gz
  fastq.manifest.json

dataset/
  bigWig/SRX123.bw
  genomes/Homo_sapiens.fasta.gz
  descriptions/SRX123.json
  manifest.json
```

`.downloads/<assembly>/` exists under `genomes/` only while an NCBI archive is being fetched
or extracted. The ZIP is removed after the compressed FASTA passes a full read and header
validation. Failed SRA conversion retains its unit directory for diagnosis; once conversion
succeeds, no `.sra` file remains even if later processing fails.

The default `PipelinePolicy(cleanup="after_success")` deletes only provider-declared SRA/FASTQ
roots after all processor outputs and checksums validate. Failed inputs are retained. Genomes,
results, task work directories, state, and logs are not removed. Set `cleanup="never"` when the
input cache is more valuable than bounded storage.

One heartbeat-protected coordinator lock is allowed per workspace. A second local or Slurm build
fails instead of racing shared run caches and cleanup. If a coordinator is killed, its lock
becomes reclaimable after five minutes; running task records are then recovered from their
provider manifests and appended unit logs.

## Preflight and smoke test

```bash
ncbi-dataset --workspace workspace preflight \
  --processor ncbi_dataset_builder.processing.atac:default_atac_processor
```

Then run one small accession through metadata, planning, download, and processing before a cluster-scale submission. `preflight` proves executables are visible; it cannot prove network access, reference compatibility, quota, scratch capacity, or Slurm policy.

## Network behavior

E-utilities calls use a user agent, timeout, exponential backoff with jitter, `Retry-After`, and bounded retries. Rate is 3 requests/second without an NCBI key and 10 with one. Large searches use Entrez History and paginated EFetch instead of constructing huge URL lists.

Successful accession-resolution and EFetch responses are read through `metadata_cache/raw`. Expiring Entrez History WebEnv responses are recorded but never read from cache. Completed `fetch_metadata` and `enrich_metadata` calls additionally store a normalized bundle keyed by the selected accessions, `include_raw`, and cache schema. Repeating the same call makes no NCBI requests. Pass `refresh=True` in Python or `--refresh` in the CLI to bypass and replace both cache layers. Invalid bundle-cache JSON is ignored and rebuilt from valid raw batches or NCBI.

## Progress diagnostics

Progress output is enabled by default. For metadata enrichment, read the messages in this order:

1. `Sample descriptions` reports whether the exact normalized metadata bundle was reusable.
2. Each SRA or BioSample phase reports raw-cache request batches and batches that will contact
   NCBI.
3. `Entrez responses for this operation` reports the actual raw-cache hits and network requests.

An exact normalized bundle is keyed by the selected accessions and `include_raw`; changing the
catalog or that option creates a different cache key. `refresh=True` deliberately bypasses both
normalized and raw Entrez caches. Progress bars require the optional `progress` dependency; text
status and standard `ncbi_dataset_builder` logger events remain available without it.

Metadata output files are content-aware: unchanged JSON and NDJSON files are not replaced or `fsync`ed again. This is particularly important when `sample_descriptions/` is stored on NFS.

HTTP supplementary downloads write `filename.part`, use Range resume when supported, validate
optional size in GB and SHA-256, then atomically rename. NCBI SRA downloads delegate resume to
`prefetch` and are accepted only after `vdb-validate` and a raw completion marker.

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

Retry through a fresh coordinator submission:

```bash
ncbi-dataset --workspace workspace submit-slurm --plan workspace/plans/dataset.json \
  --processor my_pipeline:process --retry-failed
```

Successful tasks are skipped by the coordinator and failed tasks are reclaimed only when retry
is explicit.

## Slurm details

Single-node mode is the compatibility default. It requests one coordinator allocation with
aggregate CPU and memory and runs bounded local workers inside that allocation.

The generated script requests one coordinator allocation and includes aggregate CPU, aggregate
memory in GB, time, optional partition/account/QoS, a coordinator log, and `set -euo pipefail`.
It uses `sbatch --parsable`; the same Python pipeline used locally runs inside the allocation.

Requirements:

- the saved plan and workspace are on storage visible to every node;
- the same `ncbi_dataset_builder` package and processor module are importable by the script's Python;
- `NCBI_API_KEY`, if used, is exported to jobs rather than written to a plan;
- cluster modules/containers are loaded before `submit_slurm`, or the generated script is extended with site setup lines;
- log directories exist at submission time (the API creates them).

In single-node mode, `SlurmOptions.max_parallel` controls unit workers inside the allocation.
The requested CPUs and memory are the per-unit `ResourceSpec` multiplied by that worker count, unless
`BuilderConfig.total_threads` or `total_memory_gb` supplies an allocation-wide limit.

Distributed mode is intended for quotas spanning many 128-CPU nodes:

```bash
ncbi-dataset --workspace workspace submit-slurm \
  --plan workspace/plans/dataset.json --processor my_pipeline:process \
  --slurm-mode distributed --total-cpu-quota 500 --max-running-jobs 50 \
  --coordinator-cpus 1 --coordinator-memory-gb 4 --cpus-per-node 128 \
  --partition amd_256M,amd_1Tb,amd_2Tb \
  --prefetch-batches 1 --max-staged-gb 500 --minimum-free-gb 50
```

If each unit requests 16 CPUs, the worker ceiling is
`min(floor((500 - 1) / 16), 50 - 1) = 31`. Including the coordinator, that is at most 497 CPUs
and 32 running jobs. Memory is requested per unit from its `ResourceSpec`; the coordinator has
its own small CPU, memory, and wall-time request. `max_parallel` can lower this computed ceiling
but cannot raise it.

The distributed sequence is: stage batch N, submit its array, stage N+1 while N runs, wait for
N, finalize its state, and advance. The next batch is never processed before the current array
finishes. Quotas are ceilings created by this workflow; they do not count unrelated jobs already
running under the same Slurm account. Compute nodes must be permitted to invoke `sbatch`, because
the coordinator submits its child arrays.

Before a local build or Slurm submission, the builder reports one deduplicated genome-cache
inventory for all selected tasks. During execution, every task still resolves its required
genome, but validated references are reused within a process. Inside the coordinator, a shared
per-taxid filesystem lock ensures that only the lock owner selects and downloads a missing
genome; other unit workers wait and then reuse it.

Routine INFO output inside individual units is hidden from the console but retained in that
unit's single log. Local and Slurm builds report batch staging/processing summaries; failures are
also immediate. Use `status` for task counts and durable batch manifests.

Every command and processor invocation for a unit appends to its one unit log. Array-level
Slurm output from all elements of one batch appends to one `<job-id>.batch.log`; detailed unit
logs remain separate and stable across retries.

## Publishing a completed dataset

```bash
ncbi-dataset --workspace workspace publish \
  --plan workspace/plans/dataset.json --destination /shared/model-dataset
```

The command fails rather than publishing missing, failed, ambiguous, or multi-BigWig units.
`--mode auto` hard-links large files when source and destination share a filesystem, then falls
back to copying. Use `--overwrite` only when an existing published tree should be atomically
replaced. `manifest.json` maps each experiment to its sample, runs, assembly, species genome,
description, BigWig, SHA-256 values, sizes in decimal GB, and publication method.

Pass `batch_ids={3, 4}` in Python or repeat `--batch-id 3 --batch-id 4` in the CLI to build,
submit, or inspect only selected deterministic batches. Selected batches retain their immutable
IDs and are pipelined in ascending order.

## Scale and performance notes

- Use Polars expressions rather than Python row predicates for large filters.
- Set `max_workers`, `total_threads`, and `total_memory_gb`; the local executor will not intentionally oversubscribe declared CPU or memory budgets.
- Put SRA temporary data and FASTQ output on high-throughput scratch where possible.
- `fasterq-dump` requires substantial temporary space, often more than the final compressed FASTQ.
- `pigz` is optional but strongly recommended.
- Genome and Bowtie2 indexes are shared and lock-protected; do not place them on node-local storage unless each job receives a separate root.
- RunInfo `size_MB` is converted to decimal `total_size_gb` for packing. Batch manifests measure actual recursive input and free-space values in GB.
