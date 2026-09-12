# Command-line interface

Installation creates `ncbi-dataset`. The equivalent module entry point is:

```bash
python -m ncbi_dataset_builder
```

The CLI covers catalog fetching, local execution, both Slurm modes, status,
and compact publication. It maps arguments directly to the same Python
configuration classes described in the execution guides.

## Commands

| Command | Purpose | Main Python method |
| --- | --- | --- |
| `fetch-catalog` | Fetch and save an NCBI SRA RunInfo CSV | `fetch_runs()` |
| `build-local` | Run the streaming queue on the current server | `build()` |
| `submit-single-node` | Generate/submit one Slurm allocation | `submit_slurm()` |
| `submit-distributed` | Generate/submit a coordinator that launches sample jobs | `submit_slurm()` |
| `status` | Print latest or selected execution/unit state | `status()` |
| `publish` | Publish a compact experiment dataset | `publish_dataset()` |

Run:

```bash
ncbi-dataset COMMAND --help
```

for argparse’s generated usage.

## Shared builder options

Every command accepts these options:

| Option | Required/default | Python field | Meaning/restriction |
| --- | --- | --- | --- |
| `--workspace PATH` | Required | `BuilderConfig.workspace` | Durable workspace root |
| `--email ADDRESS` | `NCBI_EMAIL` or unset | `email` | Required for live NCBI catalog/metadata work |
| `--ncbi-api-key KEY` | `NCBI_API_KEY` or unset | `ncbi_api_key` | Optional higher Entrez request rate |
| `--group-by LEVEL` | `experiment` | `group_by` | `run`, `experiment`, `sra_sample`, or `biosample` |
| `--prefetch-max-size VALUE` | `u` | `prefetch_max_size` | SRA Toolkit archive limit such as `100G` or unlimited `u` |

`group_by`, description profile, and genome policy are stable workspace
semantics once unit state exists. The CLI exposes grouping but currently uses
default description and genome policies.

## Catalog-source options

Processing commands require exactly one source:

| Option | Meaning | Interaction |
| --- | --- | --- |
| `--catalog PATH` | Load an existing RunInfo CSV | Email is not required; `--refresh` has no effect on file loading |
| `--query EXPR` | Fetch catalog from NCBI | Email is required; cached by query |
| `--refresh` | Bypass matching NCBI catalog cache | Relevant only with `--query` |

The mutual-exclusion rule applies to `build-local`,
`submit-single-node`, and `submit-distributed`.

## Queue options

All three processing commands accept:

| Option | Default | Python mapping | Meaning/restriction |
| --- | --- | --- | --- |
| `--download-workers N` | `2` | `QueuePolicy.download_workers` | Positive local/single-node staging-pool size; not a separate pool in distributed mode |
| `--max-inflight-gb GB` | Unset | `max_inflight_gb` | Positive estimated workload window |
| `--processing-storage-multiplier X` | `1.0` | `processing_storage_multiplier` | Total peak processing footprint / raw size; at least 1 |
| `--keep-inputs` | False | `cleanup="never"` when present | Preserve provider-owned input after all outcomes |
| `--discard-failed-inputs` | False | `keep_failed_inputs=False` when present | Permit cleanup after failure when cleanup is active |
| `--retry-failed` | False | Call argument | Reclaim matching failed state |

If both `--keep-inputs` and `--discard-failed-inputs` are present, cleanup is
`never`; therefore the discard flag has no practical effect.

`fsync_logs` and `scheduler_poll_seconds` are not exposed by the CLI and use
their Python defaults (`True` and `1.0`). Use Python when those values must be
changed. Read [Storage](Storage.md) before sizing the queue.

## `fetch-catalog`

```bash
ncbi-dataset fetch-catalog \
  --workspace /data/ncbi-workspace \
  --email researcher@example.org \
  --query '"ATAC-seq"[Strategy] AND "Homo sapiens"[Organism]' \
  --output /data/catalogs/human-atac.csv
```

### Command-specific options

| Option | Required/default | Meaning |
| --- | --- | --- |
| `--query EXPR` | Required | NCBI SRA expression |
| `--output PATH` | Required | Destination CSV; parent directories are created |
| `--refresh` | False | Bypass and replace query cache |

The command returns exit code `0` after writing the CSV. See
[Catalogs](Catalogs.md) for query/cache and catalog validation behavior.

## `build-local`

```bash
ncbi-dataset build-local \
  --workspace /data/ncbi-workspace \
  --catalog /data/catalogs/runinfo.csv \
  --processor ncbi_dataset_builder.processing.atac:process_atac \
  --total-cpus 32 \
  --min-cpus-per-job 4 \
  --max-cpus-per-job 8 \
  --max-running-jobs 2 \
  --reserve-free-gb 300 \
  --download-workers 1 \
  --max-inflight-gb 600 \
  --processing-storage-multiplier 2.5
```

### Local options

| Option | Required/default | Python field | Meaning/restriction |
| --- | --- | --- | --- |
| `--processor REFERENCE` | Required | `processor` | Importable `module:callable`; CLI cannot accept an in-memory callable |
| `--total-cpus N` | Required | `total_cpus` | Positive aggregate processor CPU pool |
| `--min-cpus-per-job N` | `1` | `min_cpus_per_job` | Positive minimum |
| `--max-cpus-per-job N` | Total CPUs | `max_cpus_per_job` | At least minimum and no greater than total |
| `--max-running-jobs N` | `1` | `max_running_jobs` | Positive processing concurrency |
| `--reserve-free-gb GB` | `0.0` | `FilesystemStorage.reserve_free_gb` | Non-negative free-space reserve |

Exit code is `1` if any unit failed and `0` otherwise. Matching skipped
failures do not increment `report.failed` because their outcome status is
`skipped`; inspect `status` and unit errors as well.

Read [Local execution](LocalExecution.md) for sizing and memory limitations.

## Shared Slurm options

Both Slurm commands accept:

| Option | Required/default | Python field | Meaning/restriction |
| --- | --- | --- | --- |
| `--partition VALUE` | Unset | `partition` | Optional comma-separated safe partition names |
| `--account VALUE` | Unset | `account` | Optional safe Slurm account |
| `--qos VALUE` | Unset | `qos` | Optional safe QoS |
| `--quota-gb GB` | Required | `QuotaStorage.quota_gb` | Positive total user/project quota |
| `--quota-reserve-gb GB` | `0.0` | `reserve_gb` | Non-negative and below quota |
| `--quota-usage-root PATH` | Workspace | `usage_root` | Directory recursively measured as used |
| `--script-path PATH` | Workspace default | `script_path` | Coordinator script destination |
| `--no-submit` | False | `submit=False` | Write execution/script without calling `sbatch` |

Use `--no-submit` on the first configuration and inspect the generated file.
The CLI does not print the returned script/job tuple; find the default script
under `workspace/slurm/`, or provide `--script-path`.

## `submit-single-node`

```bash
ncbi-dataset submit-single-node \
  --workspace /scratch/project-owner/ncbi-workspace \
  --catalog /scratch/project-owner/catalogs/runinfo.csv \
  --processor ncbi_dataset_builder.processing.atac:process_atac \
  --allocation-cpus 64 \
  --allocation-memory-gb 512 \
  --allocation-time-limit 2-00:00:00 \
  --min-cpus-per-job 4 \
  --max-cpus-per-job 16 \
  --memory-gb-per-job 80 \
  --max-running-jobs 4 \
  --quota-gb 5000 \
  --quota-reserve-gb 500 \
  --quota-usage-root /scratch/project-owner \
  --partition highmem \
  --download-workers 2 \
  --max-inflight-gb 1000 \
  --processing-storage-multiplier 2.5 \
  --no-submit
```

### Single-node options

| Option | Required/default | Python field | Meaning/restriction |
| --- | --- | --- | --- |
| `--processor REFERENCE` | Required | `processor_reference` | Importable on compute node |
| `--allocation-cpus N` | Required | `allocation_cpus` | Positive allocation `--cpus-per-task` |
| `--allocation-memory-gb GB` | Required | `allocation_memory_gb` | Positive allocation `--mem`, rounded up |
| `--allocation-time-limit TIME` | Required | `allocation_time_limit` | Whole queue wall time |
| `--min-cpus-per-job N` | `1` | `min_cpus_per_job` | Positive sample minimum |
| `--max-cpus-per-job N` | Allocation CPUs | `max_cpus_per_job` | Between minimum and allocation |
| `--memory-gb-per-job GB` | Required | `memory_gb_per_job` | Positive internal reservation, at most allocation memory |
| `--max-running-jobs N` | `1` | `max_running_jobs` | Positive in-allocation processor concurrency |

Remove `--no-submit` after inspecting the script. A successful `sbatch` call
returns CLI exit code `0`; later batch failure is observed through Slurm logs
and `status`, not the original CLI exit code.

Read [Single-node Slurm](SlurmSingleNodeExecution.md) for the distinction
between hard allocation memory and internal per-sample reservation.

## `submit-distributed`

```bash
ncbi-dataset submit-distributed \
  --workspace /scratch/project-owner/ncbi-workspace \
  --query '"ATAC-seq"[Strategy] AND "Mus musculus"[Organism]' \
  --email researcher@example.org \
  --processor ncbi_dataset_builder.processing.atac:process_atac \
  --total-cpu-quota 257 \
  --max-running-jobs 8 \
  --cpus-per-node 64 \
  --min-cpus-per-job 8 \
  --max-cpus-per-job 32 \
  --memory-gb-per-job 128 \
  --worker-time-limit 2-00:00:00 \
  --coordinator-cpus 1 \
  --coordinator-memory-gb 4 \
  --coordinator-time-limit 7-00:00:00 \
  --quota-gb 10000 \
  --quota-reserve-gb 1000 \
  --quota-usage-root /scratch/project-owner \
  --partition compute,highmem \
  --max-inflight-gb 2000 \
  --processing-storage-multiplier 2.5 \
  --no-submit
```

### Distributed options

| Option | Required/default | Python field | Meaning/restriction |
| --- | --- | --- | --- |
| `--processor REFERENCE` | Required | `processor_reference` | Importable on every worker |
| `--total-cpu-quota N` | Required | `total_cpu_quota` | Coordinator plus active worker CPU limit |
| `--max-running-jobs N` | Required | `max_running_jobs` | Worker job ceiling; coordinator is additional |
| `--cpus-per-node N` | Required | `cpus_per_node` | Hard upper bound for one worker CPU request |
| `--min-cpus-per-job N` | Required | `min_cpus_per_job` | Positive minimum that fits worker quota |
| `--max-cpus-per-job N` | Required | `max_cpus_per_job` | At least minimum; no greater than node ceiling |
| `--memory-gb-per-job GB` | Required | `memory_gb_per_job` | Hard common worker `--mem`, rounded up |
| `--worker-time-limit TIME` | Required | `worker_time_limit` | Common per-worker wall time |
| `--coordinator-cpus N` | `1` | `coordinator_cpus` | Included in total quota; below it |
| `--coordinator-memory-gb GB` | `4.0` | `coordinator_memory_gb` | Positive coordinator `--mem` |
| `--coordinator-time-limit TIME` | `7-00:00:00` | `coordinator_time_limit` | Whole campaign coordinator time |

`--download-workers` is accepted because the queue object is shared, but the
current distributed coordinator does not create a separate download pool.
Each admitted worker stages its own input; use `--max-running-jobs` and storage
limits to control simultaneous staging.

Remove `--no-submit` after inspection. The command exits `0` after successful
coordinator submission, not after the distributed dataset finishes.

Read [Distributed Slurm](SlurmDistributedExecution.md) for held-job
submission, monitoring, and restart restrictions.

## `status`

```bash
# Latest execution record.
ncbi-dataset status \
  --workspace /data/ncbi-workspace

# Specific execution.
ncbi-dataset status \
  --workspace /data/ncbi-workspace \
  --execution-id execution-20260913T120000Z-abc123
```

| Option | Default | Meaning |
| --- | --- | --- |
| `--execution-id ID` | Latest written execution | Select exact execution snapshot |

Output is a Python mapping containing:

- `counts` for pending/submitted/running/succeeded/failed;
- `units` with persisted state records; and
- `execution_id`.

The command exits `0` after printing. It does not wait for running Slurm jobs.

## `publish`

```bash
ncbi-dataset publish \
  --workspace /data/ncbi-workspace \
  --destination /data/model-dataset \
  --execution-id execution-20260913T120000Z-abc123 \
  --mode auto \
  --overwrite
```

| Option | Required/default | Meaning/restriction |
| --- | --- | --- |
| `--destination PATH` | Workspace | External compact dataset root or in-place publication |
| `--execution-id ID` | Latest | Execution snapshot to publish |
| `--mode MODE` | `auto` | `auto`, `hardlink`, or `copy` |
| `--overwrite` | False | Permit atomic replacement of existing external destination |

The command prints the exported manifest path and exits `0` on success.
Publication requires experiment grouping, successful states, exactly one SRA
Sample and BigWig per experiment, metadata descriptions, and consistent genome
taxonomy. See [Architecture](Architecture.md#publication).

## CLI-to-Python mapping

| CLI group | Python object/call |
| --- | --- |
| Shared builder flags | `BuilderConfig(...)` |
| Local resource flags | `LocalExecution(...)` |
| Single-node resource flags | `SlurmSingleNodeExecution(...)` |
| Distributed resource flags | `SlurmDistributedExecution(...)` |
| Quota flags | `QuotaStorage(...)` |
| Queue flags | `QueuePolicy(...)` |
| `--retry-failed` | `retry_failed=True` on build/submission |
| `--no-submit` | `submit=False` |

## Python-only features

Use the Python API when you need:

| Feature | Python parameter/object |
| --- | --- |
| Direct callable processor | `builder.build(catalog, callable, ...)` |
| Custom FASTQ provider or genome manager | `DatasetBuilder(..., fastq_provider=..., genome_manager=...)` |
| Explicit genome pins | `genome_pins={taxid: accession}` |
| Explicit dynamic processor identity | `processor_id=...` |
| Custom description profile/policy methods | `BuilderConfig` and metadata APIs |
| Queue log sync or poll interval | `fsync_logs`, `scheduler_poll_seconds` |
| Custom progress reporter | `DatasetBuilder(..., progress=...)` |
| Programmatic `BuildReport`/script job ID handling | Method return values |

## Exit codes

| Command | `0` | `1` |
| --- | --- | --- |
| `fetch-catalog` | CSV written | Unhandled error produces nonzero process failure |
| `build-local` | No returned unit outcome has status `failed` | At least one unit failed |
| Slurm submit commands | Script generation/submission call succeeded | Later batch failures do not change original CLI exit |
| `status` | Mapping printed | Unhandled error |
| `publish` | Manifest path printed | Unhandled error |

## Common CLI mistakes

| Symptom | Cause | Correction |
| --- | --- | --- |
| Parser rejects catalog arguments | Both or neither of `--catalog`/`--query` supplied | Supply exactly one |
| Live query says email required | No flag/environment email | Set `--email` or `NCBI_EMAIL` |
| Local CLI cannot find processor | Reference not importable | Use `module:callable` and install module |
| `--no-submit` seems silent | CLI does not print tuple | Look below `workspace/slurm/` or set `--script-path` |
| Distributed downloads exceed expectation | `--download-workers` is not distributed staging concurrency | Lower `--max-running-jobs` |
| Slurm command exits 0 but job later fails | CLI only submitted | Inspect logs and run `status` |
| Existing inputs remain despite discard flag | `--keep-inputs` sets cleanup to never | Remove `--keep-inputs` |

## Related pages

- [Documentation index](README.md)
- [Choosing an execution system](ExecutionSystems.md)
- [Local execution](LocalExecution.md)
- [Single-node Slurm](SlurmSingleNodeExecution.md)
- [Distributed Slurm](SlurmDistributedExecution.md)
- [CLI implementation reference](../src/ncbi_dataset_builder/cli/README.md)
