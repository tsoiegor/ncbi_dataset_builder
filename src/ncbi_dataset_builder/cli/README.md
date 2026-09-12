# Command-line implementation

The `ncbi_dataset_builder.cli` subpackage maps shell commands onto the same
`BuilderConfig`, `DatasetBuilder`, execution, storage, and queue classes
used by the Python API.

Installation creates `ncbi-dataset`. Running
`python -m ncbi_dataset_builder` is equivalent. The user-facing complete
reference is [Command-line interface](../../../docs/CommandLineInterface.md);
this page explains exact argument-to-object mapping.

## Module map

| Module | Contents |
| --- | --- |
| `main.py` | Argument groups, parser construction, object factories, and `main(argv=None)` |
| `__init__.py` | Re-exports `main` |
| `../__main__.py` | Calls `cli.main()` for `python -m` |

## Commands

| Command | Python operation | Exit behavior |
| --- | --- | --- |
| `fetch-catalog` | `builder.fetch_runs()`, then Polars `write_csv()` | 0 on success |
| `build-local` | `builder.build(..., LocalExecution(...))` | 1 when any unit failed, otherwise 0 |
| `submit-single-node` | `builder.submit_slurm(..., SlurmSingleNodeExecution(...))` | 0 after script creation/submission |
| `submit-distributed` | `builder.submit_slurm(..., SlurmDistributedExecution(...))` | 0 after coordinator script creation/submission |
| `status` | `builder.status()` | Prints a Python mapping |
| `publish` | `builder.publish_dataset()` | Prints manifest path |

## Stable builder arguments

Every command accepts:

| Option | Default | `BuilderConfig` field |
| --- | --- | --- |
| `--workspace PATH` | required | `workspace` |
| `--email ADDRESS` | `NCBI_EMAIL` | `email` |
| `--ncbi-api-key KEY` | `NCBI_API_KEY` | `ncbi_api_key` |
| `--group-by LEVEL` | `experiment` | `group_by` |
| `--prefetch-max-size VALUE` | `u` | `prefetch_max_size` |

`LEVEL` is `run`, `experiment`, `sra_sample`, or `biosample`.

The API key is not embedded directly into generated Slurm command lines.
Workers read `NCBI_API_KEY` from their environment. The email is included as
an explicit worker argument when configured.

## Catalog-source arguments

Processing commands require exactly one:

| Option | Behavior |
| --- | --- |
| `--catalog PATH` | `builder.load_runs(PATH)`; no NCBI credentials needed |
| `--query EXPR` | `builder.fetch_runs(EXPR, refresh=...)`; email required |

`--refresh` applies only to the live query path.

## Queue arguments

| Option | Default | `QueuePolicy` field |
| --- | --- | --- |
| `--download-workers N` | 2 | `download_workers` |
| `--max-inflight-gb GB` | unset | `max_inflight_gb` |
| `--processing-storage-multiplier X` | 1.0 | `processing_storage_multiplier` |
| `--keep-inputs` | false | Sets `cleanup="never"`; otherwise `"after_success"` |
| `--discard-failed-inputs` | false | Sets `keep_failed_inputs=False` |
| `--retry-failed` | false | Passed to build/coordinator |

`fsync_logs` and `scheduler_poll_seconds` currently keep their Python API
defaults; the CLI has no flags for them.

# Command details

## `fetch-catalog`

```bash
# Fetch one complete SRA RunInfo query and write it as CSV.
ncbi-dataset fetch-catalog +  --workspace /data/ncbi-workspace +  --email researcher@example.org +  --query '"ATAC-seq"[Strategy] AND "Homo sapiens"[Organism]' +  --output runinfo.csv
```

`--query` and `--output` are required. Add `--refresh` to bypass the
matching workspace catalog cache.

## `build-local`

```bash
# Run the streaming queue in this process on an ordinary server.
ncbi-dataset build-local +  --workspace /data/ncbi-workspace +  --catalog runinfo.csv +  --processor my_pipeline.processors:process_sample +  --total-cpus 64 +  --min-cpus-per-job 4 +  --max-cpus-per-job 16 +  --max-running-jobs 6 +  --reserve-free-gb 300 +  --download-workers 3 +  --max-inflight-gb 900 +  --processing-storage-multiplier 2
```

| Option | Default | `LocalExecution` meaning |
| --- | --- | --- |
| `--processor REF` | required | Callable import reference |
| `--total-cpus N` | required | Aggregate processing CPU budget |
| `--min-cpus-per-job N` | 1 | Launch minimum |
| `--max-cpus-per-job N` | total CPUs | Per-unit ceiling |
| `--max-running-jobs N` | 1 | Processing concurrency |
| `--reserve-free-gb GB` | 0 | `FilesystemStorage.reserve_free_gb` |

The CLI always loads the processor by import reference even though the Python
API can accept an in-memory callable.

## Shared Slurm options

Both submission commands accept:

| Option | Meaning |
| --- | --- |
| `--partition NAME[,NAME]` | Optional partition directive |
| `--account NAME` | Optional account directive |
| `--qos NAME` | Optional QoS directive |
| `--quota-gb GB` | Required total quota for `QuotaStorage` |
| `--quota-reserve-gb GB` | Capacity to leave unused; default 0 |
| `--quota-usage-root PATH` | Directory counted against quota; defaults to workspace |
| `--script-path PATH` | Override coordinator script destination |
| `--no-submit` | Write the execution snapshot and script without calling `sbatch` |

## `submit-single-node`

```bash
# Request one allocation, then run several sample processors inside it.
ncbi-dataset submit-single-node +  --workspace /scratch/project-owner/ncbi-workspace +  --catalog runinfo.csv +  --processor ncbi_dataset_builder.processing.atac:process_atac +  --allocation-cpus 128 +  --allocation-memory-gb 1000 +  --allocation-time-limit 2-00:00:00 +  --min-cpus-per-job 8 +  --max-cpus-per-job 32 +  --memory-gb-per-job 100 +  --max-running-jobs 8 +  --quota-gb 5000 +  --quota-reserve-gb 250 +  --quota-usage-root /scratch/project-owner +  --partition amd_1Tb,amd_2Tb
```

Required mode-specific options are allocation CPUs, memory, time, per-job
memory, and processor. Per-job CPU bounds and maximum running jobs default as
in the Python class.

## `submit-distributed`

```bash
# Submit a coordinator that launches one independent job per active sample.
ncbi-dataset submit-distributed +  --workspace /scratch/project-owner/ncbi-workspace +  --catalog runinfo.csv +  --processor ncbi_dataset_builder.processing.atac:process_atac +  --total-cpu-quota 500 +  --max-running-jobs 50 +  --cpus-per-node 128 +  --min-cpus-per-job 8 +  --max-cpus-per-job 64 +  --memory-gb-per-job 100 +  --worker-time-limit 3-00:00:00 +  --coordinator-cpus 1 +  --coordinator-memory-gb 4 +  --coordinator-time-limit 7-00:00:00 +  --quota-gb 5000 +  --quota-reserve-gb 250 +  --quota-usage-root /scratch/project-owner
```

All worker resource fields are required. Coordinator defaults are 1 CPU, 4 GB,
and seven days.

## `status`

```bash
# Latest execution in the workspace.
ncbi-dataset status --workspace /data/ncbi-workspace

# One explicit immutable execution snapshot.
ncbi-dataset status +  --workspace /data/ncbi-workspace +  --execution-id execution-20260912-abc123
```

Stable builder arguments are accepted because the same builder initializes the
workspace, but status reads saved execution and unit state.

## `publish`

```bash
# Publish the latest verified experiment execution.
ncbi-dataset publish +  --workspace /data/ncbi-workspace +  --destination /data/model-dataset +  --mode auto
```

| Option | Meaning |
| --- | --- |
| `--destination PATH` | Separate target; omitted means in-place publication. |
| `--execution-id ID` | Explicit snapshot; omitted selects the latest. |
| `--mode auto|hardlink|copy` | File materialization policy. |
| `--overwrite` | Permit atomic replacement of an existing separate target. |

# Python entry point

```python
from ncbi_dataset_builder.cli import main

# Supplying argv makes the entry point testable without modifying sys.argv.
exit_code = main(
    [
        "status",
        "--workspace",
        "/data/ncbi-workspace",
    ]
)
```

`main(argv=None) -> int` builds the parser, creates `DatasetBuilder`, maps
arguments to dataclasses, performs one command, and returns the exit code.

Underscore-prefixed parser and factory functions—`_add_builder_arguments`,
`_add_catalog_arguments`, `_add_queue_arguments`,
`_add_slurm_arguments`, `_parser`, `_builder`, `_catalog`, and
`_quota`—are internal.
