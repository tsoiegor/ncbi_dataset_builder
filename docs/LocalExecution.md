# Local execution on one server

Use `LocalExecution` when one machine owns the workspace and the calling Python
process may remain alive for the complete run. It works on a workstation,
dedicated server, or a login-like machine where running long work directly is
permitted.

The local scheduler overlaps input staging with processing, dynamically assigns
processor CPUs when a unit becomes ready, records state per unit, and reuses
validated successes.

## When this mode fits

| Situation | Fit |
| --- | --- |
| No Slurm is installed | Good |
| One server has enough CPU and storage | Good |
| Processor can run several external commands concurrently | Good |
| Per-sample RAM must be hard-enforced | Poor; local mode has no memory setting |
| The calling terminal/notebook may disconnect | Poor unless the process is managed by `tmux`, a service, or another supervisor |
| Processor is a notebook-local callable or custom Python object | Good; local mode accepts direct callables |
| Samples must run on several physical nodes | Not supported |

## Runtime topology

```text
calling Python process
├── download/staging thread pool       size = QueuePolicy.download_workers
│   ├── genome resolution
│   └── provider staging
└── processing thread pool             size = LocalExecution.max_running_jobs
    ├── FASTQ materialization
    └── processor(fastq, genome, allocated_cpus)
```

Both pools share the same machine, disks, network, and process memory.

## Before filling the template

Record these server facts:

| Question | Example | Used for |
| --- | --- | --- |
| CPUs permitted for this workflow | `32` | `total_cpus` |
| Smallest useful CPU count per sample | `4` | `min_cpus_per_job` |
| Largest useful CPU count per sample | `8` or `16` | `max_cpus_per_job` |
| Measured peak RAM per sample | `40 GB` | Manual concurrency calculation; not enforced |
| Server RAM safely available | `192 GB` | Manual `max_running_jobs` bound |
| Current free workspace storage | `1.6 TB` | `FilesystemStorage` |
| Storage that must remain free | `300 GB` | `reserve_free_gb` |
| Largest raw unit | `120 GB` | In-flight sizing |
| Peak processing footprint / raw size | `2.5` | `processing_storage_multiplier` |

If RAM has not been measured, begin with `max_running_jobs=1`.

## Complete Python template

```python
from pathlib import Path

from ncbi_dataset_builder import (
    BuilderConfig,
    DatasetBuilder,
    FilesystemStorage,
    LocalExecution,
    QueuePolicy,
)
from ncbi_dataset_builder.processing.atac import process_atac

# 1. Stable workspace and NCBI behavior.
config = BuilderConfig(
    workspace=Path("/data/ncbi-workspace"),
    email="researcher@example.org",
    ncbi_api_key=None,
    group_by="experiment",
    description_profile="training",
    prefetch_max_size="u",
    show_progress=True,
    progress_bars=True,
)
builder = DatasetBuilder(config)

# 2. Load an existing RunInfo CSV.
# Use builder.fetch_runs(...) instead when NCBI should create the catalog.
catalog = builder.load_runs(Path("/data/catalogs/runinfo.csv"))

# 3. Describe the resources this process may use.
execution = LocalExecution(
    total_cpus=32,
    min_cpus_per_job=4,
    max_cpus_per_job=8,
    max_running_jobs=2,
    storage=FilesystemStorage(
        reserve_free_gb=300,
    ),
)

# 4. Describe staging, estimated in-flight storage, and cleanup.
queue = QueuePolicy(
    download_workers=1,
    max_inflight_gb=600,
    processing_storage_multiplier=2.5,
    cleanup="after_success",
    keep_failed_inputs=True,
    fsync_logs=True,
    scheduler_poll_seconds=1.0,
)

# 5. Run and wait for every selected unit to become terminal.
report = builder.build(
    catalog,
    process_atac,
    execution=execution,
    queue=queue,
    group_by=None,
    genome_pins=None,
    query=None,
    retry_failed=False,
    processor_id=None,
)

print(report.execution_id)
print(report.succeeded, report.failed, report.skipped)
```

Replace the values only after reading the tables below.

## `BuilderConfig` parameters

| Parameter | Default | Meaning in local mode | Starting choice | Restriction |
| --- | --- | --- | --- | --- |
| `workspace` | Required | Local durable root for all package data | A dedicated large data volume | Must remain writable for the complete run |
| `email` | `None` | NCBI Entrez contact | Supply for live catalog/metadata/GEO requests | Existing CSV loading works without it |
| `ncbi_api_key` | `None` | Optional higher Entrez rate | Use your own environment/secret handling | Not a processing resource |
| `genome_policy` | Default policy | Assembly selection behavior | Start with default or pin exact assemblies in `build()` | Stable after workspace state exists |
| `group_by` | `"experiment"` | Default catalog entity per processor call | Usually experiment for ATAC | Only four supported values; stable after state exists |
| `description_profile` | `"training"` | Saved metadata projection | `training` for compact output | `training` or `full`; stable after state exists |
| `prefetch_max_size` | `"u"` | Per-SRA-run `prefetch` archive limit | Use `u` when queue storage is configured | Non-empty; too small can block a run |
| `show_progress` | `True` | Enables operation progress | Keep during first runs | Logs are separate |
| `progress_bars` | `True` | Requests tqdm display | Keep in interactive terminals | Falls back to text |

## `LocalExecution` parameters

| Parameter | Default | Exact meaning | How to choose | Restrictions |
| --- | --- | --- | --- | --- |
| `total_cpus: int` | Detected logical CPUs | Aggregate CPU budget used when assigning processor `threads` | Use the CPUs you are actually allowed to consume, not necessarily `os.cpu_count()` | Positive |
| `min_cpus_per_job: int` | `1` | Minimum CPU count needed before a ready unit may start; also used for provider staging/materialization | Smallest count at which the provider and processor work acceptably | Positive and no greater than maximum |
| `max_cpus_per_job: int \| None` | `None` → `total_cpus` | Ceiling for one processor launch | Measured useful scaling limit of the processor | Positive and no greater than `total_cpus` |
| `max_running_jobs: int` | `1` | Maximum simultaneous processor calls | Minimum of CPU, measured RAM, storage, and I/O-safe concurrency | Positive; downloads do not count |
| `storage: FilesystemStorage` | No reserve | Physical free-space admission policy | Set a deliberate reserve | Must be `FilesystemStorage` |

### CPU example

With:

```text
total_cpus = 32
min_cpus_per_job = 4
max_cpus_per_job = 8
max_running_jobs = 3
```

three schedulable ready units can each receive at most 8 CPUs. If only one unit
is ready, it still receives at most 8. If two 8-CPU units are running, 16 CPUs
remain available for later work. Allocations are selected when a unit starts
and are not resized while it runs.

### CPU-bound Python restriction

The processing pool uses Python threads. External tools such as Bowtie2,
samtools, and fastp run in subprocesses and can use parallel CPUs. A processor
implemented as CPU-bound pure Python may be limited by the Python GIL despite a
larger `threads` value. Such a processor must create its own multiprocessing or
native parallelism if required.

## `FilesystemStorage` parameters

| Parameter | Default | Meaning | Starting choice |
| --- | --- | --- | --- |
| `reserve_free_gb` | `0.0` | Space subtracted from current free space before admission | Operating-system/data safety margin plus expected unmodeled growth |

The scheduler compares a candidate’s estimated raw size with:

```text
current filesystem free GB - reserve_free_gb
```

It does not enforce a user quota. See the
[full storage guide](Storage.md#filesystemstorage).

## `QueuePolicy` parameters

| Parameter | Default | Local behavior | Starting choice | Restriction |
| --- | --- | --- | --- | --- |
| `download_workers` | `2` | Maximum simultaneous genome/input staging operations | `1`; increase after measuring network and storage | Positive |
| `max_inflight_gb` | `None` | Estimated raw/processing window across downloading, ready, and processing units | Enough for one or two largest units | Positive when set |
| `processing_storage_multiplier` | `1.0` | Processing unit peak total size / raw catalog size | Measure; begin conservatively at `2`–`4` for alignment | At least `1` |
| `cleanup` | `"after_success"` | Removes provider roots below `workspace/fastq/` after verified success | Keep default if input is reproducible | `"after_success"` or `"never"` |
| `keep_failed_inputs` | `True` | Preserves staged inputs after processor failure | Keep during validation | Ignored when cleanup is `"never"` |
| `fsync_logs` | `True` | Synchronizes phase-boundary unit logs | Keep for first/long runs | Turning off weakens immediate durability |
| `scheduler_poll_seconds` | `1.0` | Maximum idle wait between queue checks | Keep default | Positive |

See [Storage](Storage.md#queuepolicy) for exact formulas and cleanup matrix.

## Build-call parameters

| Parameter | Required | Meaning | How to choose |
| --- | --- | --- | --- |
| `catalog` | Yes | Runs that will be deduplicated and grouped | Start with a small representative catalog |
| `processor` | Yes | Direct callable or import string accepting `(fastq, genome, threads)` | Use the built-in ATAC callable or your tested processor |
| `execution` | No | Local resource policy | Pass explicitly; default is one-job `LocalExecution()` |
| `queue` | No | Streaming and cleanup policy | Pass explicitly for a production run |
| `group_by` | No | Per-call grouping override | Usually leave `None` and set `BuilderConfig.group_by` |
| `genome_pins` | No | Mapping from integer taxonomy ID to exact assembly accession | Use when reference identity must be fixed |
| `query` | No | Source-query provenance stored in the record | Supply if the catalog originated from a known query |
| `retry_failed` | `False` | Reclaim matching failed state | Set `True` after diagnosing/correcting the failure |
| `processor_id` | `None` | Explicit semantic identity for a dynamic callable | Set a version string when automatic source/config identity is insufficient |

`builder.build()` returns only after every unit is succeeded, failed, or
skipped.

## How a unit moves through local execution

1. The catalog is deduplicated and grouped.
2. An execution record is written.
3. The unit is claimed in atomic state.
4. Its genome is resolved and provider input is staged in the download pool.
5. It enters the ready list.
6. CPU, memory-free job count, and storage rules are evaluated.
7. Provider input is materialized.
8. The processor receives `FastqSet`, `GenomeRef`, and allocated CPUs.
9. Declared output files are validated and checksummed.
10. Success/failure state is written.
11. Provider input cleanup follows `QueuePolicy`.
12. The workspace manifest is synchronized.

## Memory planning

Local execution does not accept or enforce memory fields. Calculate a manual
upper bound:

```text
RAM-safe jobs =
floor((RAM available to workflow - safety reserve) / measured peak RAM per job)
```

Then choose:

```text
max_running_jobs <= min(
    RAM-safe jobs,
    floor(total_cpus / min_cpus_per_job),
    I/O-safe jobs,
    desired concurrency,
)
```

If a hard memory request or per-job cgroup is required, use
[single-node Slurm](SlurmSingleNodeExecution.md) or
[distributed Slurm](SlurmDistributedExecution.md).

## Selecting a first catalog

```python
import polars as pl

# Keep two representative experiments for the first run.
small = (
    catalog
    .filter(pl.col("LibraryLayout") == "PAIRED", description="paired only")
)
first_ids = (
    small.frame
    .select("Experiment")
    .unique(maintain_order=True)
    .head(2)
    .get_column("Experiment")
    .to_list()
)
small = small.filter(
    pl.col("Experiment").is_in(first_ids),
    description="first two experiments",
)
```

Review [Catalogs](Catalogs.md) before changing grouping or discarding rows.

## Inspecting results

```python
print(report.execution_id)
for outcome in report.outcomes:
    print(outcome.unit_id, outcome.status, outcome.error)

status = builder.status(report.execution_id)
print(status["counts"])
```

Use unit logs below `workspace/logs/<species>/` for failure diagnosis. Successful
reuse appears as `skipped`, with the persisted result attached.

## Retry and restart

| Situation | Action |
| --- | --- |
| Python process was interrupted | Run the same call again; local coordinator reclaims interrupted running state |
| Matching unit failed | Diagnose logs, then set `retry_failed=True` |
| Output was deleted/corrupted | Same call detects invalid success and rebuilds |
| Processor behavior changed | Update source/config or set a new `processor_id` so fingerprint changes |
| Grouping/genome policy must change | Use a new workspace once existing unit state is present |

Do not manually delete all state as a first response. The workspace is designed
to preserve valid unit successes.

## Tuning sequence

1. Run one representative unit with `max_running_jobs=1`.
2. Measure peak RAM, elapsed time, CPU utilization, and peak disk usage.
3. Increase `max_cpus_per_job` until additional CPUs stop helping materially.
4. Compute a RAM-safe job count manually.
5. Increase `max_running_jobs` without exceeding CPU, RAM, and I/O limits.
6. Increase `download_workers` only if processors are waiting for input.
7. Refine `processing_storage_multiplier` and `max_inflight_gb` from observed
   disk growth.

## Common configuration errors

| Symptom | Cause | Correction |
| --- | --- | --- |
| Constructor rejects CPU values | Minimum exceeds maximum, or maximum exceeds total | Reconcile all three CPU fields |
| Queue cannot admit a unit | CPU minimum, storage capacity, or in-flight estimate blocks it | Check the named unit’s size and configured reserves |
| Server swaps or becomes unresponsive | Local mode does not model RAM | Reduce `max_running_jobs` or use Slurm |
| More CPUs do not speed up a sample | Processor/tool scaling ceiling | Lower `max_cpus_per_job` and run more samples concurrently |
| NCBI/storage becomes overloaded | Too many concurrent stages | Reduce `download_workers` |
| Successful provider FASTQs disappear | Default post-success cleanup | Set `cleanup="never"` when they must be retained |
| Failed unit is not retried | Matching failure is protected by default | Set `retry_failed=True` after diagnosis |

## Hard restrictions

- One machine only.
- No hard memory enforcement.
- The calling Python process must remain alive.
- `LocalExecution.storage` is `FilesystemStorage`.
- CPU allocations are integers and fixed for the duration of each processor
  call.
- Pure-Python CPU work does not automatically become parallel.
- Storage admission is based on catalog estimates and point-in-time free space.

## Related pages

- [Choosing an execution system](ExecutionSystems.md)
- [Storage and queue policy](Storage.md)
- [Writing a processor](Processors.md)
- [ATAC-seq processing](AtacSeqProcessing.md)
- [Local CLI command](CommandLineInterface.md#build-local)
- [Execution API reference](../src/ncbi_dataset_builder/execution/README.md)
