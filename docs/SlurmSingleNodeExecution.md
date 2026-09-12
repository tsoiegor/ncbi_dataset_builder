# Single-node Slurm execution

`SlurmSingleNodeExecution` submits one Slurm job. That job owns one allocation
and runs the package’s streaming scheduler inside it. Several units may process
concurrently, but all share the same node, wall time, memory allocation,
filesystem, and Python process.

Use this mode when one node is large enough for the desired concurrency and a
single allocation is operationally simpler than one scheduler job per sample.

## When this mode fits

| Situation | Fit |
| --- | --- |
| One node has enough CPU and RAM for several samples | Good |
| Cluster limits the number of submitted jobs | Good |
| Downloads and processing should overlap inside one allocation | Good |
| Samples require different partitions or memory requests | Poor; all share one allocation |
| One very slow sample could outlive the allocation | Risk; wall time applies to the whole queue |
| Samples must spread over many nodes | Use distributed Slurm |
| Processor exists only in a notebook | Not supported until packaged as an importable callable |

## Runtime topology

```text
submission process
└── sbatch coordinator script
    └── one Slurm allocation
        ├── download/staging thread pool
        │   size = QueuePolicy.download_workers
        └── processing thread pool
            size = SlurmSingleNodeExecution.max_running_jobs
```

Slurm hard-enforces the allocation-level `--cpus-per-task`, `--mem`, and
`--time`. Inside the allocation, the package performs its own CPU, per-sample
memory-reservation, job-count, and storage admission.

## Before filling the template

Obtain these values from cluster documentation and a representative sample:

| Question | Used for |
| --- | --- |
| Which partition has a node large enough? | `partition` |
| Maximum CPUs available on one chosen node | `allocation_cpus` |
| Maximum usable memory on that node/partition | `allocation_memory_gb` |
| Maximum permitted wall time | `allocation_time_limit` |
| Smallest/largest useful processor CPU counts | Per-job CPU limits |
| Measured peak memory for one sample | `memory_gb_per_job` |
| User/project storage quota and current usage root | `QuotaStorage` |
| Peak processing footprint relative to raw input | Queue multiplier |
| Number of samples that safely fit by CPU, RAM, and I/O | `max_running_jobs` |

Begin with one processing unit and `max_running_jobs=1`.

## Complete Python template

```python
from pathlib import Path

from ncbi_dataset_builder import (
    BuilderConfig,
    DatasetBuilder,
    QueuePolicy,
    QuotaStorage,
    SlurmSingleNodeExecution,
)

# Stable workspace and NCBI behavior.
builder = DatasetBuilder(
    BuilderConfig(
        workspace=Path("/scratch/project-owner/ncbi-workspace"),
        email="researcher@example.org",
        ncbi_api_key=None,
        group_by="experiment",
        description_profile="training",
        prefetch_max_size="u",
        show_progress=True,
        progress_bars=True,
    )
)

# The CSV is read on the submission node and saved into an execution snapshot.
catalog = builder.load_runs(Path("/scratch/project-owner/catalogs/runinfo.csv"))

execution = SlurmSingleNodeExecution(
    allocation_cpus=64,
    allocation_memory_gb=512,
    allocation_time_limit="2-00:00:00",
    min_cpus_per_job=4,
    max_cpus_per_job=16,
    memory_gb_per_job=80,
    max_running_jobs=4,
    partition="highmem",
    account=None,
    qos=None,
    storage=QuotaStorage(
        quota_gb=5_000,
        reserve_gb=500,
        usage_root=Path("/scratch/project-owner"),
    ),
)

queue = QueuePolicy(
    download_workers=2,
    max_inflight_gb=1_000,
    processing_storage_multiplier=2.5,
    cleanup="after_success",
    keep_failed_inputs=True,
    fsync_logs=True,
    scheduler_poll_seconds=1.0,
)

# First use submit=False. Inspect the generated script before submitting.
script, job_id = builder.submit_slurm(
    catalog,
    processor_reference="ncbi_dataset_builder.processing.atac:process_atac",
    execution=execution,
    queue=queue,
    group_by=None,
    genome_pins=None,
    query=None,
    retry_failed=False,
    script_path=None,
    submit=False,
)

assert job_id is None
print(script)
print(script.read_text())
```

After inspection, repeat with `submit=True`.

## `BuilderConfig` parameters

| Parameter | Default | Meaning here | How to choose | Restriction |
| --- | --- | --- | --- | --- |
| `workspace` | Required | Shared durable root used by submit process and allocation | Place directly on shared cluster storage | Same resolved absolute path must exist on both sides |
| `email` | `None` | NCBI contact passed into the generated worker command | Supply for live NCBI/provider work | Required for live catalog/metadata methods |
| `ncbi_api_key` | `None` | Higher Entrez request rate | Export `NCBI_API_KEY` into batch environment if needed | The generated script does not pass the key as a command argument |
| `genome_policy` | Default | Submission-side semantic identity and intended assembly selection | Prefer defaults or exact `genome_pins` for Slurm reproducibility | Standard worker reconstruction currently uses default manager/policy |
| `group_by` | `"experiment"` | Processing-unit grouping stored in execution | Usually experiment | Stable after state exists |
| `description_profile` | `"training"` | Workspace metadata semantic setting | Choose before first state | Standard worker reconstruction uses default profile; workspace remains configured at submission |
| `prefetch_max_size` | `"u"` | Submission-side SRA archive limit setting | Use default or ensure worker behavior matches requirements | Standard worker reconstruction currently creates the default provider |
| `show_progress` | `True` | Submission-process display | Any | Worker creates its standard reporter |
| `progress_bars` | `True` | Submission-process bar preference | Any | Batch logs may render plain progress more cleanly |

### Current worker-reconstruction restriction

The generated single-node command passes workspace, email, grouping through the
execution record, retry choice, and processor reference. It creates a new
`DatasetBuilder` in the allocation with the default SRA provider and genome
manager. Custom provider/manager objects attached to the submitting builder
are not serialized. Validate non-default acquisition/genome requirements before
using this mode.

## `SlurmSingleNodeExecution` parameters

| Parameter | Required/default | Exact meaning | How to choose | Restriction |
| --- | --- | --- | --- | --- |
| `allocation_cpus: int` | Required | Slurm `--cpus-per-task` for the one coordinator allocation and internal CPU pool | CPUs granted on one chosen node | Positive |
| `allocation_memory_gb: float` | Required | Slurm `--mem`, rounded up to whole GB; also internal total memory budget | Node memory safely available to this job | Positive |
| `allocation_time_limit: str` | Required | Slurm `--time` for the complete queue | Worst total allocation duration, including downloads and all samples | Digits, colons, hyphens; site/partition must accept it |
| `min_cpus_per_job: int` | `1` | Minimum processor allocation and provider stage/materialization CPU value | Smallest useful processor/tool count | Positive |
| `max_cpus_per_job: int \| None` | `None` → allocation CPUs | Per-processor dynamic ceiling | Measured useful scaling limit | At least minimum; no greater than allocation |
| `memory_gb_per_job: float` | `1.0` | Internal reservation for each active processor | Measured peak RSS plus safety margin | Positive; no greater than allocation memory |
| `max_running_jobs: int` | `1` | Maximum simultaneous processor calls inside allocation | Minimum CPU/RAM/I/O-safe count | Positive |
| `partition: str \| None` | `None` | Optional comma-separated Slurm partition directive | Partitions providing the requested node | Safe identifier characters only |
| `account: str \| None` | `None` | Optional Slurm account | Site allocation/account | Safe identifier characters only |
| `qos: str \| None` | `None` | Optional Slurm QoS | Site-specific policy | Safe identifier characters only |
| `storage: QuotaStorage` | Required | Quota-aware physical admission | Real user/project quota and usage root | Must be `QuotaStorage` |

## Resource-admission math

Compute independent upper bounds:

```text
CPU-safe jobs =
floor(allocation_cpus / min_cpus_per_job)

memory-safe jobs =
floor(allocation_memory_gb / memory_gb_per_job)

effective configured ceiling =
min(CPU-safe jobs, memory-safe jobs, max_running_jobs)
```

Storage and in-flight checks may reduce concurrency further.

### Worked example

```text
allocation_cpus       = 64
min_cpus_per_job      = 4
allocation_memory_gb  = 512
memory_gb_per_job     = 80
max_running_jobs      = 4

CPU-safe jobs         = floor(64 / 4)   = 16
memory-safe jobs      = floor(512 / 80) = 6
configured ceiling    = min(16, 6, 4)   = 4
```

With four ready jobs, each processor is capped by `max_cpus_per_job=16`, so
four 16-CPU processors can fill the allocation.

### Meaning of `memory_gb_per_job`

This field is not a separate Slurm `--mem` request for each in-process sample.
It is accounting used by the scheduler to avoid starting more processors than
the allocation memory should hold. All processors remain in one Slurm job and
share its hard allocation memory limit.

If a processor uses more than its assumed reservation, Slurm may kill the
entire allocation, not only that unit.

## `QuotaStorage` parameters

| Parameter | Default | Meaning | How to choose |
| --- | --- | --- | --- |
| `quota_gb` | Required | Total writable user/project quota | Site-reported quota |
| `reserve_gb` | `0.0` | Capacity intentionally unavailable to queue | Safety margin plus unrelated files |
| `usage_root` | Workspace | Directory recursively counted as current usage | Actual root covered by the quota |

The package computes:

```text
quota_gb - reserve_gb - measured files below usage_root
```

It does not ask a site-specific quota command. See
[QuotaStorage details](Storage.md#quotastorage).

## `QueuePolicy` parameters in this mode

| Parameter | Default | Single-node behavior | Starting choice |
| --- | --- | --- | --- |
| `download_workers` | `2` | Size of staging pool inside allocation | `1`–`2` |
| `max_inflight_gb` | `None` | Estimated downloading + ready + processing window | Enough for one or two largest units |
| `processing_storage_multiplier` | `1.0` | Multiplies raw size only while processing | Measured value, often initially `2`–`4` |
| `cleanup` | `"after_success"` | Provider input cleanup after validated success | Default if reacquisition is acceptable |
| `keep_failed_inputs` | `True` | Retains failed inputs | Keep while stabilizing |
| `fsync_logs` | `True` | Durable unit phase logs | Keep |
| `scheduler_poll_seconds` | `1.0` | Maximum internal queue wait | Keep unless polling is noisy |

Download/staging threads are not deducted from `allocation_cpus` or
`allocation_memory_gb` by the internal admission math. The default SRA stage is
primarily download/validation work, but a custom CPU- or memory-heavy staging
provider can oversubscribe the allocation. Reduce `download_workers` in that
case.

Full formulas and cleanup rules are in [Storage](Storage.md).

## Submission parameters

| Parameter | Meaning | Starting choice |
| --- | --- | --- |
| `catalog` | Catalog captured into execution record | One or two representative experiments |
| `processor_reference` | Importable `module:object` callable | Built-in ATAC reference or packaged custom processor |
| `execution` | This single-node configuration | Explicit object |
| `queue` | Streaming/cleanup policy | Conservative explicit object |
| `group_by` | Optional per-call override | `None` |
| `genome_pins` | Exact assembly by taxonomy ID | Use for strict reference reproducibility |
| `query` | Optional provenance | Original query when applicable |
| `retry_failed` | Reclaim matching failures | `False` until failures are diagnosed |
| `script_path` | Custom coordinator script path | `None` to use workspace |
| `submit` | Whether to call `sbatch` | `False` first, then `True` |

## Generated script

The coordinator script includes:

| Directive/command | Source |
| --- | --- |
| `--job-name=ncbi-dataset` | Fixed package name |
| `--cpus-per-task` | `allocation_cpus` |
| `--mem` | Ceiling of `allocation_memory_gb` in GB |
| `--time` | `allocation_time_limit` |
| `--partition`, `--account`, `--qos` | Optional execution fields |
| stdout/stderr | One append-mode `%j.coordinator.log` |
| Python executable | Absolute submitting interpreter |
| execution JSON | Absolute saved record path |
| workspace | Absolute workspace path |
| processor | Import reference |
| email/retry | Added only when supplied |

Inspect all of these with `submit=False`.

## Path and environment requirements

| Requirement | Why |
| --- | --- |
| Shared workspace has identical absolute path | Script contains the resolved path |
| Submitting Python executable exists on compute node | Script invokes it directly |
| Package and processor import there | Worker reconstructs them |
| SRA/genome/processor commands are on batch `PATH` | Generated script does not activate Conda/modules |
| `NCBI_API_KEY` is exported when required | Worker reads it from environment |
| Quota `usage_root` is visible in allocation | Capacity scan happens there |

## Run, monitor, and retry

```python
# Submit after the dry-run script is correct.
script, job_id = builder.submit_slurm(
    catalog,
    processor_reference="ncbi_dataset_builder.processing.atac:process_atac",
    execution=execution,
    queue=queue,
    submit=True,
)
print(job_id)
```

Use site tools such as `squeue`/`sacct` and inspect:

```text
workspace/logs/slurm/<job-id>.coordinator.log
workspace/logs/<species>/<unit-id>.log
workspace/state/units/<unit-id>.json
```

If the allocation ends early, submit the same semantic work again. Valid
successes are reused. Matching failures require `retry_failed=True`.

## Tuning sequence

1. Run one sample with generous memory and time.
2. Measure peak RSS and total elapsed time.
3. Set `memory_gb_per_job` above measured peak.
4. Find useful per-sample CPU scaling.
5. Compute CPU-safe and memory-safe concurrency.
6. Set `max_running_jobs` to the smaller value.
7. Ensure the complete catalog can finish within one allocation time.
8. Increase `download_workers` only if processors wait for staging.
9. Refine storage window from measured peak disk use.

## Common configuration errors

| Symptom | Cause | Correction |
| --- | --- | --- |
| Constructor rejects resources | Per-job CPU/memory exceeds allocation or values are non-positive | Recalculate the table |
| Allocation is killed for memory | Actual aggregate RAM exceeded `--mem` | Raise allocation memory, raise per-job reservation, or lower concurrency |
| Allocation times out with units unfinished | One wall time covers the entire queue | Request longer time, reduce catalog, or use distributed mode |
| CPUs are idle | Maximum per job or ready cohort is small; inputs may not be ready | Inspect unit phases and staging throughput |
| Node is oversubscribed during staging | Download provider consumes unaccounted CPU/RAM | Reduce `download_workers` |
| Import fails only in batch | Python/path environment differs | Test exact generated interpreter in `srun` |
| Queue says storage blocked despite global free space | User quota/usage root is restrictive | Correct `QuotaStorage`; do not use global `df` |

## Hard restrictions

- Exactly one Slurm allocation and therefore one node.
- One hard allocation memory limit, not independent sample cgroups.
- One allocation wall time covers all downloads and processors.
- Direct callable processors are not accepted; use an import reference.
- Custom provider and genome-manager objects are not serialized to the worker.
- Download activity is not subtracted from internal processing CPU/memory
  admission.
- All runtime paths and tools must be available inside the allocation.

## Related pages

- [Choosing an execution system](ExecutionSystems.md)
- [Storage and queue policy](Storage.md)
- [Distributed Slurm comparison](SlurmDistributedExecution.md)
- [ATAC-seq processing](AtacSeqProcessing.md)
- [Single-node CLI command](CommandLineInterface.md#submit-single-node)
- [Execution API reference](../src/ncbi_dataset_builder/execution/README.md)
