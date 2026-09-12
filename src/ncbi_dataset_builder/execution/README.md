# Execution and scheduling

The `ncbi_dataset_builder.execution` subpackage defines storage admission,
sample-streaming policy, and the three supported execution systems. It also
contains durable queue/state records and Slurm script generation used
internally by `DatasetBuilder`.

Read [Choosing an execution system](../../../docs/ExecutionSystems.md) first
for user-oriented selection and path examples. This page is the exact API
reference.

## Module map

| Module | Contents |
| --- | --- |
| `config.py` | Storage protocols, `QueuePolicy`, three execution dataclasses, and serialization |
| `records.py` | Internal `UnitResources`, `QueueItem`, and `ExecutionRecord` |
| `state.py` | Atomic per-unit `UnitStateStore` |
| `slurm.py` | `SlurmExecutor` script generation and scheduler commands |
| `storage.py` | Recursive decimal-GB measurement helpers |
| `single_node_worker.py` | Internal entry point for one allocation |
| `distributed_worker.py` | Internal multi-job coordinator |
| `sample_worker.py` | Internal entry point for one distributed sample job |
| `__init__.py` | Public config-class export list |

## Shared contracts

### `StoragePolicy`

A typing `Protocol`, not a required runtime base class:

```python
available_gb(workspace: Path) -> float
```

The queue asks this method how many decimal gigabytes are currently usable.
`FilesystemStorage` and `QuotaStorage` implement it.

### `FilesystemStorage`

```python
FilesystemStorage(reserve_free_gb=0.0)
```

| Argument | Meaning |
| --- | --- |
| `reserve_free_gb: float` | Non-negative free space that must remain on the filesystem containing the workspace. |

`available_gb(workspace)` creates the workspace if needed, reads filesystem
free space, subtracts the reserve, and never returns a negative value. Use this
for local storage where system free space reflects what the process may write.

### `QuotaStorage`

```python
QuotaStorage(
    quota_gb,
    reserve_gb=0.0,
    usage_root=None,
)
```

| Argument | Meaning |
| --- | --- |
| `quota_gb: float` | Positive total user or project quota in decimal GB. |
| `reserve_gb: float` | Non-negative capacity to keep unused; must be below the quota. |
| `usage_root: Path | None` | Directory whose current recursive file size counts against quota; `None` uses the workspace. |

`available_gb(workspace)` returns
`max(0, quota_gb - reserve_gb - measured_usage)`. It does not query Slurm or
a site-specific quota service.

## `QueuePolicy`

```python
QueuePolicy(
    download_workers=2,
    max_inflight_gb=None,
    processing_storage_multiplier=1.0,
    cleanup="after_success",
    keep_failed_inputs=True,
    fsync_logs=True,
    scheduler_poll_seconds=1.0,
)
```

| Argument | Meaning |
| --- | --- |
| `download_workers: int` | Positive maximum simultaneous unit downloads. |
| `max_inflight_gb: float | None` | Optional positive estimated window for downloading, ready, and processing units. |
| `processing_storage_multiplier: float` | Estimated peak total processor storage divided by raw size; at least one. A value of 2 means two times raw size total. |
| `cleanup` | `"after_success"` removes provider-owned input roots after verified success; `"never"` keeps them. |
| `keep_failed_inputs: bool` | Preserve staged provider input after processor failure. |
| `fsync_logs: bool` | Synchronize unit logs at phase boundaries. |
| `scheduler_poll_seconds: float` | Positive maximum wait between queue state checks. |

Provider cleanup is constrained below `workspace/fastq/`. It is separate from
processor-specific intermediate retention.

# Execution-system classes

## `LocalExecution`

```python
LocalExecution(
    total_cpus=os.cpu_count() or 1,
    min_cpus_per_job=1,
    max_cpus_per_job=None,
    max_running_jobs=1,
    storage=FilesystemStorage(),
)
```

| Argument | Meaning |
| --- | --- |
| `total_cpus` | Positive aggregate CPU budget for active processors. |
| `min_cpus_per_job` | Positive minimum needed to launch one unit. |
| `max_cpus_per_job` | Optional ceiling; `None` becomes `total_cpus`. |
| `max_running_jobs` | Positive processing concurrency limit. Downloads do not count. |
| `storage` | Local free-space admission policy. |

The CPU ceiling must fit inside `total_cpus` and cannot be below the minimum.
There is deliberately no memory field or memory enforcement.

Used by `DatasetBuilder.build()`. The current process owns the coordinator
lock and uses thread pools for download and processing tasks.

## `SlurmSingleNodeExecution`

```python
SlurmSingleNodeExecution(
    allocation_cpus,
    allocation_memory_gb,
    allocation_time_limit,
    storage,
    min_cpus_per_job=1,
    max_cpus_per_job=None,
    memory_gb_per_job=1.0,
    max_running_jobs=1,
    partition=None,
    account=None,
    qos=None,
)
```

| Argument | Meaning |
| --- | --- |
| `allocation_cpus: int` | Positive `--cpus-per-task` for the one submitted allocation. |
| `allocation_memory_gb: float` | Positive total `--mem` in GB. |
| `allocation_time_limit: str` | Slurm wall time containing digits, colons, and optional day separator. |
| `storage: QuotaStorage` | Explicit shared-storage quota policy. |
| `min_cpus_per_job: int` | Minimum CPUs passed to one in-allocation processor. |
| `max_cpus_per_job: int | None` | Ceiling; `None` becomes `allocation_cpus`. |
| `memory_gb_per_job: float` | Positive admission reservation for each concurrent processor. |
| `max_running_jobs: int` | Maximum concurrent processors inside the allocation. |
| `partition: str | None` | Optional comma-separated safe Slurm partition names. |
| `account: str | None` | Optional safe Slurm account. |
| `qos: str | None` | Optional safe Slurm QoS. |

`memory_gb_per_job` must not exceed allocation memory. It controls internal
admission; individual processors are not separate Slurm tasks.

Used by `DatasetBuilder.submit_slurm()` and reconstructed by
`single_node_worker.py`.

## `SlurmDistributedExecution`

```python
SlurmDistributedExecution(
    total_cpu_quota,
    max_running_jobs,
    cpus_per_node,
    min_cpus_per_job,
    max_cpus_per_job,
    memory_gb_per_job,
    worker_time_limit,
    storage,
    coordinator_cpus=1,
    coordinator_memory_gb=4.0,
    coordinator_time_limit="7-00:00:00",
    partition=None,
    account=None,
    qos=None,
)
```

| Argument | Meaning |
| --- | --- |
| `total_cpu_quota: int` | Positive maximum CPUs for coordinator and active workers together. |
| `max_running_jobs: int` | Positive active sample-job ceiling; coordinator is additional. |
| `cpus_per_node: int` | Largest worker request supported by a node. |
| `min_cpus_per_job: int` | Minimum worker CPU request. |
| `max_cpus_per_job: int` | Maximum worker CPU request; no larger than `cpus_per_node`. |
| `memory_gb_per_job: float` | Positive hard `--mem` request for every worker. |
| `worker_time_limit: str` | Worker `--time`. |
| `storage: QuotaStorage` | Explicit quota-based admission. |
| `coordinator_cpus: int` | CPUs reserved by the coordinator; must be below total quota. |
| `coordinator_memory_gb: float` | Positive coordinator `--mem`. |
| `coordinator_time_limit: str` | Coordinator `--time`. |
| `partition`, `account`, `qos` | Optional safe scheduler directive values. |

The CPU quota after subtracting coordinator CPUs must admit at least one
minimum-sized worker.

### `worker_capacity(cpus_per_job=None) -> int`

Return the smaller of `max_running_jobs` and the quota-derived worker count.
`cpus_per_job=None` uses the configured minimum. An explicit value outside
the configured per-job range raises.

# Scheduling behavior

## Local and single-node

The same in-process streaming scheduler:

1. downloads up to `QueuePolicy.download_workers` units;
2. moves completed downloads into a ready queue;
3. admits processors only when CPU, optional memory, job-count, in-flight, and
   storage policies allow;
4. selects a fair CPU share at launch between per-job minimum and maximum;
5. validates and records outputs independently; and
6. applies provider-input cleanup after each terminal outcome.

The launch-time cohort contains active and downloaded-ready units; pending
downloads do not reserve processing CPUs.

## Distributed

The coordinator:

1. reconstructs the saved execution record;
2. excludes reusable successes and non-retried failures;
3. observes active job IDs through `squeue`;
4. selects a worker CPU request within quota and per-job limits;
5. submits the sample script on hold;
6. persists job ID and resources in unit state;
7. releases the held job; and
8. waits for durable sample state to become succeeded or failed.

The submitted CPU request is fixed for that worker. The coordinator counts
`total_cpu_quota - coordinator_cpus` as worker capacity.

# Serialization functions

These functions support execution snapshots and worker reconstruction:

| Function | Behavior |
| --- | --- |
| `queue_policy_to_dict(policy)` | Dataclass-to-dictionary serialization. |
| `queue_policy_from_dict(value)` | Restore and revalidate `QueuePolicy`. |
| `execution_to_dict(execution)` | Store class kind plus config; stringify quota usage path. |
| `execution_from_dict(value)` | Restore and revalidate one of the three execution classes. |

They are module-level implementation API and are not exported from the package
root.

# Durable implementation classes

These classes are intentionally absent from `execution.__all__`. They are
documented so maintainers can follow state flow.

## `UnitResources`

Frozen record `UnitResources(cpus, memory_gb=None, time_limit=None)`.
`cpus` is the unit’s minimum. Memory and time are absent for local work.

## `QueueItem`

```python
QueueItem(
    item_id,
    unit,
    resources,
    genome_pin=None,
    fingerprint="",
)
```

It binds a `ProcessingUnit` to its safe workspace identity, minimum
resources, optional exact assembly, and semantic resume fingerprint.
`to_dict()` and `from_dict(value)` persist it.

## `ExecutionRecord`

```python
ExecutionRecord(
    execution_id,
    created_at,
    query,
    group_by,
    items,
    processor_identity,
    execution_type,
    execution_config,
    queue_config,
    catalog_audit=(),
    metadata={},
)
```

It is the immutable snapshot written under `workspace/executions/`.
`to_dict()` and `from_dict(value)` round-trip all queue items and settings.

## `UnitStateStore`

```python
UnitStateStore(root)
```

| Method | Arguments and behavior |
| --- | --- |
| `get(unit_id)` | Read current JSON state or return `None`. |
| `start(unit_id, *, fingerprint, execution_id, item, log_path, retry_failed=False, reclaim_running=False, stale_after_seconds=604800, force=False)` | Atomically claim a unit; return true only when work should run. |
| `record_submission(unit_id, *, slurm_job_id, cpus, memory_gb, fingerprint, execution_id, item, log_path)` | Persist a held distributed job before release. |
| `set_phase(unit_id, phase)` | Update the current staging/processing phase. |
| `set_runtime_resources(unit_id, *, cpus, memory_gb)` | Record actual launch resources. |
| `succeed(unit_id, result)` | Persist terminal successful payload. |
| `fail(unit_id, error)` | Persist terminal failure with a bounded traceback tail. |
| `summary(unit_ids=None)` | Return counts and selected state records. |

The builder uses this class to decide whether success is reusable, failure
needs an explicit retry, or a running claim is still owned.

## `SlurmExecutor`

```python
SlurmExecutor(
    *,
    runner=None,
    python_executable=None,
    progress=None,
)
```

| Argument | Meaning |
| --- | --- |
| `runner: CommandRunner | None` | External command abstraction. |
| `python_executable: str | None` | Absolute interpreter embedded in generated scripts; defaults to `sys.executable`. |
| `progress: ProgressReporter | None` | Optional reporting sink. |

| Method | Key arguments and result |
| --- | --- |
| `create_single_node_script(...)` | Write one allocation script from a record path, processor reference, builder config, output path, execution config, and retry flag. |
| `create_distributed_script(...)` | Write the small coordinator script with analogous arguments. |
| `create_sample_script(...)` | Write one worker script for an item index, workspace, CPU request, and distributed config. |
| `submit(script, *, hold=False)` | Run `sbatch --parsable`; return job ID. |
| `release(job_id)` | Run `scontrol release`. |
| `cancel(job_id)` | Run `scancel`. |

Paths and scheduler values are shell-quoted or validated before rendering.

### Script-generation arguments

| Argument | Used by | Meaning |
| --- | --- | --- |
| `record_path: Path` | All three creation methods | Absolute execution-record JSON path read by the coordinator or sample worker. |
| `processor_reference: str` | All three | Importable `module:object` processor reference. Lambdas and notebook-local objects cannot be reconstructed by Slurm workers. |
| `builder_config: BuilderConfig` | Single-node and distributed coordinator | Supplies the workspace and optional NCBI email embedded in coordinator arguments. |
| `output_path: Path` | All three | Destination `.sbatch` file, returned after atomic writing. |
| `execution` | All three | Matching single-node or distributed resource and scheduler configuration. |
| `retry_failed: bool` | All three | Whether the launched process may reclaim matching failed state. |
| `item_index: int` | Sample script | Zero-based item position in the execution record. |
| `workspace: Path` | Sample script | Shared durable workspace passed to the worker. |
| `email: str | None` | Sample script | Optional NCBI contact email forwarded to the worker. |
| `cpus: int` | Sample script | Fixed `--cpus-per-task` value and processing CPU budget for that submitted job. |
| `script: Path` | `submit` | Existing script passed to `sbatch`. |
| `hold: bool` | `submit` | Add `--hold` so a dependency-aware coordinator can release the job later. |
| `job_id: str` | `release`, `cancel` | Scheduler job identifier passed to `scontrol release` or `scancel`. |

## Remaining internal types

| Type/function | Role |
| --- | --- |
| `distributed_worker._RunningSample` | Mutable in-memory job ID, CPU, submission time, and missing-state tracker. |
| `path_size_gb(path)` | Recursive regular-file size in decimal GB. |
| `paths_size_gb(paths)` | Non-overlapping size of several paths. |
| Worker `main(argv=None)` functions | Internal module entry points generated into Slurm scripts. |

# Example

```python
from pathlib import Path

from ncbi_dataset_builder import (
    QueuePolicy,
    QuotaStorage,
    SlurmDistributedExecution,
)

storage = QuotaStorage(
    quota_gb=5_000,
    reserve_gb=250,
    usage_root=Path("/scratch/project-owner"),
)

execution = SlurmDistributedExecution(
    total_cpu_quota=500,
    max_running_jobs=40,
    cpus_per_node=128,
    min_cpus_per_job=8,
    max_cpus_per_job=64,
    memory_gb_per_job=100,
    worker_time_limit="3-00:00:00",
    storage=storage,
    coordinator_cpus=1,
)

queue = QueuePolicy(
    download_workers=8,
    max_inflight_gb=1_500,
    processing_storage_multiplier=2,
)

# This is a dry run: it writes the snapshot and script but does not call sbatch.
script, job_id = builder.submit_slurm(
    catalog,
    processor_reference="ncbi_dataset_builder.processing.atac:process_atac",
    execution=execution,
    queue=queue,
    submit=False,
)

print(script)
assert job_id is None
```
