# `execution`

This subpackage describes the machine running the queue, persists per-sample
state, and generates Slurm scripts. Most callers construct only one execution
configuration plus `QueuePolicy`; `DatasetBuilder` owns the other classes.

## Storage policies

`StoragePolicy` is the queue-admission interface:
`available_gb(workspace)` returns currently usable decimal GB.

`FilesystemStorage(reserve_free_gb=0)` is for local execution.
`available_gb(workspace)` uses free space on the filesystem containing the
workspace and subtracts the reserve.

`QuotaStorage(quota_gb, reserve_gb=0, usage_root=None)` is for Slurm.
`available_gb(workspace)` subtracts files below `usage_root` (or the workspace)
and `reserve_gb` from the user quota. It never treats a shared filesystem's
global free space as the user's allocation.

## `QueuePolicy`

Independent sample-streaming controls:

- `download_workers`: simultaneous sample downloads.
- `max_inflight_gb`: optional estimate cap across downloading, ready, and
  processing samples.
- `processing_storage_multiplier`: estimated peak total storage divided by raw
  input size.
- `cleanup`: `"after_success"` or `"never"` for provider-owned inputs.
- `keep_failed_inputs`: retain inputs after processor failure.
- `fsync_logs`: synchronize logs at phase boundaries.
- `scheduler_poll_seconds`: maximum interval between queue checks.

For example, `QueuePolicy(download_workers=10, max_inflight_gb=1500,
processing_storage_multiplier=2, cleanup="after_success",
keep_failed_inputs=True)` allows ten downloads while processing admission stays
bounded by the selected execution system.

## Execution systems

`LocalExecution(total_cpus, min_cpus_per_job=1, max_cpus_per_job=None,
max_running_jobs=1, storage=FilesystemStorage())` runs inside one ordinary
server. It has no memory field. The scheduler redistributes available CPUs
among processing samples up to each sample ceiling.

`SlurmSingleNodeExecution(allocation_cpus, allocation_memory_gb,
allocation_time_limit, storage, min_cpus_per_job=1,
max_cpus_per_job=None, memory_gb_per_job=1, max_running_jobs=1,
partition=None, account=None, qos=None)` requests one hard Slurm allocation.
Samples stream concurrently inside it; CPU and memory admission never exceed
the allocation.

`SlurmDistributedExecution(total_cpu_quota, max_running_jobs, cpus_per_node,
min_cpus_per_job, max_cpus_per_job, memory_gb_per_job, worker_time_limit,
storage, coordinator_cpus=1, coordinator_memory_gb=4,
coordinator_time_limit="7-00:00:00", partition=None, account=None, qos=None)`
runs a small coordinator plus independent sample jobs. `worker_capacity(
cpus_per_job=None)` returns the concurrency allowed by CPU quota and
`max_running_jobs`.

Full workflows: [local](../../../docs/LocalExecution.md), [single-node
Slurm](../../../docs/SlurmSingleNodeExecution.md), and [distributed
Slurm](../../../docs/SlurmDistributedExecution.md).

## Durable internal classes

`UnitResources(cpus, memory_gb=None, time_limit=None)`, `QueueItem(item_id,
unit, resources, genome_pin=None, fingerprint="")`, and
`ExecutionRecord(execution_id, created_at, query, group_by, items,
processor_identity, execution_type, execution_config, queue_config,
catalog_audit=(), metadata={})` are serialized into `workspace/executions`.
Their `to_dict()` and `from_dict(value)` methods are used by the workspace and
workers. They are internal records, not extra user configuration layers.

`UnitStateStore(root)` owns atomic state files. `get(unit_id)` reads one;
`start(unit_id, fingerprint, execution_id, item, log_path, retry_failed=False,
reclaim_running=False, stale_after_seconds=604800, force=False)` claims work;
`record_submission(unit_id, slurm_job_id, cpus, memory_gb, fingerprint,
execution_id, item, log_path)` records a distributed Slurm job;
`set_phase(unit_id, phase)` and `set_runtime_resources(unit_id, cpus,
memory_gb)` update running state; `succeed(unit_id, result)` and
`fail(unit_id, error)` finalize it; and `summary(unit_ids=None)` returns counts
and records. `retry_failed` allows another attempt with the same fingerprint;
`reclaim_running` is for a known interrupted worker; `force` archives/replaces
even reusable state.

`SlurmExecutor(runner=None, python_executable=None, progress=None)` writes
scripts using an optional command runner, explicit worker Python, and progress
reporter:

- `create_single_node_script(record_path, processor_reference,
  builder_config, output_path, execution, retry_failed=False)` writes the
  coordinator for `SlurmSingleNodeExecution`. `builder_config` is
  [`BuilderConfig`](../README.md).
- `create_distributed_script(record_path, processor_reference,
  builder_config, output_path, execution, retry_failed=False)` writes the
  coordinator for `SlurmDistributedExecution`.
- `create_sample_script(record_path, item_index, processor_reference,
  workspace, email, output_path, execution, cpus, retry_failed)` writes one
  sample job. `item_index` addresses the saved queue and `cpus` is the actual
  request.
- `submit(script, hold=False)` returns the scheduler job ID; `release(job_id)`
  and `cancel(job_id)` control held jobs.

`DatasetBuilder` and the distributed coordinator call it, so normal users need
not instantiate it.

`single_node_worker.py`, `distributed_worker.py`, and `sample_worker.py` are
module entry points embedded in generated scripts. They reconstruct the saved
execution and never introduce a second configuration surface.
