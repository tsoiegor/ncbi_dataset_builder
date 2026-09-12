# Distributed Slurm execution

`SlurmDistributedExecution` submits a small coordinator job. The coordinator
admits work within CPU, job-count, and storage limits, then submits one
independent Slurm job per processing unit.

Use this mode when samples should run across multiple nodes, need hard
per-worker memory requests, or benefit from independent scheduler failure
boundaries.

## When this mode fits

| Situation | Fit |
| --- | --- |
| Many samples can run on different nodes | Good |
| Each sample should have its own Slurm job and log | Good |
| Per-sample memory must be hard requested | Good |
| Samples need different CPU allocations selected at admission | Supported within configured minimum/maximum |
| Samples need different memory or wall-time values | Not currently supported; those fields are common to all workers |
| Cluster strongly limits submitted/running job count | Use conservative `max_running_jobs` or single-node mode |
| Workspace is node-local rather than shared | Not supported |
| Custom provider exists only as an in-memory object | Not supported by standard worker reconstruction |

## Runtime topology

```text
submission process
└── sbatch coordinator script
    └── coordinator Slurm job
        ├── inspect execution and unit state
        ├── check CPU quota, job count, and storage
        ├── create held sample job
        ├── persist job ID and resources
        ├── release sample job
        └── monitor jobs with squeue

sample job 0 ── stage genome/input ── processor ── state/cleanup
sample job 1 ── stage genome/input ── processor ── state/cleanup
sample job 2 ── stage genome/input ── processor ── state/cleanup
```

Every worker reads and writes the same workspace. It reconstructs the saved
execution/queue configuration and loads the processor from an import reference.

## Before filling the template

Collect:

| Question | Used for |
| --- | --- |
| Maximum CPUs allowed across coordinator and workers | `total_cpu_quota` |
| Maximum simultaneous user/project jobs | `max_running_jobs` |
| Largest CPU count on one eligible node | `cpus_per_node` |
| Smallest/largest useful CPU count for one sample | Per-job CPU limits |
| Measured peak RAM of one sample | `memory_gb_per_job` |
| Worst sample wall time | `worker_time_limit` |
| Coordinator time needed for the entire campaign | `coordinator_time_limit` |
| Coordinator minimum CPU/RAM | Coordinator fields |
| Shared storage quota and usage root | `QuotaStorage` |
| Typical/largest raw unit and peak multiplier | Queue storage fields |
| Partitions/account/QoS available to both coordinator and workers | Scheduler fields |

`total_cpu_quota` is a package admission limit. It should be no greater than
the CPU capacity your Slurm account or site policy allows, but the package
does not query that policy.

## Complete Python template

```python
from pathlib import Path

from ncbi_dataset_builder import (
    BuilderConfig,
    DatasetBuilder,
    QueuePolicy,
    QuotaStorage,
    SlurmDistributedExecution,
)

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

catalog = builder.load_runs(
    Path("/scratch/project-owner/catalogs/runinfo.csv")
)

execution = SlurmDistributedExecution(
    total_cpu_quota=257,
    max_running_jobs=8,
    cpus_per_node=64,
    min_cpus_per_job=8,
    max_cpus_per_job=32,
    memory_gb_per_job=128,
    worker_time_limit="2-00:00:00",
    coordinator_cpus=1,
    coordinator_memory_gb=4,
    coordinator_time_limit="7-00:00:00",
    partition="compute,highmem",
    account=None,
    qos=None,
    storage=QuotaStorage(
        quota_gb=10_000,
        reserve_gb=1_000,
        usage_root=Path("/scratch/project-owner"),
    ),
)

queue = QueuePolicy(
    # Distributed workers stage their own data; this field does not create
    # a separate coordinator-side download pool in the current implementation.
    download_workers=1,
    max_inflight_gb=2_000,
    processing_storage_multiplier=2.5,
    cleanup="after_success",
    keep_failed_inputs=True,
    fsync_logs=True,
    scheduler_poll_seconds=5.0,
)

# Generate and inspect the coordinator script before calling sbatch.
script, coordinator_job_id = builder.submit_slurm(
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

assert coordinator_job_id is None
print(script.read_text())
```

Repeat with `submit=True` only after checking paths, resources, and imports.

## `BuilderConfig` parameters

| Parameter | Default | Meaning here | How to choose | Restriction |
| --- | --- | --- | --- | --- |
| `workspace` | Required | Shared root read/written by submission, coordinator, and every worker | Use a high-throughput shared path with sufficient quota | Same resolved absolute path everywhere |
| `email` | `None` | NCBI contact passed to coordinator and workers | Supply for live NCBI/provider work | Required for live catalog/metadata methods |
| `ncbi_api_key` | `None` | Optional Entrez rate increase | Export `NCBI_API_KEY` to batch jobs | Not added as a generated command argument |
| `genome_policy` | Default | Intended genome selection and workspace semantic | Prefer defaults or explicit genome pins | Standard workers reconstruct default manager/policy |
| `group_by` | `"experiment"` | Processing-unit grouping in the saved record | Usually experiment | Stable after state exists |
| `description_profile` | `"training"` | Workspace metadata semantic | Choose before first state | Worker reconstruction uses default profile |
| `prefetch_max_size` | `"u"` | Intended SRA archive-size limit | Validate behavior with representative run | Standard workers reconstruct default provider |
| `show_progress` | `True` | Submission-side progress | Any | Coordinator/worker reporting is created independently |
| `progress_bars` | `True` | Submission-side bars | Any | Batch logs may be plain text |

### Current reconstruction restriction

Custom FASTQ-provider and genome-manager objects on the submitting builder are
not serialized. Standard distributed workers create the default
`SraToolkitProvider` and `GenomeManager`. If your workflow requires a custom
provider/manager or non-default acquisition configuration, local execution is
the directly supported object-injection path; otherwise extend/package worker
construction deliberately.

## `SlurmDistributedExecution` parameters

| Parameter | Required/default | Exact meaning | How to choose | Restriction |
| --- | --- | --- | --- | --- |
| `total_cpu_quota: int` | Required | Maximum package-accounted CPUs for coordinator plus active workers | Site/project CPU allowance | Positive; coordinator CPUs must be smaller |
| `max_running_jobs: int` | Required | Maximum active sample jobs; coordinator is additional | Site job limit and desired concurrency | Positive |
| `cpus_per_node: int` | Required | Largest worker CPU request supported by one node | Maximum on eligible node type | Positive; worker max cannot exceed it |
| `min_cpus_per_job: int` | Required | Smallest worker `--cpus-per-task` | Minimum useful processor count | Positive; must fit worker CPU budget |
| `max_cpus_per_job: int` | Required | Largest worker `--cpus-per-task` | Measured useful scaling ceiling | At least minimum; no greater than `cpus_per_node` |
| `memory_gb_per_job: float` | Required | Hard Slurm `--mem` for every worker, rounded up to whole GB | Peak RSS plus safety margin | Positive; same for every worker |
| `worker_time_limit: str` | Required | Slurm `--time` for every worker | Worst representative sample duration plus margin | Digits/colon/hyphen and site-valid |
| `storage: QuotaStorage` | Required | Shared quota admission | Real quota, reserve, and usage root | Must be `QuotaStorage` |
| `coordinator_cpus: int` | `1` | Coordinator `--cpus-per-task`, included in total quota | Usually `1`; coordinator is lightweight | Positive and below total quota |
| `coordinator_memory_gb: float` | `4.0` | Hard coordinator `--mem`, rounded up | Enough for catalog/state bookkeeping | Positive |
| `coordinator_time_limit: str` | `"7-00:00:00"` | Coordinator wall time for the whole campaign | Longer than expected last worker completion | Digits/colon/hyphen and site-valid |
| `partition: str \| None` | `None` | Comma-separated partitions applied to coordinator and every worker | Partitions accepting both resource shapes | Safe identifier characters only |
| `account: str \| None` | `None` | Slurm account for all generated jobs | Site account | Safe identifier characters only |
| `qos: str \| None` | `None` | Slurm QoS for all generated jobs | Site policy | Safe identifier characters only |

## CPU capacity and allocation

Worker CPU budget:

```text
worker_cpu_budget = total_cpu_quota - coordinator_cpus
```

Maximum jobs allowed by CPUs at a chosen worker size:

```text
min(
    max_running_jobs,
    floor(worker_cpu_budget / cpus_per_job),
)
```

The class exposes this calculation:

```python
print(execution.worker_capacity())     # Uses min_cpus_per_job.
print(execution.worker_capacity(32))   # Capacity if workers use 32 CPUs.
```

The optional value must lie between the configured minimum and maximum.

### Launch-time fair share

Before submitting a worker, the coordinator computes a fair share from the
running plus pending cohort, then chooses:

```text
min(
    max_cpus_per_job,
    currently unused worker CPU budget,
    max(min_cpus_per_job, worker_cpu_budget / cohort size),
)
```

The selected integer becomes:

- the worker script’s `#SBATCH --cpus-per-task`;
- the worker command’s `--cpus`; and
- the processor’s `threads` argument.

It cannot change after `sbatch`. Later workers may receive different values.

### Worked example

```text
total_cpu_quota  = 257
coordinator_cpus = 1
worker budget    = 256
max jobs         = 8
worker range     = 8..32 CPUs
```

At 32 CPUs, eight workers fit exactly. With many pending units, each may
receive 32. If only two units remain and the maximum stays 32, unused quota is
not assigned beyond that ceiling.

## Memory and time

### Worker memory

Each worker gets its own hard:

```text
#SBATCH --mem=ceil(memory_gb_per_job)G
```

This provides better failure isolation than single-node mode. If one worker
exceeds memory, Slurm can fail that job without necessarily terminating other
sample jobs.

### Worker time

`worker_time_limit` applies separately to each sample job and includes:

- genome resolution/download when uncached;
- SRA staging and validation;
- FASTQ materialization;
- processor execution;
- output validation/checksums; and
- cleanup.

### Coordinator resources

The coordinator does not process samples. It reads state, scans quota usage,
writes scripts, runs scheduler commands, and waits for terminal state. Its time
limit must cover the entire campaign. If it exits early, workers already
released may continue, but no process remains to admit later units or finalize
the complete summary.

## `QuotaStorage` parameters

| Parameter | Meaning | Starting choice |
| --- | --- | --- |
| `quota_gb` | Total user/project capacity | Site-reported value |
| `reserve_gb` | Capacity the queue may not consume | Safety margin plus unrelated data |
| `usage_root` | Directory recursively measured | Root corresponding to that quota |

The scan happens in the coordinator. It does not use a site quota command and
does not model inode limits. See [Storage](Storage.md#quotastorage).

## `QueuePolicy` in distributed mode

| Parameter | Current distributed behavior | Starting choice |
| --- | --- | --- |
| `download_workers` | Serialized but does not limit a separate download pool; every admitted worker stages its own unit | Leave `1`; control simultaneous stages with `max_running_jobs` |
| `max_inflight_gb` | Limits estimated peak size of active workers | Enough for one or two large workers initially |
| `processing_storage_multiplier` | Multiplies raw size for every active/candidate worker | Measured peak ratio |
| `cleanup` | Each worker removes provider-owned input after success when configured | `"after_success"` if reacquisition is acceptable |
| `keep_failed_inputs` | Each worker preserves failed provider input by default | Keep `True` during validation |
| `fsync_logs` | Worker unit logs synchronize at phase boundaries | Keep `True` |
| `scheduler_poll_seconds` | Coordinator sleep between checks, capped at 60 seconds | `5`–`30` may reduce scheduler chatter; must be positive |

The current coordinator counts active worker estimates, not a separate ready or
download queue. Full storage details are in [Storage](Storage.md).

## Submission protocol

For each admitted processing unit:

1. create `workspace/slurm/<execution-id>/<unit-id>.sbatch`;
2. call `sbatch --parsable --hold`;
3. parse the scheduler job ID;
4. atomically persist job ID, CPUs, memory, fingerprint, item, and log path;
5. call `scontrol release <job-id>`;
6. if persistence/release setup fails after submission, call `scancel`.

Holding the job prevents it from starting before durable ownership is written.

## Generated scripts

### Coordinator

| Directive/value | Source |
| --- | --- |
| Job name | `ncbi-coordinator` |
| `--cpus-per-task` | `coordinator_cpus` |
| `--mem` | Ceiling of `coordinator_memory_gb` |
| `--time` | `coordinator_time_limit` |
| Output/error | Append-mode `%j.coordinator.log` |
| Scheduler fields | Shared partition/account/QoS |

### Sample worker

| Directive/value | Source |
| --- | --- |
| Job name | `ncbi-<item-index>` |
| `--cpus-per-task` | Launch-selected CPU count |
| `--mem` | Ceiling of `memory_gb_per_job` |
| `--time` | `worker_time_limit` |
| Output/error | `sample-<item-index>.<job-id>.log`, append mode |
| Scheduler fields | Same partition/account/QoS as coordinator |

The same scheduler fields apply to coordinator and workers; separate
coordinator/worker partitions are not currently configurable.

## Path and environment requirements

| Requirement | Nodes that need it |
| --- | --- |
| Workspace at identical resolved absolute path | Submission, coordinator, every worker |
| Execution JSON and generated scripts | Shared through workspace |
| Submitting Python executable | Coordinator and every worker |
| Package and processor module | Coordinator and every worker |
| SRA/genome/processor external tools | Every worker |
| `sbatch`, `scontrol`, `scancel`, `squeue` | Coordinator |
| Quota `usage_root` | Coordinator and shared filesystem |
| `NCBI_API_KEY` when used | Worker batch environment |

## Monitoring behavior

The coordinator calls `squeue` for running job IDs:

- if `squeue` fails, it prints a warning and pauses new admissions;
- if a job remains in `squeue`, it is treated as active;
- if it disappears, the coordinator checks unit state;
- a succeeded/failed unit is removed from the running set; and
- if no terminal state appears within 30 seconds, the unit is marked failed.

The code does not use `sacct` to reconstruct historical completion.

Monitor:

```text
workspace/logs/slurm/<coordinator-job-id>.coordinator.log
workspace/logs/slurm/sample-<index>.<worker-job-id>.log
workspace/logs/<species>/<unit-id>.log
workspace/state/units/<unit-id>.json
```

## Restart and coordinator-loss procedure

Do not immediately launch a second coordinator while the first coordinator or
its sample jobs are still active.

The current coordinator initializes its in-memory running set from new
admissions; it does not reattach existing `submitted`/`running` worker jobs
after restart. A second coordinator can therefore submit duplicate work while
old workers still exist.

Use this safe sequence:

1. inspect `squeue` for the original coordinator and sample job IDs recorded in
   unit state;
2. allow active workers to finish, or cancel the explicitly identified old
   jobs;
3. verify terminal state and logs;
4. resubmit the same semantic execution request;
5. valid successes are reused;
6. set `retry_failed=True` only for diagnosed matching failures.

## Tuning sequence

1. Submit one representative worker with generous memory/time.
2. Measure peak RSS, CPU scaling, wall time, and disk peak.
3. Set worker CPU minimum/maximum and memory/time with safety margins.
4. Set `total_cpu_quota` to the allowed aggregate including coordinator.
5. Compute capacity at both minimum and maximum worker sizes.
6. Set `max_running_jobs` below site limits and storage/I/O capacity.
7. Set quota root/reserve and in-flight window.
8. Use a small multi-sample catalog to verify submission and monitoring.
9. Scale gradually while watching queue wait, storage scan cost, and NCBI I/O.

## Common configuration errors

| Symptom | Cause | Correction |
| --- | --- | --- |
| Constructor says quota cannot admit one job | Minimum worker CPUs exceed budget after coordinator | Raise total quota or lower minimum/coordinator CPUs |
| Maximum CPU rejected | Greater than `cpus_per_node` or below minimum | Reconcile node and per-job limits |
| Workers remain pending in Slurm | Site capacity/QoS/partition, not package admission | Inspect `squeue` reason and site policy |
| Coordinator admits no new work | CPU/job/storage limit or failed `squeue` | Read coordinator log and quota usage |
| Worker import fails | Embedded interpreter cannot import package/processor | Test exact Python path on compute node |
| Job vanishes and unit becomes failed | No terminal state within 30 seconds after `squeue` disappearance | Inspect worker log and scheduler accounting |
| Too many simultaneous downloads | Each worker stages independently | Lower `max_running_jobs`; `download_workers` is not the control here |
| Duplicate work after restart | Second coordinator started while old workers remained | Reconcile/cancel existing jobs before resubmission |

## Hard restrictions

- Shared workspace with identical absolute path is mandatory.
- Processor must be an importable `module:object`.
- Worker CPU allocation is fixed after submission.
- All workers share one memory value and one worker time limit.
- Coordinator and workers share partition/account/QoS settings.
- `download_workers` does not independently limit distributed staging.
- Custom provider and genome-manager objects are not serialized.
- The coordinator uses `squeue`, not scheduler history, for active-state
  monitoring.
- Automatic reattachment to pre-existing submitted/running workers is not
  implemented.

## Related pages

- [Choosing an execution system](ExecutionSystems.md)
- [Storage and queue policy](Storage.md)
- [Single-node Slurm](SlurmSingleNodeExecution.md)
- [ATAC-seq processing](AtacSeqProcessing.md)
- [Distributed CLI command](CommandLineInterface.md#submit-distributed)
- [Execution API reference](../src/ncbi_dataset_builder/execution/README.md)
