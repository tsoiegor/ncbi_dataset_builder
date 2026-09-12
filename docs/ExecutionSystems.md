# Choosing an execution system

The package exposes one biological workflow through three execution systems:

- `LocalExecution` runs everything in the current Python process on one
  ordinary server.
- `SlurmSingleNodeExecution` requests one Slurm allocation and runs the same
  streaming scheduler inside that allocation.
- `SlurmDistributedExecution` requests a small coordinator and creates one
  separate Slurm job per admitted processing unit.

The catalog, workspace layout, processing-unit identity, output validation,
and resume model stay the same. CPU allocation, memory enforcement, job
topology, path visibility, and some queue semantics change.

## Quick decision

| Question | If yes | If no |
| --- | --- | --- |
| Is Slurm unavailable? | Use [local execution](LocalExecution.md) | Continue |
| Can one node hold the desired concurrent samples in CPU and RAM? | Prefer [single-node Slurm](SlurmSingleNodeExecution.md) for simplicity | Continue |
| Do samples need independent wall times, memory limits, or multi-node concurrency? | Use [distributed Slurm](SlurmDistributedExecution.md) | Reconsider a smaller single-node allocation |
| Is your processor a notebook-local function or custom provider object? | Use local execution, or package it as importable code first | Either Slurm mode is possible |
| Is the workspace unavailable at one identical absolute path on all compute nodes? | Use local execution or fix shared storage first | Slurm paths are suitable |

## System comparison

| Property | `LocalExecution` | `SlurmSingleNodeExecution` | `SlurmDistributedExecution` |
| --- | --- | --- | --- |
| Top-level process | Calling Python process | One submitted coordinator/allocation | One submitted coordinator job |
| Sample processors | Thread-pool tasks in the current process | Thread-pool tasks inside one allocation | Separate Slurm jobs |
| Nodes used | One | One | Potentially many |
| Processor form | Callable or `module:object` string | Importable `module:object` | Importable `module:object` |
| CPU pool | `total_cpus` | `allocation_cpus` | `total_cpu_quota - coordinator_cpus` |
| Processor CPU assignment | Dynamic when processing starts | Dynamic when processing starts | Fixed when the worker is submitted |
| Sample memory | Not modeled or enforced | Admission reservation inside the allocation | Hard `--mem` request per worker |
| Storage policy | `FilesystemStorage` | `QuotaStorage` | `QuotaStorage` |
| Download topology | Shared download thread pool | Shared download thread pool in allocation | Each sample worker stages its own input |
| `download_workers` effect | Limits concurrent staging | Limits concurrent staging | No separate coordinator download pool; worker concurrency is controlled by `max_running_jobs` |
| Failure isolation | Per-unit state, same process | Per-unit state, same Slurm allocation | Per-unit state and separate Slurm job |
| Main API | `builder.build()` | `builder.submit_slurm()` | `builder.submit_slurm()` |
| Return at submission | Completed `BuildReport` | Script path and coordinator job ID | Script path and coordinator job ID |

## What must be measured before filling a template

Do not choose concurrency from CPU count alone. Record these facts first:

| Measurement | Where to obtain it | Used for |
| --- | --- | --- |
| Total CPUs you may consume | Server specification or Slurm allocation/account policy | Total CPU pool |
| CPUs that one processor can use efficiently | Tool documentation plus one measured sample | `min_cpus_per_job`, `max_cpus_per_job` |
| Peak RAM for one representative sample | `/usr/bin/time -v`, Slurm `sacct`, or site monitoring | Single-node reservation or worker `--mem` |
| Typical and largest raw sample size | RunInfo `size_MB`/`bases` plus catalog inspection | Storage window and quota |
| Peak workspace growth per sample | Measure raw archive + FASTQ + work + outputs | `processing_storage_multiplier` |
| Storage limit you can actually write | Filesystem free space or user/project quota | `FilesystemStorage` or `QuotaStorage` |
| Desired simultaneous samples | Operational goal, bounded by the resources above | `max_running_jobs` |
| Maximum cluster node size | Slurm partition/node documentation | `cpus_per_node` and per-worker maximum |
| Typical and worst wall time | One representative run or prior accounting | Allocation/worker time limit |
| Shared absolute path | Mount visible from login and compute nodes | Slurm workspace and package visibility |

Start conservatively with one or two representative processing units. Measure
CPU utilization, peak RSS, elapsed time, and workspace growth. Increase
concurrency only after those measurements fit the server or cluster limits.

## Parameter ownership

Four layers are intentionally separate:

| Layer | Object | Examples |
| --- | --- | --- |
| Stable workspace and NCBI choices | `BuilderConfig` | `workspace`, `group_by`, `prefetch_max_size` |
| Runtime resources | One execution class | CPU pool, memory, concurrency, Slurm time |
| Streaming and cleanup | `QueuePolicy` | downloads, in-flight window, storage multiplier, cleanup |
| One invocation | `build()` or `submit_slurm()` | catalog, processor, genome pins, retry, dry run |

Changing runtime CPU or concurrency does not change biological identity.
Changing processing-unit membership, genome pins, processor identity, or
input-provider identity changes the per-unit fingerprint.

## `BuilderConfig` parameters

These parameters are common to all modes.

| Parameter | Default | Meaning | How to choose | Restrictions |
| --- | --- | --- | --- | --- |
| `workspace: Path` | Required | Durable root for caches, state, inputs, work, outputs, scripts, and logs | Put it on storage large enough for the full run; use shared storage for Slurm | Slurm nodes must see the same resolved absolute path |
| `email: str \| None` | `None` | NCBI contact email used by Entrez and passed to Slurm workers | Supply a real monitored address for live NCBI calls | Required by `fetch_runs`, GEO resolution, and metadata fetching; not needed to load a CSV |
| `ncbi_api_key: str \| None` | `None` | Optional NCBI API key for a higher Entrez request rate | Use your key for large metadata/catalog work | Never put a real key in committed examples; Slurm workers read `NCBI_API_KEY` from their environment |
| `genome_policy: GenomeSelectionPolicy` | Default policy | Deterministic assembly filtering and ranking | Start with defaults; pin exact assemblies when reproducibility requires them | Stable workspace semantic setting after state exists |
| `group_by` | `"experiment"` | Entity represented by one processing unit | Usually experiment for ATAC and publication; use run/sample only when scientifically intended | One of `run`, `experiment`, `sra_sample`, `biosample`; stable after state exists |
| `description_profile` | `"training"` | Metadata projection written for descriptions | Use `training` for compact model inputs; `full` for relationship-rich metadata | Only `training` or `full`; stable after state exists |
| `prefetch_max_size: str` | `"u"` | Value sent to SRA Toolkit `prefetch --max-size` | Start with `u` if storage admission already protects capacity; otherwise set a deliberate per-run limit such as `100G` | Must be non-empty; too-small values can make a run impossible to stage |
| `show_progress: bool` | `True` | Enables progress reporting | Keep for interactive work; disable in very controlled logging environments | Does not disable per-unit logs |
| `progress_bars: bool` | `True` | Uses tqdm bars when available | Use interactively; plain text is safer for some batch logs | Falls back when tqdm is absent |

Detailed genome policy fields are in the
[acquisition API](../src/ncbi_dataset_builder/acquisition/README.md).

## `QueuePolicy` parameters

`QueuePolicy` is shared syntactically, but not every field has the same
operational effect in every topology.

| Parameter | Default | Meaning | Starting point | Restrictions and interactions |
| --- | --- | --- | --- | --- |
| `download_workers: int` | `2` | Maximum simultaneous stage operations in the local/single-node download pool | Start at `1`–`2`; increase only if network/storage tolerate it | Positive; distributed mode has no separate pool, so this does not cap worker downloads |
| `max_inflight_gb: float \| None` | `None` | Estimated workload window for downloading, ready, and processing units | Set after measuring sample sizes; leave `None` only when physical capacity is comfortably large | Positive when set; it is an estimate, not a disk quota |
| `processing_storage_multiplier: float` | `1.0` | Peak total processing footprint divided by raw sample size | For raw + FASTQ + BAM workflows, begin around `2`–`4`, then measure | At least `1`; not “additional copies” |
| `cleanup` | `"after_success"` | Removes provider-owned staged input after verified success | Keep default when inputs are reproducible from NCBI | Only `"after_success"` or `"never"` |
| `keep_failed_inputs: bool` | `True` | Preserves provider input after a failed unit | Keep `True` while stabilizing a workflow | Matters only when `cleanup="after_success"`; `False` permits cleanup after failure |
| `fsync_logs: bool` | `True` | Flushes and synchronizes unit logs at phase boundaries | Keep `True` for durable cluster diagnostics | Turning it off may improve metadata-heavy filesystems but weakens immediate durability |
| `scheduler_poll_seconds: float` | `1.0` | Maximum local/single-node wait between state checks; distributed sleep is capped at 60 seconds | Keep `1.0` unless scheduler/filesystem polling is too noisy | Must be positive |

See [Storage and queue policy](Storage.md) for formulas, cleanup boundaries,
oversized-sample behavior, and a sizing worksheet.

## How CPU allocation works

### Local and single-node Slurm

The scheduler starts from the configured minimum resource request for each
processing unit. When a ready unit is launched, its processor allocation is:

```text
min(
    max_cpus_per_job,
    CPUs currently unused,
    max(minimum CPUs for this unit, total CPU pool / schedulable ready cohort),
)
```

The cohort contains active processing units plus downloaded-ready units that
fit the CPU, memory, and job-count limits. Pending or still-downloading units
do not dilute the processor’s fair share.

Important consequences:

- allocations are chosen at launch, not continuously resized;
- a running processor keeps its assigned integer `threads`;
- later units may receive different allocations;
- the FASTQ provider stages/materializes using the unit’s configured minimum
  CPU request, while the processor receives the dynamic launch allocation; and
- the processor must respect the `threads` argument.

### Distributed Slurm

The worker CPU budget is:

```text
total_cpu_quota - coordinator_cpus
```

For each admission, the coordinator divides this budget by the running plus
pending worker cohort, then clamps the result to the configured minimum,
maximum, and currently available CPUs. That integer becomes both
`#SBATCH --cpus-per-task` and the processor’s `threads` value.

A submitted Slurm job cannot be resized. A later job can receive another
request when the cohort or available quota changes.

## How concurrency is limited

| Limit | Local | Single-node Slurm | Distributed Slurm |
| --- | --- | --- | --- |
| Job-count limit | `max_running_jobs` | `max_running_jobs` | `max_running_jobs` workers; coordinator is additional |
| CPU limit | `total_cpus` | `allocation_cpus` | `total_cpu_quota - coordinator_cpus` |
| Memory limit | None | Sum of active `memory_gb_per_job` reservations cannot exceed `allocation_memory_gb` | Slurm enforces each worker’s `--mem`; coordinator has its own request |
| Storage limit | Filesystem usable free space | Quota usable space | Quota usable space |
| Workload estimate | `max_inflight_gb` | `max_inflight_gb` | `max_inflight_gb` |
| Download limit | `download_workers` | `download_workers` | Worker admission; no separate download pool |

Effective concurrency is always the smallest limit that currently permits a
unit. Configuring `max_running_jobs=50` does not guarantee 50 simultaneous
processors.

## Path model

### Paths used before execution starts

The catalog file passed to `load_runs()` is read by the submitting process and
converted into an execution snapshot. Slurm workers do not reopen that CSV.

### Paths used during execution

| Path | Local | Single-node Slurm | Distributed Slurm |
| --- | --- | --- | --- |
| Workspace | Visible to current server | Same absolute path on submit and allocation node | Same absolute path on submit, coordinator, and every worker |
| Python executable | Current interpreter | Submitting interpreter path embedded in script | Same path embedded in coordinator and sample scripts |
| Processor module | Current environment | Importable on allocation node | Importable on coordinator/worker environment |
| Custom registered genome | Visible locally | Visible on allocation node | Visible on every worker |
| Generated scripts | Not used | Coordinator script, usually in `workspace/slurm/` | Coordinator plus per-unit scripts in `workspace/slurm/` |
| Logs | `workspace/logs/<species>/` | Unit logs plus `%j.coordinator.log` | Unit logs, coordinator log, and `sample-<index>.<jobid>.log` |

The Slurm generator resolves workspace and execution-record paths before
writing commands. A workstation path such as
`C:\data\ncbi-workspace` is not usable on a Linux cluster unless that exact
path really exists there.

## Slurm value restrictions

| Field | Accepted by package validation | Additional responsibility |
| --- | --- | --- |
| `partition` | Letters, numbers, underscore, dot, hyphen, and comma-separated values | Every named partition must exist and accept the requested resources |
| `account` | Letters, numbers, underscore, dot, or hyphen | Must be valid for your user |
| `qos` | Letters, numbers, underscore, dot, or hyphen | Must be allowed by the site |
| Time limits | Digits, colon, and hyphen | Slurm must accept the resulting syntax and the partition limit |
| Memory GB | Positive float | Generated `--mem` is rounded up to a whole GB |

Whitespace, shell fragments, slashes, and arbitrary Slurm expressions are
rejected in scheduler fields.

## Submission parameters

`DatasetBuilder.submit_slurm()` accepts:

| Parameter | Required | Meaning | Restriction |
| --- | --- | --- | --- |
| `catalog: RunCatalog` | Yes | Selected runs to snapshot and group | Deduplicated again before execution creation |
| `processor_reference: str` | Yes | Importable `module:object` callable | Must import in the compute-node Python environment |
| `execution` | Yes | Single-node or distributed execution object | Local execution is not accepted here |
| `queue: QueuePolicy \| None` | No | Streaming and cleanup policy | Defaults to `QueuePolicy()` |
| `group_by` | No | Per-call grouping override | Must remain compatible with stable workspace configuration |
| `genome_pins: dict[int, str] \| None` | No | Exact assembly accession by taxonomy ID | Taxonomy must match the unit |
| `query: str \| None` | No | Provenance stored in the execution record | Does not itself fetch a catalog |
| `retry_failed: bool` | No | Reclaims matching failed units | Default `False` leaves matching failures untouched |
| `script_path: Path \| None` | No | Coordinator `.sbatch` destination | Defaults below `workspace/slurm/` |
| `submit: bool` | No | Call `sbatch` after script generation | `False` is the safe first-run dry run |

The return is `(script_path, job_id)`. `job_id` is `None` for a dry run.

## Safe first configuration

Use this sequence for any mode:

1. Select one or two representative units, including the largest if practical.
2. Set minimum CPUs to the smallest value that lets the processor run
   acceptably.
3. Set maximum CPUs to a measured useful limit, not automatically the whole
   server.
4. Set `max_running_jobs=1`.
5. Reserve generous storage and, on Slurm, memory and time.
6. Use `QueuePolicy(download_workers=1, keep_failed_inputs=True)`.
7. For Slurm, use `submit=False` and inspect the generated script.
8. Run the small set and measure peak memory, disk growth, and elapsed time.
9. Compute safe concurrency from CPU, memory, and storage separately; use the
   smallest result.
10. Increase download and processing concurrency one dimension at a time.

## Minimal templates

### Local

```python
execution = LocalExecution(
    total_cpus=32,
    min_cpus_per_job=4,
    max_cpus_per_job=8,
    max_running_jobs=1,
    storage=FilesystemStorage(reserve_free_gb=200),
)
```

Continue with [Local execution](LocalExecution.md) before scaling it.

### Single-node Slurm

```python
execution = SlurmSingleNodeExecution(
    allocation_cpus=32,
    allocation_memory_gb=128,
    allocation_time_limit="1-00:00:00",
    min_cpus_per_job=4,
    max_cpus_per_job=8,
    memory_gb_per_job=64,
    max_running_jobs=1,
    storage=QuotaStorage(quota_gb=2_000, reserve_gb=200),
)
```

Continue with [Single-node Slurm](SlurmSingleNodeExecution.md).

### Distributed Slurm

```python
execution = SlurmDistributedExecution(
    total_cpu_quota=33,       # 1 coordinator CPU + up to 32 worker CPUs.
    max_running_jobs=1,
    cpus_per_node=32,
    min_cpus_per_job=8,
    max_cpus_per_job=32,
    memory_gb_per_job=128,
    worker_time_limit="1-00:00:00",
    coordinator_cpus=1,
    coordinator_memory_gb=4,
    storage=QuotaStorage(quota_gb=2_000, reserve_gb=200),
)
```

Continue with [Distributed Slurm](SlurmDistributedExecution.md).

## Current implementation restrictions

- Local execution has no memory parameter or memory enforcement.
- Single-node `memory_gb_per_job` is an admission reservation inside one
  allocation, not a separate Slurm memory cgroup per sample.
- Distributed workers stage their own input. `download_workers` does not
  create a separate distributed download pool.
- Distributed CPU requests are fixed after worker submission.
- Slurm requires an importable processor reference; direct callable objects
  are local-only.
- The standard Slurm worker reconstruction uses the default SRA provider and
  default genome manager. Custom provider/manager objects attached to the
  submitting builder are not serialized.
- Generated Slurm scripts pass the workspace, email, grouping, execution
  record, and processor reference. They do not reproduce arbitrary shell
  initialization.
- Queue storage values are estimates based on catalog size. They do not
  measure processor-specific temporary files in advance.
- One sample larger than `max_inflight_gb` may be admitted when nothing else is
  in flight, preventing a permanent deadlock; physical storage policy must
  still admit its raw size.
- A failed cleanup is logged but does not turn already validated processing
  outputs into failure.

## Next pages

- [Local execution](LocalExecution.md)
- [Single-node Slurm](SlurmSingleNodeExecution.md)
- [Distributed Slurm](SlurmDistributedExecution.md)
- [Storage and queue policy](Storage.md)
- [Architecture and restart behavior](Architecture.md)
- [Execution API reference](../src/ncbi_dataset_builder/execution/README.md)
