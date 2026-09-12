# Storage and queue policy

Storage control has two independent layers:

1. the execution system asks a storage policy how much physical capacity is
   currently usable; and
2. `QueuePolicy` optionally limits the estimated size of work admitted at the
   same time.

Use both layers on large datasets. A quota or free-space reserve protects the
filesystem. The in-flight window prevents the queue from filling that capacity
with too many simultaneous samples.

## Which storage policy to use

| Setup | Storage class | Why |
| --- | --- | --- |
| Dedicated local disk where reported free space is yours to use | `FilesystemStorage` | It reads current filesystem free space |
| Shared filesystem with a user/project quota | `QuotaStorage` | Global free space does not describe your writable capacity |
| Slurm single-node execution | `QuotaStorage` | Required by the execution class |
| Slurm distributed execution | `QuotaStorage` | Required by the execution class |
| Local server on quota-controlled shared storage | `LocalExecution` currently requires `FilesystemStorage` | Leave a conservative reserve or use Slurm; a quota policy cannot be passed to `LocalExecution` |

## `FilesystemStorage`

```python
FilesystemStorage(
    reserve_free_gb=300,
)
```

### Parameters

| Parameter | Default | Meaning | How to choose | Restriction |
| --- | --- | --- | --- | --- |
| `reserve_free_gb: float` | `0.0` | Free filesystem space that must remain unavailable to the queue | Include operating-system needs, other users/processes, unexpected processor growth, and safety margin | Cannot be negative |

For workspace `W`, usable capacity is:

```text
max(0, free space on filesystem containing W - reserve_free_gb)
```

The method creates the workspace directory if needed before checking its
filesystem. Values use decimal GB.

### Starting value

On a dedicated 2 TB data filesystem with 1.6 TB currently free, you might
reserve 300 GB:

```text
reported free       1,600 GB
reserve              -300 GB
usable for admission 1,300 GB
```

The queue compares a candidate’s estimated raw size with the current usable
value. The reserve is not preallocated; another process can still consume it.

### Restrictions

- It observes filesystem-wide free space, not per-user quota.
- It cannot enforce RAM or processor-specific temporary-file limits.
- Another process can change free space between admission and writing.
- Local execution accepts this concrete class, not an arbitrary
  `StoragePolicy`.

## `QuotaStorage`

```python
QuotaStorage(
    quota_gb=5_000,
    reserve_gb=250,
    usage_root=Path("/scratch/project-owner"),
)
```

### Parameters

| Parameter | Default | Meaning | How to choose | Restriction |
| --- | --- | --- | --- | --- |
| `quota_gb: float` | Required | Total storage quota available to the user or project | Use the quota granted by the storage administrator, not global `df` capacity | Must be positive |
| `reserve_gb: float` | `0.0` | Part of the quota the queue must leave unused | Include unrelated work, logs, safety margin, and quota-reporting lag | Must be non-negative and strictly below `quota_gb` |
| `usage_root: Path \| None` | `None` | Root recursively measured as already used | Point to the directory whose contents count against the same quota | `None` measures only the builder workspace |

Usable capacity is:

```text
max(0, quota_gb - reserve_gb - recursive regular-file size of usage_root)
```

If `usage_root=None`, the workspace is measured.

### Choosing `usage_root`

| Quota definition | Recommended `usage_root` |
| --- | --- |
| Quota applies only to this workspace | Leave `None` |
| Quota applies to `/scratch/project-owner` | That project root |
| Quota applies to a user home/scratch root | The corresponding user quota root |
| Site quota cannot be represented by one directory tree | Use the closest conservative root and a larger reserve |

`QuotaStorage` does not call site-specific quota tools. It recursively sums
regular files. On a directory containing millions of files, repeated scans can
be expensive.

### Worked example

```text
project quota         5,000 GB
current usage        -2,900 GB
reserved margin        -300 GB
usable for admission  1,800 GB
```

### Restrictions

- The configured quota is trusted; the class does not verify it with Slurm or
  the filesystem.
- Files outside `usage_root` are invisible even if they count against the real
  quota.
- Quota features such as inode limits are not modeled.
- A scan is a point-in-time measurement; concurrent writers can change usage.
- Slurm execution requires this class even if the underlying filesystem also
  reports free space.

## `QueuePolicy`

```python
QueuePolicy(
    download_workers=2,
    max_inflight_gb=800,
    processing_storage_multiplier=2.5,
    cleanup="after_success",
    keep_failed_inputs=True,
    fsync_logs=True,
    scheduler_poll_seconds=1.0,
)
```

### Parameters

| Parameter | Default | What it controls | How to choose | Restrictions |
| --- | --- | --- | --- | --- |
| `download_workers: int` | `2` | Size of the shared staging thread pool in local and single-node modes | Begin with `1` or `2`; increase after checking NCBI/network and disk throughput | Must be positive; does not create a separate download pool in distributed mode |
| `max_inflight_gb: float \| None` | `None` | Estimated total admitted workload | Set below usable storage after reserving space for caches, outputs, metadata, and estimation error | Must be positive when set |
| `processing_storage_multiplier: float` | `1.0` | Estimated peak total processing footprint relative to raw size | Measure a representative sample; use a conservative initial value such as `2`–`4` for archive + FASTQ + BAM pipelines | Must be at least `1` |
| `cleanup` | `"after_success"` | Whether provider-owned inputs are removed after verified success | Keep default if NCBI input can be reacquired; choose `"never"` for offline reuse | Only `"after_success"` or `"never"` |
| `keep_failed_inputs: bool` | `True` | Whether a failed unit retains provider-owned input | Keep `True` during development and debugging | Has no effect when `cleanup="never"` |
| `fsync_logs: bool` | `True` | Whether unit logs are synchronized at phase boundaries | Keep for durable diagnostics; consider `False` only after measuring filesystem metadata overhead | Boolean |
| `scheduler_poll_seconds: float` | `1.0` | Maximum local/single-node wait between queue checks | Usually leave at `1.0`; increase if polling creates filesystem/scheduler noise | Must be positive |

## Exact in-flight estimate

### Local and single-node modes

The queue tracks:

- raw size for units downloading;
- raw size for units downloaded and ready;
- raw size multiplied by `processing_storage_multiplier` for units processing.

Equivalent estimate:

```text
sum(raw size of downloading and ready units)
+ sum(raw size of processing units × processing_storage_multiplier)
```

Before admitting a new download, its raw size is added to that estimate.

### Distributed mode

There is no separate coordinator download pool. Each active worker stages and
processes its own unit, so the estimate is:

```text
sum(raw size of active worker units × processing_storage_multiplier)
```

A candidate adds its own multiplied raw estimate before admission.

### Oversized single-unit exception

If nothing is currently in flight, one unit may be admitted even when its
estimate exceeds `max_inflight_gb`. This prevents a permanent deadlock where
the queue can never start the only remaining sample.

The physical storage policy must still report enough capacity for that unit’s
raw size. Therefore:

- `max_inflight_gb` is a concurrency/window control, not a hard single-sample
  size limit;
- `prefetch_max_size` is the separate per-SRA-run archive limit; and
- a conservative free-space/quota reserve remains essential.

## Choosing `processing_storage_multiplier`

The value means total peak processing storage divided by raw catalog size.
It does not mean “this many additional copies.”

Measure:

```text
multiplier = peak bytes attributable to one active unit / catalog raw bytes
```

Include the provider archive, converted FASTQs, processor staging, temporary
BAMs, final outputs, and any other files that coexist at peak.

| Observed workflow | Conservative initial range |
| --- | --- |
| Processor reads compressed FASTQ and writes one small result | `1.2`–`2` |
| SRA archive + compressed FASTQ + alignment intermediates | `2`–`4` |
| Large uncompressed intermediates or several BAM copies | Measure explicitly; often above `4` |

These are starting ranges, not guarantees.

## Choosing `max_inflight_gb`

Start with:

```text
usable storage
- permanent caches already expected to grow
- final outputs for the run
- unrelated workspace growth
- estimation safety margin
= maximum safe in-flight window
```

Then cap it further if you want fewer simultaneous large samples.

Example:

```text
usable quota after reserve        1,800 GB
expected genome/index caches       -150 GB
expected final outputs             -250 GB
additional uncertainty             -300 GB
candidate max_inflight_gb         1,100 GB
```

## Cleanup matrix

| `cleanup` | Unit outcome | `keep_failed_inputs` | Provider-owned cleanup |
| --- | --- | --- | --- |
| `"never"` | Any | Any | Keep |
| `"after_success"` | Success | Any | Remove |
| `"after_success"` | Failure | `True` | Keep |
| `"after_success"` | Failure | `False` | Remove |

Cleanup is restricted to provider-declared roots strictly below
`workspace/fastq/`. The queue refuses to delete the `fastq` root itself or a
path outside that boundary.

Cleanup does not remove:

- `workspace/work/units/`;
- `workspace/outputs/`;
- normalized metadata;
- genome caches or indexes;
- state, execution records, or logs; or
- processor outputs declared in `ProcessingResult`.

The processor has its own retention policy. For the built-in ATAC processor,
see [ATAC retention](AtacSeqProcessing.md#retention-parameters).

## First-run worksheet

Fill this table before creating `QueuePolicy`:

| Question | Your value |
| --- | --- |
| Typical raw unit size | |
| Largest raw unit size | |
| Measured peak size of one active unit | |
| Derived multiplier plus safety margin | |
| Current usable free space or quota | |
| Space reserved for non-in-flight data | |
| Safe in-flight window | |
| Safe simultaneous NCBI stage operations | |
| Whether successful input can be downloaded again | |
| Whether failed input must be retained for debugging | |

## Recommended conservative starting policy

```python
queue = QueuePolicy(
    download_workers=1,
    max_inflight_gb=largest_raw_unit_gb * 2.5,
    processing_storage_multiplier=2.5,
    cleanup="after_success",
    keep_failed_inputs=True,
    fsync_logs=True,
)
```

Replace the multiplier with measured data. If one unit is larger than the
window, remember the oversized-unit exception.

## Related pages

- [Choosing an execution system](ExecutionSystems.md)
- [Local execution](LocalExecution.md)
- [Single-node Slurm](SlurmSingleNodeExecution.md)
- [Distributed Slurm](SlurmDistributedExecution.md)
- [Architecture and cleanup boundaries](Architecture.md)
- [Execution API reference](../src/ncbi_dataset_builder/execution/README.md)
