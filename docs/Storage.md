# Storage policies

Storage admission is system-specific because local filesystems and Slurm
clusters expose different usable capacity.

## Ordinary server

```python
FilesystemStorage(reserve_free_gb=500)
```

The queue reads free space on the filesystem containing the workspace and
admits a sample only above the 500 GB reserve. This is appropriate when the
process can actually consume the reported free space.

## Slurm cluster

```python
QuotaStorage(
    quota_gb=5_000,
    reserve_gb=250,
    usage_root=Path("/scratch/project-owner"),
)
```

Shared storage may report hundreds of terabytes free while the user can write
only 5 TB. `QuotaStorage` therefore measures files owned below `usage_root` and
subtracts them plus the reserve from the configured quota. If `usage_root` is
omitted, only the builder workspace counts. The library does not query a
site-specific quota command; set `usage_root` to the directory that best
matches how the scheduler/site accounts your files.

`QueuePolicy.max_inflight_gb` is an additional workload window. The scheduler
estimates raw plus processor storage with `processing_storage_multiplier`; it
does not replace either storage policy.

