# Examples

These files are small, runnable compositions of the public API. Read the
[project introduction](../README.md) first, then choose the example matching
your server.

## Example map

| File | What it demonstrates | Read next |
| --- | --- | --- |
| [`catalog_transform.py`](catalog_transform.py) | Offline RunInfo loading plus audited Polars filtering/derived columns | [Catalog API](../src/ncbi_dataset_builder/catalog/README.md) |
| [`custom_processor.py`](custom_processor.py) | Minimal `FastqSet, GenomeRef, threads -> ProcessingResult` callable | [Processing API](../src/ncbi_dataset_builder/processing/README.md) |
| [`local_execution.py`](local_execution.py) | Streaming on one 100-CPU server with free-space reserve | [Local guide](../docs/LocalExecution.md) |
| [`slurm_single_node.py`](slurm_single_node.py) | Several samples sharing one 128-CPU Slurm allocation | [Single-node guide](../docs/SlurmSingleNodeExecution.md) |
| [`slurm_distributed.py`](slurm_distributed.py) | Coordinator plus independent sample jobs under CPU/storage quotas | [Distributed guide](../docs/SlurmDistributedExecution.md) |

## Before running

The paths, email, API key, partitions, quotas, and resource limits in these
files are examples. The displayed API key is fake.

1. Install the package in the active environment.
2. Install external tools required by your chosen provider/processor.
3. Replace workspace and catalog paths.
4. Set a real NCBI email for live queries.
5. Check CPU, memory, storage, partition, account, and QoS values against your
   server.
6. Make `custom_processor.py` importable on Slurm compute nodes if you use it.

## Suggested order

```bash
# 1. Inspect how a catalog is transformed without running bioinformatics tools.
python examples/catalog_transform.py

# 2. Read the callable that every execution system can use.
python -c "from examples.custom_processor import process_sample; print(process_sample)"

# 3. Edit one execution example for your actual system, then run it.
python examples/local_execution.py
```

`local_execution.py` imports `custom_processor` as a neighboring module.
When launched from another working directory, run it in a way that keeps the
examples directory importable or copy the processor into your own package.

## What the custom processor does

`custom_processor.process_sample()` validates the FASTQ and genome, streams
all FASTQ bytes through SHA-256, writes one unit summary, and declares that
file in `ProcessingResult`. It is intentionally scientifically trivial; its
purpose is to show the scheduling contract, output path, CPU argument, metrics,
and reproducible result shape.

Use the built-in
`ncbi_dataset_builder.processing.atac:process_atac` for the documented
ATAC-seq workflow.
