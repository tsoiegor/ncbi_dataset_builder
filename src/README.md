# Source tree

Python source uses a `src/` layout so importing from a checkout does not
accidentally bypass installation.

| Path | Purpose |
| --- | --- |
| [`ncbi_dataset_builder/README.md`](ncbi_dataset_builder/README.md) | Complete package API index |
| [`ncbi_dataset_builder/acquisition/`](ncbi_dataset_builder/acquisition/README.md) | FASTQ/GEO/genome acquisition |
| [`ncbi_dataset_builder/catalog/`](ncbi_dataset_builder/catalog/README.md) | RunInfo table API |
| [`ncbi_dataset_builder/execution/`](ncbi_dataset_builder/execution/README.md) | Queue, resources, state, and Slurm |
| [`ncbi_dataset_builder/metadata/`](ncbi_dataset_builder/metadata/README.md) | NCBI metadata clients and models |
| [`ncbi_dataset_builder/processing/`](ncbi_dataset_builder/processing/README.md) | Processor contract |
| [`ncbi_dataset_builder/processing/atac/`](ncbi_dataset_builder/processing/atac/README.md) | Built-in ATAC-seq API |
| [`ncbi_dataset_builder/workspace/`](ncbi_dataset_builder/workspace/README.md) | Workspace and publication |
| [`ncbi_dataset_builder/support/`](ncbi_dataset_builder/support/README.md) | Advanced support helpers |
| [`ncbi_dataset_builder/cli/`](ncbi_dataset_builder/cli/README.md) | CLI implementation |

Install the checkout before running examples or tests:

```bash
# Editable install with test and progress dependencies.
python -m pip install -e ".[dev,progress]"
```

The package [project README](../README.md) is the user starting point.
