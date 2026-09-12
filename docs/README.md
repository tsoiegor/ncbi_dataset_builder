# Documentation

- [Architecture](Architecture.md): package and workspace data flow.
- [Installation](Installation.md): Conda, optional dependencies, and tools.
- [CLI reference](CommandLineInterface.md): every command, flag, and example.
- [Catalogs](Catalogs.md): NCBI/CSV loading and Polars transformations.
- [Local execution](LocalExecution.md): one ordinary server.
- [Single-node Slurm](SlurmSingleNodeExecution.md): one hard allocation in slurm.
- [Distributed Slurm](SlurmDistributedExecution.md): slurm coordinator plus sample jobs.
- [Storage](Storage.md): local filesystem reserve and Slurm quota accounting.
- [Processors](Processors.md): custom processor contract.
- [ATAC-seq processing](AtacSeqProcessing.md): built-in workflow and retention.

Class-by-class implementation references live beside each source subpackage in
[`src/ncbi_dataset_builder`](../src/ncbi_dataset_builder/README.md).
