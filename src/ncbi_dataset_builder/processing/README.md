# `processing`

Processors are assay-specific callables separated from acquisition and
execution. The scheduler only knows their small typed contract, so a custom
processor works locally and on either Slurm system without scheduler-specific
code.

## `Processor`

A processor implements:

```python
def process(fastq: FastqSet, genome: GenomeRef, threads: int) -> ProcessingResult:
    ...
```

`fastq` contains local inputs plus package-owned work/output directories;
`genome` is the selected reference; and `threads` is the current CPU allocation
for that sample. A successful `ProcessingResult` must declare at least one
non-empty final output. `DatasetBuilder.build()` accepts either the callable or
an import string such as `"my_package.processor:process"`.

`load_processor(reference)` validates the `module:object` form, imports it, and
returns the callable. Slurm workers use it because compute nodes reconstruct
the processor from the saved reference.

The built-in implementation is documented in [`atac`](atac/README.md). See
[Processors](../../../docs/Processors.md) for a complete custom function.

