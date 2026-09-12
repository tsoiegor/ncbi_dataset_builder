# Writing a processor

A processor is a Python callable with exactly three inputs and one typed
result:

```python
from ncbi_dataset_builder import FastqSet, GenomeRef, ProcessingResult

def process_sample(
    fastq: FastqSet,
    genome: GenomeRef,
    threads: int,
) -> ProcessingResult:
    """Process one sample.

    Args:
        fastq: Local input paths and unit work/output directories.
        genome: Selected, validated reference genome.
        threads: CPUs currently assigned to this sample.
    """

    fastq.validate()
    genome.validate()
    fastq.output_dir.mkdir(parents=True, exist_ok=True)
    output = fastq.output_dir / f"{fastq.unit_id}.result.txt"
    output.write_text(f"assembly={genome.accession}\ncpus={threads}\n")
    return ProcessingResult(
        success=True,
        outputs=(output,),
        metrics={"threads": threads},
        tool_versions={"my-tool": "1.0"},
    )
```

## Contract

- Read `fastq.layout`, `read1`, `read2`, and `single`; call `validate()` before
  expensive work.
- Put recoverable intermediates below `fastq.work_dir` and final outputs below
  `fastq.output_dir`.
- Respect `threads`; the scheduler may redistribute CPUs between samples.
- Return `ProcessingResult(success=True, outputs=(...))` only after each output
  is complete. Outputs must exist and be non-empty.
- Put compact structured measurements in `metrics` and executable versions in
  `tool_versions`. Set `message` when returning a failure.

Local `builder.build()` accepts the function directly. Slurm requires an import
reference such as `"my_processors:process_sample"`, and the module must be
importable in the compute-node environment. A local workflow may also use the
same reference to test import behavior.

The built-in [`AtacSeqProcessor`](AtacSeqProcessing.md) is a more complete
example with external commands, caching, validation, and explicit cleanup.

