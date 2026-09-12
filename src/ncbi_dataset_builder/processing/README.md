# Processing API

The `ncbi_dataset_builder.processing` subpackage defines the small contract
between dataset scheduling and assay-specific science. A processor receives
local FASTQ paths, one validated genome reference, and a CPU allocation. It
must return a `ProcessingResult` declaring every artifact that makes the unit
complete.

The package includes a full [ATAC-seq processor](atac/README.md). Other assays
can implement the same callable shape without changing catalog, acquisition,
execution, state, or publication code.

## Module map

| Module | Contents |
| --- | --- |
| `base.py` | `Processor` protocol and `load_processor` |
| [`atac/`](atac/README.md) | Built-in ATAC-seq processor, config, and retention |
| `__init__.py` | Public processing exports |

## `Processor`

`Processor` is a typing `Protocol`, not a concrete base class. A compatible
callable implements:

```python
__call__(
    fastq: FastqSet,
    genome: GenomeRef,
    threads: int,
) -> ProcessingResult
```

| Argument | Meaning |
| --- | --- |
| `fastq` | Validated single, paired, or mixed local inputs plus unit-specific work and output directories. |
| `genome` | Checksum-validated local reference selected for the unit’s taxonomy ID. |
| `threads` | CPUs assigned at launch. Respect this value; it can differ between units. |
| Return | A `ProcessingResult` whose declared output files exist and are non-empty. |

Runtime inheritance from `Processor` is unnecessary. Functions, callable
instances, and importable callable objects all work.

## Processor rules

1. Call `fastq.validate()` and `genome.validate()` before expensive work.
2. Read inputs from `fastq.read1`, `read2`, and `single` according to
   `fastq.layout`.
3. Put recoverable intermediates below `fastq.work_dir`.
4. Put final artifacts below `fastq.output_dir`.
5. Use `threads` as the processor’s CPU budget.
6. Publish files atomically when possible; a partial file must not look final.
7. Return success only after validating the artifacts.
8. Declare every artifact required for reuse in `ProcessingResult.outputs`.
9. Store compact QC in `metrics` and executable versions in
   `tool_versions`.

The builder calls `ProcessingResult.validate()`, computes SHA-256 values and
file metadata, then persists the result in per-unit state.

## `load_processor(reference) -> Processor`

`reference: str` must use `"package.module:object"` form. The function
imports the module, resolves the attribute, verifies that it is callable, and
returns it.

```python
from ncbi_dataset_builder.processing import load_processor

processor = load_processor("my_pipeline.processors:process_sample")
```

Local `DatasetBuilder.build()` accepts either this string or a callable
directly. Slurm requires a string because a different Python process imports
the object on a compute node.

## Minimal processor

```python
from pathlib import Path

from ncbi_dataset_builder import FastqSet, GenomeRef, ProcessingResult


def process_sample(
    fastq: FastqSet,
    genome: GenomeRef,
    threads: int,
) -> ProcessingResult:
    """Write one small final artifact for a processing unit."""

    # Fail early if provider files or the selected FASTA are unusable.
    fastq.validate()
    genome.validate()

    # The builder owns this unit-specific output path.
    fastq.output_dir.mkdir(parents=True, exist_ok=True)
    output: Path = fastq.output_dir / f"{fastq.unit_id}.summary.txt"

    # A real processor would pass `threads` to its external commands.
    output.write_text(
        f"unit={fastq.unit_id}\n"
        f"assembly={genome.accession}\n"
        f"cpus={threads}\n",
        encoding="utf-8",
    )

    # Declared outputs define a reusable success.
    return ProcessingResult(
        success=True,
        outputs=(output,),
        metrics={
            "input_fastq_files": (
                len(fastq.read1) + len(fastq.read2) + len(fastq.single)
            ),
        },
        tool_versions={"my-pipeline": "1.0"},
    )
```

Save the function in an importable module for Slurm:

```python
script, job_id = builder.submit_slurm(
    catalog,
    processor_reference="my_pipeline.processors:process_sample",
    execution=execution,
)
```

The package must be installed, or its parent directory must be on
`PYTHONPATH`, in the same compute-node environment embedded in the Slurm
script.

## Built-in exports

| Name | Purpose |
| --- | --- |
| `AtacIntermediateFiles` | Retention policy for each processor-created file category. |
| `AtacSeqConfig` | Executables, alignment/coverage choices, strict mixed-layout thresholds, and retention. |
| `AtacSeqProcessor` | Configurable callable implementation. |
| `default_atac_processor` | Module-level default instance, exported from `ncbi_dataset_builder.processing` and `.processing.atac`. |
| `process_atac` | Importable function delegating to the default instance; also exported at package top level. |

See the [ATAC API](atac/README.md) before changing retention or mixed-layout
settings.

## Errors and retries

A processor may raise any exception. The builder catches exceptions at the
sample boundary, records the traceback, marks that unit failed, and allows
other units to continue.

Matching failed state is not retried automatically:

```python
report = builder.build(
    catalog,
    process_sample,
    execution=execution,
    retry_failed=True,  # Retry units whose fingerprint still matches.
)
```

If processor semantics changed, also provide a new importable implementation or
`processor_id` so stale successful work is not reused as if it were current.

## Internal versus provider cleanup

There are two independent cleanup layers:

| Layer | Configured by | Can remove |
| --- | --- | --- |
| Queue input cleanup | `QueuePolicy` | Provider-declared roots below `workspace/fastq/` |
| Processor intermediate cleanup | Processor-specific config | Files below its work/output/cache paths that it explicitly owns |

A processor must never treat arbitrary input paths as owned cleanup roots.

For a longer guide, see [Writing a processor](../../../docs/Processors.md).
