# Writing a processor

A processor owns assay-specific work after FASTQ and genome preparation. It is
a callable with three inputs and one `ProcessingResult`:

```python
processor(
    fastq: FastqSet,
    genome: GenomeRef,
    context: ProcessingContext,
) -> ProcessingResult
```

The builder controls unit identity, state, paths, retries, validation, and
cleanup. The processor controls external commands, intermediates, scientific
outputs, metrics, and tool versions.

## Minimal complete processor

```python
from pathlib import Path
import subprocess

from ncbi_dataset_builder import FastqSet, GenomeRef, ProcessingContext, ProcessingResult


def process_sample(
    fastq: FastqSet,
    genome: GenomeRef,
    context: ProcessingContext,
) -> ProcessingResult:
    """Process one prepared unit.

    Args:
        fastq: Validated local FASTQ paths and acquisition provenance.
        genome: Selected local reference and its provenance.
        context: Unit identity, processor-owned output directory, log, execution, and CPU budget.
    """

    # Fail before expensive commands if inputs are missing or inconsistent.
    fastq.validate()
    genome.validate()

    # The processor may organize artifacts and intermediates however it wants.
    work = context.output_dir / "work"
    work.mkdir(parents=True, exist_ok=True)

    context.output_dir.mkdir(parents=True, exist_ok=True)
    output = context.output_dir / f"{context.unit_id}.result.txt"
    partial = output.with_name(output.name + ".part")

    # Pass the assigned CPU budget to tools that support it.
    subprocess.run(
        [
            "my-tool",
            "--threads",
            str(context.threads),
            "--genome",
            str(genome.fasta),
            "--output",
            str(partial),
            *map(str, fastq.read1),
        ],
        check=True,
    )

    # Publish the final name only after the command succeeds.
    partial.replace(output)

    result = ProcessingResult(
        success=True,
        outputs={"result": output},
        metrics={"input_layout": fastq.layout.value},
        tool_versions={"my-tool": "1.0"},
    )
    result.validate()
    return result
```

## Processor arguments

| Argument | Meaning | Processor responsibility |
| --- | --- | --- |
| `fastq: FastqSet` | Local input paths, layout, and acquisition provenance | Validate and obey layout |
| `genome: GenomeRef` | Selected FASTA, accession, taxonomy, checksum, and rationale | Validate; build/reuse indexes safely |
| `context: ProcessingContext` | Unit identity, one owned output path, log, execution ID, and CPUs | Keep every created artifact below `output_dir` |

### Optional experiment-description hook

A callable may expose `description_profile = "training"`. Before invoking that
processor, the builder loads `workspace/runtime/metadata/metadata.json`, requires
exactly one Experiment in the unit, projects
`bundle.descriptions_by_experiment(profile="training")`, and writes the matching
record as `<output_dir>/<unit-id>.json`. The processor must declare that file in
`ProcessingResult.outputs` if it is a durable artifact. Run
`builder.enrich_metadata(catalog)` before building with such a processor.

## `FastqSet` fields

| Field | Meaning |
| --- | --- |
| `layout` | `FastqLayout.SINGLE`, `PAIRED`, or `MIXED` |
| `run_accessions` | Source runs in merge order |
| `read1` | First-mate paths |
| `read2` | Matching second-mate paths |
| `single` | Single-end or orphan paths |
| `source` | Provider label such as `sra` or `geo` |
| `checksums` | Provider checksums keyed by path where available |
| `provider_metadata` | FASTQ-provider provenance |

### Layout restrictions

| Layout | Required paths |
| --- | --- |
| Single | At least one `single` file |
| Paired | Non-empty `read1` and equal number of `read2` files |
| Mixed | Valid paired paths plus at least one `single` file |

Every referenced input must exist and be non-empty.

## `GenomeRef` fields

| Field | Meaning |
| --- | --- |
| `taxid`, `scientific_name` | Species identity |
| `accession` | Versioned assembly or custom reference ID |
| `fasta` | Local non-empty FASTA |
| `sha256` | Reference checksum |
| `source_database` | NCBI or custom source |
| `assembly_level`, `refseq_category` | Assembly provenance |
| `selection_rationale` | Human-readable selection reasons |
| `indexes` | Optional named index paths |

If several units share a genome index, protect index construction with a lock
and finalize completed index files atomically. The built-in ATAC processor is an
example.

## `ProcessingResult` parameters

| Parameter | Default | Meaning | Restriction |
| --- | --- | --- | --- |
| `success: bool` | Required | Whether scientific processing succeeded | Must be `True` for builder success |
| `outputs: dict[str, Path]` | Empty | Durable files keyed by stable artifact role | Paths may be relative to `output_dir`; every resolved path must remain inside it and be non-empty |
| `metrics: dict` | Empty | Compact structured QC/summary values | Must be JSON-serializable for state |
| `tool_versions: dict[str, str]` | Empty | Executable/software versions | Prefer actual reported versions |
| `message: str \| None` | `None` | Failure/status explanation | Used when `success=False` |

The builder calls `result.validate()` and then records SHA-256 and file facts
for every declared output.

## Path ownership

| Location | Put here | Restart behavior |
| --- | --- | --- |
| `context.output_dir` | All processor artifacts and intermediates | The complete unit directory may be reset when the unit rebuilds |
| Original `fastq.read*` paths | Provider-owned input | Do not delete from processor |
| Genome cache/index area | Shared reference artifacts | Use locks and complete-file validation |
| Arbitrary external path | Forbidden for declared output | Builder rejects artifacts outside the owned directory |

Use `.part` files plus atomic rename so interruption cannot leave a final name
that appears complete.

## CPU behavior by execution mode

| Mode | How `context.threads` is chosen |
| --- | --- |
| Local | Dynamic launch allocation between configured minimum/maximum |
| Single-node Slurm | Same dynamic allocation inside one Slurm CPU pool |
| Distributed Slurm | Fixed worker CPU request chosen before submission |

The processor must not assume every unit receives the same value. Also account
for pipelines where two commands run concurrently: giving both the full
`threads` value can oversubscribe CPUs.

## Callable forms

### Direct callable

```python
report = builder.build(
    catalog,
    process_sample,
    execution=local_execution,
)
```

Direct callables are local-only.

### Import reference

```python
report = builder.build(
    catalog,
    "my_package.processors:process_sample",
    execution=local_execution,
)
```

```python
script, job_id = builder.submit_slurm(
    catalog,
    processor_reference="my_package.processors:process_sample",
    execution=slurm_execution,
)
```

The reference must contain `module:attribute`; the attribute must be callable.

## Configured callable for Slurm

Save an importable module:

```python
# my_package/atac_processors.py
from pathlib import Path

from ncbi_dataset_builder.processing.atac import AtacSeqConfig, AtacSeqProcessor

# Module-level object can be loaded as a callable by every worker.
custom_atac = AtacSeqProcessor(
    AtacSeqConfig(
        bam2bw_script=Path("/shared/ExpressionPredict/src/bam2bw.py"),
        min_coverage=1_000_000,
    )
)
```

Then use:

```python
processor_reference="my_package.atac_processors:custom_atac"
```

The module and its dependencies must exist in the exact Python environment
used on compute nodes.

## Processor identity and rebuilds

For a direct callable, the builder derives identity from:

- module and qualified name;
- `config` representation when present; and
- source-file SHA-256 when available.

An import string uses that string as identity. Local `build()` also accepts
`processor_id` for an explicit semantic version.

| Change | Recommended action |
| --- | --- |
| Function source changed | Derived local source hash normally changes |
| Callable configuration changed | Expose it as `.config` or change explicit identity |
| Import string points to behavior changed in place | Use a versioned reference/module when old success must not be reused |
| Notebook/dynamic callable lacks stable source | Pass `processor_id="my-processor-v2"` |

## Failure rules

Raise an exception when:

- an external command fails;
- an expected intermediate is invalid;
- scientific validation fails; or
- a final output cannot be finalized safely.

Returning `ProcessingResult(success=False, message=...)` also fails validation,
but raising preserves a useful traceback in unit state/logs.

## Preflight pattern

```python
def preflight() -> dict[str, str]:
    # Resolve required commands and capture versions before expensive work.
    completed = subprocess.run(
        ["my-tool", "--version"],
        check=True,
        capture_output=True,
        text=True,
    )
    return {"my-tool": completed.stdout.strip()}
```

Run preflight in the actual compute environment.

## Review checklist

- [ ] All three inputs have correct types.
- [ ] `fastq.validate()` and `genome.validate()` run first.
- [ ] Every layout is handled or rejected explicitly.
- [ ] Every created file stays below `output_dir`.
- [ ] External commands receive safe argument lists.
- [ ] CPU usage respects `threads`.
- [ ] Shared indexes use locking and complete validation.
- [ ] Final names appear atomically.
- [ ] `ProcessingResult.outputs` contains every artifact required for reuse.
- [ ] Outputs are non-empty before success.
- [ ] Metrics are compact and JSON-serializable.
- [ ] Tool versions are recorded.
- [ ] Processor imports in the Slurm environment.

## Related pages

- [Processing API reference](../src/ncbi_dataset_builder/processing/README.md)
- [Built-in ATAC processor](AtacSeqProcessing.md)
- [Local execution](LocalExecution.md)
- [Choosing an execution system](ExecutionSystems.md)
- [Architecture and fingerprints](Architecture.md)
