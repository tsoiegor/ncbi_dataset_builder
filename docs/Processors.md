# Writing a processor

A processor owns assay-specific work after FASTQ and genome preparation. It is
a callable with three inputs and one `ProcessingResult`:

```python
processor(
    fastq: FastqSet,
    genome: GenomeRef,
    threads: int,
) -> ProcessingResult
```

The builder controls unit identity, state, paths, retries, validation, and
cleanup. The processor controls external commands, intermediates, scientific
outputs, metrics, and tool versions.

## Minimal complete processor

```python
from pathlib import Path
import subprocess

from ncbi_dataset_builder import FastqSet, GenomeRef, ProcessingResult


def process_sample(
    fastq: FastqSet,
    genome: GenomeRef,
    threads: int,
) -> ProcessingResult:
    """Process one prepared unit.

    Args:
        fastq: Validated local input paths plus package-owned work/output roots.
        genome: Selected local reference and its provenance.
        threads: CPU budget selected for this invocation.
    """

    # Fail before expensive commands if inputs are missing or inconsistent.
    fastq.validate()
    genome.validate()

    # Recoverable intermediates belong below work_dir.
    work = fastq.work_dir / "my_processor"
    work.mkdir(parents=True, exist_ok=True)

    # Final reusable artifacts belong below output_dir.
    fastq.output_dir.mkdir(parents=True, exist_ok=True)
    output = fastq.output_dir / f"{fastq.unit_id}.result.txt"
    partial = output.with_name(output.name + ".part")

    # Pass the assigned CPU budget to tools that support it.
    subprocess.run(
        [
            "my-tool",
            "--threads",
            str(threads),
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
        outputs=(output,),
        metrics={"input_layout": fastq.layout.value},
        tool_versions={"my-tool": "1.0"},
    )
    result.validate()
    return result
```

## Processor arguments

| Argument | Meaning | Processor responsibility |
| --- | --- | --- |
| `fastq: FastqSet` | Local input paths, layout, provenance, and unit-specific directories | Validate and obey layout/path ownership |
| `genome: GenomeRef` | Selected FASTA, accession, taxonomy, checksum, and rationale | Validate; build/reuse indexes safely |
| `threads: int` | CPUs allocated at this launch | Treat as an upper budget and pass to tools deliberately |

## `FastqSet` fields

| Field | Meaning |
| --- | --- |
| `unit_id` | Stable processing-unit identifier |
| `layout` | `FastqLayout.SINGLE`, `PAIRED`, or `MIXED` |
| `run_accessions` | Source runs in merge order |
| `read1` | First-mate paths |
| `read2` | Matching second-mate paths |
| `single` | Single-end or orphan paths |
| `source` | Provider label such as `sra` or `geo` |
| `work_dir` | Package-owned unit work root |
| `output_dir` | Package-owned unit final-output root |
| `checksums` | Provider checksums keyed by path where available |
| `metadata` | Provider provenance, including unit log path |

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
and publish completed index files atomically. The built-in ATAC processor is an
example.

## `ProcessingResult` parameters

| Parameter | Default | Meaning | Restriction |
| --- | --- | --- | --- |
| `success: bool` | Required | Whether scientific processing succeeded | Must be `True` for builder success |
| `outputs: tuple[Path, ...]` | Empty | Final files required for reuse | At least one; every path must exist and be non-empty |
| `metrics: dict` | Empty | Compact structured QC/summary values | Must be JSON-serializable for state |
| `tool_versions: dict[str, str]` | Empty | Executable/software versions | Prefer actual reported versions |
| `message: str \| None` | `None` | Failure/status explanation | Used when `success=False` |

The builder calls `result.validate()` and then records SHA-256 and file facts
for every declared output.

## Path ownership

| Location | Put here | Restart behavior |
| --- | --- | --- |
| `fastq.work_dir` | Intermediates that can be recreated | May be reset before rebuilt processing |
| `fastq.output_dir` | Final processor outputs | May be reset when the unit must rebuild |
| Original `fastq.read*` paths | Provider-owned input | Do not delete from processor |
| Genome cache/index area | Shared reference artifacts | Use locks and complete-file validation |
| Arbitrary external path | Avoid | Builder cannot safely manage/reuse it |

Use `.part` files plus atomic rename so interruption cannot leave a final name
that appears complete.

## CPU behavior by execution mode

| Mode | How `threads` is chosen |
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
from ncbi_dataset_builder.processing.atac import AtacSeqConfig, AtacSeqProcessor

# Module-level object can be loaded as a callable by every worker.
custom_atac = AtacSeqProcessor(
    AtacSeqConfig(
        bin_size=10,
        normalize_using="CPM",
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
- a final output cannot be published safely.

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
- [ ] Temporary files use `work_dir`.
- [ ] Final files use `output_dir`.
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
