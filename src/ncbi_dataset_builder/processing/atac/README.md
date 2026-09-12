# ATAC-seq processing

The `ncbi_dataset_builder.processing.atac` subpackage implements a complete
FASTQ-to-BigWig ATAC-seq processor using fastp, Bowtie2, samtools, and deepTools.
It accepts single-end, paired-end, or mixed `FastqSet` inputs, validates final
artifacts, and returns a standard `ProcessingResult`.

Read the parent [processing API](../README.md) for the shared callable
contract. This page documents every class, argument, method, file, and
retention boundary in `processing/atac/processor.py`.

## Module map

| Module | Contents |
| --- | --- |
| `processor.py` | `AtacIntermediateFiles`, `AtacSeqConfig`, `AtacSeqProcessor`, default instance, and wrapper function |
| `__init__.py` | Public ATAC export list |

## Processing stages

For one unit, `AtacSeqProcessor.__call__()`:

1. validates `FastqSet` and `GenomeRef`;
2. checks external tool availability and records versions;
3. merges ordered run FASTQs into paired and/or single staging files;
4. runs fastp separately for the paired and single components;
5. evaluates the strict mixed-layout defense when both components exist;
6. creates or reuses a Bowtie2 index for the selected assembly;
7. aligns accepted components through Bowtie2 → `samtools view` →
   `samtools sort`;
8. copies one component BAM or merges several into the final BAM;
9. creates a CSI index;
10. creates one unstranded BigWig or requested strand-specific BigWigs;
11. verifies all produced files;
12. loads fastp JSON into result metrics;
13. removes processor-owned intermediates according to
    `AtacIntermediateFiles`; and
14. returns and validates `ProcessingResult`.

## External commands

| Stage | Command behavior |
| --- | --- |
| Filtering | fastp with `--trim_poly_g`; optional `--dedup --dup_calc_accuracy 5` |
| Genome index | `bowtie2-build --threads <threads>` under the genome cache |
| Alignment | Bowtie2 `--very-sensitive --mm -p <threads>`; paired input also gets `-X maximum_insert_size` |
| BAM stream | `samtools view -b -` piped to `samtools sort -@ <threads>` |
| Component merge | `samtools merge -f -@ <threads>` when both paired and accepted single data exist |
| Final index | `samtools index -c -@ <threads>` |
| Coverage | `bamCoverage --binSize ... --numberOfProcessors <threads> --skipNAs` plus configured filters |

Bowtie2 and `samtools sort` both receive the full `threads` value while they
run in the same pipeline. Account for this tool-level concurrency when choosing
per-job CPUs. fastp alone is capped by `fastp_max_threads`, which defaults to
16.

# `AtacIntermediateFiles`

Frozen retention configuration:

```python
AtacIntermediateFiles(
    keep_staged_fastq=False,
    keep_cleaned_fastq=False,
    keep_fastp_json=True,
    keep_fastp_html=True,
    keep_component_bams=False,
    keep_final_bam=True,
    keep_final_bam_index=True,
    keep_uncompressed_genome=True,
    keep_bowtie2_index=True,
)
```

| Argument | Default | File category and effect |
| --- | ---: | --- |
| `keep_staged_fastq` | false | Keep processor-created merged/recompressed `input.*.fastq[.gz]` below unit work. Does not control original provider FASTQs. |
| `keep_cleaned_fastq` | false | Keep `*.clean*.fastq.gz` files created by fastp. These are never declared final outputs. |
| `keep_fastp_json` | true | Keep and declare paired/single `*.fastp.json` reports. Metrics are loaded before optional removal. |
| `keep_fastp_html` | true | Keep and declare paired/single `*.fastp.html` reports. |
| `keep_component_bams` | false | Keep `paired.sorted.bam` and/or `single.sorted.bam` under unit work. |
| `keep_final_bam` | true | Keep and declare `<unit>.bam`. |
| `keep_final_bam_index` | true | Keep and declare `<unit>.bam.csi`; requires `keep_final_bam=True`. |
| `keep_uncompressed_genome` | true | Keep the `<accession>.fna` created beside the Bowtie2 index. |
| `keep_bowtie2_index` | true | Keep six `.bt2` or `.bt2l` files shared by samples using the assembly. |

BigWigs are always kept and declared. Constructing a policy that retains the
CSI but removes its BAM raises `ValueError`.

Disabling genome/index retention is safest only when processing is serialized:
several samples can share those cache files.

# `AtacSeqConfig`

```python
AtacSeqConfig(
    bowtie2="bowtie2",
    bowtie2_build="bowtie2-build",
    samtools="samtools",
    fastp="fastp",
    bam_coverage="bamCoverage",
    maximum_insert_size=2000,
    bin_size=1,
    normalize_using=None,
    coverage_strands=(),
    fastp_deduplicate=True,
    fastp_max_threads=16,
    strict_mixed_layout=True,
    mixed_count_tolerance=0.001,
    mixed_max_short_read_length=30,
    mixed_min_length_ratio=0.5,
    mixed_min_retained_fraction=0.1,
    coverage_ignore_duplicates=True,
    intermediates=AtacIntermediateFiles(),
)
```

## Executable fields

| Argument | Meaning |
| --- | --- |
| `bowtie2` | Bowtie2 executable name or path. |
| `bowtie2_build` | Bowtie2 index-builder executable. |
| `samtools` | Samtools executable. |
| `fastp` | Fastp executable. |
| `bam_coverage` | deepTools `bamCoverage` executable. |

Names are resolved through `PATH` by default. Absolute executable paths are
allowed when they are valid on the machine or every relevant compute node.

## Alignment and coverage fields

| Argument | Meaning |
| --- | --- |
| `maximum_insert_size: int` | Positive Bowtie2 `-X` value for paired-end alignment. |
| `bin_size: int` | Positive `bamCoverage --binSize`. |
| `normalize_using: str | None` | Optional deepTools `--normalizeUsing` value such as one supported by the installed deepTools version. |
| `coverage_strands: tuple[str, ...]` | Empty creates one unstranded `coverage` BigWig. Otherwise each entry must be `"forward"` or `"reverse"` and creates a separate file. |
| `coverage_ignore_duplicates: bool` | Add `bamCoverage --ignoreDuplicates`. |

## Fastp fields

| Argument | Meaning |
| --- | --- |
| `fastp_deduplicate: bool` | Add fastp duplicate removal with calculation accuracy 5. |
| `fastp_max_threads: int` | Positive cap; actual fastp threads are `min(max(1, threads), fastp_max_threads)`. |

## Mixed-layout fields

| Argument | Valid range | Meaning |
| --- | --- | --- |
| `strict_mixed_layout` | Boolean | Enable conservative inspection of paired and single fastp reports. |
| `mixed_count_tolerance` | `[0, 1)` | Maximum relative difference between single-read count and paired-fragment count that is suspicious. |
| `mixed_max_short_read_length` | positive integer | Single mean length at or below this number is suspicious. |
| `mixed_min_length_ratio` | `(0, 1]` | Single-to-shorter-paired mean-length ratio below this threshold is suspicious. |
| `mixed_min_retained_fraction` | `[0, 1]` | Single reads retained by fastp below this fraction are suspicious. |
| `intermediates` | `AtacIntermediateFiles` | Processor-created retention policy. |

# Strict mixed-layout defense

This defense runs only for `FastqLayout.MIXED`, after fastp and before
alignment. It compares:

- `single.before_filtering.total_reads`;
- `paired.before_filtering.total_reads / 2`, because the paired report counts
  both mates;
- single and paired mean read lengths; and
- the single component’s after/before retained fraction.

The complete single-end branch is excluded when any configured warning rule
fires:

1. single-read count matches paired-fragment count within tolerance;
2. single mean length is at or below the short-read threshold;
3. the single/paired length ratio is below threshold;
4. fastp retained too small a single fraction; or
5. either report is missing, malformed, non-positive, or internally
   inconsistent.

Exclusion sets the in-memory cleaned single path to `None`; it does not
immediately delete a provider FASTQ. Paired-end processing continues. The
decision is recorded in
`ProcessingResult.metrics["mixed_layout_defense"]` with:

- `enabled`;
- `action` equal to `"kept_single_end"`,
  `"excluded_single_end"`, or `"disabled"`;
- human-readable `reasons`; and
- validated statistics when available.

`strict_mixed_layout=False` explicitly opts out and aligns both branches.

# `AtacSeqProcessor`

```python
AtacSeqProcessor(
    config=None,
    *,
    runner=None,
    progress=None,
)
```

| Argument | Meaning |
| --- | --- |
| `config: AtacSeqConfig | None` | Processor settings; defaults to `AtacSeqConfig()`. |
| `runner: CommandRunner | None` | Injectable external-command implementation. |
| `progress: ProgressReporter | None` | Progress/cache event reporter. |

## `preflight() -> dict[str, str]`

Require all five configured executables. Return version strings for fastp,
Bowtie2, samtools, and bamCoverage. `bowtie2-build` is required but does not
receive a separate version entry.

## `__call__(fastq, genome, threads) -> ProcessingResult`

| Argument | Meaning |
| --- | --- |
| `fastq: FastqSet` | Validated inputs and unit-specific work/output paths. |
| `genome: GenomeRef` | Selected reference FASTA and assembly identity. |
| `threads: int` | Runtime CPU allocation forwarded to tools, with the fastp cap described above. |

The method raises on invalid input, missing tools, command failures, or
missing/empty produced files. `DatasetBuilder` converts such exceptions to
failed unit state.

# Files and cache locations

For unit `SRX123` and assembly `GCF_123.1`:

```text
workspace/
├── work/units/SRX123/processing/atac/
│   ├── input.R1.fastq.gz
│   ├── input.R2.fastq.gz
│   ├── input.single.fastq.gz
│   ├── paired.clean.R1.fastq.gz
│   ├── paired.clean.R2.fastq.gz
│   ├── single.clean.fastq.gz
│   ├── paired.fastp.json
│   ├── paired.fastp.html
│   ├── single.fastp.json
│   ├── single.fastp.html
│   ├── paired.sorted.bam
│   └── single.sorted.bam
├── outputs/SRX123/
│   ├── SRX123.bam
│   ├── SRX123.bam.csi
│   └── SRX123.coverage.bw
└── work/genome_cache/
    └── .../indexes/GCF_123.1/
        ├── GCF_123.1.fna
        └── GCF_123.1.*.bt2
```

Only paths relevant to the input layout are created. With
`coverage_strands=("forward", "reverse")`, the BigWigs are
`SRX123.forward.bw` and `SRX123.reverse.bw`.

Existing non-empty staged inputs, fastp result sets, component BAMs, complete
Bowtie2 indexes, final BAMs, and BigWigs are reused by the processor’s internal
stage checks. Builder-level success reuse additionally validates the declared
output fingerprint.

# Output contract

## Always declared

- every BigWig created for configured coverage modes.

## Declared when retained

- final BAM;
- CSI index;
- fastp JSON reports; and
- fastp HTML reports.

Cleaned/staged FASTQs, component BAMs, uncompressed index FASTA, and Bowtie2
index files are never declared as `ProcessingResult.outputs`, even when
retained. Their purpose is restart/reuse, not compact dataset publication.

`metrics` contains parsed fastp JSON keyed by report stem, plus the optional
mixed-layout decision. `tool_versions` contains preflight results.

# Two cleanup layers

Do not confuse processor retention with queue cleanup:

| Layer | Timing | Controlled by | Paths |
| --- | --- | --- | --- |
| ATAC intermediate retention | After ATAC output validation | `AtacIntermediateFiles` | Processor-created unit work, final BAM/index, and genome-index files |
| Provider input cleanup | After the unit outcome | `QueuePolicy` | Exact provider roots below `workspace/fastq/` |

Default queue cleanup removes the provider-owned SRA and converted FASTQ root
after success. Default ATAC retention separately removes its staged/cleaned
FASTQs and component BAMs, while retaining reports, final BAM/CSI, BigWig, and
genome/index cache.

# Custom configuration example

```python
from ncbi_dataset_builder import AtacIntermediateFiles, AtacSeqConfig
from ncbi_dataset_builder.processing.atac import AtacSeqProcessor

processor = AtacSeqProcessor(
    AtacSeqConfig(
        maximum_insert_size=2_000,
        bin_size=10,
        normalize_using="CPM",
        coverage_strands=(),             # One unstranded coverage BigWig.
        fastp_deduplicate=True,
        fastp_max_threads=16,
        strict_mixed_layout=True,
        intermediates=AtacIntermediateFiles(
            keep_staged_fastq=False,
            keep_cleaned_fastq=False,
            keep_fastp_json=True,
            keep_fastp_html=True,
            keep_component_bams=False,
            keep_final_bam=True,
            keep_final_bam_index=True,
            keep_uncompressed_genome=True,
            keep_bowtie2_index=True,
        ),
    )
)

# Local execution accepts the configured callable directly.
report = builder.build(
    catalog,
    processor,
    execution=local_execution,
)
```

For Slurm, put a configured callable in an importable module:

```python
# my_pipeline/atac.py
from ncbi_dataset_builder.processing.atac import AtacSeqConfig, AtacSeqProcessor

process_sample = AtacSeqProcessor(
    AtacSeqConfig(bin_size=10, normalize_using="CPM")
)
```

Then pass
`processor_reference="my_pipeline.atac:process_sample"` to
`submit_slurm()`.

# Default wrapper

`default_atac_processor = AtacSeqProcessor()` is the shared default instance.

`process_atac(fastq, genome, threads)` delegates to that instance. Its stable
import path is:

```text
ncbi_dataset_builder.processing.atac:process_atac
```

This is convenient for Slurm when all default settings are appropriate.

# Internal methods

`_merge_inputs`, `_uncompressed_fasta`, `_ensure_index`, `_run_fastp`,
`_load_fastp_summary`, `_mixed_layout_defense`, `_align`, and cleanup
helpers are private implementation details. Their behavior is documented above
to explain files and performance; call `AtacSeqProcessor` rather than invoking
them directly.
