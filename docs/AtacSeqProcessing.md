# Built-in ATAC-seq processing

`AtacSeqProcessor` turns a validated single, paired, or mixed `FastqSet` and a
validated genome into forward/reverse BigWigs plus configurable retained reports,
alignment files, and a coverage-annotated experiment description.

Use the default callable when the built-in scientific choices are suitable:

```python
from ncbi_dataset_builder.processing.atac import process_atac
```

For Slurm:

```python
processor_reference="ncbi_dataset_builder.processing.atac:process_atac"
```

That reference uses the default `AtacSeqConfig`. Non-default settings require
an importable configured processor object.

## Pipeline stages

| Stage | Tool/operation | Inputs | Outputs/cache |
| --- | --- | --- | --- |
| Validate | Python | `FastqSet`, `GenomeRef` | Reject missing/empty inputs |
| Stage streams | Python copy/concatenation/gzip | All R1, R2, and single paths | `input.*.fastq[.gz]` when merging is needed |
| Clean paired reads | fastp | Staged R1/R2 | Clean R1/R2 and paired JSON/HTML |
| Clean single reads | fastp | Staged single | Clean single and single JSON/HTML |
| Mixed-layout defense | Python JSON inspection | Paired/single reports | Decision in metrics; possibly remove single branch from alignment |
| Prepare genome | Python decompression | Genome FASTA | Uncompressed FASTA near index |
| Build index | `bowtie2-build` | Uncompressed FASTA | Six `.bt2` or `.bt2l` files |
| Align paired | Bowtie2 → samtools view → sort | Clean paired reads | `paired.sorted.bam` |
| Align single | Same pipeline | Clean single reads when retained | `single.sorted.bam` |
| Final BAM | copy or `samtools merge` | Component BAM(s) | `<unit>.bam` |
| Index | `samtools index -c` | Final BAM | `<unit>.bam.csi` |
| Materialize description | Builder metadata projection | Normalized metadata | `<unit>.json` in the unit output directory |
| Coverage | ExpressionPredict `bam2bw.py` → `bamCoverage` | Final BAM, CSI, description, chromosome sizes | `<unit>.forward.bw`, `<unit>.reverse.bw`, updated description |
| Validate/retain | Python | All produced files/reports | `ProcessingResult`, metrics, selected retained files |

## Input layouts

| `FastqLayout` | Required input | Processing |
| --- | --- | --- |
| `SINGLE` | Non-empty `single` paths | One fastp and one single alignment branch |
| `PAIRED` | Matching non-empty R1/R2 path counts | One paired fastp and paired alignment branch |
| `MIXED` | Valid paired paths plus single paths | Separate fastp branches; strict defense may drop complete single branch |

Multiple paths in one logical stream are merged in order. If all inputs are
gzip files, gzip members are concatenated. If all are uncompressed, bytes are
concatenated. Mixed compression is read and recompressed.

## Default configuration

```python
import sys
from pathlib import Path

from ncbi_dataset_builder.processing.atac import (
    AtacIntermediateFiles,
    AtacSeqConfig,
    AtacSeqProcessor,
)

processor = AtacSeqProcessor(
    AtacSeqConfig(
        bowtie2="bowtie2",
        bowtie2_build="bowtie2-build",
        samtools="samtools",
        fastp="fastp",
        bam2bw_script=Path("/shared/ExpressionPredict/src/bam2bw.py"),
        python_executable=sys.executable,
        maximum_insert_size=2_000,
        min_coverage=1_000_000,
        fastp_deduplicate=True,
        fastp_max_threads=16,
        strict_mixed_layout=True,
        mixed_count_tolerance=0.001,
        mixed_max_short_read_length=30,
        mixed_min_length_ratio=0.5,
        mixed_min_retained_fraction=0.1,
        intermediates=AtacIntermediateFiles(),
    )
)
```

## Executable parameters

| Parameter | Default | Used for | How to choose | Restriction |
| --- | --- | --- | --- | --- |
| `bowtie2` | `"bowtie2"` | Read alignment | Command name on `PATH` or absolute executable path | Must be available on processing node |
| `bowtie2_build` | `"bowtie2-build"` | Index creation | Matching Bowtie2 installation | Must be available even if an index cache may exist |
| `samtools` | `"samtools"` | BAM conversion, sorting, merge, CSI | Compatible command on `PATH` | Must support `index -c` |
| `fastp` | `"fastp"` | Read cleaning and QC reports | Command name/path | Must write valid JSON/HTML and non-empty cleaned reads |
| `bam2bw_script` | Required argument, or `$BAM2BW_SCRIPT` when omitted | Strand splitting, BigWig generation, coverage annotation | Set an absolute path in code or the environment for Slurm | There is no filesystem default; the script and sibling `utils.py`/`bam_utils.py` must be readable |
| `python_executable` | Current interpreter | Runs `bam2bw.py` | Use the environment containing the script dependencies | Must resolve as an executable |

`preflight()` requires fastp, Bowtie2, samtools, Python, and `bamCoverage`, then
runs `bam2bw.py --help` to validate imports. It records tool versions and the
script SHA-256.

## Processing parameters

| Parameter | Default | Exact effect | How to choose | Validation/restriction |
| --- | --- | --- | --- | --- |
| `maximum_insert_size: int` | `2000` | Adds Bowtie2 `-X` for paired alignment | Choose for expected ATAC fragment distribution; keep default unless protocol requires otherwise | Positive |
| `min_coverage: int` | `1_000_000` | Passed to `bam2bw.py --min-coverage` for each strand | Lower only for deliberately small/test libraries | Non-negative |
| `fastp_deduplicate: bool` | `True` | Adds `--dedup --dup_calc_accuracy 5` | Default removes duplicates at cleaning; align with study policy | Separate from coverage duplicate filtering |
| `fastp_max_threads: int` | `16` | Caps `fastp --thread` at `min(assigned threads, cap)` | Keep 16 unless your fastp deployment justifies another cap | Positive |
| `intermediates` | Default retention object | Controls processor-created file deletion/declaration | Choose from retention table | CSI retention requires final BAM retention |

### Coverage roles

ATAC is declared unstranded in the one-row file mapping. `bam2bw.py` always
splits reads into forward and reverse signal, and the manifest records them as
`coverage_forward` and `coverage_reverse`.

## Commands and fixed options

| Tool | Fixed/options derived by processor |
| --- | --- |
| fastp | `--trim_poly_g`; JSON and HTML reports; optional deduplication; 24-hour command timeout |
| `bowtie2-build` | `--threads <assigned>` |
| Bowtie2 | `--very-sensitive --mm -p <assigned>`; paired input uses `-X maximum_insert_size` |
| samtools view | `view -b -` with no explicit thread option |
| samtools sort | `sort -@ <assigned>` |
| samtools merge | `merge -f -@ <assigned>` when both branches exist |
| samtools index | `index -c -@ <assigned>` |
| `bam2bw.py` | One ATAC mapping row; both CPU flags use assigned threads; minimum coverage is configured; statistics are always recomputed; 24-hour timeout |

The external script invokes `bamCoverage` with bin size 1, `--filterRNAstrand`
for each direction, `--skipNAs`, and `--ignoreDuplicates`.

Bowtie2 and samtools sort run concurrently in one pipe and both receive the
full assigned thread value. On systems where both fully occupy those threads,
this may temporarily oversubscribe the nominal per-unit CPU allocation.
Measure real utilization when setting execution concurrency.

## Strict mixed-layout defense

The defense runs only for `FastqLayout.MIXED`, after both fastp branches and
before alignment.

### Parameters

| Parameter | Default | Suspicious condition | Validation |
| --- | --- | --- | --- |
| `strict_mixed_layout` | `True` | Enables conservative inspection | Boolean |
| `mixed_count_tolerance` | `0.001` | Relative count difference is at most 0.1% | In `[0, 1)` |
| `mixed_max_short_read_length` | `30` | Single mean length is at most 30 bp | Positive |
| `mixed_min_length_ratio` | `0.5` | Single/shorter-paired mean length is below 0.5 | In `(0, 1]` |
| `mixed_min_retained_fraction` | `0.1` | Single reads retained by fastp are below 10% | In `[0, 1]` |

### Count calculation

fastp paired `total_reads` counts both mates. The comparison therefore uses:

```text
paired fragments = paired before-filtering reads / 2

relative difference =
abs(single reads - paired fragments)
/ max(single reads, paired fragments)
```

### Decision

Any one suspicious condition excludes the entire cleaned single-end branch
from alignment. Missing, malformed, non-positive, or inconsistent required
fastp statistics also exclude it.

| Outcome | Behavior |
| --- | --- |
| No reasons | Align paired and single branches |
| Any reason | Set the active single branch to `None`; continue paired-only |
| Defense disabled | Keep both branches |

The defense does not immediately delete provider FASTQs. Retention may later
delete the processor-created cleaned single file; queue cleanup independently
controls provider-owned input.

The decision and statistics are recorded in:

```python
result.metrics["mixed_layout_defense"]
```

with action `kept_single_end`, `excluded_single_end`, or `disabled`.

## Retention parameters

`AtacIntermediateFiles` controls every processor-created category:

```python
retention = AtacIntermediateFiles(
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

| Parameter | Default | File category | Typical path | Declared output? |
| --- | --- | --- | --- | --- |
| `keep_staged_fastq` | `False` | Processor-created merged/recompressed input | `output/<id>/work/atac/input.*.fastq[.gz]` | No |
| `keep_cleaned_fastq` | `False` | fastp cleaned reads | `*.clean.fastq.gz` | No |
| `keep_fastp_json` | `True` | Machine-readable fastp reports | `*.fastp.json` | Yes when kept |
| `keep_fastp_html` | `True` | Human fastp reports | `*.fastp.html` | Yes when kept |
| `keep_component_bams` | `False` | Paired/single sorted BAMs | `paired.sorted.bam`, `single.sorted.bam` | No |
| `keep_final_bam` | `True` | Final copied/merged BAM | `output/<id>/<id>.bam` | Yes when kept |
| `keep_final_bam_index` | `True` | Final CSI | `output/<id>/<id>.bam.csi` | Yes when kept |
| `keep_uncompressed_genome` | `True` | FASTA used for index build | Genome cache index directory | No |
| `keep_bowtie2_index` | `True` | Six index files | Genome cache `indexes/<accession>/` | No |

BigWigs are always retained and declared.

### Retention restrictions

- `keep_final_bam_index=True` requires `keep_final_bam=True`.
- Disabling the shared Bowtie2 index or its uncompressed FASTA is safest only
  with serialized processing; concurrent units can share the same cache.
- Reports are loaded into `metrics` before optional deletion.
- Original provider input is not governed by this class.

## Output names

| Output | Name |
| --- | --- |
| Final BAM | `<safe-unit-id>.bam` |
| CSI | `<safe-unit-id>.bam.csi` |
| Forward BigWig | `<safe-unit-id>.forward.bw` |
| Reverse BigWig | `<safe-unit-id>.reverse.bw` |
| Experiment description | `<safe-unit-id>.json` |
| Script mapping | `work/atac/file_mappings.csv` |
| Paired reports | `paired.fastp.json`, `paired.fastp.html` |
| Single reports | `single.fastp.json`, `single.fastp.html` |

With default retention, `ProcessingResult.outputs` maps stable roles such as
`alignment_bam`, `alignment_index`, `coverage_forward`, `coverage_reverse`,
`description`, and `file_mapping` to the corresponding files, along with named
fastp JSON/HTML report roles.

## Metrics

| Key | Value |
| --- | --- |
| `paired.fastp` | Parsed complete paired JSON report when paired input exists |
| `single.fastp` | Parsed complete single JSON report when single input exists |
| `mixed_layout_defense` | Decision, reasons, and derived statistics for mixed layout |

Tool versions are stored separately in `ProcessingResult.tool_versions`.

## Cache and restart behavior

| Artifact | Reuse rule |
| --- | --- |
| Merged/staged FASTQ | Reused when destination exists and is non-empty |
| fastp branch | Reused only when cleaned file and both JSON/HTML reports all exist and are non-empty |
| Bowtie2 index | Reused only when all six standard or all six large index files are non-empty |
| Final BAM | Reused when non-empty |
| BigWig | `bam2bw.py` may reuse a valid file; the processor still verifies both files and coverage metadata |
| CSI | Recreated by the indexing command |

Index construction uses an exclusive lock per assembly. Partial files and
atomic replacements protect staged FASTQ, genome FASTA, BAM, and index
finalization where implemented.

At the higher builder layer, a valid succeeded unit is reused from state before
the processor is called.

## Processor cleanup versus queue cleanup

| Layer | Controls | Boundary |
| --- | --- | --- |
| `AtacIntermediateFiles` | Files created by ATAC processor | Unit work/output and genome index files named by processor |
| `QueuePolicy.cleanup` | Provider-owned staged/downloaded input | Declared roots strictly below `workspace/runtime/fastq/` |

With default settings, provider SRA/FASTQ input is removed after verified
success, processor-created staged/cleaned FASTQ and component BAMs are removed,
while final BAM/CSI, both BigWigs, the description, mapping CSV, reports, genome
FASTA, and Bowtie2 index remain.

## Non-default Slurm configuration

Create an importable module:

```python
# my_project/processors.py
from pathlib import Path

from ncbi_dataset_builder.processing.atac import (
    AtacIntermediateFiles,
    AtacSeqConfig,
    AtacSeqProcessor,
)

atac_with_retained_intermediates = AtacSeqProcessor(
    AtacSeqConfig(
        bam2bw_script=Path("/shared/ExpressionPredict/src/bam2bw.py"),
        intermediates=AtacIntermediateFiles(
            keep_cleaned_fastq=True,
            keep_component_bams=True,
        ),
    )
)
```

Submit:

```python
processor_reference="my_project.processors:atac_with_retained_intermediates"
```

The module, package checkout, and external tools must be available to every
worker.

## Preflight

```python
processor = AtacSeqProcessor()
versions = processor.preflight()
for command, version in versions.items():
    print(command, version)
```

Run this in the actual execution environment before a large job.

## Choosing first values

| Decision | Safe starting point |
| --- | --- |
| Insert size | Keep 2000 unless assay protocol says otherwise |
| Bin size | 1 for raw detail; test larger bins if files are too large |
| Normalization | `None` until downstream requirements are explicit |
| Strands | Fixed forward/reverse outputs from an unstranded ATAC mapping |
| fastp deduplication | Keep enabled if consistent with analysis policy |
| Duplicate coverage | `bam2bw.py` fixes `--ignoreDuplicates`; change the external script if policy differs |
| Mixed defense | Keep enabled |
| Retention | Defaults; keep cleaned/component files temporarily during validation |
| Threads | Measure; remember fastp cap and alignment-pipe overlap |

## Common problems

| Symptom | Cause | Action |
| --- | --- | --- |
| Preflight reports missing command | Tool absent from execution `PATH` | Install/activate environment on compute node |
| Paired input validation fails | R1/R2 count mismatch | Repair provider output before processing |
| Single branch unexpectedly excluded | Any strict rule or invalid report triggered | Inspect `mixed_layout_defense` reasons and fastp JSON |
| Downstream script expects one track | Processor now always emits two strand tracks | Read `coverage_forward` and `coverage_reverse` from the manifest and reshape downstream |
| Description is missing | `enrich_metadata()` was not run or the unit is not one Experiment | Enrich metadata before build and keep experiment grouping |
| Script exits 0 but unit fails | `bam2bw.py` swallowed a per-record exception | Read the unit log; the processor intentionally verifies both tracks and coverage keys |
| Index rebuilds repeatedly | Retention disabled or incomplete cache | Keep index files and verify shared filesystem |
| CPU usage exceeds expectation | Bowtie2 and samtools sort both use assigned threads concurrently | Lower per-job CPUs/concurrency after measurement |
| Success loses raw input | Queue cleanup, not ATAC retention | Change `QueuePolicy.cleanup` |

## Related pages

- [ATAC API reference](../src/ncbi_dataset_builder/processing/atac/README.md)
- [Writing a processor](Processors.md)
- [Storage and cleanup](Storage.md)
- [Local execution](LocalExecution.md)
- [Single-node Slurm](SlurmSingleNodeExecution.md)
- [Distributed Slurm](SlurmDistributedExecution.md)
