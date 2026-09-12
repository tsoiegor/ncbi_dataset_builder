# `processing.atac`

This subpackage implements the built-in ATAC-seq workflow: fastp cleaning,
Bowtie2 alignment, Samtools sorting/merging/indexing, and deepTools BigWig
coverage. Use `process_atac` as the default importable function or construct an
`AtacSeqProcessor` for custom settings.

## `AtacSeqProcessor`

`AtacSeqProcessor(config=None, runner=None, progress=None)` accepts an
`AtacSeqConfig`, an injectable command runner, and progress reporter.
`preflight()` verifies all executables and returns version strings.
Calling `processor(fastq, genome, threads)` validates inputs, runs the workflow,
applies retention settings only after output validation and metric extraction,
and returns [`ProcessingResult`](../../README.md). `DatasetBuilder` uses that
call protocol directly.

## `AtacSeqConfig`

Executable fields are `bowtie2`, `bowtie2_build`, `samtools`, `fastp`, and
`bam_coverage`. Processing fields are `maximum_insert_size`, coverage
`bin_size`, optional `normalize_using`, optional `coverage_strands` (`forward`
and/or `reverse`), `fastp_deduplicate`, `fastp_max_threads`, and
`coverage_ignore_duplicates`.

Mixed input protection is controlled by `strict_mixed_layout`,
`mixed_count_tolerance`, `mixed_max_short_read_length`,
`mixed_min_length_ratio`, and `mixed_min_retained_fraction`. Suspicious
single-end reads in a mixed sample are excluded while valid paired reads
continue.

The `intermediates` argument accepts `AtacIntermediateFiles`:

| Field | Created files | Default |
|---|---|---:|
| `keep_staged_fastq` | merged/recompressed `input.*.fastq[.gz]` inside ATAC work | `False` |
| `keep_cleaned_fastq` | fastp `clean.*.fastq.gz` | `False` |
| `keep_fastp_json` | fastp JSON reports | `True` |
| `keep_fastp_html` | fastp HTML reports | `True` |
| `keep_component_bams` | `paired.sorted.bam` and `single.sorted.bam` | `False` |
| `keep_final_bam` | final merged `<sample>.bam` | `True` |
| `keep_final_bam_index` | final `<sample>.bam.csi`; requires final BAM | `True` |
| `keep_uncompressed_genome` | materialized index FASTA | `True` |
| `keep_bowtie2_index` | reusable `.bt2`/`.bt2l` index files | `True` |

BigWigs are final outputs and are always retained. An original provider FASTQ
is never removed by `keep_staged_fastq=False`; only copies created under the
processor work directory qualify. `QueuePolicy.cleanup` separately controls
provider-owned downloaded inputs after a successful sample.

Example:

```python
from ncbi_dataset_builder.processing.atac import (
    AtacIntermediateFiles,
    AtacSeqConfig,
    AtacSeqProcessor,
)

processor = AtacSeqProcessor(
    AtacSeqConfig(
        fastp_max_threads=16,
        intermediates=AtacIntermediateFiles(
            keep_staged_fastq=False,
            keep_cleaned_fastq=True,
            keep_fastp_json=True,
            keep_fastp_html=False,
            keep_component_bams=False,
            keep_final_bam=False,
            keep_final_bam_index=False,
            keep_uncompressed_genome=True,
            keep_bowtie2_index=True,
        ),
    )
)
```

See [ATAC-seq processing](../../../../docs/AtacSeqProcessing.md) for paths and
operational notes.
