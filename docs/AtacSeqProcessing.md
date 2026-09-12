# ATAC-seq processing

The built-in processor runs:

1. merge/recompress each logical FASTQ stream when necessary;
2. clean paired and single inputs separately with fastp;
3. reject a suspicious single-end component of mixed input when strict defense
   is enabled;
4. create or reuse a Bowtie2 genome index;
5. align paired and/or single reads and sort component BAMs;
6. merge/copy the final BAM, write a CSI index, and create BigWig coverage;
7. read fastp metrics, validate outputs, then apply the retention policy.

## Configuration

```python
from ncbi_dataset_builder.processing.atac import (
    AtacIntermediateFiles,
    AtacSeqConfig,
    AtacSeqProcessor,
)

processor = AtacSeqProcessor(
    AtacSeqConfig(
        maximum_insert_size=2_000,
        bin_size=1,
        normalize_using=None,
        coverage_strands=(),
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
```

## File ownership and retention

Every processor-created intermediate category has an explicit Boolean in
`AtacIntermediateFiles`:

| Category | Typical path | Default |
|---|---|---|
| staged/merged FASTQ | `work/units/<id>/processing/atac/input.*.fastq[.gz]` | remove |
| cleaned FASTQ | `work/units/<id>/processing/atac/*.clean.fastq.gz` | remove |
| fastp JSON | `work/units/<id>/processing/atac/*.fastp.json` | keep |
| fastp HTML | `work/units/<id>/processing/atac/*.fastp.html` | keep |
| component BAM | `work/units/<id>/processing/atac/*.sorted.bam` | remove |
| final BAM | `outputs/<id>/<id>.bam` | keep |
| final CSI | `outputs/<id>/<id>.bam.csi` | keep |
| materialized genome FASTA | genome cache `<assembly>.fna` | keep |
| Bowtie2 index | genome cache/index files | keep |

BigWigs are final required outputs and are always retained. If final BAM
retention is disabled, its CSI retention must also be disabled. Reports can be
deleted after their metrics have been loaded into `ProcessingResult.metrics`.

An original provider FASTQ is never deleted by the processor when it was used
directly instead of copied into processor work. Provider input cleanup is a
separate queue concern controlled by `QueuePolicy.cleanup` and
`keep_failed_inputs`. Disabling a shared genome index is best used with
serialized sample processing because concurrent samples may refer to it.

For the default function use
`"ncbi_dataset_builder.processing.atac:process_atac"`. To use custom retention
on Slurm, expose your configured `AtacSeqProcessor` through an importable module.

