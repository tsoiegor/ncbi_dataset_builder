import json
from pathlib import Path
from typing import ClassVar

import pytest

from ncbi_dataset_builder.models import FastqLayout, FastqSet, GenomeRef
from ncbi_dataset_builder.processing.atac import (
    AtacIntermediateFiles,
    AtacSeqConfig,
    AtacSeqProcessor,
)


class FakeRunner:
    base_env: ClassVar[dict[str, str]] = {}

    def run(self, command, **kwargs):
        del kwargs
        if command[1] == "index":
            Path(str(command[-1]) + ".csi").write_bytes(b"index")
        elif "--outFileName" in command:
            Path(command[command.index("--outFileName") + 1]).write_bytes(b"bigwig")


class LightweightAtacProcessor(AtacSeqProcessor):
    def preflight(self):
        return {"mock": "1"}

    def _ensure_index(self, genome, threads):
        del genome, threads
        return Path("mock-index")

    def _run_fastp(self, *, read1, read2, single, root, threads):
        del read1, read2, threads
        clean = root / "single.clean.fastq.gz"
        clean.write_bytes(b"clean")
        report_json = root / "single.fastp.json"
        report_json.write_text("{}", encoding="utf-8")
        report_html = root / "single.fastp.html"
        report_html.write_text("report", encoding="utf-8")
        return None, None, clean if single else None, [report_json, report_html]

    def _align(self, *, output, **kwargs):
        del kwargs
        output.write_bytes(b"bam")
        return output


class IndexingAtacProcessor(LightweightAtacProcessor):
    def _ensure_index(self, genome, threads):
        del threads
        index_dir = genome.fasta.parent / "indexes" / genome.accession
        index_dir.mkdir(parents=True, exist_ok=True)
        (index_dir / f"{genome.accession}.fna").write_text(">chr1\nACGT\n")
        prefix = index_dir / genome.accession
        for suffix in (".1", ".2", ".3", ".4", ".rev.1", ".rev.2"):
            Path(str(prefix) + suffix + ".bt2").write_bytes(b"index")
        return prefix


def test_default_atac_coverage_is_unstranded_and_outputs_are_validated(tmp_path):
    reads = tmp_path / "reads.fastq.gz"
    reads.write_bytes(b"reads")
    fasta = tmp_path / "genome.fna"
    fasta.write_text(">chr1\nACGT\n", encoding="utf-8")
    fastq = FastqSet(
        "SRX1",
        FastqLayout.SINGLE,
        ("SRR1",),
        single=(reads,),
        work_dir=tmp_path / "work",
        output_dir=tmp_path / "output",
    )
    genome = GenomeRef(9606, "Homo sapiens", "GCF_TEST", fasta, "not-used")
    result = LightweightAtacProcessor(runner=FakeRunner())(fastq, genome, 2)
    assert result.success
    assert (tmp_path / "output" / "SRX1.coverage.bw") in result.outputs
    assert not any("forward" in path.name or "reverse" in path.name for path in result.outputs)
    assert not (tmp_path / "work" / "processing" / "atac" / "single.clean.fastq.gz").exists()
    assert not (tmp_path / "work" / "processing" / "atac" / "single.sorted.bam").exists()


def test_atac_intermediate_policy_can_retain_unit_work_files(tmp_path):
    reads = tmp_path / "reads.fastq.gz"
    reads.write_bytes(b"reads")
    second_reads = tmp_path / "reads-2.fastq.gz"
    second_reads.write_bytes(b"more reads")
    fasta = tmp_path / "genome.fna"
    fasta.write_text(">chr1\nACGT\n", encoding="utf-8")
    fastq = FastqSet(
        "SRX1",
        FastqLayout.SINGLE,
        ("SRR1",),
        single=(reads, second_reads),
        work_dir=tmp_path / "work",
        output_dir=tmp_path / "output",
    )
    genome = GenomeRef(9606, "Homo sapiens", "GCF_TEST", fasta, "not-used")
    config = AtacSeqConfig(
        intermediates=AtacIntermediateFiles(
            keep_staged_fastq=True,
            keep_cleaned_fastq=True,
            keep_fastp_json=True,
            keep_fastp_html=True,
            keep_component_bams=True,
        )
    )

    result = LightweightAtacProcessor(config=config, runner=FakeRunner())(fastq, genome, 2)
    work = tmp_path / "work" / "processing" / "atac"

    assert (work / "single.clean.fastq.gz").is_file()
    assert (work / "single.sorted.bam").is_file()
    assert (work / "input.single.fastq.gz").is_file()
    assert (work / "single.fastp.json") in result.outputs
    assert (work / "single.fastp.html") in result.outputs


def test_atac_policy_can_publish_bigwig_without_bam_or_reports(tmp_path):
    reads = tmp_path / "reads.fastq.gz"
    reads.write_bytes(b"reads")
    fasta = tmp_path / "genome.fna"
    fasta.write_text(">chr1\nACGT\n", encoding="utf-8")
    fastq = FastqSet(
        "SRX1",
        FastqLayout.SINGLE,
        ("SRR1",),
        single=(reads,),
        work_dir=tmp_path / "work",
        output_dir=tmp_path / "output",
    )
    genome = GenomeRef(9606, "Homo sapiens", "GCF_TEST", fasta, "not-used")
    config = AtacSeqConfig(
        intermediates=AtacIntermediateFiles(
            keep_fastp_json=False,
            keep_fastp_html=False,
            keep_final_bam=False,
            keep_final_bam_index=False,
        )
    )

    result = LightweightAtacProcessor(config=config, runner=FakeRunner())(fastq, genome, 2)

    assert [path.suffix for path in result.outputs] == [".bw"]
    assert not (tmp_path / "output" / "SRX1.bam").exists()
    assert not (tmp_path / "output" / "SRX1.bam.csi").exists()
    assert not (tmp_path / "work" / "processing" / "atac" / "single.fastp.json").exists()
    assert not (tmp_path / "work" / "processing" / "atac" / "single.fastp.html").exists()


def test_atac_policy_can_remove_cached_index_and_materialized_genome(tmp_path):
    reads = tmp_path / "reads.fastq.gz"
    reads.write_bytes(b"reads")
    fasta = tmp_path / "genome.fna"
    fasta.write_text(">chr1\nACGT\n", encoding="utf-8")
    fastq = FastqSet(
        "SRX1",
        FastqLayout.SINGLE,
        ("SRR1",),
        single=(reads,),
        work_dir=tmp_path / "work",
        output_dir=tmp_path / "output",
    )
    genome = GenomeRef(9606, "Homo sapiens", "GCF_TEST", fasta, "not-used")
    config = AtacSeqConfig(
        intermediates=AtacIntermediateFiles(
            keep_bowtie2_index=False,
            keep_uncompressed_genome=False,
        )
    )

    IndexingAtacProcessor(config=config, runner=FakeRunner())(fastq, genome, 2)
    index_dir = tmp_path / "indexes" / "GCF_TEST"

    assert not (index_dir / "GCF_TEST.fna").exists()
    assert not list(index_dir.glob("*.bt2"))


def test_atac_policy_rejects_index_without_bam():
    with pytest.raises(ValueError, match="requires keep_final_bam"):
        AtacIntermediateFiles(keep_final_bam=False, keep_final_bam_index=True)


def _write_fastp_report(
    path: Path,
    *,
    before_reads: int,
    after_reads: int,
    read1_length: int,
    read2_length: int | None = None,
) -> None:
    before = {
        "total_reads": before_reads,
        "read1_mean_length": read1_length,
    }
    if read2_length is not None:
        before["read2_mean_length"] = read2_length
    path.write_text(
        json.dumps(
            {
                "summary": {
                    "before_filtering": before,
                    "after_filtering": {"total_reads": after_reads},
                }
            }
        ),
        encoding="utf-8",
    )


def test_strict_mixed_layout_excludes_attached_sample_pattern(tmp_path):
    _write_fastp_report(
        tmp_path / "paired.fastp.json",
        before_reads=682_111_344,
        after_reads=406_606_240,
        read1_length=99,
        read2_length=99,
    )
    _write_fastp_report(
        tmp_path / "single.fastp.json",
        before_reads=341_055_672,
        after_reads=2_829_904,
        read1_length=16,
    )

    decision = AtacSeqProcessor()._mixed_layout_defense(tmp_path)

    assert decision["action"] == "excluded_single_end"
    assert decision["statistics"]["count_relative_difference"] == 0
    assert decision["statistics"]["single_mean_length"] == 16
    assert len(decision["reasons"]) == 4


def test_strict_mixed_layout_aligns_only_paired_reads(tmp_path):
    class MixedAtacProcessor(LightweightAtacProcessor):
        def __init__(self):
            super().__init__(runner=FakeRunner())
            self.aligned: list[str] = []

        def _run_fastp(self, *, read1, read2, single, root, threads):
            del read1, read2, single, threads
            clean1 = root / "paired.clean.R1.fastq.gz"
            clean2 = root / "paired.clean.R2.fastq.gz"
            clean_single = root / "single.clean.fastq.gz"
            for path in (clean1, clean2, clean_single):
                path.write_bytes(b"clean")
            paired_json = root / "paired.fastp.json"
            single_json = root / "single.fastp.json"
            _write_fastp_report(
                paired_json,
                before_reads=682_111_344,
                after_reads=406_606_240,
                read1_length=99,
                read2_length=99,
            )
            _write_fastp_report(
                single_json,
                before_reads=341_055_672,
                after_reads=2_829_904,
                read1_length=16,
            )
            paired_html = root / "paired.fastp.html"
            single_html = root / "single.fastp.html"
            paired_html.write_text("report", encoding="utf-8")
            single_html.write_text("report", encoding="utf-8")
            return clean1, clean2, clean_single, [
                paired_json,
                paired_html,
                single_json,
                single_html,
            ]

        def _align(self, *, output, single=None, **kwargs):
            del kwargs
            self.aligned.append("single" if single is not None else "paired")
            output.write_bytes(b"bam")
            return output

    read1 = tmp_path / "reads_1.fastq.gz"
    read2 = tmp_path / "reads_2.fastq.gz"
    single = tmp_path / "technical.fastq.gz"
    for path in (read1, read2, single):
        path.write_bytes(b"reads")
    fasta = tmp_path / "genome.fna"
    fasta.write_text(">chr1\nACGT\n", encoding="utf-8")
    fastq = FastqSet(
        "SRX34494525",
        FastqLayout.MIXED,
        ("SRR1", "SRR2"),
        read1=(read1,),
        read2=(read2,),
        single=(single,),
        work_dir=tmp_path / "work",
        output_dir=tmp_path / "output",
    )
    genome = GenomeRef(9606, "Homo sapiens", "GCF_TEST", fasta, "not-used")
    processor = MixedAtacProcessor()

    result = processor(fastq, genome, 2)

    assert processor.aligned == ["paired"]
    assert result.metrics["mixed_layout_defense"]["action"] == "excluded_single_end"
    assert not (tmp_path / "work" / "processing" / "atac" / "single.sorted.bam").exists()


def test_strict_mixed_layout_keeps_plausible_single_reads(tmp_path):
    _write_fastp_report(
        tmp_path / "paired.fastp.json",
        before_reads=2_000,
        after_reads=1_800,
        read1_length=75,
        read2_length=75,
    )
    _write_fastp_report(
        tmp_path / "single.fastp.json",
        before_reads=100,
        after_reads=90,
        read1_length=70,
    )

    decision = AtacSeqProcessor()._mixed_layout_defense(tmp_path)

    assert decision == {
        "enabled": True,
        "action": "kept_single_end",
        "reasons": [],
        "statistics": {
            "single_before_reads": 100,
            "paired_before_reads": 2_000,
            "paired_fragments": 1_000.0,
            "single_mean_length": 70.0,
            "paired_read1_mean_length": 75.0,
            "paired_read2_mean_length": 75.0,
            "count_relative_difference": 0.9,
            "single_to_paired_length_ratio": 70 / 75,
            "single_retained_fraction": 0.9,
        },
    }


def test_strict_mixed_layout_excludes_single_reads_when_reports_are_invalid(tmp_path):
    (tmp_path / "paired.fastp.json").write_text("{}", encoding="utf-8")
    (tmp_path / "single.fastp.json").write_text("{}", encoding="utf-8")

    decision = AtacSeqProcessor()._mixed_layout_defense(tmp_path)

    assert decision["action"] == "excluded_single_end"
    assert "unavailable" in decision["reasons"][0]


def test_mixed_layout_defense_can_be_disabled(tmp_path):
    processor = AtacSeqProcessor(config=AtacSeqConfig(strict_mixed_layout=False))

    assert processor._mixed_layout_defense(tmp_path) == {
        "enabled": False,
        "action": "disabled",
        "reasons": [],
    }
