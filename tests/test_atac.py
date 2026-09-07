from pathlib import Path
from typing import ClassVar

from ncbi_dataset_builder.models import FastqLayout, FastqSet, GenomeRef
from ncbi_dataset_builder.processing.atac import AtacSeqProcessor


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
