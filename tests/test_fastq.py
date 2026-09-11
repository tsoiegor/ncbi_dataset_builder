import gzip
from pathlib import Path

import pytest

from ncbi_dataset_builder.errors import DownloadError, ExternalToolError
from ncbi_dataset_builder.fastq import GeoFastqProvider, SraToolkitProvider
from ncbi_dataset_builder.models import FastqLayout, ProcessingUnit


class FakeDownloader:
    def download(self, url, destination, **kwargs):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(url.encode())
        return destination


class FakeSraRunner:
    def __init__(self):
        self.commands = []

    def which(self, executable):
        return None if executable == "pigz" else executable

    def require(self, *executables):
        return None

    def run(self, command, **kwargs):
        del kwargs
        self.commands.append(command)
        if command[0] == "prefetch":
            accession = command[1]
            output = command[command.index("--output-directory") + 1]
            target = Path(output) / f"{accession}.sra"
            target.write_bytes(b"sra")
        elif command[0] == "fasterq-dump":
            source = Path(command[1])
            accession = source.parent.name if source.name == "data.sra" else source.stem
            output = Path(command[command.index("-O") + 1])
            (output / f"{accession}_1.fastq").write_bytes(f"{accession}-R1\n".encode())
            (output / f"{accession}_2.fastq").write_bytes(f"{accession}-R2\n".encode())
            if accession == "SRR1":
                (output / f"{accession}.fastq").write_bytes(b"SRR1-orphan\n")


class RecoveringPrefetchRunner(FakeSraRunner):
    """Fail prefetch twice and verify that locks and partial data are recovered."""

    def __init__(self):
        super().__init__()
        self.prefetch_attempts = 0

    def run(self, command, **kwargs):
        if command[0] != "prefetch":
            return super().run(command, **kwargs)
        del kwargs
        self.commands.append(command)
        self.prefetch_attempts += 1
        accession = command[1]
        output = Path(command[command.index("--output-directory") + 1])
        accession_dir = output / accession
        lock = accession_dir / f"{accession}.sra.lock"
        partial = accession_dir / "incomplete.part"
        if self.prefetch_attempts > 1:
            assert not lock.exists()
        if self.prefetch_attempts <= 2:
            accession_dir.mkdir(parents=True, exist_ok=True)
            lock.write_text("stale")
            partial.write_text("partial")
            raise ExternalToolError("prefetch exited with code 3: lock exists")
        assert not lock.exists()
        assert not partial.exists()
        target = accession_dir / f"{accession}.sra"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"sra")


def test_geo_provider_pairs_by_sample_key_and_preserves_single_reads(tmp_path):
    unit = ProcessingUnit("GSE1", ())
    provider = GeoFastqProvider(
        {
            "GSE1": [
                "https://example.org/sampleB_R2.fastq.gz",
                "https://example.org/sampleA_R1.fastq.gz",
                "https://example.org/orphans.fastq.gz",
                "https://example.org/sampleB_R1.fastq.gz",
                "https://example.org/sampleA_R2.fastq.gz",
            ]
        },
        downloader=FakeDownloader(),
    )
    result = provider.fetch(unit, tmp_path / "fastq", threads=2)
    assert result.layout is FastqLayout.MIXED
    assert [path.name for path in result.read1] == [
        "sampleA_R1.fastq.gz",
        "sampleB_R1.fastq.gz",
    ]
    assert [path.name for path in result.read2] == [
        "sampleA_R2.fastq.gz",
        "sampleB_R2.fastq.gz",
    ]
    assert result.single[0].name == "orphans.fastq.gz"


def test_geo_provider_rejects_an_unpaired_mate(tmp_path):
    provider = GeoFastqProvider(
        {"GSE1": ["https://example.org/sample_R1.fastq.gz"]}, downloader=FakeDownloader()
    )
    with pytest.raises(DownloadError, match="Incomplete GEO FASTQ"):
        provider.fetch(ProcessingUnit("GSE1", ()), tmp_path / "fastq", threads=1)


def test_sra_provider_validates_converts_and_merges_runs_in_accession_order(tmp_path):
    runner = FakeSraRunner()
    provider = SraToolkitProvider(runner=runner)
    unit = ProcessingUnit("SRX1", ("SRR2", "SRR1"))
    result = provider.fetch(unit, tmp_path / "fastq", threads=2)
    assert result.layout is FastqLayout.MIXED
    with gzip.open(result.read1[0], "rb") as handle:
        assert handle.read() == b"SRR2-R1\nSRR1-R1\n"
    with gzip.open(result.single[0], "rb") as handle:
        assert handle.read() == b"SRR1-orphan\n"
    command_count = len(runner.commands)
    cached = provider.fetch(unit, tmp_path / "fastq", threads=2)
    assert cached.read1 == result.read1
    assert len(runner.commands) == command_count


def test_sra_staging_prefetches_without_materializing_and_reuses_raw_cache(tmp_path):
    runner = FakeSraRunner()
    provider = SraToolkitProvider(runner=runner)
    unit = ProcessingUnit("SRX1", ("SRR1",))
    stale_output = tmp_path / "fastq" / "SRX1" / "SRX1_1.fastq.gz"
    stale_output.parent.mkdir(parents=True)
    stale_output.write_bytes(b"interrupted merge")

    staged = provider.stage(unit, tmp_path / "fastq", threads=2)

    assert not stale_output.exists()
    assert staged.metadata["sra_files"]["SRR1"] == str(
        tmp_path / "fastq" / "SRX1" / "sra" / "SRR1" / "data.sra"
    )
    assert any(command[0] == "prefetch" for command in runner.commands)
    assert any(command[0] == "vdb-validate" for command in runner.commands)
    assert not any(command[0] == "fasterq-dump" for command in runner.commands)
    command_count = len(runner.commands)
    provider.stage(unit, tmp_path / "fastq", threads=2)
    assert len(runner.commands) == command_count

    result = provider.materialize(unit, staged, tmp_path / "fastq", threads=2)
    assert result.layout is FastqLayout.MIXED
    assert any(command[0] == "fasterq-dump" for command in runner.commands)
    unit_root = tmp_path / "fastq" / "SRX1"
    assert not list(unit_root.rglob("*.sra"))
    assert not list(unit_root.glob("*.fastq"))
    assert not (unit_root / "runs").exists()
    assert sorted(path.name for path in unit_root.glob("*.fastq.gz")) == [
        "SRX1.fastq.gz",
        "SRX1_1.fastq.gz",
        "SRX1_2.fastq.gz",
    ]
    recovered_raw = unit_root / "sra" / "SRR1" / "data.sra"
    recovered_raw.parent.mkdir(parents=True)
    recovered_raw.write_bytes(b"stale")

    cached = provider.fetch(unit, tmp_path / "fastq", threads=2)

    assert cached.read1 == result.read1
    assert not (unit_root / "sra").exists()


def test_sra_prefetch_removes_locks_resets_staging_and_retries_forever(tmp_path):
    runner = RecoveringPrefetchRunner()
    provider = SraToolkitProvider(
        runner=runner,
        prefetch_reset_after_failures=2,
        prefetch_retry_max_delay_seconds=0,
    )

    archive = provider._download_run("SRR1", tmp_path / "sra")

    assert archive.read_bytes() == b"sra"
    assert runner.prefetch_attempts == 3
    assert not list(tmp_path.rglob("*.lock"))
    assert not list(tmp_path.rglob("incomplete.part"))
    assert (tmp_path / "sra" / "SRR1" / "sra.complete.json").is_file()


def test_sra_provider_rejects_catalogued_run_above_configured_limit(tmp_path):
    runner = FakeSraRunner()
    provider = SraToolkitProvider(runner=runner, prefetch_max_size="100G")
    unit = ProcessingUnit(
        "SRX1",
        ("SRR1",),
        metadata={"run_size_gb": {"SRR1": 140.4}},
    )

    with pytest.raises(DownloadError, match="SRR1=140.400 GB"):
        provider.stage(unit, tmp_path / "fastq", threads=2)

    assert not runner.commands
