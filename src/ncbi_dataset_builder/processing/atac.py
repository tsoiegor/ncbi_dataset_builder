from __future__ import annotations

import gzip
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from ..commands import CommandRunner
from ..errors import ExternalToolError, ProcessingError
from ..models import FastqSet, GenomeRef, ProcessingResult
from ..util import exclusive_file_lock, existing_nonempty, sanitize_identifier


@dataclass(frozen=True)
class AtacSeqConfig:
    bowtie2: str = "bowtie2"
    bowtie2_build: str = "bowtie2-build"
    samtools: str = "samtools"
    fastp: str = "fastp"
    bam_coverage: str = "bamCoverage"
    maximum_insert_size: int = 2000
    bin_size: int = 1
    normalize_using: str | None = None
    coverage_strands: tuple[str, ...] = ()
    fastp_deduplicate: bool = True
    fastp_max_threads: int = 16
    coverage_ignore_duplicates: bool = True
    keep_intermediates: bool = True

    def __post_init__(self) -> None:
        if self.maximum_insert_size < 1 or self.bin_size < 1 or self.fastp_max_threads < 1:
            raise ValueError(
                "maximum_insert_size, bin_size, and fastp_max_threads must be positive"
            )
        invalid = set(self.coverage_strands) - {"forward", "reverse"}
        if invalid:
            raise ValueError(f"Unknown coverage strand(s): {sorted(invalid)}")


class AtacSeqProcessor:
    """A fail-fast replacement for the repository's original shell-heavy ATAC path."""

    def __init__(
        self,
        config: AtacSeqConfig | None = None,
        *,
        runner: CommandRunner | None = None,
    ) -> None:
        self.config = config or AtacSeqConfig()
        self.runner = runner or CommandRunner()

    def preflight(self) -> dict[str, str]:
        tools = (
            self.config.fastp,
            self.config.bowtie2,
            self.config.bowtie2_build,
            self.config.samtools,
            self.config.bam_coverage,
        )
        self.runner.require(*tools)
        return {
            self.config.fastp: self.runner.version(self.config.fastp, "--version"),
            self.config.bowtie2: self.runner.version(self.config.bowtie2, "--version"),
            self.config.samtools: self.runner.version(self.config.samtools, "--version"),
            self.config.bam_coverage: self.runner.version(self.config.bam_coverage, "--version"),
        }

    @staticmethod
    def _merge_inputs(paths: tuple[Path, ...], destination: Path) -> Path | None:
        if not paths:
            return None
        if len(paths) == 1:
            return paths[0]
        all_compressed = all(path.suffix == ".gz" for path in paths)
        none_compressed = all(path.suffix != ".gz" for path in paths)
        if none_compressed:
            destination = destination.with_suffix("")
        if existing_nonempty(destination):
            return destination
        destination.parent.mkdir(parents=True, exist_ok=True)
        partial = destination.with_name(destination.name + ".part")
        if all_compressed or none_compressed:
            with partial.open("wb") as output:
                for path in paths:
                    with path.open("rb") as source:
                        shutil.copyfileobj(source, output, length=8 * 1024 * 1024)
                output.flush()
                os.fsync(output.fileno())
        else:
            with gzip.open(partial, "wb", compresslevel=6) as output:
                for path in paths:
                    opener = gzip.open if path.suffix == ".gz" else open
                    with opener(path, "rb") as source:
                        shutil.copyfileobj(source, output, length=8 * 1024 * 1024)
        os.replace(partial, destination)
        return destination

    @staticmethod
    def _uncompressed_fasta(source: Path, destination: Path) -> Path:
        if existing_nonempty(destination):
            return destination
        destination.parent.mkdir(parents=True, exist_ok=True)
        partial = destination.with_name(destination.name + ".part")
        opener = gzip.open if source.suffix == ".gz" else open
        with opener(source, "rb") as input_handle, partial.open("wb") as output_handle:
            shutil.copyfileobj(input_handle, output_handle, length=8 * 1024 * 1024)
            output_handle.flush()
            os.fsync(output_handle.fileno())
        os.replace(partial, destination)
        return destination

    @staticmethod
    def _index_complete(prefix: Path) -> bool:
        suffixes = (".1", ".2", ".3", ".4", ".rev.1", ".rev.2")
        standard = [Path(str(prefix) + suffix + ".bt2") for suffix in suffixes]
        large = [Path(str(prefix) + suffix + ".bt2l") for suffix in suffixes]
        return all(existing_nonempty(path) for path in standard) or all(
            existing_nonempty(path) for path in large
        )

    def _ensure_index(self, genome: GenomeRef, threads: int) -> Path:
        index_dir = genome.fasta.parent / "bowtie2"
        prefix = index_dir / genome.accession
        if self._index_complete(prefix):
            return prefix
        index_dir.mkdir(parents=True, exist_ok=True)
        with exclusive_file_lock(index_dir / ".build.lock", timeout_seconds=12 * 60 * 60):
            if self._index_complete(prefix):
                return prefix
            fasta = self._uncompressed_fasta(genome.fasta, index_dir / f"{genome.accession}.fna")
            self.runner.run(
                [
                    self.config.bowtie2_build,
                    "--threads",
                    str(max(1, threads)),
                    str(fasta),
                    str(prefix),
                ],
                timeout=24 * 60 * 60,
            )
            if not self._index_complete(prefix):
                raise ProcessingError(f"Bowtie2 index is incomplete for {genome.accession}")
        return prefix

    def _run_fastp(
        self,
        *,
        read1: Path | None,
        read2: Path | None,
        single: Path | None,
        root: Path,
        threads: int,
    ) -> tuple[Path | None, Path | None, Path | None, list[Path]]:
        fastp_threads = min(max(1, threads), self.config.fastp_max_threads)
        outputs: list[Path] = []
        clean1: Path | None = None
        clean2: Path | None = None
        clean_single: Path | None = None
        if read1 and read2:
            clean1 = root / "paired.clean.R1.fastq.gz"
            clean2 = root / "paired.clean.R2.fastq.gz"
            report_json = root / "paired.fastp.json"
            report_html = root / "paired.fastp.html"
            if not all(
                existing_nonempty(path) for path in (clean1, clean2, report_json, report_html)
            ):
                command = [
                    self.config.fastp,
                    "--in1",
                    str(read1),
                    "--in2",
                    str(read2),
                    "--out1",
                    str(clean1),
                    "--out2",
                    str(clean2),
                    "--thread",
                    str(fastp_threads),
                    "--trim_poly_g",
                    "--json",
                    str(report_json),
                    "--html",
                    str(report_html),
                ]
                if self.config.fastp_deduplicate:
                    command.extend(("--dedup", "--dup_calc_accuracy", "5"))
                self.runner.run(command, timeout=24 * 60 * 60)
            outputs.extend((report_json, report_html))
        if single:
            clean_single = root / "single.clean.fastq.gz"
            report_json = root / "single.fastp.json"
            report_html = root / "single.fastp.html"
            if not all(
                existing_nonempty(path) for path in (clean_single, report_json, report_html)
            ):
                command = [
                    self.config.fastp,
                    "--in1",
                    str(single),
                    "--out1",
                    str(clean_single),
                    "--thread",
                    str(fastp_threads),
                    "--trim_poly_g",
                    "--json",
                    str(report_json),
                    "--html",
                    str(report_html),
                ]
                if self.config.fastp_deduplicate:
                    command.extend(("--dedup", "--dup_calc_accuracy", "5"))
                self.runner.run(command, timeout=24 * 60 * 60)
            outputs.extend((report_json, report_html))
        for path in (clean1, clean2, clean_single):
            if path is not None and not existing_nonempty(path):
                raise ProcessingError(f"fastp output is missing or empty: {path}")
        return clean1, clean2, clean_single, outputs

    def _align(
        self,
        *,
        prefix: Path,
        output: Path,
        log: Path,
        threads: int,
        read1: Path | None = None,
        read2: Path | None = None,
        single: Path | None = None,
    ) -> Path:
        if existing_nonempty(output):
            return output
        bowtie = [
            self.config.bowtie2,
            "--very-sensitive",
            "--mm",
            "-p",
            str(max(1, threads)),
            "-x",
            str(prefix),
        ]
        if read1 and read2:
            bowtie.extend(
                (
                    "-1",
                    str(read1),
                    "-2",
                    str(read2),
                    "-X",
                    str(self.config.maximum_insert_size),
                )
            )
        elif single:
            bowtie.extend(("-U", str(single)))
        else:
            raise ValueError("Alignment requires paired or single FASTQ")
        output.parent.mkdir(parents=True, exist_ok=True)
        partial = output.with_name(output.name + ".part")
        environment = os.environ.copy()
        environment.update(self.runner.base_env)
        with log.open("ab") as log_handle:
            processes: list[subprocess.Popen] = []
            try:
                aligner = subprocess.Popen(
                    bowtie, stdout=subprocess.PIPE, stderr=log_handle, env=environment
                )
                processes.append(aligner)
                viewer = subprocess.Popen(
                    [self.config.samtools, "view", "-b", "-"],
                    stdin=aligner.stdout,
                    stdout=subprocess.PIPE,
                    stderr=log_handle,
                    env=environment,
                )
                processes.append(viewer)
                if aligner.stdout:
                    aligner.stdout.close()
                sorter = subprocess.Popen(
                    [
                        self.config.samtools,
                        "sort",
                        "-@",
                        str(max(1, threads)),
                        "-o",
                        str(partial),
                        "-",
                    ],
                    stdin=viewer.stdout,
                    stderr=log_handle,
                    env=environment,
                )
                processes.append(sorter)
                if viewer.stdout:
                    viewer.stdout.close()
                codes = [sorter.wait(), viewer.wait(), aligner.wait()]
            except FileNotFoundError as exc:
                for process in processes:
                    process.kill()
                raise ExternalToolError(f"Alignment executable not found: {exc.filename}") from exc
        if any(code != 0 for code in codes):
            raise ProcessingError(f"Alignment pipeline failed with codes {codes}; see {log}")
        if not existing_nonempty(partial):
            raise ProcessingError(f"Alignment produced no BAM: {partial}")
        os.replace(partial, output)
        return output

    def __call__(self, fastq: FastqSet, genome: GenomeRef, threads: int) -> ProcessingResult:
        fastq.validate()
        genome.validate()
        versions = self.preflight()
        output_dir = fastq.output_dir
        work = fastq.work_dir / "processing" / "atac"
        output_dir.mkdir(parents=True, exist_ok=True)
        work.mkdir(parents=True, exist_ok=True)
        safe_id = sanitize_identifier(fastq.unit_id)
        staged1 = self._merge_inputs(fastq.read1, work / "input.R1.fastq.gz")
        staged2 = self._merge_inputs(fastq.read2, work / "input.R2.fastq.gz")
        staged_single = self._merge_inputs(fastq.single, work / "input.single.fastq.gz")
        clean1, clean2, clean_single, reports = self._run_fastp(
            read1=staged1,
            read2=staged2,
            single=staged_single,
            root=work,
            threads=threads,
        )
        prefix = self._ensure_index(genome, threads)
        bams: list[Path] = []
        if clean1 and clean2:
            bams.append(
                self._align(
                    prefix=prefix,
                    output=work / "paired.sorted.bam",
                    log=work / "paired.alignment.log",
                    threads=threads,
                    read1=clean1,
                    read2=clean2,
                )
            )
        if clean_single:
            bams.append(
                self._align(
                    prefix=prefix,
                    output=work / "single.sorted.bam",
                    log=work / "single.alignment.log",
                    threads=threads,
                    single=clean_single,
                )
            )
        final_bam = output_dir / f"{safe_id}.bam"
        if not existing_nonempty(final_bam):
            if len(bams) == 1:
                shutil.copyfile(bams[0], final_bam.with_name(final_bam.name + ".part"))
                os.replace(final_bam.with_name(final_bam.name + ".part"), final_bam)
            else:
                partial = final_bam.with_name(final_bam.name + ".part")
                self.runner.run(
                    [
                        self.config.samtools,
                        "merge",
                        "-f",
                        "-@",
                        str(max(1, threads)),
                        str(partial),
                        *map(str, bams),
                    ]
                )
                os.replace(partial, final_bam)
        self.runner.run(
            [
                self.config.samtools,
                "index",
                "-c",
                "-@",
                str(max(1, threads)),
                str(final_bam),
            ]
        )
        coverage_outputs: list[Path] = []
        coverage_modes: tuple[str | None, ...] = self.config.coverage_strands or (None,)
        for strand in coverage_modes:
            label = strand or "coverage"
            bigwig = output_dir / f"{safe_id}.{label}.bw"
            if not existing_nonempty(bigwig):
                command = [
                    self.config.bam_coverage,
                    "--bam",
                    str(final_bam),
                    "--outFileName",
                    str(bigwig),
                    "--outFileFormat",
                    "bigwig",
                    "--binSize",
                    str(self.config.bin_size),
                    "--numberOfProcessors",
                    str(max(1, threads)),
                    "--skipNAs",
                ]
                if strand:
                    command.extend(("--filterRNAstrand", strand))
                if self.config.coverage_ignore_duplicates:
                    command.append("--ignoreDuplicates")
                if self.config.normalize_using:
                    command.extend(("--normalizeUsing", self.config.normalize_using))
                self.runner.run(command, timeout=24 * 60 * 60)
            coverage_outputs.append(bigwig)
        outputs = (
            final_bam,
            Path(str(final_bam) + ".csi"),
            *coverage_outputs,
            *reports,
        )
        missing = [str(path) for path in outputs if not existing_nonempty(path)]
        if missing:
            raise ProcessingError(f"ATAC processor outputs are missing or empty: {missing}")
        metrics = {}
        for report in reports:
            if report.suffix == ".json" and existing_nonempty(report):
                with report.open("r", encoding="utf-8") as handle:
                    metrics[report.stem] = json.load(handle)
        result = ProcessingResult(
            success=True,
            outputs=tuple(outputs),
            metrics=metrics,
            tool_versions=versions,
        )
        result.validate()
        if not self.config.keep_intermediates:
            for path in bams:
                if path != final_bam:
                    path.unlink(missing_ok=True)
        return result


default_atac_processor = AtacSeqProcessor()


def process_atac(fastq: FastqSet, genome: GenomeRef, threads: int) -> ProcessingResult:
    """Importable default processor for local and Slurm execution."""

    return default_atac_processor(fastq, genome, threads)
