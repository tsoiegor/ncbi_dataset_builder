from __future__ import annotations

import gzip
import json
import logging
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ...errors import ExternalToolError, ProcessingError
from ...models import FastqLayout, FastqSet, GenomeRef, ProcessingResult
from ...support.commands import CommandRunner
from ...support.progress import ProgressReporter
from ...support.unit_logging import current_unit_log_handle
from ...support.util import (
    bytes_to_gb,
    exclusive_file_lock,
    existing_nonempty,
    sanitize_identifier,
)

LOGGER = logging.getLogger("ncbi_dataset_builder.processing.atac")


@dataclass(frozen=True)
class AtacIntermediateFiles:
    """Control retention of every file category created by the ATAC processor.

    Args:
        keep_staged_fastq: Keep merged or recompressed ``input.*.fastq`` files
            created below the unit work directory. Original provider FASTQs are
            never removed by this option.
        keep_cleaned_fastq: Keep the filtered FASTQs written by fastp.
        keep_fastp_json: Keep fastp JSON reports and declare them as outputs.
        keep_fastp_html: Keep fastp HTML reports and declare them as outputs.
        keep_component_bams: Keep the paired and single BAMs used to assemble
            the final BAM.
        keep_final_bam: Keep the final merged BAM and declare it as an output.
        keep_final_bam_index: Keep the final BAM CSI index. This requires
            :attr:`keep_final_bam`.
        keep_uncompressed_genome: Keep the uncompressed FASTA materialized for
            Bowtie2 index construction.
        keep_bowtie2_index: Keep the reusable Bowtie2 index. Disabling this is
            intended for serialized processing because samples may share it.

    The defaults remove unit-local working files while retaining reports,
    final BAM/CSI files, BigWigs, and reusable genome caches.
    """

    keep_staged_fastq: bool = False
    keep_cleaned_fastq: bool = False
    keep_fastp_json: bool = True
    keep_fastp_html: bool = True
    keep_component_bams: bool = False
    keep_final_bam: bool = True
    keep_final_bam_index: bool = True
    keep_uncompressed_genome: bool = True
    keep_bowtie2_index: bool = True

    def __post_init__(self) -> None:
        """Reject a retained CSI index without its corresponding BAM."""

        if self.keep_final_bam_index and not self.keep_final_bam:
            raise ValueError("keep_final_bam_index requires keep_final_bam")


@dataclass(frozen=True)
class AtacSeqConfig:
    """Configure the built-in ATAC-seq processor.

    Args:
        bowtie2: Bowtie2 executable name or path.
        bowtie2_build: Bowtie2 index-builder executable.
        samtools: Samtools executable.
        fastp: Fastp executable.
        bam_coverage: deepTools ``bamCoverage`` executable.
        maximum_insert_size: Maximum paired-end alignment insert size.
        bin_size: BigWig coverage bin size.
        normalize_using: Optional ``bamCoverage`` normalization method.
        coverage_strands: Optional forward/reverse RNA-strand filters.
        fastp_deduplicate: Enable fastp duplicate removal.
        fastp_max_threads: Maximum threads passed to fastp.
        strict_mixed_layout: Inspect fastp reports for mixed paired/single input
            and exclude the complete single-end component when it looks
            technically suspicious or cannot be validated.
        mixed_count_tolerance: Maximum relative difference when comparing the
            single-read count with the paired-fragment count.
        mixed_max_short_read_length: Single-end mean length at or below this
            value is suspicious in a mixed ATAC-seq input.
        mixed_min_length_ratio: Minimum acceptable ratio of the single-end mean
            length to the shorter paired-end mean length.
        mixed_min_retained_fraction: Minimum acceptable fraction of single-end
            reads remaining after fastp.
        coverage_ignore_duplicates: Ignore duplicate reads in coverage output.
        intermediates: Retention policy for every processor-created file
            category. See :class:`AtacIntermediateFiles`.
    """

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
    strict_mixed_layout: bool = True
    mixed_count_tolerance: float = 0.001
    mixed_max_short_read_length: int = 30
    mixed_min_length_ratio: float = 0.5
    mixed_min_retained_fraction: float = 0.1
    coverage_ignore_duplicates: bool = True
    intermediates: AtacIntermediateFiles = field(default_factory=AtacIntermediateFiles)

    def __post_init__(self) -> None:
        """Validate positive numeric settings and supported strand labels."""

        if self.maximum_insert_size < 1 or self.bin_size < 1 or self.fastp_max_threads < 1:
            raise ValueError(
                "maximum_insert_size, bin_size, and fastp_max_threads must be positive"
            )
        if not 0 <= self.mixed_count_tolerance < 1:
            raise ValueError("mixed_count_tolerance must be in [0, 1)")
        if self.mixed_max_short_read_length < 1:
            raise ValueError("mixed_max_short_read_length must be positive")
        if not 0 < self.mixed_min_length_ratio <= 1:
            raise ValueError("mixed_min_length_ratio must be in (0, 1]")
        if not 0 <= self.mixed_min_retained_fraction <= 1:
            raise ValueError("mixed_min_retained_fraction must be in [0, 1]")
        invalid = set(self.coverage_strands) - {"forward", "reverse"}
        if invalid:
            raise ValueError(f"Unknown coverage strand(s): {sorted(invalid)}")


class AtacSeqProcessor:
    """Build checked BAM and BigWig outputs from ATAC-seq FASTQ inputs."""

    def __init__(
        self,
        config: AtacSeqConfig | None = None,
        *,
        runner: CommandRunner | None = None,
        progress: ProgressReporter | None = None,
    ) -> None:
        """Initialize with optional *config*, command *runner*, and *progress*."""

        self.config = config or AtacSeqConfig()
        self.runner = runner or CommandRunner()
        self.progress = progress or ProgressReporter()

    def preflight(self) -> dict[str, str]:
        """Require all configured tools and return their reported versions."""

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

    def _merge_inputs(self, paths: tuple[Path, ...], destination: Path) -> Path | None:
        """Merge ordered FASTQ *paths* into *destination*, preserving gzip members."""

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
            with (
                partial.open("wb") as output,
                self.progress.task(
                    f"Stage {destination.name}",
                    total=sum(bytes_to_gb(path.stat().st_size) for path in paths),
                    unit="GB",
                ) as progress,
            ):
                for path in paths:
                    with path.open("rb") as source:
                        while chunk := source.read(8 * 1024 * 1024):
                            output.write(chunk)
                            progress.update(bytes_to_gb(len(chunk)))
                output.flush()
                os.fsync(output.fileno())
        else:
            with (
                gzip.open(partial, "wb", compresslevel=6) as output,
                self.progress.task(f"Stage {destination.name}", unit="GB") as progress,
            ):
                for path in paths:
                    opener = gzip.open if path.suffix == ".gz" else open
                    with opener(path, "rb") as source:
                        while chunk := source.read(8 * 1024 * 1024):
                            output.write(chunk)
                            progress.update(bytes_to_gb(len(chunk)))
        os.replace(partial, destination)
        return destination

    def _uncompressed_fasta(self, source: Path, destination: Path) -> Path:
        """Materialize possibly gzipped FASTA *source* at *destination*."""

        if existing_nonempty(destination):
            return destination
        destination.parent.mkdir(parents=True, exist_ok=True)
        partial = destination.with_name(destination.name + ".part")
        opener = gzip.open if source.suffix == ".gz" else open
        with (
            opener(source, "rb") as input_handle,
            partial.open("wb") as output_handle,
            self.progress.task(f"Prepare {destination.name}", unit="GB") as progress,
        ):
            while chunk := input_handle.read(8 * 1024 * 1024):
                output_handle.write(chunk)
                progress.update(bytes_to_gb(len(chunk)))
            output_handle.flush()
            os.fsync(output_handle.fileno())
        os.replace(partial, destination)
        return destination

    @staticmethod
    def _index_complete(prefix: Path) -> bool:
        """Return whether all six Bowtie2 index files exist for *prefix*."""

        suffixes = (".1", ".2", ".3", ".4", ".rev.1", ".rev.2")
        standard = [Path(str(prefix) + suffix + ".bt2") for suffix in suffixes]
        large = [Path(str(prefix) + suffix + ".bt2l") for suffix in suffixes]
        return all(existing_nonempty(path) for path in standard) or all(
            existing_nonempty(path) for path in large
        )

    def _ensure_index(self, genome: GenomeRef, threads: int) -> Path:
        """Return a complete Bowtie2 index for *genome*, building with *threads*."""

        index_dir = genome.fasta.parent / "indexes" / sanitize_identifier(genome.accession)
        prefix = index_dir / genome.accession
        if self._index_complete(prefix):
            self.progress.message(f"Bowtie2 index cache hit: {genome.accession}")
            return prefix
        index_dir.mkdir(parents=True, exist_ok=True)
        with exclusive_file_lock(index_dir / ".build.lock", timeout_seconds=12 * 60 * 60):
            if self._index_complete(prefix):
                self.progress.message(f"Bowtie2 index cache hit after lock: {genome.accession}")
                return prefix
            self.progress.message(f"Build Bowtie2 index: {genome.accession}")
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

    @staticmethod
    def _bowtie2_index_files(prefix: Path) -> tuple[Path, ...]:
        """Return all standard or large Bowtie2 index files for *prefix*."""

        suffixes = (".1", ".2", ".3", ".4", ".rev.1", ".rev.2")
        return tuple(
            path
            for extension in (".bt2", ".bt2l")
            for suffix in suffixes
            if (path := Path(str(prefix) + suffix + extension)).exists()
        )

    @staticmethod
    def _remove_files(paths: list[Path] | tuple[Path, ...]) -> None:
        """Remove existing regular files in *paths* without following directories."""

        for path in dict.fromkeys(paths):
            if path.is_file() or path.is_symlink():
                path.unlink(missing_ok=True)

    def _run_fastp(
        self,
        *,
        read1: Path | None,
        read2: Path | None,
        single: Path | None,
        root: Path,
        threads: int,
    ) -> tuple[Path | None, Path | None, Path | None, list[Path]]:
        """Clean paired and/or single reads with fastp and return reads plus reports.

        *read1* and *read2* are paired mates, *single* contains unpaired reads,
        *root* stores outputs, and *threads* controls fastp concurrency.
        """

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
                self.progress.message("Run fastp for paired-end reads")
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
            else:
                self.progress.message("fastp paired-end cache hit")
            outputs.extend((report_json, report_html))
        if single:
            clean_single = root / "single.clean.fastq.gz"
            report_json = root / "single.fastp.json"
            report_html = root / "single.fastp.html"
            if not all(
                existing_nonempty(path) for path in (clean_single, report_json, report_html)
            ):
                self.progress.message("Run fastp for single-end reads")
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
            else:
                self.progress.message("fastp single-end cache hit")
            outputs.extend((report_json, report_html))
        for path in (clean1, clean2, clean_single):
            if path is not None and not existing_nonempty(path):
                raise ProcessingError(f"fastp output is missing or empty: {path}")
        return clean1, clean2, clean_single, outputs

    @staticmethod
    def _load_fastp_summary(path: Path) -> dict[str, float]:
        """Return validated before/after read statistics from a fastp JSON report."""

        with path.open("r", encoding="utf-8") as handle:
            report: Any = json.load(handle)
        try:
            before = report["summary"]["before_filtering"]
            after = report["summary"]["after_filtering"]
            summary = {
                "before_reads": float(before["total_reads"]),
                "after_reads": float(after["total_reads"]),
                "read1_mean_length": float(before["read1_mean_length"]),
            }
            if "read2_mean_length" in before:
                summary["read2_mean_length"] = float(before["read2_mean_length"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"invalid fastp summary in {path}") from exc
        if (
            summary["before_reads"] <= 0
            or summary["after_reads"] < 0
            or summary["after_reads"] > summary["before_reads"]
            or summary["read1_mean_length"] <= 0
            or summary.get("read2_mean_length", 1.0) <= 0
        ):
            raise ValueError(f"non-positive or inconsistent fastp summary in {path}")
        return summary

    def _mixed_layout_defense(self, root: Path) -> dict[str, Any]:
        """Decide whether a mixed input's complete single-end branch is suspicious."""

        paired_path = root / "paired.fastp.json"
        single_path = root / "single.fastp.json"
        decision: dict[str, Any] = {
            "enabled": self.config.strict_mixed_layout,
            "action": "kept_single_end",
            "reasons": [],
        }
        if not self.config.strict_mixed_layout:
            decision["action"] = "disabled"
            return decision
        try:
            paired = self._load_fastp_summary(paired_path)
            single = self._load_fastp_summary(single_path)
            paired_read2_length = paired["read2_mean_length"]
        except (OSError, ValueError, KeyError) as exc:
            decision["action"] = "excluded_single_end"
            decision["reasons"] = [f"mixed-layout fastp statistics are unavailable: {exc}"]
            return decision

        paired_fragments = paired["before_reads"] / 2.0
        count_difference = abs(single["before_reads"] - paired_fragments) / max(
            single["before_reads"], paired_fragments
        )
        paired_mean_length = min(paired["read1_mean_length"], paired_read2_length)
        length_ratio = single["read1_mean_length"] / paired_mean_length
        single_retained_fraction = single["after_reads"] / single["before_reads"]
        reasons: list[str] = []
        if count_difference <= self.config.mixed_count_tolerance:
            reasons.append(
                "single-read count matches paired-fragment count "
                f"(relative difference {count_difference:.6g})"
            )
        if single["read1_mean_length"] <= self.config.mixed_max_short_read_length:
            reasons.append(
                f"single-end mean length is only {single['read1_mean_length']:g} bp"
            )
        if length_ratio < self.config.mixed_min_length_ratio:
            reasons.append(
                "single-end reads are substantially shorter than paired reads "
                f"(length ratio {length_ratio:.3f})"
            )
        if single_retained_fraction < self.config.mixed_min_retained_fraction:
            reasons.append(
                "fastp retained an unusually small single-end fraction "
                f"({single_retained_fraction:.3%})"
            )
        decision["statistics"] = {
            "single_before_reads": int(single["before_reads"]),
            "paired_before_reads": int(paired["before_reads"]),
            "paired_fragments": paired_fragments,
            "single_mean_length": single["read1_mean_length"],
            "paired_read1_mean_length": paired["read1_mean_length"],
            "paired_read2_mean_length": paired_read2_length,
            "count_relative_difference": count_difference,
            "single_to_paired_length_ratio": length_ratio,
            "single_retained_fraction": single_retained_fraction,
        }
        if reasons:
            decision["action"] = "excluded_single_end"
            decision["reasons"] = reasons
        return decision

    def _align(
        self,
        *,
        prefix: Path,
        output: Path,
        threads: int,
        read1: Path | None = None,
        read2: Path | None = None,
        single: Path | None = None,
    ) -> Path:
        """Align one paired or single input and write a sorted BAM.

        *prefix* is the Bowtie2 index, *output* is the BAM, and *threads*
        controls alignment. Supply either paired
        *read1*/*read2* or one *single* FASTQ.
        """

        if existing_nonempty(output):
            self.progress.message(f"Alignment cache hit: {output}")
            return output
        self.progress.message(f"Align reads and sort BAM: {output.name}")
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
        log_handle = current_unit_log_handle()
        LOGGER.info(
            "Run alignment pipeline: %s | %s | %s",
            bowtie,
            [self.config.samtools, "view", "-b", "-"],
            [self.config.samtools, "sort", "-@", str(max(1, threads)), "-o", str(partial), "-"],
        )
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
            raise ProcessingError(f"Alignment pipeline failed with codes {codes}")
        if not existing_nonempty(partial):
            raise ProcessingError(f"Alignment produced no BAM: {partial}")
        os.replace(partial, output)
        return output

    def __call__(self, fastq: FastqSet, genome: GenomeRef, threads: int) -> ProcessingResult:
        """Process *fastq* against *genome* with *threads* and return validated outputs."""

        fastq.validate()
        genome.validate()
        self.progress.message(f"ATAC processing started: {fastq.unit_id}")
        versions = self.preflight()
        output_dir = fastq.output_dir
        work = fastq.work_dir / "processing" / "atac"
        output_dir.mkdir(parents=True, exist_ok=True)
        work.mkdir(parents=True, exist_ok=True)
        safe_id = sanitize_identifier(fastq.unit_id)
        self.progress.message("Stage FASTQ inputs")
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
        all_cleaned_fastq = tuple(
            path for path in (clean1, clean2, clean_single) if path is not None
        )
        mixed_layout_decision: dict[str, Any] | None = None
        if fastq.layout == FastqLayout.MIXED:
            mixed_layout_decision = self._mixed_layout_defense(work)
            if mixed_layout_decision["action"] == "excluded_single_end":
                reasons = "; ".join(mixed_layout_decision["reasons"])
                self.progress.message(
                    "Strict mixed-layout defense excluded all single-end reads; "
                    f"continuing with paired-end reads only: {reasons}"
                )
                clean_single = None
        prefix = self._ensure_index(genome, threads)
        bams: list[Path] = []
        if clean1 and clean2:
            bams.append(
                self._align(
                    prefix=prefix,
                    output=work / "paired.sorted.bam",
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
                    threads=threads,
                    single=clean_single,
                )
            )
        final_bam = output_dir / f"{safe_id}.bam"
        if not existing_nonempty(final_bam):
            self.progress.message(f"Create final BAM: {final_bam.name}")
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
        self.progress.message(f"Index final BAM: {final_bam.name}")
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
                self.progress.message(f"Create BigWig coverage: {bigwig.name}")
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
            else:
                self.progress.message(f"BigWig cache hit: {bigwig.name}")
            coverage_outputs.append(bigwig)
        final_index = Path(str(final_bam) + ".csi")
        produced = (
            final_bam,
            final_index,
            *coverage_outputs,
            *reports,
        )
        missing = [str(path) for path in produced if not existing_nonempty(path)]
        if missing:
            raise ProcessingError(f"ATAC processor outputs are missing or empty: {missing}")
        metrics = {}
        for report in reports:
            if report.suffix == ".json" and existing_nonempty(report):
                with report.open("r", encoding="utf-8") as handle:
                    metrics[report.stem] = json.load(handle)
        if mixed_layout_decision is not None:
            metrics["mixed_layout_defense"] = mixed_layout_decision
        retention = self.config.intermediates
        staged_fastq = tuple(
            path
            for path in (staged1, staged2, staged_single)
            if path is not None and path.parent.resolve() == work.resolve()
        )
        json_reports = tuple(path for path in reports if path.suffix == ".json")
        html_reports = tuple(path for path in reports if path.suffix == ".html")
        index_fasta = prefix.parent / f"{genome.accession}.fna"
        index_files = self._bowtie2_index_files(prefix)

        if not retention.keep_staged_fastq:
            self._remove_files(staged_fastq)
        if not retention.keep_cleaned_fastq:
            self._remove_files(all_cleaned_fastq)
        if not retention.keep_component_bams:
            self._remove_files(tuple(bams))
        if not retention.keep_fastp_json:
            self._remove_files(json_reports)
        if not retention.keep_fastp_html:
            self._remove_files(html_reports)
        if not retention.keep_final_bam_index:
            self._remove_files((final_index,))
        if not retention.keep_final_bam:
            self._remove_files((final_bam,))
        if not retention.keep_bowtie2_index:
            self._remove_files(index_files)
        if not retention.keep_uncompressed_genome:
            self._remove_files((index_fasta,))

        outputs: list[Path] = list(coverage_outputs)
        if retention.keep_final_bam:
            outputs.insert(0, final_bam)
        if retention.keep_final_bam_index:
            outputs.insert(1, final_index)
        if retention.keep_fastp_json:
            outputs.extend(json_reports)
        if retention.keep_fastp_html:
            outputs.extend(html_reports)
        result = ProcessingResult(
            success=True,
            outputs=tuple(outputs),
            metrics=metrics,
            tool_versions=versions,
        )
        result.validate()
        self.progress.message(f"ATAC processing complete: {fastq.unit_id}")
        return result


default_atac_processor = AtacSeqProcessor()


def process_atac(fastq: FastqSet, genome: GenomeRef, threads: int) -> ProcessingResult:
    """Run the default ATAC processor on *fastq* and *genome* with *threads*."""

    return default_atac_processor(fastq, genome, threads)
