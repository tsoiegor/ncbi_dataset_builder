from __future__ import annotations

import gzip
import hashlib
import os
import random
import re
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Protocol

from .commands import CommandRunner
from .errors import DownloadError, ExternalToolError
from .models import FastqLayout, FastqSet, ProcessingUnit, StagedFastq
from .pipeline import paths_size_gb
from .progress import ProgressReporter, get_progress
from .util import (
    atomic_write_json,
    bytes_to_gb,
    exclusive_file_lock,
    existing_nonempty,
    gb_to_bytes,
    read_json,
    sanitize_identifier,
    sha256_file,
)


class FastqProvider(Protocol):
    """Protocol for providers that materialize FASTQ files for processing units."""

    def fetch(self, unit: ProcessingUnit, destination: Path, *, threads: int) -> FastqSet:
        """Fetch *unit* below *destination* using at most *threads* threads."""

        ...


class StagedFastqProvider(FastqProvider, Protocol):
    """Protocol for providers separating network staging from materialization."""

    def stage(self, unit: ProcessingUnit, destination: Path, *, threads: int) -> StagedFastq:
        """Download and validate *unit* at *destination* using *threads*."""

        ...

    def materialize(
        self,
        unit: ProcessingUnit,
        staged: StagedFastq,
        destination: Path,
        *,
        threads: int,
    ) -> FastqSet:
        """Convert *staged* data for *unit* at *destination* using *threads*."""

        ...


def _from_manifest(path: Path, *, progress: ProgressReporter | None = None) -> FastqSet | None:
    """Restore cached FASTQ from *path*, reporting checksums with *progress*."""

    if not path.is_file():
        return None
    data = read_json(path)
    try:
        value = FastqSet.from_dict(data)
        value.validate()
        for filename, expected in value.checksums.items():
            candidate = Path(filename)
            if candidate not in (*value.read1, *value.read2, *value.single):
                return None
            if sha256_file(candidate, progress=progress) != expected:
                return None
        return value
    except (KeyError, ValueError, OSError):
        return None


class AtomicDownloader:
    """Download HTTP data with resume, validation, retries, and atomic publication."""

    def __init__(
        self,
        *,
        user_agent: str,
        retries: int = 5,
        timeout_seconds: float = 120.0,
        progress: ProgressReporter | None = None,
    ) -> None:
        """Configure download identity, retries, timeout, and *progress*.

        *user_agent* identifies requests, *retries* bounds retries, and
        *timeout_seconds* limits each attempt.
        """

        self.user_agent = user_agent
        self.retries = retries
        self.timeout_seconds = timeout_seconds
        self.progress = get_progress(progress)

    def download(
        self,
        url: str,
        destination: Path,
        *,
        expected_sha256: str | None = None,
        expected_size_gb: float | None = None,
    ) -> Path:
        """Download *url* to *destination*, checking *expected_sha256* and *expected_size_gb*."""

        destination.parent.mkdir(parents=True, exist_ok=True)
        expected_size = gb_to_bytes(expected_size_gb) if expected_size_gb is not None else None
        if existing_nonempty(destination):
            if (
                expected_size is not None
                and destination.stat().st_size != expected_size
                or expected_sha256 is not None
                and sha256_file(destination, progress=self.progress) != expected_sha256.lower()
            ):
                destination.unlink()
            else:
                self.progress.message(f"Download cache hit: {destination}")
                return destination
        partial = destination.with_name(destination.name + ".part")
        last_error: BaseException | None = None
        for attempt in range(self.retries + 1):
            offset = partial.stat().st_size if partial.exists() else 0
            headers = {"User-Agent": self.user_agent, "Accept-Encoding": "identity"}
            if offset:
                headers["Range"] = f"bytes={offset}-"
            request = urllib.request.Request(url, headers=headers)
            try:
                self.progress.message(
                    f"Download {url} (attempt {attempt + 1}/{self.retries + 1}, "
                    f"resume={bytes_to_gb(offset):.3f} GB)"
                )
                with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                    append = offset > 0 and response.status == 206
                    mode = "ab" if append else "wb"
                    with partial.open(mode) as handle:
                        length = response.headers.get("Content-Length")
                        total = expected_size or (
                            (offset if append else 0) + int(length) if length else None
                        )
                        with self.progress.task(
                            f"Download {destination.name}",
                            total=bytes_to_gb(total) if total is not None else None,
                            unit="GB",
                        ) as progress:
                            if append:
                                progress.update(bytes_to_gb(offset))
                            while chunk := response.read(8 * 1024 * 1024):
                                handle.write(chunk)
                                progress.update(bytes_to_gb(len(chunk)))
                        handle.flush()
                        os.fsync(handle.fileno())
                if expected_size is not None and partial.stat().st_size != expected_size:
                    raise DownloadError(
                        f"Downloaded size mismatch for {url}: "
                        f"{bytes_to_gb(partial.stat().st_size):.9f} GB != "
                        f"{expected_size_gb:.9f} GB"
                    )
                if (
                    expected_sha256 is not None
                    and sha256_file(partial, progress=self.progress) != expected_sha256.lower()
                ):
                    raise DownloadError(f"SHA-256 mismatch for {url}")
                os.replace(partial, destination)
                return destination
            except (
                urllib.error.URLError,
                TimeoutError,
                ConnectionError,
                OSError,
                DownloadError,
            ) as exc:
                last_error = exc
                if attempt == self.retries:
                    break
                self.progress.message(f"Download retry required for {url}: {exc}")
                if isinstance(exc, DownloadError):
                    partial.unlink(missing_ok=True)
                time.sleep(min(30.0, 0.75 * 2**attempt) + random.random())
        raise DownloadError(
            f"Failed to download {url} after {self.retries + 1} attempts"
        ) from last_error


class SraToolkitProvider:
    """Fetch SRA runs with prefetch, validate them, and convert them to FASTQ."""

    def __init__(
        self,
        *,
        runner: CommandRunner | None = None,
        retries: int = 3,
        prefetch_max_size: str = "100G",
        progress: ProgressReporter | None = None,
    ) -> None:
        """Configure *runner*, *retries*, *prefetch_max_size*, and *progress*."""

        self.runner = runner or CommandRunner()
        self.retries = retries
        self.prefetch_max_size = prefetch_max_size
        self.progress = get_progress(progress)

    def preflight(self) -> dict[str, str]:
        """Require SRA Toolkit commands and return available tool versions."""

        self.runner.require("prefetch", "vdb-validate", "fasterq-dump")
        versions = {}
        for tool in ("prefetch", "vdb-validate", "fasterq-dump"):
            versions[tool] = self.runner.version(tool, "--version")
        if self.runner.which("pigz"):
            versions["pigz"] = self.runner.version("pigz", "--version")
        return versions

    def _retry_command(self, command: Sequence[str], *, timeout: float | None = None) -> None:
        """Run *command* with provider retries and optional per-attempt *timeout*."""

        last_error: BaseException | None = None
        for attempt in range(self.retries + 1):
            try:
                self.progress.message(
                    f"Run {command[0]} (attempt {attempt + 1}/{self.retries + 1})"
                )
                self.runner.run(command, timeout=timeout)
                return
            except ExternalToolError as exc:
                last_error = exc
                if attempt == self.retries:
                    break
                self.progress.message(f"Retry {command[0]} after failure: {exc}")
                time.sleep(min(30.0, 2**attempt) + random.random())
        raise DownloadError(f"Command failed after retries: {list(command)!r}") from last_error

    def _download_run(self, accession: str, sra_root: Path) -> Path:
        """Prefetch and validate SRA *accession* below unit-local *sra_root*."""

        run_dir = sra_root / accession
        run_dir.mkdir(parents=True, exist_ok=True)
        complete = run_dir / "sra.complete.json"
        if complete.is_file():
            candidates = list(run_dir.rglob(f"{accession}.sra"))
            if len(candidates) == 1 and existing_nonempty(candidates[0]):
                self.progress.message(f"SRA raw cache hit: {accession}")
                return candidates[0]
        self.progress.message(f"Prefetch and validate SRA run {accession}")
        # prefetch is resumable and inexpensive for an already complete run; it
        # is deliberately called again so an interrupted/corrupt cache repairs.
        self._retry_command(
            [
                "prefetch",
                accession,
                "--output-directory",
                str(run_dir),
                "--max-size",
                self.prefetch_max_size,
            ]
        )
        candidates = list(run_dir.rglob(f"{accession}.sra"))
        if len(candidates) != 1 or not existing_nonempty(candidates[0]):
            raise DownloadError(
                f"Expected one non-empty {accession}.sra below {run_dir}; found {len(candidates)}"
            )
        self._retry_command(["vdb-validate", str(run_dir)])
        atomic_write_json(
            complete,
            {
                "accession": accession,
                "sra_file": str(candidates[0]),
                "size_gb": bytes_to_gb(candidates[0].stat().st_size),
            },
        )
        return candidates[0]

    def _compress(self, path: Path, threads: int) -> Path:
        """Compress FASTQ *path* with pigz or gzip using available *threads*."""

        target = path.with_suffix(path.suffix + ".gz")
        if existing_nonempty(target):
            self.progress.message(f"Compressed FASTQ cache hit: {target}")
            return target
        self.progress.message(f"Compress FASTQ: {path}")
        if self.runner.which("pigz"):
            self.runner.run(["pigz", "-p", str(max(1, threads)), "-f", str(path)])
            if not existing_nonempty(target):
                raise DownloadError(f"pigz did not create {target}")
            return target
        partial = target.with_name(target.name + ".part")
        with (
            path.open("rb") as source,
            gzip.open(partial, "wb", compresslevel=6) as output,
            self.progress.task(
                f"Compress {path.name}", total=bytes_to_gb(path.stat().st_size), unit="GB"
            ) as progress,
        ):
            while chunk := source.read(8 * 1024 * 1024):
                output.write(chunk)
                progress.update(bytes_to_gb(len(chunk)))
        os.replace(partial, target)
        path.unlink()
        return target

    @staticmethod
    def _validate_gzip(path: Path) -> None:
        """Read compressed FASTQ *path* fully and raise when its gzip stream is invalid."""

        try:
            with gzip.open(path, "rb") as handle:
                while handle.read(8 * 1024 * 1024):
                    pass
        except (OSError, EOFError) as exc:
            raise DownloadError(f"Compressed FASTQ failed gzip validation: {path}") from exc

    def _cached_run_fastq(self, accession: str, root: Path) -> dict[str, Path] | None:
        """Return validated per-run FASTQ for *accession* below unit *root*, when cached."""

        run_fastq = root / ".runs" / accession
        complete = run_fastq / "run.complete.json"
        if not complete.is_file():
            return None
        known = {
            "read1": run_fastq / f"{accession}_1.fastq.gz",
            "read2": run_fastq / f"{accession}_2.fastq.gz",
            "single": run_fastq / f"{accession}.fastq.gz",
        }
        present = {key: path for key, path in known.items() if existing_nonempty(path)}
        if not present or ("read1" in present) != ("read2" in present):
            return None
        try:
            record = read_json(complete)
            expected = record.get("checksums", {})
            for path in present.values():
                if expected.get(path.name) != sha256_file(path, progress=self.progress):
                    return None
                self._validate_gzip(path)
        except (OSError, ValueError):
            return None
        return present

    def _materialize_run(
        self, accession: str, sra_file: Path, root: Path, threads: int
    ) -> dict[str, Path]:
        """Convert *sra_file* for *accession* into validated FASTQ files.

        *root* stores unit-local run files and temporary data; *threads* controls
        ``fasterq-dump`` and compression concurrency.
        """

        cached = self._cached_run_fastq(accession, root)
        if cached is not None:
            self.progress.message(f"FASTQ conversion cache hit: {accession}")
            return cached
        run_fastq = root / ".runs" / accession
        run_fastq.mkdir(parents=True, exist_ok=True)
        complete = run_fastq / "run.complete.json"
        known = {
            "read1": run_fastq / f"{accession}_1.fastq.gz",
            "read2": run_fastq / f"{accession}_2.fastq.gz",
            "single": run_fastq / f"{accession}.fastq.gz",
        }
        for path in (*known.values(), *(run_fastq.glob("*.fastq"))):
            path.unlink(missing_ok=True)
        complete.unlink(missing_ok=True)
        temporary = root / ".tmp" / accession
        temporary.mkdir(parents=True, exist_ok=True)
        self._retry_command(
            [
                "fasterq-dump",
                str(sra_file),
                "--split-3",
                "-e",
                str(max(1, threads)),
                "-t",
                str(temporary),
                "-O",
                str(run_fastq),
            ]
        )
        for fastq in sorted(run_fastq.glob("*.fastq")):
            self._compress(fastq, threads)
        present = {key: path for key, path in known.items() if existing_nonempty(path)}
        if ("read1" in present) != ("read2" in present):
            raise DownloadError(f"Run {accession} has only one mate after fasterq-dump")
        if not present:
            raise DownloadError(f"fasterq-dump produced no FASTQ for {accession}")
        checksums: dict[str, str] = {}
        for path in present.values():
            self._validate_gzip(path)
            checksums[path.name] = sha256_file(path, progress=self.progress)
        atomic_write_json(
            complete,
            {
                "accession": accession,
                "files": {key: str(path) for key, path in present.items()},
                "checksums": checksums,
            },
        )
        raw_root = root / ".raw" / accession
        if raw_root.is_dir():
            shutil.rmtree(raw_root)
            self.progress.message(f"Removed validated SRA archive for {accession}")
        if temporary.is_dir():
            shutil.rmtree(temporary)
        return present

    def _merge_gzip_members(self, paths: Iterable[Path], destination: Path) -> Path | None:
        """Concatenate gzip member *paths* in order into *destination*."""

        inputs = list(paths)
        if not inputs:
            return None
        if existing_nonempty(destination):
            return destination
        self.progress.message(f"Merge {len(inputs):,} gzip members into {destination.name}")
        partial = destination.with_name(destination.name + ".part")
        destination.parent.mkdir(parents=True, exist_ok=True)
        partial.unlink(missing_ok=True)
        if len(inputs) == 1:
            try:
                os.link(inputs[0], partial)
                os.replace(partial, destination)
                return destination
            except OSError:
                partial.unlink(missing_ok=True)
        with (
            partial.open("wb") as output,
            self.progress.task(
                f"Merge {destination.name}",
                total=sum(bytes_to_gb(path.stat().st_size) for path in inputs),
                unit="GB",
            ) as progress,
        ):
            for path in inputs:
                with path.open("rb") as source:
                    while chunk := source.read(8 * 1024 * 1024):
                        output.write(chunk)
                        progress.update(bytes_to_gb(len(chunk)))
            output.flush()
            os.fsync(output.fileno())
        os.replace(partial, destination)
        return destination

    def fetch(self, unit: ProcessingUnit, destination: Path, *, threads: int) -> FastqSet:
        """Return FASTQ for *unit* below *destination*, using *threads* for tools."""

        staged = self.stage(unit, destination, threads=threads)
        return self.materialize(unit, staged, destination, threads=threads)

    def _unit_locations(self, unit: ProcessingUnit, destination: Path) -> tuple[str, Path, Path]:
        """Return safe ID, unit root, and manifest for *unit* at *destination*."""

        safe_id = sanitize_identifier(unit.unit_id)
        unit_root = destination / safe_id
        manifest = unit_root / "fastq.manifest.json"
        return safe_id, unit_root, manifest

    def _cleanup_roots(
        self, unit: ProcessingUnit, destination: Path, unit_root: Path
    ) -> tuple[Path, ...]:
        """Return the one unit-owned *unit_root* eligible below *destination*."""

        del unit, destination
        return (unit_root,)

    @staticmethod
    def _remove_transient_unit_files(unit_root: Path) -> None:
        """Remove raw and per-run conversion artifacts below *unit_root*."""

        for transient in (unit_root / ".raw", unit_root / ".tmp", unit_root / ".runs"):
            if transient.is_dir():
                shutil.rmtree(transient)
        for fastq in unit_root.glob("*.fastq"):
            fastq.unlink(missing_ok=True)

    def _unit_cache(self, unit: ProcessingUnit, manifest: Path) -> FastqSet | None:
        """Return *unit* FASTQ from *manifest* only when its run list still matches."""

        cached = _from_manifest(manifest, progress=self.progress)
        if cached is not None and cached.run_accessions == unit.run_accessions:
            self._remove_transient_unit_files(manifest.parent)
            return cached
        stale_outputs = list(manifest.parent.glob("*.fastq.gz"))
        if cached is not None or manifest.exists() or stale_outputs:
            manifest.unlink(missing_ok=True)
            for path in stale_outputs:
                path.unlink(missing_ok=True)
            self.progress.message(f"Invalidate changed FASTQ unit cache: {unit.unit_id}")
        return None

    def stage(self, unit: ProcessingUnit, destination: Path, *, threads: int) -> StagedFastq:
        """Prefetch raw SRA for *unit* below *destination* using *threads*."""

        del threads
        safe_id, unit_root, manifest = self._unit_locations(unit, destination)
        del safe_id
        cached = self._unit_cache(unit, manifest)
        if cached is not None:
            self.progress.message(f"FASTQ unit cache hit: {unit.unit_id}")
            roots = self._cleanup_roots(unit, destination, unit_root)
            return StagedFastq(
                unit_id=unit.unit_id,
                source="sra",
                size_gb=paths_size_gb(list(roots)),
                cleanup_roots=roots,
                ready_fastq=cached,
                metadata={"cache_hit": True},
            )
        unit_root.mkdir(parents=True, exist_ok=True)
        self.runner.require("prefetch", "vdb-validate", "fasterq-dump")
        sra_files: dict[str, str] = {}
        for accession in self.progress.track(
            unit.run_accessions, f"Stage raw SRA for {unit.unit_id}", unit="runs"
        ):
            with exclusive_file_lock(
                unit_root / f".{accession}.lock",
                timeout_seconds=7 * 24 * 60 * 60,
                stale_after_seconds=7 * 24 * 60 * 60,
            ):
                if self._cached_run_fastq(accession, unit_root) is not None:
                    continue
                sra_files[accession] = str(
                    self._download_run(accession, unit_root / ".raw")
                )
        roots = self._cleanup_roots(unit, destination, unit_root)
        return StagedFastq(
            unit_id=unit.unit_id,
            source="sra",
            size_gb=paths_size_gb(list(roots)),
            cleanup_roots=roots,
            metadata={"sra_files": sra_files},
        )

    def materialize(
        self,
        unit: ProcessingUnit,
        staged: StagedFastq,
        destination: Path,
        *,
        threads: int,
    ) -> FastqSet:
        """Convert *staged* SRA data for *unit* at *destination* using *threads*."""

        if staged.unit_id != unit.unit_id:
            raise ValueError(f"Staged unit {staged.unit_id!r} does not match {unit.unit_id!r}")
        if staged.ready_fastq is not None:
            return staged.ready_fastq
        safe_id, unit_root, manifest = self._unit_locations(unit, destination)
        unit_root.mkdir(parents=True, exist_ok=True)
        with exclusive_file_lock(
            unit_root / ".fastq.lock",
            timeout_seconds=7 * 24 * 60 * 60,
            stale_after_seconds=7 * 24 * 60 * 60,
        ):
            cached = self._unit_cache(unit, manifest)
            if cached is not None:
                self.progress.message(f"FASTQ unit cache hit after lock: {unit.unit_id}")
                return cached
            return self._materialize_staged(
                unit,
                destination,
                staged=staged,
                threads=threads,
                safe_id=safe_id,
                unit_root=unit_root,
                manifest=manifest,
            )

    def _materialize_staged(
        self,
        unit: ProcessingUnit,
        destination: Path,
        *,
        staged: StagedFastq,
        threads: int,
        safe_id: str,
        unit_root: Path,
        manifest: Path,
    ) -> FastqSet:
        """Materialize *staged* SRA FASTQ and publish *manifest*.

        *unit* identifies runs, *destination* is the FASTQ root, *threads* sets
        tool concurrency, and *safe_id*/*unit_root* are validated cache paths.
        """

        self.runner.require("prefetch", "vdb-validate", "fasterq-dump")
        by_run: dict[str, dict[str, Path]] = {}
        for accession in self.progress.track(
            unit.run_accessions,
            f"Materialize FASTQ for {unit.unit_id}",
            unit="runs",
        ):
            with exclusive_file_lock(
                unit_root / f".{accession}.lock",
                timeout_seconds=7 * 24 * 60 * 60,
                stale_after_seconds=7 * 24 * 60 * 60,
            ):
                cached_run = self._cached_run_fastq(accession, unit_root)
                staged_path = staged.metadata.get("sra_files", {}).get(accession)
                if cached_run is not None:
                    by_run[accession] = cached_run
                    continue
                sra_file = (
                    Path(staged_path)
                    if staged_path and existing_nonempty(Path(staged_path))
                    else self._download_run(accession, unit_root / ".raw")
                )
                by_run[accession] = self._materialize_run(
                    accession, sra_file, unit_root, threads
                )

        merged = unit_root
        read1 = self._merge_gzip_members(
            (by_run[run]["read1"] for run in unit.run_accessions if "read1" in by_run[run]),
            merged / f"{safe_id}_1.fastq.gz",
        )
        read2 = self._merge_gzip_members(
            (by_run[run]["read2"] for run in unit.run_accessions if "read2" in by_run[run]),
            merged / f"{safe_id}_2.fastq.gz",
        )
        single = self._merge_gzip_members(
            (by_run[run]["single"] for run in unit.run_accessions if "single" in by_run[run]),
            merged / f"{safe_id}.fastq.gz",
        )
        if read1 and read2 and single:
            layout = FastqLayout.MIXED
        elif read1 and read2:
            layout = FastqLayout.PAIRED
        elif single:
            layout = FastqLayout.SINGLE
        else:
            raise DownloadError(f"No usable FASTQ files for {unit.unit_id}")
        files = tuple(path for path in (read1, read2, single) if path is not None)
        result = FastqSet(
            unit_id=unit.unit_id,
            layout=layout,
            run_accessions=unit.run_accessions,
            read1=(read1,) if read1 else (),
            read2=(read2,) if read2 else (),
            single=(single,) if single else (),
            source="sra",
            work_dir=unit_root,
            output_dir=destination.parent / "results" / safe_id,
            checksums={str(path): sha256_file(path, progress=self.progress) for path in files},
            metadata={
                "per_run_layout": {run: sorted(value) for run, value in by_run.items()}
            },
        )
        result.validate()
        atomic_write_json(manifest, result.to_dict())
        self._remove_transient_unit_files(unit_root)
        return result


class GeoFastqProvider:
    """Download explicitly mapped GEO FASTQ URLs and infer common mate naming."""

    PAIRED = re.compile(r"(?:^|[._-])R?([12])(?:[._-]|$)", re.IGNORECASE)

    def __init__(
        self,
        urls: dict[str, list[str]],
        *,
        downloader: AtomicDownloader,
        progress: ProgressReporter | None = None,
    ) -> None:
        """Map unit IDs to *urls* using *downloader* and optional *progress*."""

        self.urls = urls
        self.downloader = downloader
        self.progress = get_progress(progress or getattr(downloader, "progress", None))

    def fetch(self, unit: ProcessingUnit, destination: Path, *, threads: int) -> FastqSet:
        """Return cached or downloaded GEO FASTQ for *unit* below *destination*.

        *threads* is accepted for provider compatibility; HTTP downloads are
        currently sequential.
        """

        del threads
        safe_id = sanitize_identifier(unit.unit_id)
        configured_urls = self.urls.get(unit.unit_id, [])
        fingerprint = hashlib.sha256("\n".join(configured_urls).encode()).hexdigest()[:16]
        root = destination / safe_id / fingerprint
        manifest = root / "fastq.manifest.json"
        cached = _from_manifest(manifest, progress=self.progress)
        if cached is not None:
            self.progress.message(f"GEO FASTQ unit cache hit: {unit.unit_id}")
            return cached
        root.mkdir(parents=True, exist_ok=True)
        with exclusive_file_lock(
            root / ".fastq.lock",
            timeout_seconds=7 * 24 * 60 * 60,
            stale_after_seconds=7 * 24 * 60 * 60,
        ):
            cached = _from_manifest(manifest, progress=self.progress)
            if cached is not None:
                self.progress.message(f"GEO FASTQ unit cache hit after lock: {unit.unit_id}")
                return cached
            return self._fetch_uncached(
                unit, destination, safe_id=safe_id, root=root, manifest=manifest
            )

    def stage(self, unit: ProcessingUnit, destination: Path, *, threads: int) -> StagedFastq:
        """Download complete GEO FASTQ for *unit* at *destination* using *threads*."""

        ready = self.fetch(unit, destination, threads=threads)
        return StagedFastq(
            unit_id=unit.unit_id,
            source="geo",
            size_gb=paths_size_gb([ready.work_dir]),
            cleanup_roots=(ready.work_dir,),
            ready_fastq=ready,
            metadata={"cache_hit_or_downloaded": True},
        )

    def materialize(
        self,
        unit: ProcessingUnit,
        staged: StagedFastq,
        destination: Path,
        *,
        threads: int,
    ) -> FastqSet:
        """Return GEO FASTQ already in *staged* for *unit* at *destination* using *threads*."""

        del destination, threads
        if staged.unit_id != unit.unit_id or staged.ready_fastq is None:
            raise ValueError(f"GEO staging record is invalid for {unit.unit_id!r}")
        return staged.ready_fastq

    def _fetch_uncached(
        self,
        unit: ProcessingUnit,
        destination: Path,
        *,
        safe_id: str,
        root: Path,
        manifest: Path,
    ) -> FastqSet:
        """Download and classify uncached GEO files, then publish *manifest*.

        *unit* chooses configured URLs, *destination* defines result placement,
        and *safe_id* and *root* identify the validated cache directory.
        """

        urls = self.urls.get(unit.unit_id, [])
        if not urls:
            raise DownloadError(f"No GEO FASTQ URLs configured for {unit.unit_id}")
        paired: dict[str, dict[str, Path]] = {}
        single: list[Path] = []
        for url in self.progress.track(
            urls, f"Download GEO FASTQ for {unit.unit_id}", unit="files"
        ):
            name = Path(urllib.request.url2pathname(urllib.parse.urlparse(url).path)).name
            if not name.lower().endswith((".fastq.gz", ".fq.gz", ".fastq", ".fq")):
                continue
            target = self.downloader.download(url, root / "downloads" / name)
            match = self.PAIRED.search(name)
            if match:
                mate = match.group(1)
                pair_key = name[: match.start(1)] + "#" + name[match.end(1) :]
                if mate in paired.setdefault(pair_key, {}):
                    raise DownloadError(
                        f"Duplicate GEO mate {mate} for pair key {pair_key!r} in {unit.unit_id}"
                    )
                paired[pair_key][mate] = target
            else:
                single.append(target)
        incomplete = [key for key, mates in paired.items() if set(mates) != {"1", "2"}]
        if incomplete:
            raise DownloadError(f"Incomplete GEO FASTQ pair(s) for {unit.unit_id}: {incomplete}")
        read1 = [paired[key]["1"] for key in sorted(paired)]
        read2 = [paired[key]["2"] for key in sorted(paired)]
        layout = (
            FastqLayout.MIXED
            if read1 and single
            else FastqLayout.PAIRED
            if read1
            else FastqLayout.SINGLE
        )
        result = FastqSet(
            unit_id=unit.unit_id,
            layout=layout,
            run_accessions=unit.run_accessions,
            read1=tuple(read1),
            read2=tuple(read2),
            single=tuple(sorted(single)),
            source="geo",
            work_dir=root,
            output_dir=destination.parent / "results" / safe_id,
            checksums={
                str(path): sha256_file(path, progress=self.progress)
                for path in (*read1, *read2, *single)
            },
        )
        result.validate()
        atomic_write_json(manifest, result.to_dict())
        return result
