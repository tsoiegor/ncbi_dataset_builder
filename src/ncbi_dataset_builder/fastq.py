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
from .models import FastqLayout, FastqSet, ProcessingUnit
from .util import (
    atomic_write_json,
    exclusive_file_lock,
    existing_nonempty,
    read_json,
    sanitize_identifier,
    sha256_file,
)


class FastqProvider(Protocol):
    def fetch(self, unit: ProcessingUnit, destination: Path, *, threads: int) -> FastqSet: ...


def _from_manifest(path: Path) -> FastqSet | None:
    if not path.is_file():
        return None
    data = read_json(path)
    try:
        value = FastqSet(
            unit_id=data["unit_id"],
            layout=FastqLayout(data["layout"]),
            run_accessions=tuple(data["run_accessions"]),
            read1=tuple(Path(item) for item in data.get("read1", [])),
            read2=tuple(Path(item) for item in data.get("read2", [])),
            single=tuple(Path(item) for item in data.get("single", [])),
            source=data.get("source", "sra"),
            work_dir=Path(data["work_dir"]),
            output_dir=Path(data["output_dir"]),
            checksums=data.get("checksums", {}),
            metadata=data.get("metadata", {}),
        )
        value.validate()
        for filename, expected in value.checksums.items():
            candidate = Path(filename)
            if candidate not in (*value.read1, *value.read2, *value.single):
                return None
            if sha256_file(candidate) != expected:
                return None
        return value
    except (KeyError, ValueError, OSError):
        return None


class AtomicDownloader:
    """Streaming HTTP(S) download with range resume and atomic publication."""

    def __init__(
        self, *, user_agent: str, retries: int = 5, timeout_seconds: float = 120.0
    ) -> None:
        self.user_agent = user_agent
        self.retries = retries
        self.timeout_seconds = timeout_seconds

    def download(
        self,
        url: str,
        destination: Path,
        *,
        expected_sha256: str | None = None,
        expected_size: int | None = None,
    ) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        if existing_nonempty(destination):
            if (
                expected_size is not None
                and destination.stat().st_size != expected_size
                or expected_sha256 is not None
                and sha256_file(destination) != expected_sha256.lower()
            ):
                destination.unlink()
            else:
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
                with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                    append = offset > 0 and response.status == 206
                    mode = "ab" if append else "wb"
                    with partial.open(mode) as handle:
                        shutil.copyfileobj(response, handle, length=8 * 1024 * 1024)
                        handle.flush()
                        os.fsync(handle.fileno())
                if expected_size is not None and partial.stat().st_size != expected_size:
                    raise DownloadError(
                        f"Downloaded size mismatch for {url}: {partial.stat().st_size} != {expected_size}"
                    )
                if expected_sha256 is not None and sha256_file(partial) != expected_sha256.lower():
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
                if isinstance(exc, DownloadError):
                    partial.unlink(missing_ok=True)
                time.sleep(min(30.0, 0.75 * 2**attempt) + random.random())
        raise DownloadError(
            f"Failed to download {url} after {self.retries + 1} attempts"
        ) from last_error


class SraToolkitProvider:
    """Resumable SRA Toolkit provider using prefetch, validation, and fasterq-dump."""

    def __init__(
        self,
        *,
        runner: CommandRunner | None = None,
        retries: int = 3,
        prefetch_max_size: str = "100G",
    ) -> None:
        self.runner = runner or CommandRunner()
        self.retries = retries
        self.prefetch_max_size = prefetch_max_size

    def preflight(self) -> dict[str, str]:
        self.runner.require("prefetch", "vdb-validate", "fasterq-dump")
        versions = {}
        for tool in ("prefetch", "vdb-validate", "fasterq-dump"):
            versions[tool] = self.runner.version(tool, "--version")
        if self.runner.which("pigz"):
            versions["pigz"] = self.runner.version("pigz", "--version")
        return versions

    def _retry_command(self, command: Sequence[str], *, timeout: float | None = None) -> None:
        last_error: BaseException | None = None
        for attempt in range(self.retries + 1):
            try:
                self.runner.run(command, timeout=timeout)
                return
            except ExternalToolError as exc:
                last_error = exc
                if attempt == self.retries:
                    break
                time.sleep(min(30.0, 2**attempt) + random.random())
        raise DownloadError(f"Command failed after retries: {list(command)!r}") from last_error

    def _download_run(self, accession: str, sra_root: Path) -> Path:
        run_dir = sra_root / accession
        run_dir.mkdir(parents=True, exist_ok=True)
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
        return candidates[0]

    def _compress(self, path: Path, threads: int) -> Path:
        target = path.with_suffix(path.suffix + ".gz")
        if existing_nonempty(target):
            return target
        if self.runner.which("pigz"):
            self.runner.run(["pigz", "-p", str(max(1, threads)), "-f", str(path)])
            if not existing_nonempty(target):
                raise DownloadError(f"pigz did not create {target}")
            return target
        partial = target.with_name(target.name + ".part")
        with (
            path.open("rb") as source,
            gzip.open(partial, "wb", compresslevel=6) as output,
        ):
            shutil.copyfileobj(source, output, length=8 * 1024 * 1024)
        os.replace(partial, target)
        path.unlink()
        return target

    def _materialize_run(
        self, accession: str, sra_file: Path, root: Path, threads: int
    ) -> dict[str, Path]:
        run_fastq = root / "runs" / accession
        run_fastq.mkdir(parents=True, exist_ok=True)
        complete = run_fastq / "run.complete.json"
        known = {
            "read1": run_fastq / f"{accession}_1.fastq.gz",
            "read2": run_fastq / f"{accession}_2.fastq.gz",
            "single": run_fastq / f"{accession}.fastq.gz",
        }
        if complete.is_file():
            present = {key: path for key, path in known.items() if existing_nonempty(path)}
            if present and ("read1" in present) == ("read2" in present):
                return present
        for path in (*known.values(), *(run_fastq.glob("*.fastq"))):
            path.unlink(missing_ok=True)
        complete.unlink(missing_ok=True)
        temporary = root / "tmp" / accession
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
        atomic_write_json(
            complete,
            {
                "accession": accession,
                "files": {key: str(path) for key, path in present.items()},
            },
        )
        return present

    @staticmethod
    def _merge_gzip_members(paths: Iterable[Path], destination: Path) -> Path | None:
        inputs = list(paths)
        if not inputs:
            return None
        if existing_nonempty(destination):
            return destination
        partial = destination.with_name(destination.name + ".part")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with partial.open("wb") as output:
            for path in inputs:
                with path.open("rb") as source:
                    shutil.copyfileobj(source, output, length=8 * 1024 * 1024)
            output.flush()
            os.fsync(output.fileno())
        os.replace(partial, destination)
        return destination

    def fetch(self, unit: ProcessingUnit, destination: Path, *, threads: int) -> FastqSet:
        safe_id = sanitize_identifier(unit.unit_id)
        fingerprint = hashlib.sha256("\n".join(unit.run_accessions).encode()).hexdigest()[:16]
        unit_root = destination / safe_id / fingerprint
        manifest = unit_root / "fastq.manifest.json"
        cached = _from_manifest(manifest)
        if cached is not None:
            return cached
        unit_root.mkdir(parents=True, exist_ok=True)
        with exclusive_file_lock(
            unit_root / ".fastq.lock",
            timeout_seconds=7 * 24 * 60 * 60,
            stale_after_seconds=7 * 24 * 60 * 60,
        ):
            cached = _from_manifest(manifest)
            if cached is not None:
                return cached
            return self._fetch_uncached(
                unit,
                destination,
                threads=threads,
                safe_id=safe_id,
                unit_root=unit_root,
                manifest=manifest,
            )

    def _fetch_uncached(
        self,
        unit: ProcessingUnit,
        destination: Path,
        *,
        threads: int,
        safe_id: str,
        unit_root: Path,
        manifest: Path,
    ) -> FastqSet:
        self.runner.require("prefetch", "vdb-validate", "fasterq-dump")
        by_run: dict[str, dict[str, Path]] = {}
        shared_runs = destination / "_runs"
        for accession in unit.run_accessions:
            with exclusive_file_lock(
                shared_runs / f".{accession}.lock",
                timeout_seconds=7 * 24 * 60 * 60,
                stale_after_seconds=7 * 24 * 60 * 60,
            ):
                sra_file = self._download_run(accession, shared_runs / "sra")
                by_run[accession] = self._materialize_run(accession, sra_file, shared_runs, threads)

        merged = unit_root / "merged"
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
            checksums={str(path): sha256_file(path) for path in files},
            metadata={
                "per_run": {
                    run: {key: str(path) for key, path in value.items()}
                    for run, value in by_run.items()
                }
            },
        )
        result.validate()
        atomic_write_json(manifest, result.to_dict())
        return result


class GeoFastqProvider:
    """Download explicit FASTQ URLs discovered in GEO supplementary metadata."""

    PAIRED = re.compile(r"(?:^|[._-])R?([12])(?:[._-]|$)", re.IGNORECASE)

    def __init__(self, urls: dict[str, list[str]], *, downloader: AtomicDownloader) -> None:
        self.urls = urls
        self.downloader = downloader

    def fetch(self, unit: ProcessingUnit, destination: Path, *, threads: int) -> FastqSet:
        del threads
        safe_id = sanitize_identifier(unit.unit_id)
        configured_urls = self.urls.get(unit.unit_id, [])
        fingerprint = hashlib.sha256("\n".join(configured_urls).encode()).hexdigest()[:16]
        root = destination / safe_id / fingerprint
        manifest = root / "fastq.manifest.json"
        cached = _from_manifest(manifest)
        if cached is not None:
            return cached
        root.mkdir(parents=True, exist_ok=True)
        with exclusive_file_lock(
            root / ".fastq.lock",
            timeout_seconds=7 * 24 * 60 * 60,
            stale_after_seconds=7 * 24 * 60 * 60,
        ):
            cached = _from_manifest(manifest)
            if cached is not None:
                return cached
            return self._fetch_uncached(
                unit, destination, safe_id=safe_id, root=root, manifest=manifest
            )

    def _fetch_uncached(
        self,
        unit: ProcessingUnit,
        destination: Path,
        *,
        safe_id: str,
        root: Path,
        manifest: Path,
    ) -> FastqSet:
        urls = self.urls.get(unit.unit_id, [])
        if not urls:
            raise DownloadError(f"No GEO FASTQ URLs configured for {unit.unit_id}")
        paired: dict[str, dict[str, Path]] = {}
        single: list[Path] = []
        for url in urls:
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
            checksums={str(path): sha256_file(path) for path in (*read1, *read2, *single)},
        )
        result.validate()
        atomic_write_json(manifest, result.to_dict())
        return result
