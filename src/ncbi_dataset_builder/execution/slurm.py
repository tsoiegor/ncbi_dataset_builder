"""Slurm script generation and submission."""

from __future__ import annotations

import math
import shlex
import sys
from pathlib import Path

from ..support.commands import CommandRunner
from ..support.progress import ProgressReporter, get_progress
from ..support.util import atomic_write_text
from .config import SlurmDistributedExecution, SlurmSingleNodeExecution


class SlurmExecutor:
    """Generate and submit scripts for the two supported Slurm systems."""

    def __init__(
        self,
        *,
        runner: CommandRunner | None = None,
        python_executable: str | None = None,
        progress: ProgressReporter | None = None,
    ) -> None:
        """Initialize with optional command *runner*, Python, and *progress*."""

        self.runner = runner or CommandRunner()
        self.python_executable = python_executable or sys.executable
        self.progress = get_progress(progress)

    @staticmethod
    def _scheduler_flags(
        *,
        partition: str | None,
        account: str | None,
        qos: str | None,
    ) -> list[str]:
        """Return optional Slurm directives for scheduler fields."""

        return [
            f"#SBATCH --{name}={value}"
            for name, value in (("partition", partition), ("account", account), ("qos", qos))
            if value
        ]

    @staticmethod
    def _command(arguments: list[str]) -> str:
        """Render POSIX shell *arguments* without interpolating user values."""

        return " ".join(shlex.quote(value) for value in arguments)

    def create_single_node_script(
        self,
        *,
        record_path: Path,
        processor_reference: str,
        builder_config,
        output_path: Path,
        execution: SlurmSingleNodeExecution,
        retry_failed: bool = False,
    ) -> Path:
        """Write a one-node coordinator script.

        Args:
            record_path: Automatic execution-record JSON path.
            processor_reference: Importable ``module:object`` processor.
            builder_config: Builder configuration supplying workspace and email.
            output_path: Destination ``.sbatch`` path.
            execution: Single-node allocation and sample limits.
            retry_failed: Retry failed sample state.
        """

        logs = builder_config.workspace / "logs" / "slurm"
        command = [
            self.python_executable,
            "-m",
            "ncbi_dataset_builder.execution.single_node_worker",
            "--execution",
            str(record_path.resolve()),
            "--processor",
            processor_reference,
            "--workspace",
            str(builder_config.workspace.resolve()),
        ]
        if builder_config.email:
            command.extend(("--email", builder_config.email))
        if retry_failed:
            command.append("--retry-failed")
        lines = [
            "#!/usr/bin/env bash",
            "#SBATCH --job-name=ncbi-dataset",
            f"#SBATCH --cpus-per-task={execution.allocation_cpus}",
            f"#SBATCH --mem={math.ceil(execution.allocation_memory_gb)}G",
            f"#SBATCH --time={execution.allocation_time_limit}",
            f"#SBATCH --output={shlex.quote(str(logs / '%j.coordinator.log'))}",
            f"#SBATCH --error={shlex.quote(str(logs / '%j.coordinator.log'))}",
            "#SBATCH --open-mode=append",
            *self._scheduler_flags(
                partition=execution.partition,
                account=execution.account,
                qos=execution.qos,
            ),
            "",
            "set -euo pipefail",
            f"mkdir -p {shlex.quote(str(logs))}",
            self._command(command),
            "",
        ]
        atomic_write_text(output_path, "\n".join(lines))
        return output_path

    def create_distributed_script(
        self,
        *,
        record_path: Path,
        processor_reference: str,
        builder_config,
        output_path: Path,
        execution: SlurmDistributedExecution,
        retry_failed: bool = False,
    ) -> Path:
        """Write a distributed coordinator script.

        Args:
            record_path: Automatic execution-record JSON path.
            processor_reference: Importable ``module:object`` processor.
            builder_config: Builder configuration supplying workspace and email.
            output_path: Destination ``.sbatch`` path.
            execution: Distributed quota, coordinator, and worker limits.
            retry_failed: Retry failed sample state.
        """

        logs = builder_config.workspace / "logs" / "slurm"
        command = [
            self.python_executable,
            "-m",
            "ncbi_dataset_builder.execution.distributed_worker",
            "--execution",
            str(record_path.resolve()),
            "--processor",
            processor_reference,
            "--workspace",
            str(builder_config.workspace.resolve()),
        ]
        if builder_config.email:
            command.extend(("--email", builder_config.email))
        if retry_failed:
            command.append("--retry-failed")
        lines = [
            "#!/usr/bin/env bash",
            "#SBATCH --job-name=ncbi-coordinator",
            f"#SBATCH --cpus-per-task={execution.coordinator_cpus}",
            f"#SBATCH --mem={math.ceil(execution.coordinator_memory_gb)}G",
            f"#SBATCH --time={execution.coordinator_time_limit}",
            f"#SBATCH --output={shlex.quote(str(logs / '%j.coordinator.log'))}",
            f"#SBATCH --error={shlex.quote(str(logs / '%j.coordinator.log'))}",
            "#SBATCH --open-mode=append",
            *self._scheduler_flags(
                partition=execution.partition,
                account=execution.account,
                qos=execution.qos,
            ),
            "",
            "set -euo pipefail",
            f"mkdir -p {shlex.quote(str(logs))}",
            self._command(command),
            "",
        ]
        atomic_write_text(output_path, "\n".join(lines))
        return output_path

    def create_sample_script(
        self,
        *,
        record_path: Path,
        item_index: int,
        processor_reference: str,
        workspace: Path,
        email: str | None,
        output_path: Path,
        execution: SlurmDistributedExecution,
        cpus: int,
        retry_failed: bool,
    ) -> Path:
        """Write one distributed Slurm sample script.

        Args:
            record_path: Automatic execution-record JSON path.
            item_index: Zero-based queue-item index.
            processor_reference: Importable ``module:object`` processor.
            workspace: Durable builder workspace.
            email: Optional NCBI contact email passed to the worker.
            output_path: Destination ``.sbatch`` path.
            execution: Distributed scheduler and resource settings.
            cpus: CPU request for this sample.
            retry_failed: Retry matching failed sample state.
        """

        command = [
            self.python_executable,
            "-m",
            "ncbi_dataset_builder.execution.sample_worker",
            "--execution",
            str(record_path.resolve()),
            "--item-index",
            str(item_index),
            "--processor",
            processor_reference,
            "--workspace",
            str(workspace.resolve()),
            "--cpus",
            str(cpus),
        ]
        if email:
            command.extend(("--email", email))
        if retry_failed:
            command.append("--retry-failed")
        log = workspace / "logs" / "slurm" / f"sample-{item_index}.%j.log"
        lines = [
            "#!/usr/bin/env bash",
            f"#SBATCH --job-name=ncbi-{item_index}",
            f"#SBATCH --cpus-per-task={cpus}",
            f"#SBATCH --mem={math.ceil(execution.memory_gb_per_job)}G",
            f"#SBATCH --time={execution.worker_time_limit}",
            f"#SBATCH --output={shlex.quote(str(log))}",
            f"#SBATCH --error={shlex.quote(str(log))}",
            "#SBATCH --open-mode=append",
            *self._scheduler_flags(
                partition=execution.partition,
                account=execution.account,
                qos=execution.qos,
            ),
            "",
            "set -euo pipefail",
            self._command(command),
            "",
        ]
        atomic_write_text(output_path, "\n".join(lines))
        return output_path

    def submit(self, script: Path, *, hold: bool = False) -> str:
        """Submit *script*, optionally in *hold* state, and return its Slurm job ID."""

        self.runner.require("sbatch")
        command = ["sbatch", "--parsable"]
        if hold:
            command.append("--hold")
        command.append(str(script))
        completed = self.runner.run(command)
        job_id = (completed.stdout or "").strip().split(";", 1)[0]
        if not job_id:
            raise RuntimeError(f"sbatch returned no job ID: {completed.stdout!r}")
        return job_id

    def release(self, job_id: str) -> None:
        """Release a held Slurm *job_id*."""

        self.runner.require("scontrol")
        self.runner.run(["scontrol", "release", job_id])

    def cancel(self, job_id: str) -> None:
        """Cancel Slurm *job_id*."""

        self.runner.require("scancel")
        self.runner.run(["scancel", job_id])
