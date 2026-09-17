"""Visible runtime layout and durable execution records."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, ClassVar

from ..execution.records import ExecutionRecord
from ..support.util import atomic_write_json, exclusive_file_lock, read_json, utc_timestamp

WORKSPACE_SCHEMA_VERSION = 2


@dataclass(frozen=True)
class WorkspaceConfig:
    """Describe stable execution semantics for one workspace.

    Args:
        schema_version: Runtime-layout schema version.
        created_at: UTC timestamp of workspace initialization.
        group_by: Catalog entity represented by one processing unit.
        output_dir: Absolute processor-owned output root.
        genome_policy: Serialized genome-selection policy.
        directories: Human-readable runtime-directory roles.
    """

    schema_version: int
    created_at: str
    group_by: str
    output_dir: str
    genome_policy: dict[str, Any] = field(default_factory=dict)
    directories: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize this configuration to JSON-compatible values."""

        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> WorkspaceConfig:
        """Restore a workspace configuration from *value*."""

        return cls(
            schema_version=int(value["schema_version"]),
            created_at=str(value["created_at"]),
            group_by=str(value["group_by"]),
            output_dir=str(value.get("output_dir", "")),
            genome_policy=dict(value.get("genome_policy", {})),
            directories=dict(value.get("directories", {})),
        )


class WorkspaceStore:
    """Maintain package-owned runtime data and a public workspace manifest."""

    RUNTIME_DIRECTORIES: ClassVar[dict[str, str]] = {
        "catalogs": "cached NCBI run catalogs",
        "metadata": "normalized NCBI metadata",
        "metadata_cache": "reusable raw NCBI responses",
        "fastq": "downloaded and materialized FASTQ inputs",
        "genomes": "downloaded genome references and indexes",
        "state": "per-unit state, locks, and retry history",
        "executions": "immutable execution plans",
        "slurm": "generated scheduler scripts",
        "logs": "processing-unit and scheduler logs",
    }

    def __init__(self, root: Path, *, output_dir: Path | None = None) -> None:
        """Initialize paths without eagerly creating runtime subdirectories.

        Existing schema-1 workspaces keep their legacy paths so they remain
        resumable. New workspaces use the visible ``runtime`` directory.
        """

        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        legacy_config = self.root / "workspace.json"
        modern_config = self.root / "runtime" / "workspace.json"
        self.legacy_layout = legacy_config.is_file() and not modern_config.is_file()
        self.runtime = self.root if self.legacy_layout else self.root / "runtime"
        self.config_path = self.runtime / "workspace.json"
        default_output = self.root / ("outputs" if self.legacy_layout else "output")
        if output_dir is None:
            configured_output = None
            if self.config_path.is_file():
                configured_output = read_json(self.config_path).get("output_dir")
            self.output = Path(configured_output) if configured_output else default_output
        else:
            requested_output = Path(output_dir)
            self.output = (
                requested_output
                if requested_output.is_absolute()
                else self.root / requested_output
            )
        self.manifest_path = self.root / "manifest.json"
        self.executions = self.runtime / "executions"

    def path(self, name: str) -> Path:
        """Return the runtime path for a documented directory *name*."""

        if name not in self.RUNTIME_DIRECTORIES:
            raise KeyError(f"Unknown runtime directory: {name!r}")
        if self.legacy_layout and name == "genomes":
            return self.root / "work" / "genome_cache"
        return self.runtime / name

    def configure(self, *, group_by: str, genome_policy: dict[str, Any]) -> WorkspaceConfig:
        """Create or validate stable *group_by*, output, and *genome_policy* semantics."""

        schema_version = 1 if self.legacy_layout else WORKSPACE_SCHEMA_VERSION
        requested = WorkspaceConfig(
            schema_version=schema_version,
            created_at=utc_timestamp(),
            group_by=group_by,
            output_dir=str(self.output.resolve()),
            genome_policy=dict(genome_policy),
            directories=dict(self.RUNTIME_DIRECTORIES),
        )
        lock = self.path("state") / "workspace-config.lock"
        with exclusive_file_lock(lock):
            if not self.config_path.is_file():
                atomic_write_json(self.config_path, requested.to_dict())
                return requested
            current = WorkspaceConfig.from_dict(read_json(self.config_path))
            if current.schema_version != schema_version:
                raise ValueError(
                    f"Unsupported workspace schema {current.schema_version}; "
                    f"expected {schema_version}"
                )
            changed = {
                name: (getattr(current, name), getattr(requested, name))
                for name in ("group_by", "genome_policy")
                if getattr(current, name) != getattr(requested, name)
            }
            if current.output_dir and Path(current.output_dir) != self.output.resolve():
                changed["output_dir"] = (current.output_dir, str(self.output.resolve()))
            has_state = any((self.path("state") / "units").glob("*.json"))
            if changed and has_state:
                details = ", ".join(
                    f"{name}: {old!r} -> {new!r}" for name, (old, new) in changed.items()
                )
                raise ValueError(
                    "Workspace semantic configuration cannot change after processing: " + details
                )
            if changed or current.directories != requested.directories or not current.output_dir:
                updated = WorkspaceConfig(
                    schema_version=current.schema_version,
                    created_at=current.created_at,
                    group_by=requested.group_by,
                    output_dir=requested.output_dir,
                    genome_policy=requested.genome_policy,
                    directories=requested.directories,
                )
                atomic_write_json(self.config_path, updated.to_dict())
                return updated
            return current

    def save_execution(self, execution: ExecutionRecord) -> Path:
        """Persist automatic *execution* and return its JSON path."""

        path = self.executions / f"{execution.execution_id}.json"
        atomic_write_json(path, execution.to_dict())
        return path

    def load_execution(self, path_or_id: str | Path) -> ExecutionRecord:
        """Load an execution by identifier or JSON *path_or_id*."""

        candidate = Path(path_or_id)
        path = candidate if candidate.is_file() else self.executions / f"{path_or_id}.json"
        if not path.is_file():
            raise FileNotFoundError(f"Execution record does not exist: {path}")
        return ExecutionRecord.from_dict(read_json(path))

    def latest_execution(self) -> ExecutionRecord:
        """Load the most recently written execution record."""

        records = sorted(self.executions.glob("*.json"), key=lambda path: path.stat().st_mtime)
        if not records:
            raise FileNotFoundError("Workspace has no execution records")
        return ExecutionRecord.from_dict(read_json(records[-1]))

    def all_executions(self) -> tuple[ExecutionRecord, ...]:
        """Return every execution record ordered by creation time and identifier."""

        records = [
            ExecutionRecord.from_dict(read_json(path))
            for path in self.executions.glob("*.json")
        ]
        return tuple(sorted(records, key=lambda item: (item.created_at, item.execution_id)))

    def _output_relative(self, path: Path) -> str:
        """Return *path* relative to the configured output root."""

        try:
            return path.resolve().relative_to(self.output.resolve()).as_posix()
        except ValueError as exc:
            if self.legacy_layout:
                return str(path.resolve())
            raise ValueError(f"Processor artifact is outside output directory: {path}") from exc

    def sync_manifest(
        self,
        execution: ExecutionRecord,
        states: dict[str, dict[str, Any] | None],
    ) -> Path:
        """Write a public manifest for *execution* derived from internal *states*."""

        units: dict[str, Any] = {}
        if self.manifest_path.is_file():
            try:
                previous = read_json(self.manifest_path)
                if (
                    previous.get("schema_version") == 3
                    and isinstance(previous.get("units"), dict)
                ):
                    units.update(previous["units"])
            except (OSError, ValueError, TypeError):
                units = {}
        for item in execution.items:
            state = states.get(item.item_id) or {}
            if state.get("fingerprint") != item.fingerprint:
                state = {}
            result = state.get("result") if isinstance(state.get("result"), dict) else {}
            facts = result.get("output_files") if isinstance(result, dict) else {}
            artifacts: dict[str, dict[str, Any]] = {}
            if isinstance(facts, dict):
                for role, fact in facts.items():
                    if not isinstance(fact, dict) or not fact.get("path"):
                        continue
                    artifacts[str(role)] = {
                        **{key: value for key, value in fact.items() if key != "path"},
                        "path": self._output_relative(Path(str(fact["path"]))),
                    }
            units[item.item_id] = {
                **item.unit.to_dict(),
                "fingerprint": item.fingerprint,
                "execution_id": execution.execution_id,
                "processor_identity": execution.processor_identity,
                "status": state.get("status", "pending"),
                "phase": state.get("phase", "waiting"),
                "attempts": int(state.get("attempts", 0)),
                "output_dir": item.item_id,
                "artifacts": artifacts,
                "genome": result.get("genome") if isinstance(result, dict) else None,
                "error": state.get("error"),
                "log_path": state.get("log_path"),
            }
        try:
            output_root = self.output.resolve().relative_to(self.root.resolve()).as_posix()
        except ValueError:
            output_root = str(self.output.resolve())
        atomic_write_json(
            self.manifest_path,
            {
                "schema_version": 3,
                "updated_at": utc_timestamp(),
                "latest_execution_id": execution.execution_id,
                "group_by": execution.group_by,
                "processor_identity": execution.processor_identity,
                "output_root": output_root,
                "units": units,
            },
        )
        return self.manifest_path
