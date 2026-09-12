"""Workspace storage measurement helpers."""

from __future__ import annotations

from pathlib import Path

from ..support.util import bytes_to_gb


def path_size_gb(path: Path) -> float:
    """Return recursive regular-file size below *path* in decimal GB."""

    if path.is_file():
        return bytes_to_gb(path.stat().st_size)
    if not path.exists():
        return 0.0
    return sum(bytes_to_gb(item.stat().st_size) for item in path.rglob("*") if item.is_file())


def paths_size_gb(paths: tuple[Path, ...] | list[Path]) -> float:
    """Return the non-overlapping recursive size of *paths* in decimal GB."""

    resolved: list[Path] = []
    for path in sorted({item.resolve() for item in paths}, key=lambda item: len(item.parts)):
        if not any(path == parent or path.is_relative_to(parent) for parent in resolved):
            resolved.append(path)
    return sum(path_size_gb(path) for path in resolved)
