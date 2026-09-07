from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, ClassVar, Literal

import polars as pl

from .errors import CatalogConflictError
from .models import ProcessingUnit

GroupLevel = Literal["run", "experiment", "sra_sample", "biosample"]


class RunCatalog:
    """A one-row-per-run catalog with auditable, unrestricted filtering."""

    RUN_COLUMN = "Run"
    GROUP_COLUMNS: ClassVar[dict[GroupLevel, str]] = {
        "run": "Run",
        "experiment": "Experiment",
        "sra_sample": "SRA Sample",
        "biosample": "BioSample",
    }

    def __init__(self, frame: pl.DataFrame, *, audit: Iterable[str] = ()) -> None:
        if self.RUN_COLUMN not in frame.columns:
            raise ValueError(f"Run catalog must contain a {self.RUN_COLUMN!r} column")
        self._frame = frame
        self._audit = tuple(audit)

    @classmethod
    def from_csv(cls, path: str | Path) -> RunCatalog:
        source = Path(path)
        frame = pl.read_csv(
            source,
            infer_schema_length=None,
            null_values=["", "NA", "N/A", "null", "None"],
            try_parse_dates=True,
        )
        return cls(frame, audit=(f"loaded CSV {source}",))

    @classmethod
    def from_records(cls, records: Iterable[dict[str, Any]]) -> RunCatalog:
        return cls(pl.DataFrame(list(records)), audit=("created from records",))

    @property
    def frame(self) -> pl.DataFrame:
        return self._frame

    @property
    def audit(self) -> tuple[str, ...]:
        return self._audit

    def filter(
        self,
        predicate: pl.Expr | Callable[[dict[str, Any]], bool],
        *,
        description: str | None = None,
    ) -> RunCatalog:
        """Filter with any Polars expression or an arbitrary Python predicate."""

        before = self._frame.height
        if isinstance(predicate, pl.Expr):
            filtered = self._frame.filter(predicate)
            label = description or repr(predicate)
        elif callable(predicate):
            rows = [row for row in self._frame.iter_rows(named=True) if predicate(row)]
            filtered = (
                pl.DataFrame(rows, schema=self._frame.schema) if rows else self._frame.head(0)
            )
            label = description or getattr(predicate, "__name__", "callable")
        else:
            raise TypeError("predicate must be a Polars expression or callable")
        event = f"filter {label}: {before} -> {filtered.height} rows"
        return RunCatalog(filtered, audit=(*self._audit, event))

    where = filter

    def select(self, *columns: str) -> RunCatalog:
        retained = columns if self.RUN_COLUMN in columns else (self.RUN_COLUMN, *columns)
        selected = self._frame.select(*retained)
        return RunCatalog(selected, audit=(*self._audit, f"selected columns {columns!r}"))

    def deduplicate_runs(self) -> RunCatalog:
        value_columns = [column for column in self._frame.columns if column != self.RUN_COLUMN]
        if not value_columns:
            deduplicated = self._frame.unique(
                subset=[self.RUN_COLUMN], keep="first", maintain_order=True
            )
            return RunCatalog(
                deduplicated,
                audit=(
                    *self._audit,
                    f"deduplicated exact runs: {self._frame.height} -> {deduplicated.height}",
                ),
            )
        grouped = self._frame.group_by(self.RUN_COLUMN, maintain_order=True).agg(
            *(pl.col(column).drop_nulls().n_unique().alias(column) for column in value_columns)
        )
        conflicting_rows = grouped.filter(
            pl.any_horizontal(*(pl.col(column) > 1 for column in value_columns))
        )
        conflicts: dict[str, list[str]] = {}
        for row in conflicting_rows.head(10).iter_rows(named=True):
            conflicts[str(row[self.RUN_COLUMN])] = [
                column for column in value_columns if row[column] > 1
            ]
        if conflicts:
            raise CatalogConflictError(
                "Duplicate run accessions contain contradictory values; refusing to discard data: "
                + json.dumps(conflicts, sort_keys=True)
            )
        # Coalesce nulls across otherwise compatible duplicates instead of
        # silently keeping a null from whichever row happened to appear first.
        deduplicated = (
            self._frame.group_by(self.RUN_COLUMN, maintain_order=True)
            .agg(*(pl.col(column).drop_nulls().first().alias(column) for column in value_columns))
            .select(self._frame.columns)
        )
        return RunCatalog(
            deduplicated,
            audit=(
                *self._audit,
                f"deduplicated exact runs: {self._frame.height} -> {deduplicated.height}",
            ),
        )

    def normalized_entities(self) -> dict[str, pl.DataFrame]:
        """Return run/experiment/sample/study views without conflating accessions."""

        views = {"runs": self._frame}
        for name, candidates in {
            "experiments": ("Experiment",),
            "sra_samples": ("SRA Sample", "Sample"),
            "biosamples": ("BioSample",),
            "studies": ("SRA Study", "BioProject"),
        }.items():
            columns = [column for column in candidates if column in self._frame.columns]
            if columns:
                views[name] = self._frame.unique(subset=columns, keep="first", maintain_order=True)
        return views

    @staticmethod
    def _first_non_null(rows: list[dict[str, Any]], names: tuple[str, ...]) -> Any:
        for name in names:
            for row in rows:
                value = row.get(name)
                if value not in (None, ""):
                    return value
        return None

    @staticmethod
    def _unique(rows: list[dict[str, Any]], column: str) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(str(row[column]) for row in rows if row.get(column) not in (None, ""))
        )

    def processing_units(self, *, by: GroupLevel = "experiment") -> list[ProcessingUnit]:
        group_column = self.GROUP_COLUMNS[by]
        if group_column not in self._frame.columns:
            raise ValueError(f"Cannot group by {by!r}: column {group_column!r} is missing")
        ordered: dict[str, list[dict[str, Any]]] = {}
        for row in self._frame.iter_rows(named=True):
            key = row.get(group_column)
            if key in (None, ""):
                key = row[self.RUN_COLUMN]
            ordered.setdefault(str(key), []).append(row)

        units: list[ProcessingUnit] = []
        for key, rows in ordered.items():
            taxids = {
                int(value)
                for name in ("TaxID", "species_taxid", "taxid")
                for row in rows
                if (value := row.get(name)) not in (None, "")
            }
            species_names = {
                str(value)
                for name in ("ScientificName", "scientific_name")
                for row in rows
                if (value := row.get(name)) not in (None, "")
            }
            if len(taxids) > 1 or len(species_names) > 1:
                raise CatalogConflictError(
                    f"Processing unit {key!r} spans multiple species: taxids={sorted(taxids)!r}, "
                    f"names={sorted(species_names)!r}"
                )
            taxid = next(iter(taxids), None)
            total_bases = sum(int(float(row.get("bases") or 0)) for row in rows)
            total_bytes = sum(int(float(row.get("size_MB") or 0) * 1_000_000) for row in rows)
            metadata = {
                "library_strategy": self._unique(rows, "LibraryStrategy"),
                "library_layout": self._unique(rows, "LibraryLayout"),
                "platform": self._unique(rows, "Platform"),
            }
            units.append(
                ProcessingUnit(
                    unit_id=key,
                    run_accessions=self._unique(rows, "Run"),
                    experiment_accessions=self._unique(rows, "Experiment"),
                    sra_sample_accessions=self._unique(rows, "SRA Sample"),
                    biosample_accessions=self._unique(rows, "BioSample"),
                    scientific_name=self._first_non_null(
                        rows, ("ScientificName", "scientific_name")
                    ),
                    taxid=taxid,
                    total_bases=total_bases,
                    total_bytes=total_bytes,
                    metadata=metadata,
                )
            )
        return units

    @staticmethod
    def batch_units(
        units: Iterable[ProcessingUnit],
        *,
        max_bytes: int | None = None,
        max_units: int | None = None,
    ) -> list[list[ProcessingUnit]]:
        """Deterministic first-fit-decreasing batching; oversized units stand alone."""

        if max_bytes is not None and max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        if max_units is not None and max_units <= 0:
            raise ValueError("max_units must be positive")
        materialized = sorted(units, key=lambda item: (-item.total_bytes, item.unit_id))
        batches: list[list[ProcessingUnit]] = []
        sizes: list[int] = []
        for unit in materialized:
            placed = False
            for index, batch in enumerate(batches):
                size_ok = max_bytes is None or sizes[index] + unit.total_bytes <= max_bytes
                count_ok = max_units is None or len(batch) < max_units
                if size_ok and count_ok:
                    batch.append(unit)
                    sizes[index] += unit.total_bytes
                    placed = True
                    break
            if not placed:
                batches.append([unit])
                sizes.append(unit.total_bytes)
        return batches

    def with_columns(self, *expressions: pl.Expr) -> RunCatalog:
        frame = self._frame.with_columns(*expressions)
        return RunCatalog(frame, audit=(*self._audit, "added/updated columns"))

    def replace_frame(self, frame: pl.DataFrame, *, event: str) -> RunCatalog:
        return RunCatalog(frame, audit=(*self._audit, event))
