from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, ClassVar, Literal

import polars as pl

from ..errors import CatalogConflictError, DependencyError
from ..models import ProcessingUnit

LOGGER = logging.getLogger("ncbi_dataset_builder.catalog")

GroupLevel = Literal["run", "experiment", "sra_sample", "biosample"]


def validate_polars_runtime() -> str:
    """Return the Polars version after verifying that its modules are compatible."""

    try:
        import polars._reexport as polars_reexport

        expression = pl.col("__ncbi_dataset_builder_healthcheck__")
        if not hasattr(polars_reexport, "Expr") or not isinstance(expression, pl.Expr):
            raise AttributeError("Polars expression classes are not consistently exported")
    except (AttributeError, ImportError) as exc:
        raise DependencyError(
            "The Polars installation is inconsistent. This is usually caused by upgrading "
            "Polars in a running notebook kernel or by installing both 'polars' and "
            "'polars-lts-cpu', which write to the same Python package directory. Restart the "
            "kernel first. If the error remains, uninstall both distributions, install exactly "
            "one of them, and restart the kernel again."
        ) from exc
    return pl.__version__


class RunCatalog:
    """Store one row per SRA run together with an immutable operation audit."""

    RUN_COLUMN = "Run"
    GROUP_COLUMNS: ClassVar[dict[GroupLevel, str]] = {
        "run": "Run",
        "experiment": "Experiment",
        "sra_sample": "SRA Sample",
        "biosample": "BioSample",
    }

    def __init__(self, frame: pl.DataFrame, *, audit: Iterable[str] = ()) -> None:
        """Initialize from *frame* and optional prior *audit* messages."""

        if self.RUN_COLUMN not in frame.columns:
            raise ValueError(f"Run catalog must contain a {self.RUN_COLUMN!r} column")
        self._frame = frame
        self._audit = tuple(audit)

    @classmethod
    def from_csv(cls, path: str | Path) -> RunCatalog:
        """Load a RunInfo CSV from *path*, parsing dates and common null markers."""

        validate_polars_runtime()
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
        """Build a catalog from iterable row dictionaries in *records*."""

        validate_polars_runtime()
        return cls(pl.DataFrame(list(records)), audit=("created from records",))

    @property
    def frame(self) -> pl.DataFrame:
        """Return the underlying Polars data frame."""

        return self._frame

    @property
    def audit(self) -> tuple[str, ...]:
        """Return the ordered, immutable history of catalog operations."""

        return self._audit

    def filter(
        self,
        predicate: pl.Expr | Callable[[dict[str, Any]], bool],
        *,
        description: str | None = None,
    ) -> RunCatalog:
        """Filter with *predicate* and record *description* in the returned catalog."""

        validate_polars_runtime()
        before = self._frame.height
        LOGGER.info("Filter run catalog with %d rows", before)
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
        LOGGER.info(event)
        return RunCatalog(filtered, audit=(*self._audit, event))

    where = filter

    def transform(
        self,
        function: Callable[[pl.DataFrame], pl.DataFrame],
        *,
        description: str | None = None,
    ) -> RunCatalog:
        """Return a catalog produced by applying *function* to the data frame.

        Args:
            function: Callable receiving :attr:`frame` and returning a Polars
                :class:`~polars.DataFrame`. The returned frame must retain the
                ``Run`` column required by :class:`RunCatalog`.
            description: Optional human-readable audit label. When omitted,
                the callable's qualified name is used.

        Raises:
            TypeError: If *function* is not callable or returns another type.
            ValueError: If the transformed frame does not contain ``Run``.
        """

        if not callable(function):
            raise TypeError("function must be callable")
        validate_polars_runtime()
        transformed = function(self._frame)
        if not isinstance(transformed, pl.DataFrame):
            raise TypeError(
                "catalog transform must return a Polars DataFrame, got "
                f"{type(transformed).__name__}"
            )
        label = description or getattr(
            function,
            "__qualname__",
            getattr(function, "__name__", function.__class__.__qualname__),
        )
        event = f"transform {label}: {self._frame.height} -> {transformed.height} rows"
        return RunCatalog(transformed, audit=(*self._audit, event))

    def select(self, *columns: str) -> RunCatalog:
        """Return only *columns*, always retaining the run-accession column."""

        retained = columns if self.RUN_COLUMN in columns else (self.RUN_COLUMN, *columns)
        selected = self._frame.select(*retained)
        return RunCatalog(selected, audit=(*self._audit, f"selected columns {columns!r}"))

    def deduplicate_runs(self) -> RunCatalog:
        """Coalesce compatible duplicate runs and reject contradictory values."""

        LOGGER.info("Deduplicate %d catalog rows by Run", self._frame.height)
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
        """Return entity views deduplicated by their own accession columns."""

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
        """Return the first non-empty value from *names* while scanning *rows*."""

        for name in names:
            for row in rows:
                value = row.get(name)
                if value not in (None, ""):
                    return value
        return None

    @staticmethod
    def _unique(rows: list[dict[str, Any]], column: str) -> tuple[str, ...]:
        """Return distinct non-empty *column* values from *rows* in input order."""

        return tuple(
            dict.fromkeys(str(row[column]) for row in rows if row.get(column) not in (None, ""))
        )

    def processing_units(self, *, by: GroupLevel = "experiment") -> list[ProcessingUnit]:
        """Group runs at level *by* and return validated processing units."""

        LOGGER.info("Create processing units grouped by %s", by)
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
            total_size_gb = sum(float(row.get("size_MB") or 0) / 1_000 for row in rows)
            metadata = {
                "library_strategy": self._unique(rows, "LibraryStrategy"),
                "library_layout": self._unique(rows, "LibraryLayout"),
                "platform": self._unique(rows, "Platform"),
                "run_size_gb": {
                    str(row["Run"]): float(row.get("size_MB") or 0) / 1_000
                    for row in rows
                    if row.get("Run") not in (None, "")
                },
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
                    total_size_gb=total_size_gb,
                    metadata=metadata,
                )
            )
        LOGGER.info("Created %d processing units grouped by %s", len(units), by)
        return units

    def with_columns(self, *expressions: pl.Expr) -> RunCatalog:
        """Apply Polars *expressions* and return a catalog with an audit entry."""

        frame = self._frame.with_columns(*expressions)
        return RunCatalog(frame, audit=(*self._audit, "added/updated columns"))

    def replace_frame(self, frame: pl.DataFrame, *, event: str) -> RunCatalog:
        """Return a catalog containing *frame* and append *event* to its audit."""

        return RunCatalog(frame, audit=(*self._audit, event))
