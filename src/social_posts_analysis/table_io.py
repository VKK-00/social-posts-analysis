"""Shared parquet table IO used across normalization, analysis, and reporting.

The same "append records, deduplicate by keys, write parquet" logic used to
live in three near-identical copies (normalization/persistence.py,
analysis/service.py, analysis/cache.py); this module is the single source.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl


def frame_from_records(records: list[dict[str, Any]], schema: dict[str, Any]) -> pl.DataFrame:
    return pl.DataFrame(records, schema=schema) if records else pl.DataFrame(schema=schema)


def load_typed(path: Path, schema: dict[str, Any]) -> pl.DataFrame:
    """Read a parquet table, or return a typed empty frame when it is absent."""
    if path.exists():
        return pl.read_parquet(path)
    return pl.DataFrame(schema=schema)


def append_unique(
    path: Path,
    new_frame: pl.DataFrame,
    *,
    schema: dict[str, Any],
    key_columns: list[str],
) -> Path:
    """Append ``new_frame`` to the parquet at ``path``, deduplicating by keys.

    Existing rows keep their place; on key collisions the newly appended rows
    win (``keep="last"``), matching the historical behaviour of every caller.
    """
    if path.exists():
        existing_frame = pl.read_parquet(path)
        if new_frame.is_empty():
            combined = existing_frame
        elif existing_frame.is_empty():
            combined = new_frame
        else:
            combined = pl.concat([existing_frame, new_frame], how="diagonal_relaxed")
    else:
        combined = new_frame

    effective_keys = [column for column in key_columns if column in combined.columns]
    if effective_keys and not combined.is_empty():
        combined = combined.unique(subset=effective_keys, keep="last")
    combined.write_parquet(path)
    return path
