from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import duckdb
import polars as pl

from social_posts_analysis.paths import ProjectPaths

from .schemas import TABLE_KEYS, TABLE_SCHEMAS


def persist_table(paths: ProjectPaths, table_name: str, records: list[dict[str, Any]]) -> Path:
    path = paths.processed_root / f"{table_name}.parquet"
    schema = cast(dict[str, Any], TABLE_SCHEMAS[table_name])
    new_df = pl.DataFrame(records, schema=schema) if records else pl.DataFrame(schema=schema)
    if path.exists():
        existing_df = pl.read_parquet(path)
        if new_df.is_empty():
            combined = existing_df
        elif existing_df.is_empty():
            combined = new_df
        else:
            combined = pl.concat([existing_df, new_df], how="diagonal_relaxed")
    else:
        combined = new_df

    key_columns = [column for column in TABLE_KEYS[table_name] if column in combined.columns]
    if key_columns and not combined.is_empty():
        combined = combined.unique(subset=key_columns, keep="last")
    combined.write_parquet(path)
    return path


def sync_duckdb(database_path: Path, table_paths: dict[str, Path]) -> None:
    connection = duckdb.connect(str(database_path))
    try:
        for table_name, path in table_paths.items():
            path_str = path.as_posix().replace("'", "''")
            connection.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{path_str}')")
    finally:
        connection.close()
