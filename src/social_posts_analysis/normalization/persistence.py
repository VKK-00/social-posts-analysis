from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from social_posts_analysis.paths import ProjectPaths
from social_posts_analysis.table_io import append_unique, frame_from_records

from .schemas import TABLE_KEYS, TABLE_SCHEMAS


def persist_table(paths: ProjectPaths, table_name: str, records: list[dict[str, Any]]) -> Path:
    path = paths.processed_root / f"{table_name}.parquet"
    schema = TABLE_SCHEMAS[table_name]
    new_df = frame_from_records(records, schema)
    return append_unique(path, new_df, schema=schema, key_columns=TABLE_KEYS[table_name])


def sync_duckdb(database_path: Path, table_paths: dict[str, Path]) -> None:
    connection = duckdb.connect(str(database_path))
    try:
        for table_name, path in table_paths.items():
            path_str = path.as_posix().replace("'", "''")
            connection.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{path_str}')")
    finally:
        connection.close()
