from __future__ import annotations

from typing import Any

import polars as pl

from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.normalization.merge import (
    load_manifests,
    merge_manifests,
    resolve_source_run_ids,
    validate_source_run_ids,
)
from social_posts_analysis.normalization.persistence import persist_table, sync_duckdb
from social_posts_analysis.normalization.records import build_table_records
from social_posts_analysis.normalization.schemas import TABLE_KEYS, TABLE_SCHEMAS
from social_posts_analysis.paths import ProjectPaths


class NormalizationService:
    TABLE_KEYS = TABLE_KEYS
    TABLE_SCHEMAS = TABLE_SCHEMAS

    def __init__(self, config: ProjectConfig, paths: ProjectPaths) -> None:
        self.config = config
        self.paths = paths

    def run(self, run_id: str | None = None, source_run_ids: list[str] | None = None) -> dict[str, Any]:
        resolved_run_id = run_id or self.paths.latest_run_id()
        if not resolved_run_id:
            raise RuntimeError("No collection runs found to normalize.")

        resolved_source_run_ids = (
            validate_source_run_ids(self.paths, [item for item in source_run_ids if item])
            if source_run_ids is not None
            else resolve_source_run_ids(self.config, self.paths, resolved_run_id)
        )
        if self._has_matching_normalized_run(resolved_run_id, resolved_source_run_ids):
            outputs = {
                table_name: self.paths.processed_root / f"{table_name}.parquet"
                for table_name in self.TABLE_SCHEMAS
                if (self.paths.processed_root / f"{table_name}.parquet").exists()
            }
            if outputs:
                sync_duckdb(self.paths.database_path, outputs)
            return {
                "run_id": resolved_run_id,
                "source_run_ids": resolved_source_run_ids,
                "reused_existing_run": True,
                "tables": {name: str(path) for name, path in outputs.items()},
            }

        manifests = load_manifests(self.paths, resolved_source_run_ids)
        manifest = merge_manifests(resolved_run_id, manifests)
        table_records = build_table_records(manifest, resolved_source_run_ids)

        outputs = {
            table_name: persist_table(self.paths, table_name, records)
            for table_name, records in table_records.items()
        }
        sync_duckdb(self.paths.database_path, outputs)
        return {
            "run_id": manifest.run_id,
            "source_run_ids": resolved_source_run_ids,
            "reused_existing_run": False,
            "tables": {name: str(path) for name, path in outputs.items()},
        }

    def _has_matching_normalized_run(self, run_id: str, source_run_ids: list[str]) -> bool:
        collection_runs_path = self.paths.processed_root / "collection_runs.parquet"
        if not collection_runs_path.exists():
            return False
        collection_runs = pl.read_parquet(collection_runs_path).filter(pl.col("run_id") == run_id)
        if collection_runs.is_empty() or "source_run_ids" not in collection_runs.columns:
            return False
        raw_source_run_ids = collection_runs["source_run_ids"][0]
        existing_source_run_ids = raw_source_run_ids.to_list() if isinstance(raw_source_run_ids, pl.Series) else list(raw_source_run_ids or [])
        return existing_source_run_ids == source_run_ids
