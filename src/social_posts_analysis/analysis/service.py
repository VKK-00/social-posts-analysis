from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.paths import ProjectPaths
from social_posts_analysis.propagation import filter_origin_posts_frame
from social_posts_analysis.table_io import append_unique, frame_from_records, load_typed

from .cache import AnalysisCacheStore
from .cascades import CASCADE_METRICS_SCHEMA, compute_cascade_metrics
from .clustering import NarrativeClusterer
from .duplicates import NEAR_DUPLICATES_SCHEMA, near_duplicate_texts
from .language import LanguageDetector
from .metrics import compute_support_metrics
from .providers import build_providers


class AnalysisService:
    ANALYSIS_KEYS: dict[str, list[str]] = {
        "analysis_runs": ["run_id"],
        "detected_languages": ["run_id", "item_type", "item_id"],
        "cluster_memberships": ["run_id", "item_type", "item_id"],
        "narrative_clusters": ["run_id", "item_type", "cluster_id"],
        "stance_labels": ["run_id", "item_type", "item_id", "side_id"],
        "support_metrics": ["run_id", "scope_type", "scope_id", "side_id"],
        "cascade_metrics": ["run_id", "cascade_type", "scope_id"],
        "near_duplicates": ["run_id", "item_type_a", "item_id_a", "item_type_b", "item_id_b"],
    }
    ANALYSIS_SCHEMAS: dict[str, dict[str, Any]] = {
        "analysis_runs": {
            "run_id": pl.String,
            "embedding_provider": pl.String,
            "llm_provider": pl.String,
            "post_items": pl.Int64,
            "propagation_items": pl.Int64,
            "comment_items": pl.Int64,
        },
        "detected_languages": {
            "item_type": pl.String,
            "item_id": pl.String,
            "language": pl.String,
            "confidence": pl.Float64,
            "method": pl.String,
            "run_id": pl.String,
        },
        "cluster_memberships": {
            "item_type": pl.String,
            "item_id": pl.String,
            "cluster_id": pl.String,
            "run_id": pl.String,
        },
        "narrative_clusters": {
            "item_type": pl.String,
            "cluster_id": pl.String,
            "label": pl.String,
            "description": pl.String,
            "top_keywords": pl.List(pl.String),
            "exemplar_ids": pl.List(pl.String),
            "run_id": pl.String,
        },
        "stance_labels": {
            "item_type": pl.String,
            "item_id": pl.String,
            "side_id": pl.String,
            "label": pl.String,
            "confidence": pl.Float64,
            "model_name": pl.String,
            "run_id": pl.String,
        },
        "support_metrics": {
            "scope_type": pl.String,
            "scope_id": pl.String,
            "side_id": pl.String,
            "support_count": pl.Int64,
            "oppose_count": pl.Int64,
            "neutral_count": pl.Int64,
            "unclear_count": pl.Int64,
            "support_ratio": pl.Float64,
            "support_ratio_low": pl.Float64,
            "support_ratio_high": pl.Float64,
            "net_support": pl.Int64,
            "run_id": pl.String,
        },
        "cascade_metrics": CASCADE_METRICS_SCHEMA,
        "near_duplicates": NEAR_DUPLICATES_SCHEMA,
    }

    def __init__(self, config: ProjectConfig, paths: ProjectPaths) -> None:
        self.config = config
        self.paths = paths

    def run(self, run_id: str | None = None) -> dict[str, Any]:
        resolved_run_id = run_id or self.paths.latest_run_id()
        if not resolved_run_id:
            raise RuntimeError("No normalized run found to analyze.")

        posts = self._load_table("posts").filter(pl.col("run_id") == resolved_run_id)
        propagations = self._load_table("propagations").filter(pl.col("run_id") == resolved_run_id)
        comments = self._load_table("comments").filter(pl.col("run_id") == resolved_run_id)
        origin_posts = filter_origin_posts_frame(posts)

        detector = LanguageDetector(self.config.analysis.languages, method=self.config.analysis.language_method)
        providers = build_providers(self.config.providers.embeddings, self.config.providers.llm)
        cache_store = AnalysisCacheStore(self.config, self.paths)
        # Local providers (sentence-transformers) expose their true dimension
        # and model name so cache keys and empty-input shapes stay consistent.
        embedding_dimension = (
            getattr(providers.embeddings, "dimension", None) or self.config.providers.embeddings.dimension
        )
        provider_model_override = getattr(providers.embeddings, "model_name", None)
        clusterer = NarrativeClusterer(
            llm_provider=providers.llm,
            exemplar_count=self.config.analysis.exemplar_count,
            min_cluster_size=self.config.analysis.min_cluster_size,
            min_samples=self.config.analysis.min_samples,
        )

        post_items = self._items_from_frame(origin_posts, "post")
        propagation_items = self._items_from_frame(propagations, "propagation")
        comment_items = self._items_from_frame(comments, "comment")

        language_rows = self._detect_languages(post_items, detector, resolved_run_id)
        language_rows.extend(self._detect_languages(propagation_items, detector, resolved_run_id))
        language_rows.extend(self._detect_languages(comment_items, detector, resolved_run_id))

        post_embeddings = cache_store.embedding_matrix(
            post_items,
            provider_name=providers.embeddings.name,
            embed_many=providers.embeddings.embed_texts,
            batch_size=self.config.analysis.batch_size,
            dimension=embedding_dimension,
            model_override=provider_model_override,
        )
        propagation_embeddings = cache_store.embedding_matrix(
            propagation_items,
            provider_name=providers.embeddings.name,
            embed_many=providers.embeddings.embed_texts,
            batch_size=self.config.analysis.batch_size,
            dimension=embedding_dimension,
            model_override=provider_model_override,
        )
        comment_embeddings = cache_store.embedding_matrix(
            comment_items,
            provider_name=providers.embeddings.name,
            embed_many=providers.embeddings.embed_texts,
            batch_size=self.config.analysis.batch_size,
            dimension=embedding_dimension,
            model_override=provider_model_override,
        )

        post_clusters, post_memberships = clusterer.cluster_items("post", post_items, post_embeddings, resolved_run_id)
        propagation_clusters, propagation_memberships = clusterer.cluster_items(
            "propagation",
            propagation_items,
            propagation_embeddings,
            resolved_run_id,
        )
        comment_clusters, comment_memberships = clusterer.cluster_items(
            "comment",
            comment_items,
            comment_embeddings,
            resolved_run_id,
        )

        stance_rows = self._label_items_with_cache(
            cache_store=cache_store,
            llm_name=providers.llm.name,
            classify_one=providers.llm.classify_stance,
            item_type="post",
            items=post_items,
            run_id=resolved_run_id,
        )
        stance_rows.extend(
            self._label_items_with_cache(
                cache_store=cache_store,
                llm_name=providers.llm.name,
                classify_one=providers.llm.classify_stance,
                item_type="propagation",
                items=propagation_items,
                run_id=resolved_run_id,
            )
        )
        stance_rows.extend(
            self._label_items_with_cache(
                cache_store=cache_store,
                llm_name=providers.llm.name,
                classify_one=providers.llm.classify_stance,
                item_type="comment",
                items=comment_items,
                run_id=resolved_run_id,
            )
        )

        support_metrics = compute_support_metrics(
            pl.DataFrame(stance_rows) if stance_rows else pl.DataFrame(),
            pl.DataFrame(comment_memberships) if comment_memberships else pl.DataFrame(),
            comments,
            resolved_run_id,
        )

        comment_edges = self._load_table("comment_edges").filter(pl.col("run_id") == resolved_run_id)
        propagation_edges = self._load_table("propagation_edges").filter(pl.col("run_id") == resolved_run_id)
        cascade_metrics = compute_cascade_metrics(comment_edges, propagation_edges, resolved_run_id)

        near_duplicates = near_duplicate_texts(
            [*post_items, *propagation_items, *comment_items],
            threshold=self.config.analysis.near_duplicate_threshold,
            run_id=resolved_run_id,
        )

        analysis_run = [
            {
                "run_id": resolved_run_id,
                "embedding_provider": providers.summary["embeddings"],
                "llm_provider": providers.summary["llm"],
                "post_items": len(post_items),
                "propagation_items": len(propagation_items),
                "comment_items": len(comment_items),
            }
        ]

        outputs = {
            "analysis_runs": self._persist_table("analysis_runs", analysis_run),
            "detected_languages": self._persist_table("detected_languages", language_rows),
            "cluster_memberships": self._persist_table(
                "cluster_memberships",
                [*post_memberships, *propagation_memberships, *comment_memberships],
            ),
            "narrative_clusters": self._persist_table(
                "narrative_clusters",
                [*post_clusters, *propagation_clusters, *comment_clusters],
            ),
            "stance_labels": self._persist_table("stance_labels", stance_rows),
            "support_metrics": self._persist_table(
                "support_metrics",
                support_metrics.to_dicts() if not support_metrics.is_empty() else [],
            ),
            "cascade_metrics": self._persist_table(
                "cascade_metrics",
                cascade_metrics.to_dicts() if not cascade_metrics.is_empty() else [],
            ),
            "near_duplicates": self._persist_table(
                "near_duplicates",
                near_duplicates.to_dicts() if not near_duplicates.is_empty() else [],
            ),
        }
        self._sync_duckdb(outputs)
        return {"run_id": resolved_run_id, "providers": providers.summary}

    def _items_from_frame(self, frame: pl.DataFrame, item_type: str) -> list[dict[str, Any]]:
        if frame.is_empty():
            return []
        limit = self.config.analysis.max_items_per_item_type
        rows = frame.select(
            (
                pl.col("post_id")
                if item_type == "post"
                else pl.col("propagation_id")
                if item_type == "propagation"
                else pl.col("comment_id")
            ).alias("item_id"),
            pl.lit(item_type).alias("item_type"),
            pl.col("message").fill_null("").alias("text"),
            pl.col("parent_post_id").fill_null("").alias("parent_post_id")
            if item_type == "comment"
            else pl.lit("").alias("parent_post_id"),
        )
        rows = rows.filter(pl.col("text").str.len_chars() > 0)
        if limit:
            rows = rows.head(limit)
        return rows.to_dicts()

    @staticmethod
    def _detect_languages(
        items: list[dict[str, Any]],
        detector: LanguageDetector,
        run_id: str,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item in items:
            prediction = detector.detect(item["text"])
            rows.append(
                {
                    "item_type": item["item_type"],
                    "item_id": item["item_id"],
                    "language": prediction.language,
                    "confidence": prediction.confidence,
                    "method": prediction.method,
                    "run_id": run_id,
                }
            )
        return rows

    def _persist_table(self, table_name: str, records: list[dict[str, Any]]) -> Path:
        path = self.paths.processed_root / f"{table_name}.parquet"
        schema = self.ANALYSIS_SCHEMAS[table_name]
        new_df = frame_from_records(records, schema)
        return append_unique(path, new_df, schema=schema, key_columns=self.ANALYSIS_KEYS[table_name])

    def _label_items_with_cache(
        self,
        *,
        cache_store: AnalysisCacheStore,
        llm_name: str,
        classify_one: Any,
        item_type: str,
        items: list[dict[str, Any]],
        run_id: str,
    ) -> list[dict[str, Any]]:
        cached_rows = cache_store.stance_predictions(
            items,
            llm_name=llm_name,
            sides=self.config.sides,
            classify_one=classify_one,
        )
        return [{**row, "run_id": run_id, "item_type": item_type} for row in cached_rows]

    def _load_table(self, table_name: str) -> pl.DataFrame:
        path = self.paths.processed_root / f"{table_name}.parquet"
        from social_posts_analysis.normalization.schemas import TABLE_SCHEMAS

        schema = TABLE_SCHEMAS.get(table_name)
        # A typed empty frame keeps downstream .filter/.col calls working when
        # normalization has not run yet, instead of raising ColumnNotFoundError.
        if schema is not None:
            return load_typed(path, schema)
        return pl.read_parquet(path) if path.exists() else pl.DataFrame()

    def _sync_duckdb(self, table_paths: dict[str, Path]) -> None:
        connection = duckdb.connect(str(self.paths.database_path))
        try:
            for table_name, path in table_paths.items():
                path_str = path.as_posix().replace("'", "''")
                connection.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{path_str}')")
        finally:
            connection.close()
