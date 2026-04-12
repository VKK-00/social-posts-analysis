from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.paths import ProjectPaths
from social_posts_analysis.utils import utc_now_iso

EMBEDDING_CACHE_SCHEMA = {
    "provider_key": pl.String,
    "text_hash": pl.String,
    "text": pl.String,
    "embedding": pl.List(pl.Float64),
    "created_at": pl.String,
}

STANCE_CACHE_SCHEMA = {
    "llm_key": pl.String,
    "text_hash": pl.String,
    "text": pl.String,
    "side_id": pl.String,
    "label": pl.String,
    "confidence": pl.Float64,
    "model_name": pl.String,
    "created_at": pl.String,
}


def stable_text_hash(text: str) -> str:
    return sha256(text.encode("utf-8")).hexdigest()


@dataclass(slots=True)
class AnalysisCacheStore:
    config: ProjectConfig
    paths: ProjectPaths

    @property
    def embedding_cache_path(self) -> Path:
        return self.paths.processed_root / "embedding_cache.parquet"

    @property
    def stance_cache_path(self) -> Path:
        return self.paths.processed_root / "stance_cache.parquet"

    def embedding_provider_key(self, provider_name: str) -> str:
        provider = self.config.providers.embeddings
        return "|".join(
            [
                provider_name,
                provider.kind,
                provider.model,
                str(provider.dimension),
                provider.base_url or "",
            ]
        )

    def llm_provider_key(self, provider_name: str) -> str:
        provider = self.config.providers.llm
        return "|".join(
            [
                provider_name,
                provider.kind,
                provider.model,
                str(provider.temperature),
                provider.base_url or "",
            ]
        )

    def embedding_matrix(
        self,
        items: list[dict[str, Any]],
        *,
        provider_name: str,
        embed_many: Any,
        batch_size: int,
        dimension: int,
    ) -> np.ndarray:
        if not items:
            return np.zeros((0, dimension), dtype=float)

        provider_key = self.embedding_provider_key(provider_name)
        cache_frame = self._load_cache_frame(self.embedding_cache_path, EMBEDDING_CACHE_SCHEMA)
        provider_rows = (
            cache_frame.filter(pl.col("provider_key") == provider_key)
            if not cache_frame.is_empty()
            else self._empty_cache_frame(EMBEDDING_CACHE_SCHEMA)
        )
        cached_embeddings = {
            row["text_hash"]: np.array(row["embedding"], dtype=float)
            for row in provider_rows.to_dicts()
        }

        unique_missing: list[tuple[str, str]] = []
        seen_missing: set[str] = set()
        item_hashes: list[str] = []
        for item in items:
            text_hash = stable_text_hash(str(item["text"]))
            item_hashes.append(text_hash)
            if text_hash not in cached_embeddings and text_hash not in seen_missing:
                unique_missing.append((text_hash, str(item["text"])))
                seen_missing.add(text_hash)

        if unique_missing:
            new_rows: list[dict[str, Any]] = []
            for start in range(0, len(unique_missing), max(batch_size, 1)):
                batch = unique_missing[start : start + max(batch_size, 1)]
                embeddings = embed_many([text for _, text in batch])
                for (text_hash, text), vector in zip(batch, embeddings, strict=False):
                    normalized_vector = np.array(vector, dtype=float)
                    cached_embeddings[text_hash] = normalized_vector
                    new_rows.append(
                        {
                            "provider_key": provider_key,
                            "text_hash": text_hash,
                            "text": text,
                            "embedding": normalized_vector.tolist(),
                            "created_at": utc_now_iso(),
                        }
                    )
            self._persist_cache_rows(
                self.embedding_cache_path,
                EMBEDDING_CACHE_SCHEMA,
                new_rows,
                key_columns=["provider_key", "text_hash"],
            )

        return np.vstack([cached_embeddings[text_hash] for text_hash in item_hashes])

    def stance_predictions(
        self,
        items: list[dict[str, Any]],
        *,
        llm_name: str,
        sides: list[Any],
        classify_one: Any,
    ) -> list[dict[str, Any]]:
        if not items:
            return []

        llm_key = self.llm_provider_key(llm_name)
        cache_frame = self._load_cache_frame(self.stance_cache_path, STANCE_CACHE_SCHEMA)
        provider_rows = (
            cache_frame.filter(pl.col("llm_key") == llm_key)
            if not cache_frame.is_empty()
            else self._empty_cache_frame(STANCE_CACHE_SCHEMA)
        )
        cached_predictions = {
            (row["text_hash"], row["side_id"]): row
            for row in provider_rows.to_dicts()
        }

        missing_rows: list[dict[str, Any]] = []
        for item in items:
            text_hash = stable_text_hash(str(item["text"]))
            for side in sides:
                cache_key = (text_hash, side.side_id)
                if cache_key in cached_predictions:
                    continue
                prediction = classify_one(str(item["text"]), side)
                cached_row = {
                    "llm_key": llm_key,
                    "text_hash": text_hash,
                    "text": str(item["text"]),
                    "side_id": side.side_id,
                    "label": str(prediction["label"]),
                    "confidence": float(prediction["confidence"]),
                    "model_name": llm_name,
                    "created_at": utc_now_iso(),
                }
                cached_predictions[cache_key] = cached_row
                missing_rows.append(cached_row)

        if missing_rows:
            self._persist_cache_rows(
                self.stance_cache_path,
                STANCE_CACHE_SCHEMA,
                missing_rows,
                key_columns=["llm_key", "text_hash", "side_id"],
            )

        labels: list[dict[str, Any]] = []
        for item in items:
            text_hash = stable_text_hash(str(item["text"]))
            for side in sides:
                cached_row = cached_predictions[(text_hash, side.side_id)]
                labels.append(
                    {
                        "item_type": item["item_type"],
                        "item_id": item["item_id"],
                        "side_id": side.side_id,
                        "label": cached_row["label"],
                        "confidence": float(cached_row["confidence"]),
                        "model_name": cached_row["model_name"],
                    }
                )
        return labels

    @staticmethod
    def _load_cache_frame(path: Path, schema: Any) -> pl.DataFrame:
        if path.exists():
            return pl.read_parquet(path)
        return AnalysisCacheStore._empty_cache_frame(schema)

    @staticmethod
    def _empty_cache_frame(schema: Any) -> pl.DataFrame:
        return pl.DataFrame(
            {
                column_name: pl.Series(name=column_name, values=[], dtype=column_dtype)
                for column_name, column_dtype in schema.items()
            }
        )

    @staticmethod
    def _persist_cache_rows(
        path: Path,
        schema: dict[str, Any],
        rows: list[dict[str, Any]],
        *,
        key_columns: list[str],
    ) -> None:
        if not rows:
            return
        new_frame = pl.DataFrame(rows, schema=schema)
        if path.exists():
            existing_frame = pl.read_parquet(path)
            combined = pl.concat([existing_frame, new_frame], how="diagonal_relaxed")
        else:
            combined = new_frame
        combined = combined.unique(subset=key_columns, keep="last")
        combined.write_parquet(path)
