from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import polars as pl
import pytest
from pydantic import ValidationError

from social_posts_analysis.analysis.cache import AnalysisCacheStore
from social_posts_analysis.analysis.providers import HashEmbeddingProvider
from social_posts_analysis.analysis.service import AnalysisService
from social_posts_analysis.collectors.facebook_web_content import build_comment_snapshots
from social_posts_analysis.collectors.telegram_mtproto import TelegramMtprotoCollector
from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.raw_store import RawSnapshotStore

# ---------------------------------------------------------------------------
# 1. HashEmbeddingProvider determinism (providers.py)
# ---------------------------------------------------------------------------


def _manual_feature_vector(text: str, dimension: int) -> np.ndarray:
    vector = np.zeros(dimension, dtype=float)
    for token in re.findall(r"[\w']+", text.lower()):
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        index = int.from_bytes(digest[:8], "big") % dimension
        sign_digest = hashlib.sha256((token + "::sign").encode("utf-8")).digest()
        sign = 1.0 if int.from_bytes(sign_digest[:8], "big") % 2 else -1.0
        vector[index] += sign
    norm = np.linalg.norm(vector)
    return vector / norm if norm else vector


def test_hash_embedding_provider_matches_sha256_feature_hashing() -> None:
    provider = HashEmbeddingProvider(dimension=128)
    vectors = provider.embed_texts(["Hello world", "привіт світ"])
    assert vectors.shape == (2, 128)
    assert np.allclose(vectors[0], _manual_feature_vector("Hello world", 128))
    assert np.allclose(vectors[1], _manual_feature_vector("привіт світ", 128))


def test_hash_embedding_provider_is_stable_across_instances() -> None:
    texts = ["Deterministic embeddings", "стабільні ембеддинги"]
    first = HashEmbeddingProvider(dimension=64).embed_texts(texts)
    second = HashEmbeddingProvider(dimension=64).embed_texts(texts)
    assert np.array_equal(first, second)


# ---------------------------------------------------------------------------
# 2. Stance cache invalidation on sides changes (cache.py)
# ---------------------------------------------------------------------------


def test_llm_provider_key_changes_when_sides_config_changes(project_config, project_paths) -> None:
    store = AnalysisCacheStore(project_config, project_paths)
    original_key = store.llm_provider_key("heuristic_llm")

    side = project_config.sides[0]
    side.support_keywords = [*side.support_keywords, "нове_ключове_слово"]
    changed_key = store.llm_provider_key("heuristic_llm")

    assert original_key != changed_key
    # Same sides configuration must produce the same key.
    assert store.llm_provider_key("heuristic_llm") == changed_key


# ---------------------------------------------------------------------------
# 3. Stable Facebook comment ids without permalinks (facebook_web_content.py)
# ---------------------------------------------------------------------------


def test_facebook_comment_id_is_stable_without_permalink() -> None:
    payload_comments = [
        {
            "raw_text": "Аліса Ковальчук\n2 год\nЦе коментар без пермалинку",
            "text": "Це коментар без пермалинку",
        },
        {
            "raw_text": "Богдан Шевченко\n3 год\nЦе коментар без пермалинку",
            "text": "Це коментар без пермалинку",
        },
    ]

    first_run = build_comment_snapshots(
        post_id="post-1",
        payload_comments=payload_comments,
        raw_path="raw/comments.json",
        source_collector="facebook_web",
    )
    second_run = build_comment_snapshots(
        post_id="post-1",
        payload_comments=payload_comments,
        raw_path="raw/comments.json",
        source_collector="facebook_web",
    )

    assert len(first_run) == 2
    ids_first = [comment.comment_id for comment in first_run]
    ids_second = [comment.comment_id for comment in second_run]
    assert ids_first == ids_second
    # Identical texts from different authors must not collide.
    assert ids_first[0] != ids_first[1]
    for comment in first_run:
        assert comment.permalink is None


def test_facebook_comment_id_prefers_permalink() -> None:
    permalink = "https://www.facebook.com/comment/42"
    snapshots = build_comment_snapshots(
        post_id="post-1",
        payload_comments=[
            {
                "raw_text": "Марія Іваненко\nГарний пост",
                "text": "Гарний пост",
                "permalink": permalink,
            }
        ],
        raw_path="raw/comments.json",
        source_collector="facebook_web",
    )
    assert len(snapshots) == 1
    assert snapshots[0].permalink == permalink


# ---------------------------------------------------------------------------
# 4. MTProto date-range pagination guarantee (telegram_mtproto.py)
# ---------------------------------------------------------------------------


@dataclass
class _FakeMessage:
    id: int
    date: datetime
    message: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {"id": self.id, "date": self.date.isoformat(), "message": self.message}


def _mtproto_config(page_size: int) -> ProjectConfig:
    return ProjectConfig.model_validate(
        {
            "source": {"platform": "telegram", "source_name": "example_channel"},
            "date_range": {"start": "2026-04-01", "end": "2026-04-03"},
            "sides": [{"side_id": "side_a", "name": "Actor A"}],
            "collector": {
                "mode": "mtproto",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_mtproto": {
                    "enabled": True,
                    "session_file": ".sessions/example",
                    "api_id": 12345,
                    "api_hash": "hash",
                    "page_size": page_size,
                },
            },
        }
    )


class _PagingClient(SimpleNamespace):
    def __init__(self, pages: list[list[_FakeMessage]]) -> None:
        self.pages = pages
        self.calls: list[dict[str, object]] = []
        super().__init__(disconnect=lambda: None)

    def iter_messages(self, entity, limit=None, offset_date=None, reverse=False):  # noqa: ANN001, ANN202
        self.calls.append({"limit": limit, "offset_date": offset_date, "reverse": reverse})
        page_index = min(len(self.calls) - 1, len(self.pages) - 1)
        page = self.pages[page_index]
        if limit is not None:
            page = page[:limit]
        return iter(page)


def test_mtproto_source_scan_paginates_back_to_range_start() -> None:
    config = _mtproto_config(page_size=2)
    collector = TelegramMtprotoCollector(config)

    pages = [
        [
            _FakeMessage(1, datetime(2026, 4, 3, 12, 0, tzinfo=UTC), "newest"),
            _FakeMessage(2, datetime(2026, 4, 2, 12, 0, tzinfo=UTC), "older"),
        ],
        [
            _FakeMessage(3, datetime(2026, 4, 1, 12, 0, tzinfo=UTC), "in range"),
            _FakeMessage(4, datetime(2026, 3, 30, 12, 0, tzinfo=UTC), "before start"),
        ],
    ]
    client = _PagingClient(pages)

    messages = collector._iter_source_messages(client, "example_channel")

    # Two pages were fetched until the range start was covered...
    assert len(client.calls) == 2
    # ...and only messages inside the requested period are returned.
    assert [message.id for message in messages] == [1, 2, 3]
    assert collector._source_scan_truncated is False


def test_mtproto_source_scan_reports_truncation_when_bound_is_hit() -> None:
    config = _mtproto_config(page_size=50)
    collector = TelegramMtprotoCollector(config)

    endless_page = [
        _FakeMessage(index, datetime(2026, 4, 3, hour=(index % 24), minute=index % 60, tzinfo=UTC), "recent")
        for index in range(50)
    ]
    client = _PagingClient([endless_page])

    messages = collector._iter_source_messages(client, "example_channel")

    assert collector._source_scan_truncated is True
    # All fetched messages are within the range; the flag signals that older
    # in-range posts may exist beyond the scan bound.
    assert len(messages) > 0


def test_mtproto_collect_warns_on_truncated_range_scan(tmp_path: Path, monkeypatch) -> None:
    config = _mtproto_config(page_size=2)
    collector = TelegramMtprotoCollector(config)

    in_range_message = _FakeMessage(9, datetime(2026, 4, 2, 10, 0, tzinfo=UTC), "Channel post")
    fake_client = SimpleNamespace(disconnect=lambda: None)

    def fake_iter_source_messages(client, source_entity):  # noqa: ANN001, ANN202
        collector._source_scan_truncated = True
        return [in_range_message]

    monkeypatch.setattr(collector, "_open_client", lambda: fake_client)
    monkeypatch.setattr(collector, "_resolve_source_entity", lambda client: SimpleNamespace(id=1001, title="Example"))
    monkeypatch.setattr(collector, "_resolve_discussion_entity", lambda client, source: None)
    monkeypatch.setattr(collector, "_iter_source_messages", fake_iter_source_messages)

    manifest = collector.collect("tg-run-truncated", RawSnapshotStore(tmp_path / "raw"))

    assert any("date_range.start" in warning for warning in manifest.warnings)


def test_mtproto_single_page_without_start_date() -> None:
    config_dict = {
        "source": {"platform": "telegram", "source_name": "example_channel"},
        "sides": [{"side_id": "side_a", "name": "Actor A"}],
        "collector": {
            "mode": "mtproto",
            "meta_api": {"enabled": False},
            "public_web": {"enabled": False},
            "telegram_mtproto": {
                "enabled": True,
                "session_file": ".sessions/example",
                "api_id": 12345,
                "api_hash": "hash",
                "page_size": 5,
            },
        },
    }
    config = ProjectConfig.model_validate(config_dict)
    collector = TelegramMtprotoCollector(config)

    page = [_FakeMessage(index, datetime(2026, 4, 10, 12, 0, tzinfo=UTC), f"post {index}") for index in range(7)]
    client = _PagingClient([page])

    messages = collector._iter_source_messages(client, "example_channel")

    # Without date_range.start the previous single-page behaviour is kept.
    assert len(client.calls) == 1
    assert len(messages) == 5


# ---------------------------------------------------------------------------
# 5. Typed empty frames from missing tables (analysis/reporting service)
# ---------------------------------------------------------------------------


def test_analysis_service_missing_table_returns_typed_empty_frame(project_config, project_paths) -> None:
    service = AnalysisService(project_config, project_paths)
    frame = service._load_table("posts")
    # Must behave like a real table schema so downstream filters do not crash.
    filtered = frame.filter(pl.col("run_id") == "missing-run")
    assert filtered.is_empty()
    assert "run_id" in frame.columns


# ---------------------------------------------------------------------------
# 6. Config date validation (config_models.py)
# ---------------------------------------------------------------------------


def _base_project_payload() -> dict[str, object]:
    return {
        "source": {"platform": "telegram", "source_name": "example_channel"},
        "sides": [{"side_id": "side_a", "name": "Actor A"}],
        "collector": {
            "mode": "mtproto",
            "meta_api": {"enabled": False},
            "public_web": {"enabled": False},
            "telegram_mtproto": {
                "enabled": True,
                "session_file": ".sessions/example",
                "api_id": 12345,
                "api_hash": "hash",
            },
        },
    }


@pytest.mark.parametrize(
    ("date_range"),
    [
        {"start": "not-a-date"},
        {"end": "2026/13/40"},
        {"start": "2026-04-09", "end": "2026-04-01"},
    ],
)
def test_invalid_date_ranges_are_rejected(date_range: dict[str, str]) -> None:
    payload = _base_project_payload()
    payload["date_range"] = date_range
    with pytest.raises(ValidationError):
        ProjectConfig.model_validate(payload)


@pytest.mark.parametrize(
    ("date_range"),
    [
        {"start": "2026-04-01", "end": "2026-04-09"},
        {"start": "2026-04-01T10:00:00+00:00", "end": "2026-04-02"},
        {},
    ],
)
def test_valid_date_ranges_are_accepted(date_range: dict[str, str]) -> None:
    payload = _base_project_payload()
    payload["date_range"] = date_range
    config = ProjectConfig.model_validate(payload)
    assert config.date_range.start == date_range.get("start")
    assert config.date_range.end == date_range.get("end")


# ---------------------------------------------------------------------------
# 7. Atomic raw snapshot writes (raw_store.py)
# ---------------------------------------------------------------------------


def test_raw_snapshot_store_write_leaves_no_temp_files(tmp_path: Path) -> None:
    store = RawSnapshotStore(tmp_path / "raw")
    target = store.write_json("category", "snapshot", {"key": "значення"})

    assert target.exists()
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["key"] == "значення"
    leftovers = [path.name for path in target.parent.iterdir() if ".tmp-" in path.name]
    assert leftovers == []


def test_raw_snapshot_store_overwrite_is_atomic(tmp_path: Path) -> None:
    store = RawSnapshotStore(tmp_path / "raw")
    first = store.write_json("category", "same-stem", {"version": 1})
    second = store.write_json("category", "same-stem", {"version": 2})

    assert first == second
    assert json.loads(first.read_text(encoding="utf-8"))["version"] == 2
