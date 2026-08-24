from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

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


# ---------------------------------------------------------------------------
# 8. Wilson score interval for support metrics (metrics.py)
# ---------------------------------------------------------------------------


def test_wilson_interval_basic_properties() -> None:
    from social_posts_analysis.analysis.metrics import wilson_interval

    low, high = wilson_interval(7, 10)
    assert 0.0 < low < 0.7 < high < 1.0
    # Zero sample -> degenerate interval.
    assert wilson_interval(0, 0) == (0.0, 0.0)
    # All successes still leaves a finite upper bound below 1 for n>0.
    all_success_low, all_success_high = wilson_interval(100, 100)
    assert all_success_low > 0.9
    assert all_success_high <= 1.0


def test_support_metrics_include_wilson_bounds() -> None:
    from social_posts_analysis.analysis.metrics import compute_support_metrics

    stance = pl.DataFrame(
        {
            "item_type": ["comment"] * 4,
            "item_id": [f"c{index}" for index in range(4)],
            "side_id": ["s"] * 4,
            "label": ["support", "support", "support", "oppose"],
            "confidence": [0.9] * 4,
            "model_name": ["m"] * 4,
            "run_id": ["r"] * 4,
        }
    )
    result = compute_support_metrics(stance, pl.DataFrame(), pl.DataFrame(), "r")

    row = result.row(0, named=True)
    assert row["support_count"] == 3
    assert row["oppose_count"] == 1
    assert abs(row["support_ratio"] - 3 / 4) < 1e-9
    # The Wilson interval must contain the point estimate and be honest about
    # small-sample uncertainty.
    assert row["support_ratio_low"] < row["support_ratio"] < row["support_ratio_high"]
    assert row["support_ratio_low"] > 0.3


# ---------------------------------------------------------------------------
# 9. Author pseudonymization (normalization/records.py)
# ---------------------------------------------------------------------------


def _manifest_with_commenter() -> Any:
    from social_posts_analysis.contracts import (
        AuthorSnapshot,
        CollectionManifest,
        CommentSnapshot,
        PostSnapshot,
        SourceSnapshot,
    )

    commenter = AuthorSnapshot(author_id="user-777", name="Real Name", profile_url="https://fb.com/user-777")
    comment = CommentSnapshot(
        comment_id="c1",
        platform="facebook",
        parent_post_id="p1",
        message="Коментар",
        source_collector="facebook_web",
        author=commenter,
    )
    post_author = AuthorSnapshot(author_id="page-1", name="Page Name", profile_url=None)
    post = PostSnapshot(
        post_id="p1",
        platform="facebook",
        source_id="page-1",
        message="Пост",
        source_collector="facebook_web",
        author=post_author,
        comments=[comment],
    )
    source = SourceSnapshot(
        platform="facebook",
        source_id="page-1",
        source_name="Page Name",
        source_collector="facebook_web",
    )
    return CollectionManifest(
        run_id="run-x",
        collected_at="2026-04-01T00:00:00+00:00",
        collector="facebook_web",
        mode="web",
        source=source,
        posts=[post],
    )


def test_pseudonymize_authors_replaces_identity_but_keeps_joins() -> None:
    from social_posts_analysis.normalization.records import build_table_records

    manifest = _manifest_with_commenter()

    anon = build_table_records(manifest, ["run-x"], pseudonymize_authors=True)

    anon_comment = anon["comments"][0]
    assert anon_comment["author_id"].startswith("anon-")
    # Posts keep their (hashed) author id so joins to authors still resolve.
    assert anon["posts"][0]["author_id"].startswith("anon-")
    # No third-party ids, names or profile URLs survive in the anonymized
    # tables; the analysed source itself stays identifiable by design.
    author_ids = {row["author_id"] for row in anon["authors"]}
    assert "user-777" not in author_ids
    assert all(
        row["name"] is None and row["profile_url"] is None for row in anon["authors"] if row["author_id"] != "page-1"
    )
    # Pseudonyms are stable across calls.
    again = build_table_records(manifest, ["run-x"], pseudonymize_authors=True)
    assert again["comments"][0]["author_id"] == anon_comment["author_id"]

    plain = build_table_records(manifest, ["run-x"], pseudonymize_authors=False)
    assert plain["comments"][0]["author_id"] == "user-777"
    commenter_row = next(row for row in plain["authors"] if row["author_id"] == "user-777")
    assert commenter_row["name"] == "Real Name"


def test_pseudonymization_flag_defaults_to_off() -> None:
    config = ProjectConfig.model_validate(
        {
            "source": {"platform": "telegram", "source_name": "example_channel"},
            "sides": [{"side_id": "side_a", "name": "Actor A"}],
            "collector": {
                "mode": "mtproto",
                "meta_api": {"enabled": False},
                "public_web": {"enabled": False},
                "telegram_mtproto": {"enabled": True, "session_file": ".sessions/x", "api_id": 1, "api_hash": "h"},
            },
        }
    )
    assert config.normalization.pseudonymize_authors is False


# ---------------------------------------------------------------------------
# 10. Rate-limit handling helper (utils.py + providers.py)
# ---------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, status_code: int | None = None, retry_after: str | None = None) -> None:
        self._status_code = status_code
        self.headers = {"Retry-After": retry_after} if retry_after is not None else {}

    @property
    def status_code(self) -> int | None:
        return self._status_code


def test_handle_rate_limit_response_sleeps_on_429(monkeypatch) -> None:
    from social_posts_analysis.utils import handle_rate_limit_response

    slept: list[float] = []
    monkeypatch.setattr("social_posts_analysis.utils.time.sleep", lambda seconds: slept.append(seconds))

    assert handle_rate_limit_response(_FakeResponse(status_code=200)) is None
    assert handle_rate_limit_response(_FakeResponse(status_code=429, retry_after="7")) == 7.0
    assert slept == [7.0]

    # Missing Retry-After falls back to the default; huge values are capped.
    assert handle_rate_limit_response(_FakeResponse(status_code=429), default_seconds=2.0) == 2.0
    assert handle_rate_limit_response(_FakeResponse(status_code=429, retry_after="999"), max_seconds=30.0) == 30.0


class _SideStub:
    side_id = "side_a"
    name = "Actor A"
    aliases: list[str] = []

    @property
    def all_names(self) -> list[str]:
        return ["actor a"]


def test_openai_compatible_provider_retries_on_429(monkeypatch) -> None:
    from social_posts_analysis.analysis.providers import OpenAICompatibleLLMProvider
    from social_posts_analysis.config import LLMProviderConfig

    provider_config = LLMProviderConfig(kind="openai_compatible", base_url="https://api.example.com", api_key="k")
    provider = OpenAICompatibleLLMProvider(provider_config)

    class RateLimitedThenOkClient:
        def __init__(self) -> None:
            self.calls = 0

        def post(self, url, headers=None, json=None):  # noqa: A002, ANN001
            self.calls += 1
            response = SimpleNamespace()
            response.headers = {"Retry-After": "0"}
            if self.calls == 1:
                response.status_code = 429
            else:
                response.status_code = 200
                response.raise_for_status = lambda: None
                response.json = lambda: {
                    "choices": [{"message": {"content": '{"label": "support", "confidence": 0.8}'}}]
                }
            return response

    client = RateLimitedThenOkClient()
    monkeypatch.setattr(provider, "client", client)
    monkeypatch.setattr("social_posts_analysis.utils.time.sleep", lambda _seconds: None)

    result = provider.classify_stance("текст", _SideStub())
    assert client.calls == 2
    assert result["label"] == "support"


# ---------------------------------------------------------------------------
# 11. Optional sentence-transformers embeddings provider
# ---------------------------------------------------------------------------


class _FakeSentenceTransformer:
    last_model_name: str | None = None

    def __init__(self, model_name: str) -> None:
        type(self).last_model_name = model_name
        self.model_name = model_name

    def encode(self, texts: list[str], normalize_embeddings: bool = False) -> Any:
        vectors = []
        for index, _text in enumerate(texts):
            vector = np.zeros(384, dtype=float)
            vector[index % 384] = 1.0
            vector[(index + 7) % 384] = 0.5
            if normalize_embeddings:
                norm = np.linalg.norm(vector)
                if norm:
                    vector = vector / norm
            vectors.append(vector)
        return np.vstack(vectors)


def _install_fake_sentence_transformers(monkeypatch) -> None:
    import types

    fake_module = types.ModuleType("sentence_transformers")
    fake_module.SentenceTransformer = _FakeSentenceTransformer
    monkeypatch.setitem(__import__("sys").modules, "sentence_transformers", fake_module)


def _embedding_config(**overrides: Any) -> Any:
    from social_posts_analysis.config import EmbeddingProviderConfig

    return EmbeddingProviderConfig(kind="sentence_transformers", **overrides)


def test_sentence_transformers_provider_encodes_locally(monkeypatch) -> None:
    from social_posts_analysis.analysis.providers import SentenceTransformerEmbeddingProvider

    _install_fake_sentence_transformers(monkeypatch)
    provider = SentenceTransformerEmbeddingProvider(_embedding_config())

    # The generic API model name maps to a multilingual local default.
    assert provider.model_name == "paraphrase-multilingual-MiniLM-L12-v2"
    assert provider.dimension == 384

    texts = ["перший текст", "другий текст"]
    vectors = provider.embed_texts(texts)
    assert vectors.shape == (2, 384)
    # Normalized embeddings have unit length.
    assert np.allclose(np.linalg.norm(vectors, axis=1), 1.0)

    empty = provider.embed_texts([])
    assert empty.shape == (0, 384)


def test_sentence_transformers_provider_respects_local_model(monkeypatch) -> None:
    from social_posts_analysis.analysis.providers import SentenceTransformerEmbeddingProvider

    _install_fake_sentence_transformers(monkeypatch)
    provider = SentenceTransformerEmbeddingProvider(
        _embedding_config(model="text-embedding-3-small", local_model="/models/custom-encoder")
    )
    assert provider.model_name == "/models/custom-encoder"
    assert provider.dimension == 256


def test_sentence_transformers_provider_fails_without_extra() -> None:
    from social_posts_analysis.analysis.providers import SentenceTransformerEmbeddingProvider

    try:
        import sentence_transformers  # noqa: F401

        pytest.skip("sentence-transformers is installed; failure path not reachable")
    except ImportError:
        pass

    with pytest.raises(ValueError, match="semantic"):
        SentenceTransformerEmbeddingProvider(_embedding_config())


def test_config_accepts_sentence_transformers_embedding_kind() -> None:
    payload = _base_project_payload()
    payload["providers"] = {"embeddings": {"kind": "sentence_transformers"}}
    config = ProjectConfig.model_validate(payload)
    assert config.providers.embeddings.kind == "sentence_transformers"


# ---------------------------------------------------------------------------
# 12. Checked-in config template stays valid
# ---------------------------------------------------------------------------


def test_checked_in_project_template_is_valid() -> None:
    from social_posts_analysis.config import load_config

    config = load_config(Path("config") / "project.yaml")
    assert config.source.platform in {"facebook", "telegram", "x", "threads", "instagram"}


# ---------------------------------------------------------------------------
# 13. Order-insensitive normalized-run reuse (normalize.py)
# ---------------------------------------------------------------------------


def test_matching_normalized_run_is_order_insensitive(project_config, project_paths) -> None:
    from social_posts_analysis.normalize import NormalizationService

    service = NormalizationService(project_config, project_paths)
    project_paths.processed_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {"run_id": ["r1"], "source_run_ids": [["b-run", "a-run"]]},
        schema_overrides={"source_run_ids": pl.List(pl.String)},
    ).write_parquet(project_paths.processed_root / "collection_runs.parquet")

    # Same set of source runs in a different order still counts as merged.
    assert service._has_matching_normalized_run("r1", ["a-run", "b-run"]) is True
    assert service._has_matching_normalized_run("r1", ["b-run", "a-run"]) is True
    assert service._has_matching_normalized_run("r1", ["a-run", "c-run"]) is False


# ---------------------------------------------------------------------------
# 14. Cascade shape metrics (analysis/cascades.py)
# ---------------------------------------------------------------------------


def test_structural_virality_known_values() -> None:
    from social_posts_analysis.analysis.cascades import structural_virality

    # Star with root + 3 leaves: distances are 6x1 (root-leaf) and 6x2 (leaf-leaf).
    star = structural_virality(
        nodes=["o", "l1", "l2", "l3"],
        children={"o": ["l1", "l2", "l3"]},
    )
    expected_star = (3 * 1 * 2 + 3 * 2 * 2) / (4 * 3)
    assert abs(star - expected_star) < 1e-9

    # Chain o -> a -> b: ordered pairwise distances 1+2+1+1+2+1 = 8 over 6 pairs.
    chain = structural_virality(nodes=["o", "a", "b"], children={"o": ["a"], "a": ["b"]})
    assert abs(chain - 8 / 6) < 1e-9


def test_comment_cascade_metrics_for_nested_thread() -> None:
    from social_posts_analysis.analysis.cascades import compute_cascade_metrics

    edges = pl.DataFrame(
        {
            "comment_id": ["t1", "t2", "t1-r1", "t1-r1-r2"],
            "parent_post_id": ["post-1"] * 4,
            "parent_comment_id": [None, None, "t1", "t1-r1"],
            "depth": [0, 0, 1, 2],
            "run_id": ["run-x"] * 4,
        }
    )
    result = compute_cascade_metrics(edges, pl.DataFrame(), "run-x")

    assert result.height == 1
    row = result.row(0, named=True)
    assert row["cascade_type"] == "comment_tree"
    assert row["scope_id"] == "post-1"
    assert row["node_count"] == 4
    # origin -> t1 -> t1-r1 -> t1-r1-r2
    assert row["max_depth"] == 3
    # Level 1 under the origin holds both top-level comments.
    assert row["max_breadth"] == 2
    assert row["structural_virality"] is not None
    assert row["run_id"] == "run-x"


def test_propagation_cascade_metrics_star() -> None:
    from social_posts_analysis.analysis.cascades import compute_cascade_metrics

    edges = pl.DataFrame(
        {
            "propagation_id": ["s1", "s2", "s3"],
            "origin_post_id": ["post-9"] * 3,
            "propagation_kind": ["share"] * 3,
            "platform": ["facebook"] * 3,
            "run_id": ["run-y"] * 3,
        }
    )
    result = compute_cascade_metrics(pl.DataFrame(), edges, "run-y")

    assert result.height == 1
    row = result.row(0, named=True)
    assert row["cascade_type"] == "propagation_tree"
    assert row["scope_id"] == "post-9"
    assert row["node_count"] == 3
    assert row["max_depth"] == 1
    assert row["max_breadth"] == 3
    # A pure share-star has the virality of a 4-node star.
    expected = (3 * 1 * 2 + 3 * 2 * 2) / (4 * 3)
    assert abs(row["structural_virality"] - expected) < 1e-9


def test_cascade_metrics_empty_inputs_yield_empty_typed_frame() -> None:
    from social_posts_analysis.analysis.cascades import compute_cascade_metrics

    result = compute_cascade_metrics(pl.DataFrame(), pl.DataFrame(), "run-z")
    assert result.is_empty()
    assert "node_count" in result.columns
    assert "structural_virality" in result.columns


def test_single_comment_tree_has_minimal_virality() -> None:
    from social_posts_analysis.analysis.cascades import compute_cascade_metrics

    edges = pl.DataFrame(
        {
            "comment_id": ["only"],
            "parent_post_id": ["p"],
            "parent_comment_id": [None],
            "depth": [0],
            "run_id": ["r"],
        }
    )
    row = compute_cascade_metrics(edges, pl.DataFrame(), "r").row(0, named=True)
    assert row["node_count"] == 1
    assert row["max_depth"] == 1
    # origin <-> single comment: the smallest meaningful cascade.
    assert row["structural_virality"] == 1.0


# ---------------------------------------------------------------------------
# 15. Near-duplicate detection (analysis/duplicates.py)
# ---------------------------------------------------------------------------


def _dup_items() -> list[dict[str, Any]]:
    base = (
        "Комітет з питань оборони ухвалив рішення про додаткове фінансування "
        "програми підтримки дронових підрозділів у прифронтових районах країни"
    )
    near = (
        "Комітет з питань оборони ухвалив рішення про додаткове фінансування "
        "програми підтримки дронових підрозділів у прифронтовых районах країни"
    )  # one word differs
    unrelated = "Сонячна погода привабила тисячі туристів на гірські стежки Карпат у вихідні дні"
    return [
        {"item_type": "post", "item_id": "p1", "text": base},
        {"item_type": "propagation", "item_id": "s1", "text": near},
        {"item_type": "post", "item_id": "p2", "text": unrelated},
    ]


def test_identical_and_near_identical_texts_are_detected() -> None:
    from social_posts_analysis.analysis.duplicates import find_near_duplicates

    texts = {
        ("post", "a"): "the quick brown fox jumps over the lazy dog again and again",
        ("post", "b"): "the quick brown fox jumps over the lazy dog again and again",
        ("post", "c"): "completely different content about mountain weather forecasts today",
    }
    result = find_near_duplicates(texts, threshold=0.8, run_id="r")
    rows = result.to_dicts()
    pairs = {(row["item_id_a"], row["item_id_b"]) for row in rows}
    assert ("a", "b") in pairs
    identical_row = next(row for row in rows if {row["item_id_a"], row["item_id_b"]} == {"a", "b"})
    assert identical_row["similarity"] > 0.95


def test_unrelated_texts_are_not_reported() -> None:
    from social_posts_analysis.analysis.duplicates import near_duplicate_texts

    result = near_duplicate_texts(_dup_items(), threshold=0.8, run_id="r")
    for row in result.to_dicts():
        pair_ids = {row["item_id_a"], row["item_id_b"]}
        # The only allowed pair is the near-identical p1/s1; the unrelated post stays out.
        assert "p2" not in pair_ids


def test_near_duplicate_detection_is_deterministic() -> None:
    from social_posts_analysis.analysis.duplicates import minhash_signature, near_duplicate_texts

    signature_one = minhash_signature("детермінований тест сигнатури мінхеш")
    signature_two = minhash_signature("детермінований тест сигнатури мінхеш")
    assert signature_one == signature_two

    first = near_duplicate_texts(_dup_items(), threshold=0.8, run_id="r")
    second = near_duplicate_texts(_dup_items(), threshold=0.8, run_id="r")
    assert first.equals(second)


def test_threshold_filters_weak_pairs() -> None:
    from social_posts_analysis.analysis.duplicates import near_duplicate_texts

    strict = near_duplicate_texts(_dup_items(), threshold=0.8, run_id="r")
    loose = near_duplicate_texts(_dup_items(), threshold=0.05, run_id="r")
    # A lower threshold can only add candidate-verified pairs, never remove them.
    assert loose.height >= strict.height


def test_empty_or_single_item_yields_empty_frame() -> None:
    from social_posts_analysis.analysis.duplicates import NEAR_DUPLICATES_SCHEMA, find_near_duplicates

    empty = find_near_duplicates({}, threshold=0.8, run_id="r")
    assert empty.is_empty()
    assert list(empty.columns) == list(NEAR_DUPLICATES_SCHEMA)

    single = find_near_duplicates({("post", "x"): "тільки один текст"}, threshold=0.8, run_id="r")
    assert single.is_empty()


def test_config_exposes_near_duplicate_threshold(project_config) -> None:
    assert project_config.analysis.near_duplicate_threshold == 0.8
