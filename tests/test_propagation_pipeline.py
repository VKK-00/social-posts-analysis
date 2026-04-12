from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from social_posts_analysis.analysis.service import AnalysisService
from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.contracts import (
    AuthorSnapshot,
    CollectionManifest,
    CommentSnapshot,
    PostSnapshot,
    SourceSnapshot,
)
from social_posts_analysis.normalize import NormalizationService
from social_posts_analysis.paths import ProjectPaths
from social_posts_analysis.reporting.service import ReportService


def _test_config(*, platform: str, source: dict[str, str], collector: dict[str, object]) -> ProjectConfig:
    return ProjectConfig.model_validate(
        {
            "project_name": "social_posts_analysis",
            "source": {"platform": platform, **source},
            "collector": collector,
            "sides": [
                {
                    "side_id": "actor_a",
                    "name": "Actor A",
                    "aliases": ["actor a"],
                    "support_keywords": ["support actor a"],
                    "oppose_keywords": ["oppose actor a"],
                }
            ],
            "analysis": {
                "languages": ["en"],
                "min_cluster_size": 2,
                "min_samples": 1,
            },
            "providers": {
                "embeddings": {"kind": "hash"},
                "llm": {"kind": "heuristic"},
            },
        }
    )


def _run_pipeline(tmp_path: Path, *, run_id: str, config: ProjectConfig, manifest: CollectionManifest) -> tuple[ProjectPaths, pl.DataFrame, pl.DataFrame, dict[str, object]]:
    root = tmp_path / "project"
    paths = ProjectPaths.from_config(root, config)
    paths.ensure()
    run_dir = paths.run_raw_dir(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "manifest.json").write_text(json.dumps(manifest.model_dump(mode="json"), ensure_ascii=False, indent=2), encoding="utf-8")

    NormalizationService(config, paths).run(run_id=run_id)
    propagations = pl.read_parquet(paths.processed_root / "propagations.parquet").filter(pl.col("run_id") == run_id)
    comments = pl.read_parquet(paths.processed_root / "comments.parquet").filter(pl.col("run_id") == run_id)
    AnalysisService(config, paths).run(run_id=run_id)
    context = ReportService(config, paths)._build_context(run_id)
    return paths, propagations, comments, context


def test_normalize_and_report_include_propagation_scopes(tmp_path: Path) -> None:
    run_id = "20260410T120000Z"
    config = _test_config(
        platform="facebook",
        source={"url": "https://www.facebook.com/example-page/"},
        collector={
            "mode": "web",
            "public_web": {"enabled": True},
            "meta_api": {"enabled": False},
        },
    )
    manifest = CollectionManifest(
        run_id=run_id,
        collected_at="2026-04-10T12:00:00+00:00",
        collector="public_web",
        mode="web",
        source=SourceSnapshot(
            platform="facebook",
            source_id="page_1",
            source_name="Example Page",
            source_url="https://www.facebook.com/example-page/",
            source_type="page",
            source_collector="public_web",
            raw_path="source.json",
        ),
        posts=[
            PostSnapshot(
                post_id="facebook:page_1:post_1",
                platform="facebook",
                source_id="page_1",
                created_at="2026-04-08T10:00:00+00:00",
                message="Actor A update from the main page.",
                permalink="https://www.facebook.com/example-page/posts/1",
                comments_count=1,
                source_collector="public_web",
                raw_path="post_1.json",
                author=AuthorSnapshot(author_id="page_1", name="Example Page"),
                comments=[
                    CommentSnapshot(
                        comment_id="comment_origin_1",
                        platform="facebook",
                        parent_post_id="facebook:page_1:post_1",
                        created_at="2026-04-08T10:05:00+00:00",
                        message="I support Actor A.",
                        permalink="https://www.facebook.com/example-page/posts/1?comment_id=1",
                        source_collector="public_web",
                        raw_path="comment_origin_1.json",
                        author=AuthorSnapshot(author_id="user_1", name="User 1"),
                    )
                ],
            ),
            PostSnapshot(
                post_id="facebook:page_1:post_2",
                platform="facebook",
                source_id="page_1",
                origin_post_id="facebook:page_1:post_1",
                origin_external_id="1",
                origin_permalink="https://www.facebook.com/example-page/posts/1",
                propagation_kind="share",
                is_propagation=True,
                created_at="2026-04-08T11:00:00+00:00",
                message="Shared Actor A update to another audience.",
                permalink="https://www.facebook.com/example-page/posts/2",
                shares=1,
                comments_count=1,
                source_collector="public_web",
                raw_path="post_2.json",
                author=AuthorSnapshot(author_id="page_1", name="Example Page"),
                comments=[
                    CommentSnapshot(
                        comment_id="comment_prop_1",
                        platform="facebook",
                        parent_post_id="facebook:page_1:post_2",
                        created_at="2026-04-08T11:05:00+00:00",
                        message="I oppose Actor A.",
                        permalink="https://www.facebook.com/example-page/posts/2?comment_id=1",
                        source_collector="public_web",
                        raw_path="comment_prop_1.json",
                        author=AuthorSnapshot(author_id="user_2", name="User 2"),
                    )
                ],
            ),
        ],
    )
    _, propagations, comments, context = _run_pipeline(tmp_path, run_id=run_id, config=config, manifest=manifest)

    assert propagations.height == 1
    propagation_comment = comments.filter(pl.col("comment_id") == "comment_prop_1").to_dicts()[0]
    assert propagation_comment["parent_entity_type"] == "propagation"
    assert propagation_comment["parent_entity_id"] == "facebook:page_1:post_2"
    assert propagation_comment["origin_post_id"] == "facebook:page_1:post_1"

    assert context["post_count"] == 1
    assert context["propagation_count"] == 1
    assert context["top_propagated_items"][0]["origin_post_id"] == "facebook:page_1:post_1"
    assert context["propagation_summary"]["extracted_comments"] == 1
    assert context["propagation_comments"][0]["parent_entity_id"] == "facebook:page_1:post_2"

    origin_plus_row = next(
        row for row in context["origin_plus_support"] if row["scope_id"] == "facebook:page_1:post_1" and row["side_id"] == "actor_a"
    )
    propagation_row = next(
        row for row in context["propagation_support"] if row["scope_id"] == "facebook:page_1:post_2" and row["side_id"] == "actor_a"
    )

    assert origin_plus_row["support_count"] == 1
    assert origin_plus_row["oppose_count"] == 1
    assert propagation_row["oppose_count"] == 1


def test_telegram_forward_comments_are_counted_in_propagation_and_origin_plus_scopes(tmp_path: Path) -> None:
    run_id = "20260410T130000Z"
    config = _test_config(
        platform="telegram",
        source={"source_name": "example_channel"},
        collector={
            "mode": "web",
            "telegram_web": {"enabled": True},
            "telegram_mtproto": {"enabled": False},
        },
    )
    manifest = CollectionManifest(
        run_id=run_id,
        collected_at="2026-04-10T13:00:00+00:00",
        collector="telegram_web",
        mode="web",
        source=SourceSnapshot(
            platform="telegram",
            source_id="example_channel",
            source_name="Example Channel",
            source_url="https://t.me/s/example_channel",
            source_type="channel",
            source_collector="telegram_web",
            raw_path="source.json",
            discussion_chat_id="example_discussion",
            discussion_linked=True,
        ),
        posts=[
            PostSnapshot(
                post_id="telegram:example_channel:10",
                platform="telegram",
                source_id="example_channel",
                created_at="2026-04-09T10:00:00+00:00",
                message="Actor A original channel post.",
                permalink="https://t.me/example_channel/10",
                comments_count=1,
                source_collector="telegram_web",
                raw_path="post_origin.json",
                author=AuthorSnapshot(author_id="example_channel", name="Example Channel"),
                comments=[
                    CommentSnapshot(
                        comment_id="tg_origin_comment_1",
                        platform="telegram",
                        parent_post_id="telegram:example_channel:10",
                        created_at="2026-04-09T10:05:00+00:00",
                        message="I support Actor A.",
                        permalink="https://t.me/example_discussion/101",
                        source_collector="telegram_web",
                        raw_path="tg_origin_comment_1.json",
                        author=AuthorSnapshot(author_id="user_1", name="User 1"),
                    )
                ],
            ),
            PostSnapshot(
                post_id="telegram:example_channel:11",
                platform="telegram",
                source_id="example_channel",
                origin_post_id="telegram:example_channel:10",
                origin_external_id="10",
                origin_permalink="https://t.me/example_channel/10",
                propagation_kind="forward",
                is_propagation=True,
                created_at="2026-04-09T11:00:00+00:00",
                message="Forwarded Actor A post into another visible thread.",
                permalink="https://t.me/example_channel/11",
                forwards=1,
                comments_count=1,
                source_collector="telegram_web",
                raw_path="post_forward.json",
                author=AuthorSnapshot(author_id="example_channel", name="Example Channel"),
                comments=[
                    CommentSnapshot(
                        comment_id="tg_prop_comment_1",
                        platform="telegram",
                        parent_post_id="telegram:example_channel:11",
                        created_at="2026-04-09T11:05:00+00:00",
                        message="I oppose Actor A.",
                        permalink="https://t.me/example_discussion/111",
                        source_collector="telegram_web",
                        raw_path="tg_prop_comment_1.json",
                        author=AuthorSnapshot(author_id="user_2", name="User 2"),
                    )
                ],
            ),
        ],
    )
    _, _, comments, context = _run_pipeline(tmp_path, run_id=run_id, config=config, manifest=manifest)

    propagation_comment = comments.filter(pl.col("comment_id") == "tg_prop_comment_1").to_dicts()[0]
    assert propagation_comment["parent_entity_type"] == "propagation"
    assert propagation_comment["origin_post_id"] == "telegram:example_channel:10"
    assert context["propagation_summary"]["extracted_comments"] == 1
    assert context["propagation_comments"][0]["comment_id"] == "tg_prop_comment_1"
    assert any(
        row["scope_id"] == "telegram:example_channel:11" and row["oppose_count"] == 1 for row in context["propagation_support"]
    )
    assert any(
        row["scope_id"] == "telegram:example_channel:10"
        and row["support_count"] == 1
        and row["oppose_count"] == 1
        for row in context["origin_plus_support"]
    )


def test_x_quote_replies_are_separated_from_origin_replies(tmp_path: Path) -> None:
    run_id = "20260410T140000Z"
    config = _test_config(
        platform="x",
        source={"source_name": "example_account"},
        collector={
            "mode": "web",
            "x_web": {"enabled": True},
            "x_api": {"enabled": False},
        },
    )
    manifest = CollectionManifest(
        run_id=run_id,
        collected_at="2026-04-10T14:00:00+00:00",
        collector="x_web",
        mode="web",
        source=SourceSnapshot(
            platform="x",
            source_id="example_account",
            source_name="example_account",
            source_url="https://x.com/example_account",
            source_type="account",
            source_collector="x_web",
            raw_path="source.json",
        ),
        posts=[
            PostSnapshot(
                post_id="x:example_account:200",
                platform="x",
                source_id="example_account",
                created_at="2026-04-09T09:00:00+00:00",
                message="Actor A original tweet.",
                permalink="https://x.com/example_account/status/200",
                comments_count=1,
                source_collector="x_web",
                raw_path="origin_tweet.json",
                author=AuthorSnapshot(author_id="example_account", name="example_account"),
                comments=[
                    CommentSnapshot(
                        comment_id="x_origin_reply_1",
                        platform="x",
                        parent_post_id="x:example_account:200",
                        created_at="2026-04-09T09:05:00+00:00",
                        message="I support Actor A.",
                        permalink="https://x.com/example_account/status/200#reply1",
                        source_collector="x_web",
                        raw_path="x_origin_reply_1.json",
                        author=AuthorSnapshot(author_id="user_1", name="User 1"),
                    )
                ],
            ),
            PostSnapshot(
                post_id="x:example_account:201",
                platform="x",
                source_id="example_account",
                origin_post_id="x:example_account:200",
                origin_external_id="200",
                origin_permalink="https://x.com/example_account/status/200",
                propagation_kind="quote",
                is_propagation=True,
                created_at="2026-04-09T10:00:00+00:00",
                message="Quoted Actor A tweet with additional commentary.",
                permalink="https://x.com/example_account/status/201",
                forwards=1,
                comments_count=1,
                source_collector="x_web",
                raw_path="quote_tweet.json",
                author=AuthorSnapshot(author_id="example_account", name="example_account"),
                comments=[
                    CommentSnapshot(
                        comment_id="x_quote_reply_1",
                        platform="x",
                        parent_post_id="x:example_account:201",
                        created_at="2026-04-09T10:05:00+00:00",
                        message="I oppose Actor A.",
                        permalink="https://x.com/example_account/status/201#reply1",
                        source_collector="x_web",
                        raw_path="x_quote_reply_1.json",
                        author=AuthorSnapshot(author_id="user_2", name="User 2"),
                    )
                ],
            ),
        ],
    )
    _, _, comments, context = _run_pipeline(tmp_path, run_id=run_id, config=config, manifest=manifest)

    propagation_comment = comments.filter(pl.col("comment_id") == "x_quote_reply_1").to_dicts()[0]
    assert propagation_comment["parent_entity_type"] == "propagation"
    assert propagation_comment["parent_entity_id"] == "x:example_account:201"
    assert context["propagation_comments"][0]["propagation_kind"] == "quote"
    assert any(
        row["scope_id"] == "x:example_account:201" and row["oppose_count"] == 1 for row in context["propagation_support"]
    )
    assert any(
        row["scope_id"] == "x:example_account:200"
        and row["support_count"] == 1
        and row["oppose_count"] == 1
        for row in context["origin_plus_support"]
    )
