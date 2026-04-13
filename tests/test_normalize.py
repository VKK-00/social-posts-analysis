from __future__ import annotations

import json
import shutil
from pathlib import Path

from social_posts_analysis.normalize import NormalizationService


def test_normalization_materializes_nested_comments(project_config, project_paths) -> None:
    service = NormalizationService(project_config, project_paths)
    summary = service.run(run_id="20260402T120000Z")

    assert summary["run_id"] == "20260402T120000Z"

    import polars as pl

    comments = pl.read_parquet(project_paths.processed_root / "comments.parquet")
    edges = pl.read_parquet(project_paths.processed_root / "comment_edges.parquet")
    posts = pl.read_parquet(project_paths.processed_root / "posts.parquet")
    collection_runs = pl.read_parquet(project_paths.processed_root / "collection_runs.parquet")

    assert posts.height == 2
    assert comments.height == 2
    assert "platform" in posts.columns
    assert "thread_root_post_id" in comments.columns
    assert edges.filter(pl.col("parent_comment_id").is_not_null()).height == 1
    assert comments.sort("depth")["depth"].to_list() == [0, 1]
    assert collection_runs["source_id"][0] == "page_1"


def test_normalization_merges_recent_runs_into_snapshot(project_root: Path, project_config, project_paths) -> None:
    source_dir = project_root / "data/raw/20260402T120000Z"
    target_dir = project_root / "data/raw/20260402T121500Z"
    shutil.copytree(source_dir, target_dir)

    manifest_path = target_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["run_id"] = "20260402T121500Z"
    manifest["posts"][0]["comments"].append(
        {
            "comment_id": "comment_2",
            "platform": "facebook",
            "parent_post_id": "page_1_post_1",
            "parent_comment_id": None,
            "reply_to_message_id": None,
            "thread_root_post_id": "page_1_post_1",
            "created_at": "2026-03-30T10:00:00+00:00",
            "message": "A newly discovered supporting comment.",
            "permalink": "https://facebook.com/example/posts/1?comment_id=3",
            "reactions": 1,
            "reaction_breakdown_json": None,
            "source_collector": "meta_api",
            "depth": 0,
            "raw_path": "data/raw/20260402T121500Z/api_comment_items/comment_2.json",
            "author": {
                "author_id": "user_3",
                "name": "Late User",
                "profile_url": None
            }
        }
    )
    manifest["posts"][0]["comments_count"] = 3
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    project_config.normalization.merge_recent_runs = 2
    service = NormalizationService(project_config, project_paths)
    summary = service.run(run_id="20260402T121500Z")

    assert summary["source_run_ids"] == ["20260402T120000Z", "20260402T121500Z"]

    import polars as pl

    comments = pl.read_parquet(project_paths.processed_root / "comments.parquet").filter(
        pl.col("run_id") == "20260402T121500Z"
    )
    collection_runs = pl.read_parquet(project_paths.processed_root / "collection_runs.parquet").filter(
        pl.col("run_id") == "20260402T121500Z"
    )

    assert comments.height == 3
    assert collection_runs["source_run_count"][0] == 2


def test_normalization_merge_recent_runs_does_not_mix_different_sources(project_root: Path, project_config, project_paths) -> None:
    source_dir = project_root / "data/raw/20260402T120000Z"

    foreign_dir = project_root / "data/raw/20260402T121000Z"
    shutil.copytree(source_dir, foreign_dir)
    foreign_manifest_path = foreign_dir / "manifest.json"
    foreign_manifest = json.loads(foreign_manifest_path.read_text(encoding="utf-8"))
    foreign_manifest["run_id"] = "20260402T121000Z"
    foreign_manifest["source"]["source_id"] = "page_2"
    foreign_manifest["source"]["source_name"] = "Other Page"
    foreign_manifest["source"]["source_url"] = "https://www.facebook.com/other-page"
    foreign_manifest["posts"][0]["source_id"] = "page_2"
    foreign_manifest["posts"][0]["post_id"] = "page_2_post_1"
    foreign_manifest["posts"][1]["source_id"] = "page_2"
    foreign_manifest["posts"][1]["post_id"] = "page_2_post_2"
    foreign_manifest_path.write_text(json.dumps(foreign_manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    target_dir = project_root / "data/raw/20260402T121500Z"
    shutil.copytree(source_dir, target_dir)
    target_manifest_path = target_dir / "manifest.json"
    target_manifest = json.loads(target_manifest_path.read_text(encoding="utf-8"))
    target_manifest["run_id"] = "20260402T121500Z"
    target_manifest_path.write_text(json.dumps(target_manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    project_config.normalization.merge_recent_runs = 3
    summary = NormalizationService(project_config, project_paths).run(run_id="20260402T121500Z")

    assert summary["source_run_ids"] == ["20260402T120000Z", "20260402T121500Z"]


def test_normalization_merge_recent_runs_does_not_mix_different_date_ranges(
    project_root: Path,
    project_config,
    project_paths,
) -> None:
    source_dir = project_root / "data/raw/20260402T120000Z"

    older_range_dir = project_root / "data/raw/20260402T121000Z"
    shutil.copytree(source_dir, older_range_dir)
    older_manifest_path = older_range_dir / "manifest.json"
    older_manifest = json.loads(older_manifest_path.read_text(encoding="utf-8"))
    older_manifest["run_id"] = "20260402T121000Z"
    older_manifest["requested_date_start"] = "2026-03-01"
    older_manifest["requested_date_end"] = "2026-03-15"
    older_manifest_path.write_text(json.dumps(older_manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    target_dir = project_root / "data/raw/20260402T121500Z"
    shutil.copytree(source_dir, target_dir)
    target_manifest_path = target_dir / "manifest.json"
    target_manifest = json.loads(target_manifest_path.read_text(encoding="utf-8"))
    target_manifest["run_id"] = "20260402T121500Z"
    target_manifest["requested_date_start"] = "2026-03-16"
    target_manifest["requested_date_end"] = "2026-03-31"
    target_manifest_path.write_text(json.dumps(target_manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    project_config.normalization.merge_recent_runs = 3
    summary = NormalizationService(project_config, project_paths).run(run_id="20260402T121500Z")

    assert summary["source_run_ids"] == ["20260402T121500Z"]


def test_normalization_reuses_existing_run_for_same_source_run_ids(project_config, project_paths) -> None:
    service = NormalizationService(project_config, project_paths)

    first_summary = service.run(run_id="20260402T120000Z")
    second_summary = service.run(run_id="20260402T120000Z")

    assert first_summary["reused_existing_run"] is False
    assert second_summary["reused_existing_run"] is True
    assert second_summary["source_run_ids"] == ["20260402T120000Z"]


def test_normalization_persists_merged_warning_messages(project_root: Path, project_config, project_paths) -> None:
    source_dir = project_root / "data/raw/20260402T120000Z"
    source_manifest_path = source_dir / "manifest.json"
    source_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
    source_manifest["warnings"] = ["Older source-run warning."]
    source_manifest_path.write_text(json.dumps(source_manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    target_dir = project_root / "data/raw/20260402T121500Z"
    shutil.copytree(source_dir, target_dir)
    target_manifest_path = target_dir / "manifest.json"
    target_manifest = json.loads(target_manifest_path.read_text(encoding="utf-8"))
    target_manifest["run_id"] = "20260402T121500Z"
    target_manifest["warnings"] = []
    target_manifest_path.write_text(json.dumps(target_manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    project_config.normalization.merge_recent_runs = 2
    NormalizationService(project_config, project_paths).run(run_id="20260402T121500Z")

    import polars as pl

    collection_runs = pl.read_parquet(project_paths.processed_root / "collection_runs.parquet").filter(
        pl.col("run_id") == "20260402T121500Z"
    )

    assert "warning_messages" in collection_runs.columns
    assert "Older source-run warning." in collection_runs["warning_messages"][0]
