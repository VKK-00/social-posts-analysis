from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from social_posts_analysis.normalize import NormalizationService
from social_posts_analysis.openclaw import OpenClawExportService


def test_openclaw_export_builds_feed_bundle(project_config, project_paths) -> None:
    NormalizationService(project_config, project_paths).run(run_id="20260402T120000Z")

    outputs = OpenClawExportService(project_config, project_paths).run(run_id="20260402T120000Z")

    assert outputs.bundle_path == project_paths.reports_root / "openclaw/20260402T120000Z/bundle.json"
    assert outputs.brief_path == project_paths.reports_root / "openclaw/20260402T120000Z/brief.md"
    bundle = json.loads(outputs.bundle_path.read_text(encoding="utf-8"))
    assert bundle["schema_version"] == "openclaw.social_posts_analysis.v1"
    assert bundle["run_id"] == "20260402T120000Z"
    assert bundle["project_name"] == project_config.project_name
    assert bundle["source"]["platform"] == "facebook"
    assert bundle["source"]["source_kind"] == "feed"
    assert bundle["collector"] == "meta_api"
    assert bundle["status"] == "success"
    assert bundle["counts"]["posts"] == 2
    assert bundle["counts"]["comments"] == 2
    assert bundle["counts"]["propagations"] == 0
    assert bundle["counts"]["match_hits"] == 0
    assert bundle["person_monitor"] == {"enabled": False}
    assert bundle["artifacts"]["raw_manifest"].endswith("data\\raw\\20260402T120000Z\\manifest.json")
    assert bundle["artifacts"]["processed_dir"].endswith("data\\processed")
    assert bundle["artifacts"]["duckdb"].endswith("data\\processed\\social_posts_analysis.duckdb")


def test_openclaw_export_handles_missing_optional_tables(project_config, project_paths) -> None:
    outputs = OpenClawExportService(project_config, project_paths).run(run_id="20260402T120000Z")

    bundle = json.loads(outputs.bundle_path.read_text(encoding="utf-8"))
    assert bundle["counts"]["posts"] == 2
    assert bundle["counts"]["comments"] == 2
    assert bundle["counts"]["match_hits"] == 0
    assert bundle["counts"]["observed_sources"] == 0
    assert bundle["person_monitor"] == {"enabled": False}


def test_openclaw_export_preserves_source_run_warning_ids(project_root: Path, project_config, project_paths) -> None:
    manifest_path = project_root / "data/raw/20260402T120000Z/manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["warnings"] = ["Instagram returned login/signup UI."]
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    NormalizationService(project_config, project_paths).run(run_id="20260402T120000Z")

    outputs = OpenClawExportService(project_config, project_paths).run(run_id="20260402T120000Z")

    bundle = json.loads(outputs.bundle_path.read_text(encoding="utf-8"))
    assert bundle["counts"]["warnings"] == 1
    assert bundle["warnings"] == [
        {
            "source_run_id": "20260402T120000Z",
            "warning_index": 1,
            "warning": "Instagram returned login/signup UI.",
        }
    ]


def test_openclaw_export_builds_person_monitor_section(project_paths) -> None:
    run_id = "pm-run-1"
    run_dir = project_paths.raw_root / run_id
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "collector": "instagram_web",
                "mode": "web",
                "status": "partial",
                "warnings": ["Instagram web discovery supports explicit public profiles only."],
                "source": {
                    "platform": "instagram",
                    "source_kind": "person_monitor",
                    "source_id": "subject",
                    "source_name": "Subject",
                    "source_url": "https://www.instagram.com/subject/",
                    "source_type": "profile",
                },
                "posts": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    project_paths.processed_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "run_id": [run_id],
            "collector": ["instagram_web"],
            "mode": ["web"],
            "status": ["partial"],
            "warning_count": [1],
            "warning_messages": [["Instagram web discovery supports explicit public profiles only."]],
            "post_count": [1],
            "propagation_count": [0],
            "comment_count": [1],
            "platform": ["instagram"],
            "source_kind": ["person_monitor"],
            "source_id": ["subject"],
            "source_name": ["Subject"],
            "source_type": ["profile"],
            "source_run_ids": [[run_id]],
        }
    ).write_parquet(project_paths.processed_root / "collection_runs.parquet")
    pl.DataFrame(
        {
            "run_id": [run_id],
            "platform": ["instagram"],
            "container_source_id": ["external"],
            "container_source_name": ["External"],
            "container_source_url": ["https://www.instagram.com/external/"],
            "container_source_type": ["profile"],
            "discovery_kind": ["watchlist"],
            "status": ["success"],
            "warning_count": [0],
        }
    ).write_parquet(project_paths.processed_root / "observed_sources.parquet")
    pl.DataFrame(
        {
            "match_id": ["match-1", "match-2"],
            "run_id": [run_id, run_id],
            "item_type": ["post", "comment"],
            "item_id": ["post-1", "comment-1"],
            "match_kind": ["alias_text_mention", "authored_by_subject"],
            "matched_value": ["Subject", "subject"],
            "platform": ["instagram", "instagram"],
            "container_source_id": ["external", "external"],
        }
    ).write_parquet(project_paths.processed_root / "match_hits.parquet")
    pl.DataFrame(
        {
            "post_id": ["post-1"],
            "run_id": [run_id],
            "message": ["A post mentioning Subject."],
            "permalink": ["https://www.instagram.com/p/abc/"],
            "container_source_id": ["external"],
            "container_source_name": ["External"],
        }
    ).write_parquet(project_paths.processed_root / "posts.parquet")
    pl.DataFrame(
        {
            "comment_id": ["comment-1"],
            "run_id": [run_id],
            "parent_post_id": ["post-1"],
            "message": ["Subject commented here."],
            "permalink": ["https://www.instagram.com/p/abc/c/comment-1/"],
            "container_source_id": ["external"],
            "container_source_name": ["External"],
        }
    ).write_parquet(project_paths.processed_root / "comments.parquet")

    outputs = OpenClawExportService(project_paths=project_paths, config=None).run(run_id=run_id)

    bundle = json.loads(outputs.bundle_path.read_text(encoding="utf-8"))
    assert bundle["person_monitor"]["enabled"] is True
    assert bundle["person_monitor"]["observed_sources"][0]["container_source_id"] == "external"
    assert bundle["person_monitor"]["match_breakdown"] == {
        "alias_text_mention": 1,
        "authored_by_subject": 1,
    }
    assert bundle["person_monitor"]["top_matched_posts"][0]["item_id"] == "post-1"
    assert bundle["person_monitor"]["top_matched_comments"][0]["item_id"] == "comment-1"
