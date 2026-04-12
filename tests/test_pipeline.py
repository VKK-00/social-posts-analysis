from __future__ import annotations

from pathlib import Path

from social_posts_analysis.contracts import CollectionManifest, SourceSnapshot
from social_posts_analysis.pipeline import CollectionService, PipelineRunner


def test_pipeline_runner_passes_exact_collection_run_ids_to_normalization(project_config, project_paths, monkeypatch) -> None:
    manifests = [
        CollectionManifest(
            run_id="20260411T120000Z-p01",
            collected_at="2026-04-11T12:00:00+00:00",
            collector="public_web",
            mode="web",
            source=SourceSnapshot(
                platform="facebook",
                source_id="page_1",
                source_name="Example Page",
                source_url="https://www.facebook.com/example-page/",
                source_type="page",
                source_collector="public_web",
                raw_path="raw-1.json",
            ),
            posts=[],
        ),
        CollectionManifest(
            run_id="20260411T120000Z-p02",
            collected_at="2026-04-11T12:05:00+00:00",
            collector="public_web",
            mode="web",
            source=SourceSnapshot(
                platform="facebook",
                source_id="page_1",
                source_name="Example Page",
                source_url="https://www.facebook.com/example-page/",
                source_type="page",
                source_collector="public_web",
                raw_path="raw-2.json",
            ),
            posts=[],
        ),
    ]
    captured: dict[str, object] = {}

    def fake_run_many(self, run_id=None, passes=None):  # noqa: ANN001, ANN202
        return manifests

    def fake_normalize_run(self, run_id=None, source_run_ids=None):  # noqa: ANN001, ANN202
        captured["run_id"] = run_id
        captured["source_run_ids"] = source_run_ids
        return {"run_id": run_id, "source_run_ids": source_run_ids, "reused_existing_run": False, "tables": {}}

    def fake_analysis_run(self, run_id=None):  # noqa: ANN001, ANN202
        return {"run_id": run_id}

    def fake_review_run(self, run_id=None):  # noqa: ANN001, ANN202
        return []

    def fake_report_run(self, run_id=None):  # noqa: ANN001, ANN202
        markdown_path = project_paths.reports_root / f"report_{run_id}.md"
        html_path = project_paths.reports_root / f"report_{run_id}.html"
        markdown_path.write_text("# report", encoding="utf-8")
        html_path.write_text("<html></html>", encoding="utf-8")
        return [markdown_path, html_path]

    monkeypatch.setattr("social_posts_analysis.pipeline.CollectionService.run_many", fake_run_many)
    monkeypatch.setattr("social_posts_analysis.normalize.NormalizationService.run", fake_normalize_run)
    monkeypatch.setattr("social_posts_analysis.analysis.service.AnalysisService.run", fake_analysis_run)
    monkeypatch.setattr("social_posts_analysis.reporting.service.ReviewExportService.run", fake_review_run)
    monkeypatch.setattr("social_posts_analysis.reporting.service.ReportService.run", fake_report_run)

    result = PipelineRunner(project_config, project_paths).run(run_id="20260411T120000Z")

    assert captured["run_id"] == "20260411T120000Z-p02"
    assert captured["source_run_ids"] == ["20260411T120000Z-p01", "20260411T120000Z-p02"]
    assert result["run_id"] == "20260411T120000Z-p02"
    assert Path(result["report_markdown"]).exists()


def test_collection_service_reuses_existing_manifest_for_same_request(project_config, project_paths, monkeypatch) -> None:
    existing_manifest = CollectionManifest(
        run_id="20260411T121500Z",
        collected_at="2026-04-11T12:15:00+00:00",
        requested_date_start="2026-04-01",
        requested_date_end="2026-04-10",
        collector="public_web",
        mode="web",
        source=SourceSnapshot(
            platform="facebook",
            source_id="page_1",
            source_name="Example Page",
            source_url="https://www.facebook.com/example-page/",
            source_type="page",
            source_collector="public_web",
            raw_path="raw.json",
        ),
        posts=[],
    )
    run_dir = project_paths.run_raw_dir(existing_manifest.run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "manifest.json").write_text(existing_manifest.model_dump_json(indent=2), encoding="utf-8")

    project_config.source.platform = "facebook"
    project_config.source.source_id = "page_1"
    project_config.source.source_name = "Example Page"
    project_config.source.url = "https://www.facebook.com/example-page/"
    project_config.collector.mode = "web"
    project_config.date_range.start = "2026-04-01"
    project_config.date_range.end = "2026-04-10"

    def fail_build_collectors(self):  # noqa: ANN001, ANN202
        raise AssertionError("Collectors should not run when the existing manifest matches the current request.")

    monkeypatch.setattr("social_posts_analysis.pipeline.CollectionService._build_collectors", fail_build_collectors)

    manifest = CollectionService(project_config, project_paths).run(run_id=existing_manifest.run_id)

    assert manifest.run_id == existing_manifest.run_id
    assert manifest.requested_date_start == "2026-04-01"
    assert manifest.requested_date_end == "2026-04-10"


def test_collection_service_recollects_when_existing_manifest_request_differs(project_config, project_paths, monkeypatch) -> None:
    existing_manifest = CollectionManifest(
        run_id="20260411T121500Z",
        collected_at="2026-04-11T12:15:00+00:00",
        requested_date_start="2026-04-01",
        requested_date_end="2026-04-10",
        collector="public_web",
        mode="web",
        source=SourceSnapshot(
            platform="facebook",
            source_id="page_1",
            source_name="Example Page",
            source_url="https://www.facebook.com/example-page/",
            source_type="page",
            source_collector="public_web",
            raw_path="raw.json",
        ),
        posts=[],
    )
    run_dir = project_paths.run_raw_dir(existing_manifest.run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "manifest.json").write_text(existing_manifest.model_dump_json(indent=2), encoding="utf-8")
    stale_file = run_dir / "stale.txt"
    stale_file.write_text("old", encoding="utf-8")

    project_config.source.platform = "facebook"
    project_config.source.source_id = "page_1"
    project_config.source.source_name = "Example Page"
    project_config.source.url = "https://www.facebook.com/example-page/"
    project_config.collector.mode = "web"
    project_config.date_range.start = "2026-04-11"
    project_config.date_range.end = "2026-04-20"

    class FakeCollector:
        def collect(self, run_id, raw_store):  # noqa: ANN001, ANN202
            assert not stale_file.exists()
            return CollectionManifest(
                run_id=run_id,
                collected_at="2026-04-11T12:30:00+00:00",
                collector="public_web",
                mode="web",
                source=SourceSnapshot(
                    platform="facebook",
                    source_id="page_1",
                    source_name="Example Page",
                    source_url="https://www.facebook.com/example-page/",
                    source_type="page",
                    source_collector="public_web",
                    raw_path="fresh.json",
                ),
                posts=[],
            )

    monkeypatch.setattr(
        "social_posts_analysis.pipeline.CollectionService._build_collectors",
        lambda self: [FakeCollector()],
    )

    manifest = CollectionService(project_config, project_paths).run(run_id=existing_manifest.run_id)

    assert manifest.requested_date_start == "2026-04-11"
    assert manifest.requested_date_end == "2026-04-20"
    assert not stale_file.exists()
