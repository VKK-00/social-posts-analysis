from __future__ import annotations

from pathlib import Path

from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.paths import ProjectPaths, project_root_for_config, relative_output_paths_warning


def test_project_root_for_config_uses_parent_parent_for_config_directory(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    config_dir = project_root / "config"
    config_dir.mkdir(parents=True)
    config_path = config_dir / "project.local.yaml"
    config_path.write_text("project_name: test\n", encoding="utf-8")

    assert project_root_for_config(config_path) == project_root.resolve()


def test_project_root_for_config_uses_config_parent_when_config_is_outside_project_config_dir(tmp_path: Path) -> None:
    config_dir = tmp_path / "temp-configs"
    config_dir.mkdir(parents=True)
    config_path = config_dir / "project.local.yaml"
    config_path.write_text("project_name: test\n", encoding="utf-8")

    assert project_root_for_config(config_path) == config_dir.resolve()


def test_project_paths_resolve_relative_and_absolute_values(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    external_dir = tmp_path / "external"
    external_dir.mkdir()
    external_db = external_dir / "social.duckdb"

    config = ProjectConfig.model_validate(
        {
            "source": {"platform": "facebook", "url": "https://www.facebook.com/example"},
            "sides": [{"side_id": "a", "name": "A"}],
            "paths": {
                "raw_dir": "data/raw",
                "processed_dir": str(external_dir),
                "review_dir": "review",
                "reports_dir": "reports",
                "database_path": str(external_db),
            },
        }
    )

    paths = ProjectPaths.from_config(project_root, config)

    assert paths.raw_root == project_root / "data/raw"
    assert paths.processed_root == external_dir
    assert paths.review_root == project_root / "review"
    assert paths.reports_root == project_root / "reports"
    assert paths.database_path == external_db


def test_relative_output_paths_warning_for_temp_config_outside_project(tmp_path: Path) -> None:
    temp_config_dir = tmp_path / "temp-configs"
    temp_config_dir.mkdir(parents=True)
    config_path = temp_config_dir / "project.local.yaml"
    config_path.write_text("project_name: test\n", encoding="utf-8")
    config = ProjectConfig.model_validate(
        {
            "source": {"platform": "facebook", "url": "https://www.facebook.com/example"},
            "sides": [{"side_id": "a", "name": "A"}],
            "paths": {
                "raw_dir": "data/raw",
                "processed_dir": "data/processed",
                "review_dir": "review",
                "reports_dir": "reports",
                "database_path": "data/processed/social_posts_analysis.duckdb",
            },
        }
    )

    warning = relative_output_paths_warning(config_path, config)

    assert warning is not None
    assert str(config_path.resolve()) in warning
    assert str(temp_config_dir.resolve()) in warning
    assert "raw_dir" in warning
    assert "database_path" in warning


def test_relative_output_paths_warning_is_none_for_config_directory(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    config_dir = project_root / "config"
    config_dir.mkdir(parents=True)
    config_path = config_dir / "project.local.yaml"
    config_path.write_text("project_name: test\n", encoding="utf-8")
    config = ProjectConfig.model_validate(
        {
            "source": {"platform": "facebook", "url": "https://www.facebook.com/example"},
            "sides": [{"side_id": "a", "name": "A"}],
        }
    )

    assert relative_output_paths_warning(config_path, config) is None
