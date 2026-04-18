from __future__ import annotations

from pathlib import Path

from social_posts_analysis.config import load_config
from social_posts_analysis.paths import ProjectPaths, project_root_for_config, relative_output_paths_warning


def test_telegram_mtproto_history_smoke_config_loads_with_env_credentials(monkeypatch) -> None:
    config_path = Path("config/smoke/telegram_mtproto_history.yaml")
    monkeypatch.setenv("TELEGRAM_SESSION_FILE", ".sessions/smoke")
    monkeypatch.setenv("TELEGRAM_API_ID", "12345")
    monkeypatch.setenv("TELEGRAM_API_HASH", "hash")

    config = load_config(config_path)
    root = project_root_for_config(config_path)
    paths = ProjectPaths.from_config(root, config)

    assert config.source.platform == "telegram"
    assert config.source.source_name == "durov"
    assert config.collector.mode == "mtproto"
    assert config.collector.telegram_mtproto.enabled is True
    assert config.history.start == "2026-01-01"
    assert config.history.end == "2026-03-31"
    assert config.history.max_windows == 3
    assert config.history.max_items_per_window == 100
    assert config.history.max_comments_per_post == 200
    assert paths.raw_root == root / "data/smoke/raw"
    assert paths.reports_root == root / "reports/smoke"
    assert relative_output_paths_warning(config_path, config) is None
