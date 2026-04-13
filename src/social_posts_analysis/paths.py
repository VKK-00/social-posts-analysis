from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .config import ProjectConfig


def project_root_for_config(config_path: str | Path) -> Path:
    resolved = Path(config_path).resolve()
    return resolved.parent.parent if resolved.parent.name == "config" else resolved.parent


def resolve_project_path(root: Path, value: str | Path) -> Path:
    candidate = Path(value)
    return candidate if candidate.is_absolute() else root / candidate


def relative_output_paths_warning(config_path: str | Path, config: ProjectConfig) -> str | None:
    resolved_config_path = Path(config_path).resolve()
    if resolved_config_path.parent.name == "config":
        return None

    relative_fields = [
        field_name
        for field_name, raw_value in {
            "raw_dir": config.paths.raw_dir,
            "processed_dir": config.paths.processed_dir,
            "review_dir": config.paths.review_dir,
            "reports_dir": config.paths.reports_dir,
            "database_path": config.paths.database_path,
        }.items()
        if not Path(raw_value).is_absolute()
    ]
    if not relative_fields:
        return None

    root = project_root_for_config(resolved_config_path)
    fields = ", ".join(relative_fields)
    return (
        f"Config file {resolved_config_path} is outside a ./config directory. "
        f"Relative output paths ({fields}) will be resolved relative to {root}. "
        "Use absolute paths in the config if you want outputs elsewhere."
    )


@dataclass(slots=True)
class ProjectPaths:
    root: Path
    raw_root: Path
    processed_root: Path
    review_root: Path
    reports_root: Path
    database_path: Path

    @classmethod
    def from_config(cls, root: Path, config: ProjectConfig) -> "ProjectPaths":
        return cls(
            root=root,
            raw_root=resolve_project_path(root, config.paths.raw_dir),
            processed_root=resolve_project_path(root, config.paths.processed_dir),
            review_root=resolve_project_path(root, config.paths.review_dir),
            reports_root=resolve_project_path(root, config.paths.reports_dir),
            database_path=resolve_project_path(root, config.paths.database_path),
        )

    def ensure(self) -> None:
        self.raw_root.mkdir(parents=True, exist_ok=True)
        self.processed_root.mkdir(parents=True, exist_ok=True)
        self.review_root.mkdir(parents=True, exist_ok=True)
        self.reports_root.mkdir(parents=True, exist_ok=True)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)

    def run_raw_dir(self, run_id: str) -> Path:
        return self.raw_root / run_id

    def latest_run_id(self) -> str | None:
        run_ids = self.list_run_ids()
        return run_ids[-1] if run_ids else None

    def list_run_ids(self) -> list[str]:
        if not self.raw_root.exists():
            return []
        run_dirs = [item for item in self.raw_root.iterdir() if item.is_dir() and (item / "manifest.json").exists()]
        return sorted(item.name for item in run_dirs)
