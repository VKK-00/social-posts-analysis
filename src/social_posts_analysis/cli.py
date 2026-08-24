from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from .analysis.service import AnalysisService
from .config import ProjectConfig, load_config
from .normalize import NormalizationService
from .paths import ProjectPaths, project_root_for_config, relative_output_paths_warning
from .pipeline import CollectionService, PipelineRunner
from .reporting.service import ReportService, ReviewExportService

app = typer.Typer(add_completion=False, no_args_is_help=True)


def _version_callback(value: bool) -> None:
    if value:
        from importlib.metadata import version as package_version

        typer.echo(f"social-posts-analysis {package_version('social-posts-analysis')}")
        raise typer.Exit()


@app.callback()
def _main(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_version_callback,
        is_eager=True,
        help="Show the installed version and exit.",
    ),
) -> None:
    """Local-first social media collection and narrative analysis pipeline."""


def _load_project(config_path: Path) -> tuple[Path, ProjectPaths, ProjectConfig]:
    root = project_root_for_config(config_path)
    config = load_config(config_path)
    warning = relative_output_paths_warning(config_path, config)
    if warning:
        typer.echo(f"Warning: {warning}", err=True)
    paths = ProjectPaths.from_config(root, config)
    paths.ensure()
    return root, paths, config


@app.command()
def collect(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
) -> None:
    _, paths, config = _load_project(config_path)
    service = CollectionService(config=config, paths=paths)
    manifest = service.run(run_id=run_id)
    typer.echo(f"Collected run {manifest.run_id} with {len(manifest.posts)} posts.")


@app.command()
def normalize(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
) -> None:
    _, paths, config = _load_project(config_path)
    service = NormalizationService(config=config, paths=paths)
    summary = service.run(run_id=run_id)
    typer.echo(f"Normalized run {summary['run_id']} into DuckDB/parquet tables.")


@app.command()
def analyze(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
) -> None:
    _, paths, config = _load_project(config_path)
    service = AnalysisService(config=config, paths=paths)
    summary = service.run(run_id=run_id)
    typer.echo(f"Analyzed run {summary['run_id']} with providers {summary['providers']}.")


@app.command("review-export")
def review_export(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
) -> None:
    _, paths, config = _load_project(config_path)
    service = ReviewExportService(config=config, paths=paths)
    outputs = service.run(run_id=run_id)
    typer.echo(f"Review files written: {', '.join(str(path) for path in outputs)}")


@app.command()
def report(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
) -> None:
    _, paths, config = _load_project(config_path)
    service = ReportService(config=config, paths=paths)
    outputs = service.run(run_id=run_id)
    typer.echo(f"Report files written: {', '.join(str(path) for path in outputs)}")


@app.command("export-tables")
def export_tables(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
) -> None:
    _, paths, config = _load_project(config_path)
    service = ReportService(config=config, paths=paths)
    outputs = service.run_tabular(run_id=run_id)
    typer.echo(f"Tabular exports written: {', '.join(str(path) for path in outputs)}")


@app.command("run-all")
def run_all(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
) -> None:
    _, paths, config = _load_project(config_path)
    runner = PipelineRunner(config=config, paths=paths)
    summary = runner.run(run_id=run_id)
    typer.echo(
        "Completed run {run_id}. Report: {report}".format(
            run_id=summary["run_id"],
            report=summary["report_markdown"],
        )
    )


@app.command("run-many")
def run_many(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    passes: int = typer.Option(3, "--passes", min=1),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
) -> None:
    _, paths, config = _load_project(config_path)
    original_passes = config.collector.multi_pass_runs
    config.collector.multi_pass_runs = passes
    try:
        runner = PipelineRunner(config=config, paths=paths)
        summary = runner.run(run_id=run_id)
    finally:
        config.collector.multi_pass_runs = original_passes
    typer.echo(
        "Completed multi-pass run {run_id}. Report: {report}".format(
            run_id=summary["run_id"],
            report=summary["report_markdown"],
        )
    )


@app.command("scrape-page")
def scrape_page(
    url: str = typer.Option(..., "--url", help="Any social-media page URL to capture in the browser."),
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    scrolls: int = typer.Option(6, "--scrolls", min=0, help="Human-paced scroll passes."),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
) -> None:
    """Scrape a single social-media page through the configured browser session.

    Uses the logged-in profile when authenticated browser mode is enabled.
    Login walls and CAPTCHAs are reported as warnings - they are never bypassed.
    """
    from .page_scrape import PageScrapeService

    _, paths, config = _load_project(config_path)
    service = PageScrapeService(config=config, paths=paths)
    manifest = service.run(url=url, run_id=run_id, max_scrolls=scrolls)
    typer.echo(
        f"Scraped {manifest.run_id}: {len(manifest.posts)} posts extracted, "
        f"{len(manifest.warnings)} warning(s), status={manifest.status}."
    )


if __name__ == "__main__":
    app()
