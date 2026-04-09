# Facebook Posts Analysis

Local-first Python pipeline for collecting Facebook posts and comments, then analyzing narratives, stance, support, and conflict patterns.

The project supports two collection paths:

- Meta API, when the target object and permissions allow it
- Playwright-based web collection, including reuse of a locally logged-in browser profile

Outputs are stored locally and can be reviewed in DuckDB, parquet, CSV, Markdown, and HTML.

## What The Project Does

- collects posts, comments, and visible replies from Facebook
- stores raw snapshots per run under `data/raw/<run_id>/`
- normalizes collected data into parquet files and DuckDB tables
- detects language for `ru`, `uk`, and `en`
- groups posts and comments into narrative clusters
- labels stance toward configured sides or actors
- computes support metrics globally and across several scopes
- exports review files for manual corrections
- renders Markdown and HTML reports

Current codebase additions include:

- authenticated browser profile support for the web collector
- multi-pass collection runs
- merged normalized snapshots from several source runs
- visible reply-depth extraction
- coverage-gap reporting for posts where visible counters exceed extracted text comments

## Project Layout

```text
config/project.yaml
src/facebook_posts_analysis/
  analysis/
  collectors/
  reporting/
tests/
```

## Requirements

- Python 3.12+
- recommended: `uv`
- Playwright plus Chromium if using the web collector
- a valid Meta token if using the API collector

## Installation

Using `uv`:

```bash
uv venv
uv sync --extra dev
```

Using `pip`:

```bash
python -m venv .venv
```

Activate the environment:

- Windows PowerShell: `.venv\Scripts\Activate.ps1`
- macOS/Linux: `source .venv/bin/activate`

Then install the package:

```bash
python -m pip install -e .[dev]
```

If you plan to use Playwright:

```bash
python -m playwright install chromium
```

## Configuration

The checked-in `config/project.yaml` is a safe public template. It should be treated as an example, not as a real working target-specific config.

For actual runs, create a private local file such as `config/project.local.yaml` and pass it explicitly with `--config`. That local file should contain the real page or profile target, date range, local browser profile paths, API tokens, and provider settings.

Important settings:

- `page.url` or `page.page_id`
- `date_range.start` and `date_range.end`
- `collector.mode`: `api`, `web`, or `hybrid`
- `collector.multi_pass_runs`
- `collector.wait_between_passes_seconds`
- `collector.public_web.authenticated_browser.*`
- `normalization.merge_recent_runs`
- `sides`: stance targets
- `providers.embeddings` and `providers.llm`

Environment variables supported by default:

- `META_ACCESS_TOKEN`
- `FACEBOOK_BROWSER_USER_DATA_DIR`
- `FACEBOOK_BROWSER_PROFILE_DIRECTORY`
- `EMBEDDING_BASE_URL`
- `EMBEDDING_API_KEY`
- `LLM_BASE_URL`
- `LLM_API_KEY`

## Quickstart

1. Copy the public template:

```bash
cp config/project.yaml config/project.local.yaml
```

Windows PowerShell:

```powershell
Copy-Item config/project.yaml config/project.local.yaml
```

2. Edit `config/project.local.yaml`.
3. Run one of the modes below with `--config config/project.local.yaml`.

### Public Web Collection

Use this when you want public-only scraping without a logged-in browser.

Minimal settings:

```yaml
collector:
  mode: "web"
  public_web:
    enabled: true
    authenticated_browser:
      enabled: false
```

Run:

```bash
facebook-posts-analysis run-many --config config/project.local.yaml --passes 3
```

Expected result:

- posts and visible comments collected from public Facebook pages
- best-effort coverage only
- repeated passes can improve coverage

### Authenticated Browser Collection

Use this when public DOM is too shallow and your local browser session can see more content.

Minimal settings:

```yaml
collector:
  mode: "web"
  public_web:
    enabled: true
    authenticated_browser:
      enabled: true
      browser: "chrome"
      profile_directory: "Default"
      copy_profile: true
```

Optional environment variables:

```bash
export FACEBOOK_BROWSER_USER_DATA_DIR="/path/to/browser/User Data"
export FACEBOOK_BROWSER_PROFILE_DIRECTORY="Default"
```

Windows PowerShell:

```powershell
$env:FACEBOOK_BROWSER_USER_DATA_DIR="C:\Users\<user>\AppData\Local\Google\Chrome\User Data"
$env:FACEBOOK_BROWSER_PROFILE_DIRECTORY="Default"
```

Run:

```bash
facebook-posts-analysis run-many --config config/project.local.yaml --passes 3
```

Expected result:

- deeper comment extraction than public-only mode
- still limited to what the logged-in account can see
- no credentials stored in the project

### Meta API Collection

Use this when the target object and your Meta permissions allow API access.

Minimal settings:

```yaml
collector:
  mode: "api"
  meta_api:
    enabled: true
```

Environment variable:

```bash
export META_ACCESS_TOKEN="your-token"
```

Windows PowerShell:

```powershell
$env:META_ACCESS_TOKEN="your-token"
```

Run:

```bash
facebook-posts-analysis run-all --config config/project.local.yaml
```

Expected result:

- more stable structure than web scraping
- still depends on Meta permissions and target object type
- page or profile access is not guaranteed

## Usage

Full pipeline:

```bash
facebook-posts-analysis run-all --config config/project.local.yaml
```

Multi-pass full pipeline:

```bash
facebook-posts-analysis run-many --config config/project.local.yaml --passes 3
```

Step by step:

```bash
facebook-posts-analysis collect --config config/project.local.yaml
facebook-posts-analysis normalize --config config/project.local.yaml --run-id <run_id>
facebook-posts-analysis analyze --config config/project.local.yaml --run-id <run_id>
facebook-posts-analysis review-export --config config/project.local.yaml --run-id <run_id>
facebook-posts-analysis report --config config/project.local.yaml --run-id <run_id>
facebook-posts-analysis export-tables --config config/project.local.yaml --run-id <run_id>
```

If you use another config path, replace `config/project.local.yaml` in the commands above.

## CLI Commands

The package exposes the `facebook-posts-analysis` CLI with:

- `collect`
- `normalize`
- `analyze`
- `review-export`
- `report`
- `export-tables`
- `run-all`
- `run-many`

`run-many` is useful for unstable public-web collection, because Facebook can reveal slightly different content across repeated passes.

## Output Tables

Normalized tables:

- `posts.parquet`
- `comments.parquet`
- `comment_edges.parquet`
- `authors.parquet`
- `media_refs.parquet`
- `collection_runs.parquet`

Analysis tables:

- `detected_languages.parquet`
- `cluster_memberships.parquet`
- `narrative_clusters.parquet`
- `stance_labels.parquet`
- `support_metrics.parquet`
- `analysis_runs.parquet`

Review files:

- `review/narrative_overrides.csv`
- `review/stance_overrides.csv`

Reports:

- `reports/report_<run_id>.md`
- `reports/report_<run_id>.html`
- `reports/report_<run_id>.xlsx`
- `reports/report_<run_id>_tables/*.csv`

## Authenticated Browser Mode

For web collection, the safest supported approach is reusing an already logged-in local browser profile rather than storing credentials in the project.

Current config supports:

- Chrome
- Edge
- custom user-data directory

The collector can launch a copied snapshot of the browser profile, which reduces the chance of interfering with a live browser session.

## Private Local Files

These files or directories should stay local and should not be committed:

- `config/project.local.yaml`
- `data/`
- `reports/`
- `review/`
- local browser profile paths
- API tokens and provider keys
- virtual environments and cache directories

## Testing

Run:

```bash
uv run ruff check .
uv run mypy src
uv run pytest
```

The test suite currently covers:

- Meta API pagination and nested comments
- public-web parsing and timestamp handling
- reply and control-line cleanup
- comment hierarchy construction from visible nesting
- normalization and merged snapshots
- analysis helpers and support metrics
- review override application
- collection fallback and multi-pass behavior

## CI

GitHub Actions runs:

- `ruff check .`
- `mypy src`
- `pytest -q`

## Important Limits

- The public-web collector is best-effort. Facebook can expose different DOM states across runs.
- Authenticated browser mode still only sees what the logged-in account can see.
- Some posts may still show a visible comment counter while not yielding full text comments in the DOM.
- API-first collection depends on the current Meta permission model and the target object type.
- Heuristic fallback providers keep the pipeline usable offline, but proper embeddings and LLM providers will produce better analytical quality.

## License

This project is licensed under the MIT License. See `LICENSE`.
