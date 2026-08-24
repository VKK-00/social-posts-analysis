# Changelog

All notable changes to this project are documented here.

## 0.2.0 — research-grade hardening

A ten-stage overhaul turning the pipeline into a reproducible, publishable
research tool. 12 commits; no breaking config removals (new keys are opt-in).

### Data validity

- Deterministic `HashEmbeddingProvider` (sha256 feature hashing instead of
  Python's salted `hash()`) — cached embeddings are now stable across runs.
- Stance cache key includes the full `sides` configuration, so changing
  names/aliases/keywords invalidates stale LLM labels.
- Stable Facebook `comment_id` values even without permalinks.
- Telegram MTProto walks pages back until `date_range.start` is covered and
  warns when the scan bound stops collection early.
- Missing tables yield typed empty frames instead of `ColumnNotFoundError`.
- Config validation: ISO date checks for `date_range`, numeric bounds
  (`page_size >= 1`, positive timeouts, non-negative retries/scrolls),
  loud warnings on malformed integer env variables.
- Normalized-run reuse compares source run ids order-independently.

### Research features

- Wilson score intervals (95%) around `support_ratio` in support metrics.
- Cascade shape metrics per discussion tree / propagation star: size, depth,
  breadth, structural virality (Goel et al. 2015).
- Near-duplicate detection across posts/propagations/comments via
  deterministic MinHash + LSH banding (no new dependencies), tunable with
  `analysis.near_duplicate_threshold`.
- c-TF-IDF cluster keywords (BERTopic-style weighting) replacing raw term
  counting; multilingual ru/uk/en stopword list (~120 words).
- Optional fasttext (lid.176) language backend behind
  `analysis.language_method`, with graceful fallback.
- Author pseudonymization mode (`normalization.pseudonymize_authors`) for
  GDPR-safe exports; the analysed source stays identifiable.

### Security and resilience

- Meta API token sent via `Authorization: Bearer` header, not query string;
  same hardening applied to Telegram Bot API token handling.
- Atomic raw snapshot writes (temp file + atomic replace).
- HTTP 429 handling honouring `Retry-After` across Meta/X/Bot APIs and the
  OpenAI-compatible embedding/LLM providers.
- Chrome profile path masked in collector warnings; failed temp-profile
  cleanup is surfaced instead of silently ignored.

### Engineering

- Shared `WebTimelineCollector` base class for instagram/x/threads web
  collectors (-237 duplicated lines); platform registry replaces if/elif
  chains; dead code removed (StanceAnalyzer, pydantic-settings).
- Selector packs: Facebook DOM hooks centralized as documented constants,
  injected into evaluate() scripts with a leak-guard test.
- Single shared parquet IO module (append+dedupe, typed reads) replacing
  three copies of the logic.
- Review overrides applied via hash joins instead of row-wise callbacks.
- ruff format enforced; CI adds coverage, package build, CLI smoke test,
  and a `live` pytest marker.

### Infrastructure

- Optional `[semantic]` extra (sentence-transformers) for offline
  multilingual embeddings; embedding cache keyed by resolved model name.
- pytest-cov in dev extras (~77% coverage).
- README documents all research features; examples/analysis_queries.sql
  provides ready-made DuckDB research queries.
