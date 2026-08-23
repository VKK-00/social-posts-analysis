from __future__ import annotations

from typing import Any, Literal, Protocol

from social_posts_analysis.config import AuthenticatedBrowserConfig, ProjectConfig
from social_posts_analysis.contracts import (
    CollectionManifest,
    CommentSnapshot,
    PostSnapshot,
    SourceSnapshot,
)
from social_posts_analysis.raw_store import RawSnapshotStore
from social_posts_analysis.utils import utc_now_iso

from .base import BaseCollector, CollectorUnavailableError
from .range_utils import RangeFilter
from .web_runtime import WebCollectorRuntime, ensure_playwright_available, open_web_runtime, scroll_page


class WebTimelineSettings(Protocol):
    """Structural view of the settings shared by all web timeline collectors."""

    enabled: bool
    headless: bool
    browser_channel: str | None
    max_scrolls: int
    wait_after_scroll_ms: int
    timeout_seconds: float
    authenticated_browser: AuthenticatedBrowserConfig


WebPlatform = Literal["facebook", "telegram", "x", "threads", "instagram"]


class WebTimelineCollector(BaseCollector):
    """Shared skeleton for collectors that scrape a profile timeline and its detail pages.

    The template in :meth:`collect` implements the flow that is identical across the web
    collectors (open profile page -> scroll timeline -> parse posts -> open detail pages ->
    extract comments). Platform differences stay behind small hooks and class attributes;
    payload parsing, permalink/id conventions, and comment field mapping remain per-platform.
    """

    platform: WebPlatform
    wheel_y: int = 2600
    min_detail_scroll_passes: int = 3
    allow_missing_created_at: bool = False
    profile_copy_prefix: str = "web-profile-"
    disabled_error_message: str
    requirements_error_message: str
    custom_user_data_error: str

    config: ProjectConfig
    settings: WebTimelineSettings
    range_filter: RangeFilter

    def __init__(self, config: ProjectConfig) -> None:
        self.config = config
        self.settings = getattr(config.collector, self.name)
        self.range_filter = RangeFilter.from_strings(config.date_range.start, config.date_range.end)
        if not self.settings.enabled:
            raise CollectorUnavailableError(self.disabled_error_message)
        ensure_playwright_available(self.requirements_error_message)

    def collect(self, run_id: str, raw_store: RawSnapshotStore) -> CollectionManifest:
        from playwright.sync_api import sync_playwright

        warnings = [self._initial_warning()]
        profile_url = self._resolve_profile_url()
        with sync_playwright() as playwright:
            runtime = self._open_collection_context(playwright)
            warnings.extend(runtime.warnings)
            try:
                page = runtime.context.new_page()
                page.goto(
                    profile_url,
                    wait_until="domcontentloaded",
                    timeout=int(self.settings.timeout_seconds * 1000),
                )
                self._prepare_profile_page(page)
                self._scroll_timeline(page)
                payload = self._extract_profile_payload(page)
                source_path = raw_store.write_json(f"{self.name}_source", "profile_feed", payload)
                source_name = payload.get("source_name") or self.config.source.source_name or self._source_reference()
                source_id = payload.get("source_id") or self._source_reference()
                posts = self._build_posts_from_payload(
                    payload, source_id=source_id, source_name=source_name, raw_store=raw_store
                )
                updated_posts: list[PostSnapshot] = []
                for post in posts:
                    comments = self._collect_comments_for_post(context=runtime.context, post=post, raw_store=raw_store)
                    missing_comments_warning = self._missing_comments_warning(post, comments)
                    if missing_comments_warning:
                        warnings.append(missing_comments_warning)
                    updated_posts.append(
                        post.model_copy(
                            update={"comments": comments, "comments_count": max(post.comments_count, len(comments))}
                        )
                    )
            finally:
                runtime.close()

        source_snapshot = SourceSnapshot(
            platform=self.platform,
            source_id=source_id,
            source_name=source_name,
            source_url=profile_url,
            source_type="account",
            source_collector=self.name,
            raw_path=str(source_path),
        )
        return CollectionManifest(
            run_id=run_id,
            collected_at=utc_now_iso(),
            collector=self.name,
            mode=self.config.collector.mode,
            status="partial" if warnings else "success",
            warnings=self._finalize_warnings(warnings),
            source=source_snapshot,
            posts=updated_posts,
        )

    # Shared browser plumbing -------------------------------------------------

    def _open_collection_context(self, playwright: Any) -> WebCollectorRuntime:
        return open_web_runtime(
            playwright,
            headless=self.settings.headless,
            browser_channel=self.settings.browser_channel,
            viewport={"width": 1400, "height": 1800},
            authenticated_browser=self.settings.authenticated_browser,
            profile_copy_prefix=self.profile_copy_prefix,
            custom_user_data_error=self.custom_user_data_error,
        )

    def _scroll_timeline(self, page: Any, *, passes: int | None = None) -> None:
        scroll_page(
            page,
            max_scrolls=self.settings.max_scrolls,
            wait_after_scroll_ms=self.settings.wait_after_scroll_ms,
            passes=passes,
            wheel_y=self.wheel_y,
        )

    def _fetch_detail_payload(self, *, context: Any, post: PostSnapshot) -> dict[str, Any]:
        page = context.new_page()
        try:
            page.goto(
                post.permalink,
                wait_until="domcontentloaded",
                timeout=int(self.settings.timeout_seconds * 1000),
            )
            self._prepare_detail_page(page)
            self._scroll_timeline(page, passes=max(self.min_detail_scroll_passes, self.settings.max_scrolls // 2))
            return self._extract_detail_payload(page)
        finally:
            page.close()

    def _within_range(self, created_at: str | None) -> bool:
        return self.range_filter.contains(created_at, allow_missing=self.allow_missing_created_at)

    # Hooks -------------------------------------------------------------------

    def _initial_warning(self) -> str:
        raise NotImplementedError

    def _prepare_profile_page(self, page: Any) -> None:
        return None

    def _prepare_detail_page(self, page: Any) -> None:
        return None

    def _missing_comments_warning(self, post: PostSnapshot, comments: list[CommentSnapshot]) -> str | None:
        del post, comments
        return None

    def _finalize_warnings(self, warnings: list[str]) -> list[str]:
        return warnings

    # Platform-specific contracts ---------------------------------------------

    def _resolve_profile_url(self) -> str:
        raise NotImplementedError

    def _source_reference(self) -> str:
        raise NotImplementedError

    def _build_posts_from_payload(
        self,
        payload: dict[str, Any],
        *,
        source_id: str,
        source_name: str,
        raw_store: RawSnapshotStore,
    ) -> list[PostSnapshot]:
        raise NotImplementedError

    def _extract_profile_payload(self, page: Any) -> dict[str, Any]:
        raise NotImplementedError

    def _extract_detail_payload(self, page: Any) -> dict[str, Any]:
        raise NotImplementedError

    def _collect_comments_for_post(
        self, *, context: Any, post: PostSnapshot, raw_store: RawSnapshotStore
    ) -> list[CommentSnapshot]:
        raise NotImplementedError
