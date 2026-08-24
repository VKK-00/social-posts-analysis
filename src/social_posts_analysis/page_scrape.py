"""Browser-based scraping of an arbitrary social-media page.

This is deliberately *not* an anti-bot bypass tool: it does not solve
CAPTCHAs, spoof fingerprints beyond a normal logged-in browser profile, or
circumvent authentication walls. What it does is drive a real browser the way
a person would — reusing the user's own logged-in session when configured,
pacing actions with randomized human-like delays — and honestly report login
walls and CAPTCHAs as warnings instead of trying to defeat them.
"""

from __future__ import annotations

import random
import re
from typing import Any, Literal
from urllib.parse import urlsplit

from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.contracts import (
    CollectionManifest,
    PostSnapshot,
    SourceSnapshot,
)
from social_posts_analysis.paths import ProjectPaths
from social_posts_analysis.raw_store import RawSnapshotStore
from social_posts_analysis.utils import stable_id, utc_now_iso

_PLATFORM_HOST_PATTERNS: dict[str, tuple[str, ...]] = {
    "facebook": (r"(^|\.)facebook\.com$",),
    "instagram": (r"(^|\.)instagram\.com$",),
    "threads": (r"(^|\.)threads\.net$", r"(^|\.)threads\.com$"),
    "x": (r"(^|\.)(twitter|x)\.com$",),
    "telegram": (r"(^|\.)t\.me$",),
}

_CAPTCHA_MARKERS = ("captcha", "security check", "перевірка безпеки", "підтвердіть, що ви людина")
_LOGIN_WALL_MARKERS = (
    "log in",
    "forgot password?",
    "create new account",
    "see more on facebook",
    "email or phone number",
    "password",
    "увійдіть",
    "забули пароль",
)
_LOGIN_WALL_MIN_MARKERS = 2


def looks_like_login_wall(body_text: str) -> bool:
    normalized = (body_text or "").lower().replace("\xa0", " ")
    hits = sum(1 for marker in _LOGIN_WALL_MARKERS if marker in normalized)
    return hits >= _LOGIN_WALL_MIN_MARKERS


def detect_platform(url: str) -> str | None:
    host = urlsplit(url).netloc.lower()
    for platform, patterns in _PLATFORM_HOST_PATTERNS.items():
        if any(re.search(pattern, host) for pattern in patterns):
            return platform
    return None


def humanized_delay_ms(base_ms: int, *, jitter: float = 0.35) -> int:
    """Randomized delay around ``base_ms`` so scrolling looks human-paced."""
    low = max(int(base_ms * (1 - jitter)), 50)
    high = int(base_ms * (1 + jitter))
    return random.randint(low, high)


class PageScrapeService:
    """Scrape one arbitrary social-media URL through a real browser session."""

    def __init__(self, config: ProjectConfig, paths: ProjectPaths) -> None:
        self.config = config
        self.paths = paths

    def run(
        self,
        url: str | None = None,
        *,
        run_id: str | None = None,
        max_scrolls: int | None = None,
    ) -> CollectionManifest:
        resolved_run_id = run_id or self.paths.latest_run_id() or utc_now_iso().replace(":", "")
        target_url = url or self.config.source.url or ""
        if not target_url:
            raise ValueError("Page scrape requires --url or source.url in the config.")

        web_settings = self._web_settings()
        raw_store = RawSnapshotStore(self.paths.run_raw_dir(resolved_run_id))
        warnings: list[str] = [
            "Single-page scrape is best-effort: content behind logins or anti-bot checks is reported, not bypassed."
        ]

        from playwright.sync_api import sync_playwright

        with sync_playwright() as playwright:
            runtime = self._open_runtime(playwright)
            warnings.extend(runtime.warnings)
            try:
                page = runtime.context.new_page()
                page.goto(
                    target_url,
                    wait_until="domcontentloaded",
                    timeout=int(web_settings.timeout_seconds * 1000),
                )
                page.wait_for_timeout(humanized_delay_ms(2000))

                scroll_passes = self._humanized_scroll(page, max_scrolls or web_settings.max_scrolls)

                body_text = (page.locator("body").inner_text() or "").strip()
                html = page.content()
                title = page.title()
                final_url = page.url

                raw_store.write_json(
                    "page_scrape",
                    "page_metadata",
                    {
                        "url": target_url,
                        "final_url": final_url,
                        "title": title,
                        "scroll_passes": scroll_passes,
                        "body_chars": len(body_text),
                        "collected_at": utc_now_iso(),
                    },
                )
                raw_store.write_json("page_scrape", "page_text", {"text": body_text})
                (raw_store.run_dir / "page_scrape" / "page.html").write_text(html, encoding="utf-8")
                try:
                    page.screenshot(path=str(raw_store.run_dir / "page_scrape" / "page.png"), full_page=False)
                except Exception as exc:  # pragma: no cover - browser-dependent
                    warnings.append(f"Screenshot failed: {exc}")

                if self._looks_like_captcha(body_text):
                    warnings.append(
                        "CAPTCHA / security check detected on the page. Content may be incomplete; bypassing it is not attempted."
                    )
                if looks_like_login_wall(body_text):
                    warnings.append("Login wall detected on the page; only public content was captured.")

                posts: list[PostSnapshot] = []
                platform = detect_platform(target_url)
                if platform == "facebook":
                    posts, fb_warnings = self._extract_facebook_posts(runtime.context, page, target_url, raw_store)
                    warnings.extend(fb_warnings)
                else:
                    warnings.append(
                        f"Generic capture only for {platform or 'unknown'} URLs; "
                        "structured post extraction for this platform runs through its dedicated collector."
                    )
            finally:
                runtime.close()

        source_snapshot_id = stable_id(target_url)[:16]
        source = SourceSnapshot(
            platform=self.config.source.platform,
            source_id=source_snapshot_id,
            source_name=self.config.source.source_name,
            source_url=target_url,
            source_type="single_page",
            source_collector="page_scraper",
            raw_path=str(raw_store.run_dir / "page_scrape" / "page_metadata.json"),
        )
        status: Literal["success", "partial", "failed"] = "partial" if warnings else "success"
        return CollectionManifest(
            run_id=resolved_run_id,
            collected_at=utc_now_iso(),
            collector="page_scraper",
            mode="web",
            status=status,
            fallback_used=False,
            warnings=warnings,
            source=source,
            posts=posts,
        )

    def _web_settings(self) -> Any:
        platform = detect_platform(self.config.source.url or "") or (
            self.config.source.platform if self.config.source.platform != "facebook" else "facebook"
        )
        mapping = {
            "telegram": getattr(self.config.collector, "telegram_web", None),
            "x": getattr(self.config.collector, "x_web", None),
            "threads": getattr(self.config.collector, "threads_web", None),
            "instagram": getattr(self.config.collector, "instagram_web", None),
        }
        settings = mapping.get(platform)
        return settings or self.config.collector.public_web

    def _open_runtime(self, playwright: Any) -> Any:
        from .collectors.web_runtime import open_web_runtime

        settings = self._web_settings()
        authenticated = getattr(settings, "authenticated_browser", None)
        return open_web_runtime(
            playwright,
            headless=settings.headless,
            browser_channel=settings.browser_channel,
            viewport={"width": 1400, "height": 1600},
            authenticated_browser=authenticated,
            custom_user_data_error="custom browser requires an explicit user data directory",
        )

    def _humanized_scroll(self, page: Any, max_scrolls: int) -> int:
        """Scroll like a person: variable distance and jittered pauses."""
        previous_height = 0
        stable_passes = 0
        completed = 0
        for _ in range(max(max_scrolls, 0)):
            page.evaluate("window.scrollBy(0, Math.round(window.innerHeight * (1.5 + Math.random())))")
            page.wait_for_timeout(humanized_delay_ms(1200))
            completed += 1
            current_height = page.evaluate("document.body.scrollHeight")
            if current_height == previous_height:
                stable_passes += 1
                if stable_passes >= 2:
                    break
            else:
                stable_passes = 0
            previous_height = current_height
        return completed

    def _looks_like_captcha(self, body_text: str) -> bool:
        normalized = body_text.lower()
        return any(marker in normalized for marker in _CAPTCHA_MARKERS)

    def _extract_facebook_posts(
        self,
        context: Any,
        page: Any,
        target_url: str,
        raw_store: RawSnapshotStore,
    ) -> tuple[list[PostSnapshot], list[str]]:
        """Structured extraction for facebook.com pages via existing machinery."""
        from .collectors.facebook_web_extraction import extract_feed_candidates
        from .collectors.public_web import PublicWebCollector

        warnings: list[str] = []
        delegate = PublicWebCollector(self.config)

        candidates = delegate._postprocess_candidates(extract_feed_candidates(page) or [])
        payload = {"title": page.title(), "url": page.url, "candidates": candidates}
        raw_store.write_json("page_scrape", "facebook_feed_candidates", payload)

        if not candidates:
            # The target may itself be a permalink/detail page: try post-page extraction.
            detail_candidate = {"permalink": target_url}
            try:
                post, login_wall = delegate._collect_post_detail(
                    context=context,
                    page_id=stable_id(target_url)[:16],
                    page_name=self.config.source.source_name or "Facebook Page",
                    candidate=detail_candidate,
                    published_at=None,
                    raw_store=raw_store,
                )
                if login_wall:
                    warnings.append("Facebook returned a login wall for this page.")
                return [post], warnings
            except Exception as exc:
                warnings.append(f"Facebook structured extraction failed: {exc}")
                return [], warnings

        page_id = stable_id(target_url)[:16]
        posts: list[PostSnapshot] = []
        seen: set[str] = set()
        for candidate in candidates:
            permalink = candidate.get("permalink") or ""
            if not permalink or permalink in seen:
                continue
            seen.add(permalink)
            try:
                post, _login_wall = delegate._collect_post_detail(
                    context=context,
                    page_id=page_id,
                    page_name=self.config.source.source_name or "Facebook Page",
                    candidate=candidate,
                    published_at=candidate.get("published_at"),
                    raw_store=raw_store,
                )
            except Exception as exc:
                warnings.append(f"Detail extraction failed for {permalink}: {exc}")
                continue
            posts.append(post)
        if not posts and delegate._payload_looks_login_walled({"body_text": page.locator("body").inner_text()}):
            warnings.append("Facebook returned a login wall for this page.")
        return posts, warnings


__all__ = ["PageScrapeService", "detect_platform", "humanized_delay_ms"]
