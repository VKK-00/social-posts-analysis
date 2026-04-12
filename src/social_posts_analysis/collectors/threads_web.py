from __future__ import annotations

from typing import Any

from social_posts_analysis.config import ProjectConfig
from social_posts_analysis.contracts import (
    AuthorSnapshot,
    CollectionManifest,
    CommentSnapshot,
    PostSnapshot,
    SourceSnapshot,
)
from social_posts_analysis.raw_store import RawSnapshotStore
from social_posts_analysis.utils import parse_compact_number, slugify, utc_now_iso

from .base import BaseCollector, CollectorUnavailableError
from .range_utils import RangeFilter
from .web_runtime import WebCollectorRuntime, ensure_playwright_available, open_web_runtime, scroll_page


class ThreadsWebCollector(BaseCollector):
    name = "threads_web"

    def __init__(self, config: ProjectConfig) -> None:
        self.config = config
        self.settings = config.collector.threads_web
        self.range_filter = RangeFilter.from_strings(config.date_range.start, config.date_range.end)
        if not self.settings.enabled:
            raise CollectorUnavailableError("Threads web collector is disabled in config.collector.threads_web.enabled.")
        ensure_playwright_available("Threads web collector requires the playwright package and browser install.")

    def collect(self, run_id: str, raw_store: RawSnapshotStore) -> CollectionManifest:
        from playwright.sync_api import sync_playwright

        warnings = ["Threads web extraction is best-effort and reply visibility depends on the current public web UI."]
        profile_url = self._resolve_profile_url()
        with sync_playwright() as playwright:
            runtime = self._open_collection_context(playwright)
            warnings.extend(runtime.warnings)
            try:
                page = runtime.context.new_page()
                page.goto(profile_url, wait_until="domcontentloaded", timeout=int(self.settings.timeout_seconds * 1000))
                self._scroll_timeline(page)
                payload = self._extract_profile_payload(page)
                source_path = raw_store.write_json("threads_web_source", "profile_feed", payload)
                source_name = payload.get("source_name") or self.config.source.source_name or self._source_reference()
                source_id = payload.get("source_id") or self._source_reference()
                posts = self._build_posts_from_payload(payload, source_id=source_id, source_name=source_name, raw_store=raw_store)
                updated_posts: list[PostSnapshot] = []
                for post in posts:
                    replies = self._collect_replies_for_post(context=runtime.context, post=post, raw_store=raw_store)
                    updated_posts.append(
                        post.model_copy(update={"comments": replies, "comments_count": max(post.comments_count, len(replies))})
                    )
            finally:
                runtime.close()

        source_snapshot = SourceSnapshot(
            platform="threads",
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
            warnings=warnings,
            source=source_snapshot,
            posts=updated_posts,
        )

    def _build_posts_from_payload(
        self,
        payload: dict[str, Any],
        *,
        source_id: str,
        source_name: str,
        raw_store: RawSnapshotStore,
    ) -> list[PostSnapshot]:
        posts: list[PostSnapshot] = []
        for item in payload.get("posts") or []:
            if not self._within_range(item.get("created_at")):
                continue
            post_id = f"threads:{source_id}:{item['status_id']}"
            raw_path = raw_store.write_json("threads_web_posts", slugify(post_id), item)
            origin_external_id = item.get("origin_status_id") or None
            posts.append(
                PostSnapshot(
                    post_id=post_id,
                    platform="threads",
                    source_id=source_id,
                    origin_post_id=f"threads:origin:{origin_external_id}" if origin_external_id else None,
                    origin_external_id=origin_external_id,
                    origin_permalink=item.get("origin_permalink") or None,
                    propagation_kind=item.get("propagation_kind") or None,
                    is_propagation=bool(item.get("propagation_kind")),
                    created_at=item.get("created_at"),
                    message=item.get("text"),
                    permalink=item.get("permalink"),
                    reactions=parse_compact_number(item.get("like_count")),
                    shares=parse_compact_number(item.get("repost_count")),
                    comments_count=parse_compact_number(item.get("reply_count")),
                    views=parse_compact_number(item.get("view_count")) or None,
                    forwards=1 if item.get("propagation_kind") == "quote" else None,
                    reply_count=parse_compact_number(item.get("reply_count")),
                    has_media=bool(item.get("has_media")),
                    media_type=item.get("media_type"),
                    source_collector=self.name,
                    raw_path=str(raw_path),
                    author=AuthorSnapshot(
                        author_id=item.get("author_username") or source_id,
                        name=item.get("author_name") or source_name,
                        profile_url=f"https://www.threads.net/@{item.get('author_username')}" if item.get("author_username") else profile_url_from_name(source_id),
                    ),
                )
            )
        return posts

    def _collect_replies_for_post(self, *, context: Any, post: PostSnapshot, raw_store: RawSnapshotStore) -> list[CommentSnapshot]:
        if not post.permalink:
            return []
        page = context.new_page()
        try:
            page.goto(post.permalink, wait_until="domcontentloaded", timeout=int(self.settings.timeout_seconds * 1000))
            self._scroll_timeline(page, passes=max(3, self.settings.max_scrolls // 2))
            payload = self._extract_detail_payload(page)
            raw_store.write_json("threads_web_replies", slugify(post.post_id), payload)
        finally:
            page.close()
        comments: list[CommentSnapshot] = []
        comment_id_map: dict[str, str] = {}
        depth_map: dict[str, int] = {}
        for item in payload.get("replies") or []:
            if not self._within_range(item.get("created_at")):
                continue
            status_id = str(item.get("status_id") or "")
            if not status_id:
                continue
            comment_id = f"{post.post_id}:comment:{status_id}"
            parent_native_id = str(item.get("reply_to_status_id") or self._native_status_id(post.post_id))
            parent_comment_id = comment_id_map.get(parent_native_id) if parent_native_id != self._native_status_id(post.post_id) else None
            depth = depth_map.get(parent_comment_id, -1) + 1 if parent_comment_id else 0
            raw_path = raw_store.write_json("threads_web_reply_items", slugify(comment_id), item)
            snapshot = CommentSnapshot(
                comment_id=comment_id,
                platform="threads",
                parent_post_id=post.post_id,
                parent_comment_id=parent_comment_id,
                reply_to_message_id=parent_native_id,
                thread_root_post_id=post.post_id,
                created_at=item.get("created_at"),
                message=item.get("text"),
                permalink=item.get("permalink"),
                reactions=parse_compact_number(item.get("like_count")),
                source_collector=self.name,
                depth=depth,
                raw_path=str(raw_path),
                author=AuthorSnapshot(
                    author_id=item.get("author_username"),
                    name=item.get("author_name"),
                    profile_url=f"https://www.threads.net/@{item.get('author_username')}" if item.get("author_username") else None,
                ),
            )
            comments.append(snapshot)
            comment_id_map[status_id] = snapshot.comment_id
            depth_map[snapshot.comment_id] = snapshot.depth
        return comments

    def _extract_profile_payload(self, page: Any) -> dict[str, Any]:
        return page.evaluate(
            """
            () => {
              const articles = Array.from(document.querySelectorAll('article'));
              const posts = articles.map((node) => {
                const statusLinks = Array.from(node.querySelectorAll('a[href*="/post/"]'));
                const permalink = statusLinks[0]?.href || '';
                const originPermalink = statusLinks.length > 1 ? (statusLinks[1]?.href || '') : '';
                const statusId = permalink ? permalink.split('/post/')[1].split(/[/?#]/)[0] : '';
                const originStatusId = originPermalink ? originPermalink.split('/post/')[1].split(/[/?#]/)[0] : '';
                const timeNode = node.querySelector('time');
                const textNode = node.querySelector('[data-pressable-container="true"] div[dir="auto"], div[dir="auto"]');
                const allText = node.innerText || '';
                const metricFromText = (label) => {
                  const match = allText.match(new RegExp(`(\\\\d+(?:\\\\.\\\\d+)?[KMB]?)\\\\s+${label}`, 'i'));
                  return match ? match[1] : '';
                };
                const authorLink = Array.from(node.querySelectorAll('a[href^="/@"]')).find((anchor) => anchor.href.includes('/@'));
                const authorUsername = authorLink ? (authorLink.getAttribute('href') || '').split('@')[1].split(/[/?#]/)[0] : '';
                return {
                  permalink,
                  status_id: statusId,
                  origin_permalink: originPermalink,
                  origin_status_id: originStatusId,
                  propagation_kind: originStatusId ? (allText.toLowerCase().includes('quote') ? 'quote' : 'repost') : '',
                  created_at: timeNode?.getAttribute('datetime') || null,
                  text: textNode ? textNode.innerText.trim() : '',
                  author_name: authorUsername,
                  author_username: authorUsername,
                  reply_count: metricFromText('repl'),
                  repost_count: metricFromText('repost'),
                  like_count: metricFromText('like'),
                  view_count: metricFromText('view'),
                  has_media: Boolean(node.querySelector('img, video')),
                  media_type: node.querySelector('video') ? 'video' : (node.querySelector('img') ? 'photo' : null),
                };
              }).filter((item) => item.status_id);
              return {
                source_name: (document.querySelector('h1')?.innerText || document.title || '').trim(),
                source_id: (location.pathname.split('@')[1] || '').split(/[/?#]/)[0],
                source_url: location.href,
                posts,
              };
            }
            """
        )

    def _extract_detail_payload(self, page: Any) -> dict[str, Any]:
        return page.evaluate(
            """
            () => {
              const articles = Array.from(document.querySelectorAll('article'));
              const rows = articles.map((node) => {
                const statusLinks = Array.from(node.querySelectorAll('a[href*="/post/"]'));
                const permalink = statusLinks[0]?.href || '';
                const statusId = permalink ? permalink.split('/post/')[1].split(/[/?#]/)[0] : '';
                const repliedToPermalink = statusLinks.length > 1 ? (statusLinks[1]?.href || '') : '';
                const replyToStatusId = repliedToPermalink ? repliedToPermalink.split('/post/')[1].split(/[/?#]/)[0] : '';
                const timeNode = node.querySelector('time');
                const textNode = node.querySelector('[data-pressable-container="true"] div[dir="auto"], div[dir="auto"]');
                const allText = node.innerText || '';
                const metricFromText = (label) => {
                  const match = allText.match(new RegExp(`(\\\\d+(?:\\\\.\\\\d+)?[KMB]?)\\\\s+${label}`, 'i'));
                  return match ? match[1] : '';
                };
                const authorLink = Array.from(node.querySelectorAll('a[href^="/@"]')).find((anchor) => anchor.href.includes('/@'));
                const authorUsername = authorLink ? (authorLink.getAttribute('href') || '').split('@')[1].split(/[/?#]/)[0] : '';
                return {
                  permalink,
                  status_id: statusId,
                  reply_to_status_id: replyToStatusId,
                  created_at: timeNode?.getAttribute('datetime') || null,
                  text: textNode ? textNode.innerText.trim() : '',
                  author_name: authorUsername,
                  author_username: authorUsername,
                  like_count: metricFromText('like'),
                };
              }).filter((item) => item.status_id);
              return {
                main_status_id: rows.length ? rows[0].status_id : '',
                replies: rows.length ? rows.slice(1) : [],
              };
            }
            """
        )

    def _open_collection_context(self, playwright: Any) -> WebCollectorRuntime:
        return open_web_runtime(
            playwright,
            headless=self.settings.headless,
            browser_channel=self.settings.browser_channel,
            viewport={"width": 1400, "height": 1800},
            authenticated_browser=self.settings.authenticated_browser,
            profile_copy_prefix="threads-web-profile-",
            custom_user_data_error="Threads authenticated browser mode requires collector.threads_web.authenticated_browser.user_data_dir.",
        )

    def _scroll_timeline(self, page: Any, *, passes: int | None = None) -> None:
        scroll_page(
            page,
            max_scrolls=self.settings.max_scrolls,
            wait_after_scroll_ms=self.settings.wait_after_scroll_ms,
            passes=passes,
            wheel_y=2600,
        )

    def _resolve_profile_url(self) -> str:
        if self.config.source.url:
            return self.config.source.url.rstrip("/")
        return profile_url_from_name(self._source_reference())

    def _source_reference(self) -> str:
        if self.config.source.source_name:
            return self.config.source.source_name.lstrip("@")
        if self.config.source.source_id:
            return self.config.source.source_id
        if self.config.source.url:
            return self.config.source.url.rstrip("/").split("@")[-1].split("/")[-1]
        raise CollectorUnavailableError("Threads web collector requires source.url, source.source_name, or source.source_id.")

    def _within_range(self, raw_value: str | None) -> bool:
        return self.range_filter.contains(raw_value, allow_missing=False)

    @staticmethod
    def _native_status_id(post_id: str) -> str:
        return post_id.split(":")[-1]


def profile_url_from_name(name: str) -> str:
    return f"https://www.threads.net/@{name.lstrip('@')}"
