from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import parse_qs, urlparse, urlunparse

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
from .web_runtime import ensure_playwright_available, open_web_runtime, scroll_page


class TelegramWebCollector(BaseCollector):
    name = "telegram_web"

    def __init__(self, config: ProjectConfig) -> None:
        self.config = config
        self.settings = config.collector.telegram_web
        self.range_filter = RangeFilter.from_strings(config.date_range.start, config.date_range.end)
        if not self.settings.enabled:
            raise CollectorUnavailableError("Telegram web collector is disabled in config.collector.telegram_web.enabled.")
        ensure_playwright_available("Telegram web collector requires the playwright package and browser install.")

    def collect(self, run_id: str, raw_store: RawSnapshotStore) -> CollectionManifest:
        from playwright.sync_api import sync_playwright

        source_feed_url = self._resolve_feed_url(
            self.config.source.url,
            self.config.source.source_name,
            self.config.source.source_id,
        )
        warnings = [
            "Telegram web extraction is best-effort and currently depends on public t.me/s pages.",
        ]

        with sync_playwright() as playwright:
            runtime = open_web_runtime(
                playwright,
                headless=self.settings.headless,
                browser_channel=self.settings.browser_channel,
                viewport={"width": 1400, "height": 2200},
                custom_user_data_error="Telegram web collector does not support authenticated browser mode.",
            )
            try:
                source_page = runtime.context.new_page()
                source_page.goto(source_feed_url, wait_until="domcontentloaded", timeout=int(self.settings.timeout_seconds * 1000))
                self._scroll_feed(source_page)
                source_payload = self._extract_feed_payload(source_page)
                source_path = raw_store.write_json("telegram_web_source", "source_feed", source_payload)

                posts = self._build_posts_from_payload(source_payload, raw_store)
                posts_by_permalink = {post.permalink: post for post in posts if post.permalink}

                discussion_linked = False
                discussion_reference = self.config.source.telegram.discussion_chat_id
                if discussion_reference:
                    try:
                        discussion_feed_url = self._resolve_feed_url(discussion_reference, discussion_reference, discussion_reference)
                        discussion_page = runtime.context.new_page()
                        discussion_page.goto(
                            discussion_feed_url,
                            wait_until="domcontentloaded",
                            timeout=int(self.settings.timeout_seconds * 1000),
                        )
                        self._scroll_feed(discussion_page)
                        discussion_payload = self._extract_feed_payload(discussion_page)
                        raw_store.write_json("telegram_web_discussion", "discussion_feed", discussion_payload)
                        posts = self._attach_discussion_comments(
                            posts=posts,
                            posts_by_permalink=posts_by_permalink,
                            discussion_payload=discussion_payload,
                            raw_store=raw_store,
                        )
                        discussion_linked = True
                    except Exception as exc:
                        warnings.append(f"Telegram discussion web scraping failed: {exc}")
                else:
                    warnings.append(
                        "Telegram web collector scraped posts only. Set source.telegram.discussion_chat_id to a public discussion feed for comments."
                    )
            finally:
                runtime.close()

        source_name = source_payload.get("source_name") or self.config.source.source_name or self._source_reference()
        source_id = source_payload.get("source_id") or self._source_reference()
        source_snapshot = SourceSnapshot(
            platform="telegram",
            source_id=source_id,
            source_name=source_name,
            source_url=source_feed_url,
            source_type="channel",
            discussion_chat_id=self.config.source.telegram.discussion_chat_id,
            discussion_linked=discussion_linked,
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
            posts=posts,
        )

    def discover_person_monitor_sources(
        self,
        *,
        queries: list[str],
        include_posts: bool,
        include_comments: bool,
        max_items_per_query: int,
    ) -> list[dict[str, str | None]]:
        from playwright.sync_api import sync_playwright

        if not include_posts and not include_comments:
            return []

        discovered: dict[str, dict[str, str | None]] = {}
        with sync_playwright() as playwright:
            runtime = open_web_runtime(
                playwright,
                headless=self.settings.headless,
                browser_channel=self.settings.browser_channel,
                viewport={"width": 1400, "height": 2200},
                custom_user_data_error="Telegram web collector does not support authenticated browser mode.",
            )
            try:
                for query in queries:
                    feed_url = self._resolve_search_discovery_url(query)
                    if not feed_url:
                        continue
                    page = runtime.context.new_page()
                    try:
                        page.goto(feed_url, wait_until="domcontentloaded", timeout=int(self.settings.timeout_seconds * 1000))
                        self._scroll_feed(page)
                        payload = self._extract_feed_payload(page)
                    finally:
                        page.close()
                    surface = self._discovery_payload_from_feed_payload(payload, feed_url=feed_url)
                    if not surface:
                        continue
                    identity = surface["source_id"] or surface["source_url"] or surface["source_name"]
                    if identity:
                        discovered[str(identity)] = surface
            finally:
                runtime.close()
        return list(discovered.values())

    def _build_posts_from_payload(self, source_payload: dict[str, Any], raw_store: RawSnapshotStore) -> list[PostSnapshot]:
        source_id = str(source_payload.get("source_id") or self._source_reference())
        source_name = source_payload.get("source_name") or self.config.source.source_name or source_id
        posts: list[PostSnapshot] = []
        for item in source_payload.get("messages") or []:
            created_at = item.get("created_at")
            if not self._within_range(created_at):
                continue
            reactions = item.get("reaction_breakdown") or {}
            post_id = f"telegram:{source_id}:{item['message_id']}"
            raw_path = raw_store.write_json("telegram_web_posts", slugify(post_id), item)
            origin_post_id, origin_external_id, origin_permalink = self._forward_origin_metadata(item)
            is_forward = bool(item.get("forwarded_from_name") or item.get("forwarded_permalink") or item.get("forwarded_message_id"))
            posts.append(
                PostSnapshot(
                    post_id=post_id,
                    platform="telegram",
                    source_id=source_id,
                    origin_post_id=origin_post_id,
                    origin_external_id=origin_external_id,
                    origin_permalink=origin_permalink,
                    propagation_kind="forward" if is_forward else None,
                    is_propagation=is_forward,
                    created_at=created_at,
                    message=item.get("text"),
                    permalink=item.get("permalink"),
                    reactions=sum(int(value or 0) for value in reactions.values()),
                    comments_count=self._visible_discussion_count(item),
                    views=parse_compact_number(item.get("views")),
                    has_media=bool(item.get("has_media")),
                    media_type=item.get("media_type"),
                    reaction_breakdown_json=json.dumps(reactions, ensure_ascii=False) if reactions else None,
                    source_collector=self.name,
                    raw_path=str(raw_path),
                    author=AuthorSnapshot(
                        author_id=source_id,
                        name=source_name,
                        profile_url=source_payload.get("source_url") or self.config.source.url,
                    ),
                    comments=[],
                )
            )
        return posts

    def _attach_discussion_comments(
        self,
        *,
        posts: list[PostSnapshot],
        posts_by_permalink: dict[str, PostSnapshot],
        discussion_payload: dict[str, Any],
        raw_store: RawSnapshotStore,
    ) -> list[PostSnapshot]:
        updated_posts = {post.post_id: post.model_copy(deep=True) for post in posts}
        comment_id_by_permalink: dict[str, str] = {}
        comment_id_by_message_id: dict[str, str] = {}
        comment_depths: dict[str, int] = {}
        comment_parent_posts: dict[str, str] = {}

        for item in discussion_payload.get("messages") or []:
            created_at = item.get("created_at")
            if not self._within_range(created_at):
                continue
            reply_permalink = self._normalize_permalink(item.get("reply_permalink"))
            reply_message_id = str(
                item.get("reply_message_id")
                or self._message_id_from_permalink(reply_permalink)
                or ""
            ).strip() or None
            if not reply_permalink:
                continue

            parent_post = posts_by_permalink.get(reply_permalink)
            parent_comment_id = None
            depth = 0
            parent_post_id = ""
            thread_root_post_id = ""
            if parent_post is not None:
                parent_post_id = parent_post.post_id
                thread_root_post_id = parent_post.post_id
            else:
                parent_comment_id = None
                if reply_message_id:
                    parent_comment_id = comment_id_by_message_id.get(reply_message_id)
                if parent_comment_id is None:
                    parent_comment_id = comment_id_by_permalink.get(reply_permalink)
                if parent_comment_id is None:
                    continue
                parent_post_id = comment_parent_posts.get(parent_comment_id, "")
                if not parent_post_id:
                    continue
                thread_root_post_id = parent_post_id
                depth = comment_depths.get(parent_comment_id, 0) + 1

            reactions = item.get("reaction_breakdown") or {}
            raw_path = raw_store.write_json("telegram_web_comments", slugify(item["message_token"]), item)
            comment_id = f"telegram-web:{discussion_payload.get('source_id') or 'discussion'}:{item['message_id']}"
            comment_snapshot = CommentSnapshot(
                comment_id=comment_id,
                platform="telegram",
                parent_post_id=parent_post_id,
                parent_comment_id=parent_comment_id,
                reply_to_message_id=reply_message_id,
                thread_root_post_id=thread_root_post_id,
                created_at=created_at,
                message=item.get("text"),
                permalink=item.get("permalink"),
                reactions=sum(int(value or 0) for value in reactions.values()),
                reaction_breakdown_json=json.dumps(reactions, ensure_ascii=False) if reactions else None,
                source_collector=self.name,
                depth=depth,
                raw_path=str(raw_path),
                author=AuthorSnapshot(
                    author_id=item.get("author_id"),
                    name=item.get("author_name"),
                ),
            )
            post_snapshot = updated_posts[parent_post_id]
            updated_posts[parent_post_id] = post_snapshot.model_copy(
                update={
                    "comments": [*post_snapshot.comments, comment_snapshot],
                    "comments_count": max(post_snapshot.comments_count, len(post_snapshot.comments) + 1),
                }
            )
            comment_permalink = self._normalize_permalink(item.get("permalink"))
            if comment_permalink:
                comment_id_by_permalink[comment_permalink] = comment_id
            comment_id_by_message_id[str(item["message_id"])] = comment_id
            comment_depths[comment_id] = depth
            comment_parent_posts[comment_id] = parent_post_id

        return [
            updated_posts[post.post_id].model_copy(
                update={"comments": sorted(updated_posts[post.post_id].comments, key=lambda item: (item.depth, item.created_at or ""))}
            )
            for post in posts
        ]

    def _extract_feed_payload(self, page: Any) -> dict[str, Any]:
        page.wait_for_timeout(self.settings.wait_after_scroll_ms)
        payload = page.evaluate(
            """
            () => {
              const messageNodes = Array.from(document.querySelectorAll('.tgme_widget_message'));
              const messages = messageNodes.map((node) => {
                const token = node.getAttribute('data-post') || '';
                const [sourceToken, messageId = ''] = token.split('/');
                const dateAnchor = node.querySelector('a.tgme_widget_message_date');
                const timeNode = node.querySelector('a.tgme_widget_message_date time');
                const textNode = node.querySelector('.tgme_widget_message_text');
                const replyNode = node.querySelector('.tgme_widget_message_reply');
                const forwardedNode = node.querySelector('.tgme_widget_message_forwarded_from a, .tgme_widget_message_forwarded_from');
                const reactionNodes = Array.from(node.querySelectorAll('.tgme_reaction'));
                const reactionBreakdown = {};
                for (const reactionNode of reactionNodes) {
                  const emojiNode = reactionNode.querySelector('tg-emoji');
                  const iconNode = reactionNode.querySelector('i.icon');
                  const label = emojiNode?.getAttribute('emoji-id') || iconNode?.className || 'reaction';
                  const countText = (reactionNode.textContent || '').trim();
                  reactionBreakdown[label] = countText;
                }
                const mediaType = node.querySelector('.tgme_widget_message_video_player')
                  ? 'video'
                  : node.querySelector('.tgme_widget_message_photo_wrap')
                    ? 'photo'
                    : node.querySelector('.tgme_widget_message_grouped_wrap')
                      ? 'media_group'
                      : null;
                return {
                  message_token: token,
                  source_token: sourceToken,
                  message_id: messageId,
                  permalink: dateAnchor?.href || '',
                  created_at: timeNode?.getAttribute('datetime') || null,
                  text: textNode ? textNode.innerText.trim() : '',
                  views: (node.querySelector('.tgme_widget_message_views')?.textContent || '').trim(),
                  has_media: Boolean(mediaType),
                  media_type: mediaType,
                  author_name: (node.querySelector('.tgme_widget_message_owner_name')?.textContent || '').trim(),
                  forwarded_from_name: forwardedNode ? forwardedNode.textContent.trim() : '',
                  forwarded_permalink: forwardedNode?.href || '',
                  forwarded_message_id: forwardedNode?.href ? ((forwardedNode.href.split('/').pop() || '').split(/[?#]/)[0]) : '',
                  reply_permalink: replyNode?.getAttribute('href') || '',
                  reply_text: replyNode ? replyNode.innerText.trim() : '',
                  reaction_breakdown: reactionBreakdown,
                };
              });
              const headerTitle = (document.querySelector('.tgme_channel_info_header_title')?.textContent || document.title || '').trim();
              const firstToken = messages.length ? messages[0].source_token : '';
              return {
                page_title: document.title,
                source_name: headerTitle.replace(/\\s+/g, ' ').trim(),
                source_id: firstToken || '',
                source_url: location.href,
                messages,
              };
            }
            """
        )
        messages = []
        for item in payload.get("messages") or []:
            messages.append(
                {
                    **item,
                    "permalink": self._normalize_permalink(item.get("permalink")),
                    "forwarded_permalink": self._normalize_permalink(item.get("forwarded_permalink")),
                    "reply_permalink": self._normalize_permalink(item.get("reply_permalink")),
                    "reply_message_id": item.get("reply_message_id") or self._message_id_from_permalink(item.get("reply_permalink")),
                    "reaction_breakdown": {
                        str(key): parse_compact_number(str(value))
                        for key, value in (item.get("reaction_breakdown") or {}).items()
                    },
                }
            )
        return {
            **payload,
            "source_name": payload.get("source_name") or self.config.source.source_name or self._source_reference(),
            "source_id": payload.get("source_id") or self._source_reference(),
            "source_url": payload.get("source_url") or self.config.source.url or source_feed_url_from_name(self._source_reference()),
            "messages": messages,
        }

    def _scroll_feed(self, page: Any) -> None:
        scroll_page(
            page,
            max_scrolls=self.settings.max_scrolls,
            wait_after_scroll_ms=self.settings.wait_after_scroll_ms,
            wheel_y=3000,
        )

    def _discovery_payload_from_feed_payload(
        self,
        payload: dict[str, Any],
        *,
        feed_url: str,
    ) -> dict[str, str | None] | None:
        if self._search_url_has_query(feed_url) and not (payload.get("messages") or []):
            return None
        source_id = str(payload.get("source_id") or "").strip()
        if not source_id:
            return None
        return {
            "source_id": source_id,
            "source_name": (payload.get("source_name") or None),
            "source_url": self._normalize_permalink(feed_url) or payload.get("source_url") or None,
            "source_type": "channel",
        }

    def _within_range(self, created_at: str | None) -> bool:
        return self.range_filter.contains(created_at, allow_missing=False)

    def _forward_origin_metadata(self, item: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
        forwarded_permalink = self._normalize_permalink(item.get("forwarded_permalink"))
        forwarded_message_id = str(
            item.get("forwarded_message_id")
            or self._message_id_from_permalink(forwarded_permalink)
            or ""
        ).strip() or None
        if forwarded_permalink:
            parsed = urlparse(forwarded_permalink)
            parts = [part for part in parsed.path.split("/") if part]
            if len(parts) >= 2:
                if parts[0] == "s" and len(parts) >= 3:
                    source_token = parts[1]
                    message_id = parts[2]
                else:
                    source_token = parts[0]
                    message_id = parts[1]
                if source_token and message_id:
                    return f"telegram:{source_token}:{message_id}", message_id, forwarded_permalink
        if forwarded_message_id:
            return f"telegram:origin:{forwarded_message_id}", forwarded_message_id, forwarded_permalink
        return None, None, forwarded_permalink

    @staticmethod
    def _visible_discussion_count(item: dict[str, Any]) -> int:
        reply_text = str(item.get("reply_text") or "").strip()
        if not reply_text:
            return 0
        match = re.search(
            r"(\d+(?:[.,]\d+)?\s*[KMB]?)\s*(?:comments?|repl(?:y|ies)|discussion|комментари(?:й|я|ев)|ответ(?:ов|а|ы)?)",
            reply_text,
            flags=re.IGNORECASE,
        )
        if not match:
            return 0
        return parse_compact_number(match.group(1))

    @staticmethod
    def _normalize_permalink(value: str | None) -> str | None:
        if not value:
            return None
        return value.rstrip("/")

    @staticmethod
    def _message_id_from_permalink(value: str | None) -> str | None:
        if not value:
            return None
        parsed = urlparse(value)
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) >= 3 and parts[0] == "s":
            return parts[2]
        if len(parts) >= 2:
            return parts[1]
        return None

    def _source_reference(self) -> str:
        return self.config.source.source_name or self.config.source.source_id or self._extract_name_from_url(self.config.source.url)

    @staticmethod
    def _extract_name_from_url(url: str | None) -> str:
        if not url:
            return ""
        parsed = urlparse(url)
        parts = [part for part in parsed.path.split("/") if part and part != "s"]
        return parts[-1] if parts else ""

    @staticmethod
    def _resolve_feed_url(url: str | None, source_name: str | None, source_id: str | None) -> str:
        if url:
            parsed = urlparse(url)
            if parsed.netloc.endswith("t.me"):
                parts = [part for part in parsed.path.split("/") if part]
                if parts and parts[0] == "s":
                    return url.rstrip("/")
                if parts:
                    return f"https://t.me/s/{parts[-1]}"
        reference = source_name or source_id
        if not reference:
            raise CollectorUnavailableError("Telegram web collector requires source.url, source.source_name, or source.source_id.")
        reference = reference.strip().lstrip("@")
        return source_feed_url_from_name(reference)

    @classmethod
    def _resolve_search_discovery_url(cls, query: str | None) -> str | None:
        if not query:
            return None
        stripped = query.strip()
        if not stripped:
            return None
        if stripped.startswith("http://") or stripped.startswith("https://"):
            parsed = urlparse(stripped)
            if not parsed.netloc.endswith("t.me"):
                return None
            parts = [part for part in parsed.path.split("/") if part]
            if not parts:
                return None
            if parts[0] == "s" and len(parts) >= 2:
                return urlunparse(parsed._replace(path=f"/s/{parts[1]}", fragment=""))
            return urlunparse(parsed._replace(path=f"/s/{parts[-1]}", fragment=""))
        if re.fullmatch(r"@?[A-Za-z0-9_]{3,}", stripped):
            return source_feed_url_from_name(stripped.lstrip("@"))
        return None

    @staticmethod
    def _search_url_has_query(url: str) -> bool:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        return bool(query.get("q"))

def source_feed_url_from_name(name: str) -> str:
    return f"https://t.me/s/{name}"
