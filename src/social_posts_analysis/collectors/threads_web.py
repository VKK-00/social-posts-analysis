from __future__ import annotations

from typing import Any

from social_posts_analysis.contracts import AuthorSnapshot, CommentSnapshot, PostSnapshot
from social_posts_analysis.raw_store import RawSnapshotStore
from social_posts_analysis.utils import parse_compact_number, slugify

from .base import CollectorUnavailableError
from .web_timeline_base import WebTimelineCollector


class ThreadsWebCollector(WebTimelineCollector):
    name = "threads_web"
    platform = "threads"
    profile_copy_prefix = "threads-web-profile-"
    disabled_error_message = "Threads web collector is disabled in config.collector.threads_web.enabled."
    requirements_error_message = "Threads web collector requires the playwright package and browser install."
    custom_user_data_error = (
        "Threads authenticated browser mode requires collector.threads_web.authenticated_browser.user_data_dir."
    )

    def _initial_warning(self) -> str:
        return "Threads web extraction is best-effort and reply visibility depends on the current public web UI."

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
                        profile_url=f"https://www.threads.net/@{item.get('author_username')}"
                        if item.get("author_username")
                        else profile_url_from_name(source_id),
                    ),
                )
            )
        return posts

    def _collect_comments_for_post(
        self, *, context: Any, post: PostSnapshot, raw_store: RawSnapshotStore
    ) -> list[CommentSnapshot]:
        if not post.permalink:
            return []
        payload = self._fetch_detail_payload(context=context, post=post)
        raw_store.write_json("threads_web_replies", slugify(post.post_id), payload)
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
            parent_comment_id = (
                comment_id_map.get(parent_native_id)
                if parent_native_id != self._native_status_id(post.post_id)
                else None
            )
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
                    profile_url=f"https://www.threads.net/@{item.get('author_username')}"
                    if item.get("author_username")
                    else None,
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
        raise CollectorUnavailableError(
            "Threads web collector requires source.url, source.source_name, or source.source_id."
        )

    @staticmethod
    def _native_status_id(post_id: str) -> str:
        return post_id.split(":")[-1]


def profile_url_from_name(name: str) -> str:
    return f"https://www.threads.net/@{name.lstrip('@')}"
