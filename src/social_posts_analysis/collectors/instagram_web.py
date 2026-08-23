from __future__ import annotations

from typing import Any

from social_posts_analysis.contracts import AuthorSnapshot, CommentSnapshot, PostSnapshot
from social_posts_analysis.raw_store import RawSnapshotStore
from social_posts_analysis.utils import parse_compact_number, slugify

from .base import CollectorUnavailableError
from .web_timeline_base import WebTimelineCollector


class InstagramWebCollector(WebTimelineCollector):
    name = "instagram_web"
    platform = "instagram"
    wheel_y = 2400
    min_detail_scroll_passes = 2
    allow_missing_created_at = True
    profile_copy_prefix = "instagram-web-profile-"
    disabled_error_message = "Instagram web collector is disabled in config.collector.instagram_web.enabled."
    requirements_error_message = "Instagram web collector requires the playwright package and browser install."
    custom_user_data_error = (
        "Instagram authenticated browser mode requires collector.instagram_web.authenticated_browser.user_data_dir."
    )

    def _initial_warning(self) -> str:
        return "Instagram web extraction is best-effort and public comment visibility depends on the current web UI."

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
            post_id = f"instagram:{source_id}:{item['status_id']}"
            raw_path = raw_store.write_json("instagram_web_posts", slugify(post_id), item)
            posts.append(
                PostSnapshot(
                    post_id=post_id,
                    platform="instagram",
                    source_id=source_id,
                    created_at=item.get("created_at"),
                    message=item.get("text"),
                    permalink=item.get("permalink"),
                    reactions=parse_compact_number(item.get("like_count")),
                    shares=0,
                    comments_count=parse_compact_number(item.get("comment_count")),
                    has_media=bool(item.get("has_media")),
                    media_type=item.get("media_type"),
                    source_collector=self.name,
                    raw_path=str(raw_path),
                    author=AuthorSnapshot(
                        author_id=item.get("author_username") or source_id,
                        name=item.get("author_name") or source_name,
                        profile_url=f"https://www.instagram.com/{item.get('author_username')}/"
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
        raw_store.write_json("instagram_web_comments", slugify(post.post_id), payload)
        comments: list[CommentSnapshot] = []
        comment_id_map: dict[str, str] = {}
        depth_map: dict[str, int] = {}
        for item in payload.get("comments") or []:
            if not self._within_range(item.get("created_at")):
                continue
            status_id = str(item.get("comment_id") or "")
            if not status_id:
                continue
            comment_id = f"{post.post_id}:comment:{status_id}"
            parent_native_id = str(item.get("reply_to_comment_id") or "")
            parent_comment_id = comment_id_map.get(parent_native_id) if parent_native_id else None
            depth = depth_map.get(parent_comment_id, -1) + 1 if parent_comment_id else 0
            raw_path = raw_store.write_json("instagram_web_comment_items", slugify(comment_id), item)
            snapshot = CommentSnapshot(
                comment_id=comment_id,
                platform="instagram",
                parent_post_id=post.post_id,
                parent_comment_id=parent_comment_id,
                reply_to_message_id=parent_native_id or None,
                thread_root_post_id=post.post_id,
                created_at=item.get("created_at"),
                message=item.get("text"),
                permalink=None,
                reactions=parse_compact_number(item.get("like_count")),
                source_collector=self.name,
                depth=depth,
                raw_path=str(raw_path),
                author=AuthorSnapshot(
                    author_id=item.get("author_username"),
                    name=item.get("author_name"),
                    profile_url=f"https://www.instagram.com/{item.get('author_username')}/"
                    if item.get("author_username")
                    else None,
                ),
            )
            comments.append(snapshot)
            comment_id_map[status_id] = snapshot.comment_id
            depth_map[snapshot.comment_id] = snapshot.depth
        return comments

    def _extract_detail_payload(self, page: Any) -> dict[str, Any]:
        return self._extract_post_payload(page)

    def _extract_post_payload(self, page: Any) -> dict[str, Any]:
        return page.evaluate(
            """
            () => {
              const commentNodes = Array.from(document.querySelectorAll('ul ul, article ul ul li'));
              const comments = commentNodes.map((node, index) => {
                const authorLink = node.querySelector('a[href^="/"]');
                const timeNode = node.querySelector('time');
                const textParts = Array.from(node.querySelectorAll('span')).map((span) => (span.textContent || '').trim()).filter(Boolean);
                return {
                  comment_id: node.getAttribute('data-comment-id') || String(index + 1),
                  reply_to_comment_id: node.getAttribute('data-parent-comment-id') || '',
                  created_at: timeNode?.getAttribute('datetime') || null,
                  text: textParts.slice(1).join(' ').trim(),
                  author_name: textParts[0] || '',
                  author_username: authorLink ? (authorLink.getAttribute('href') || '').replaceAll('/', '') : '',
                  like_count: '',
                };
              });
              return { comments };
            }
            """
        )

    def _extract_profile_payload(self, page: Any) -> dict[str, Any]:
        return page.evaluate(
            """
            () => {
              const links = Array.from(document.querySelectorAll('a[href*="/p/"], a[href*="/reel/"]'));
              const seen = new Set();
              const posts = links.map((anchor) => {
                const href = anchor.href || '';
                if (seen.has(href)) return null;
                seen.add(href);
                const imageNode = anchor.querySelector('img');
                return {
                  permalink: href,
                  status_id: href.includes('/reel/')
                    ? href.split('/reel/')[1].split(/[/?#]/)[0]
                    : href.split('/p/')[1].split(/[/?#]/)[0],
                  created_at: null,
                  text: imageNode?.getAttribute('alt') || '',
                  author_name: (document.querySelector('header section h2, header section h1')?.textContent || '').trim(),
                  author_username: (location.pathname.replace(/^\\//, '').split('/')[0] || '').trim(),
                  comment_count: '',
                  like_count: '',
                  has_media: Boolean(imageNode),
                  media_type: href.includes('/reel/') ? 'reel' : 'photo',
                };
              }).filter(Boolean);
              return {
                source_name: (document.querySelector('header section h2, header section h1')?.textContent || document.title || '').trim(),
                source_id: location.pathname.replace(/^\\//, '').split('/')[0],
                source_url: location.href,
                posts,
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
            return self.config.source.url.rstrip("/").split("/")[-1]
        raise CollectorUnavailableError(
            "Instagram web collector requires source.url, source.source_name, or source.source_id."
        )


def profile_url_from_name(name: str) -> str:
    return f"https://www.instagram.com/{name.lstrip('@')}/"
