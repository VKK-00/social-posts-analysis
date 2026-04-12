from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl

from social_posts_analysis.contracts import CommentSnapshot, ParentEntityType, PostSnapshot


@dataclass(frozen=True, slots=True)
class PropagationCommentScope:
    parent_entity_type: ParentEntityType
    parent_entity_id: str
    origin_post_id: str


def is_origin_post(post: PostSnapshot) -> bool:
    return not post.is_propagation


def filter_origin_posts_frame(posts: pl.DataFrame) -> pl.DataFrame:
    if posts.is_empty() or "is_propagation" not in posts.columns:
        return posts
    return posts.filter(~pl.col("is_propagation").fill_null(False))


def resolve_comment_scope(post: PostSnapshot, comment: CommentSnapshot) -> PropagationCommentScope:
    if comment.parent_entity_type and comment.parent_entity_id and comment.origin_post_id:
        return PropagationCommentScope(
            parent_entity_type=comment.parent_entity_type,
            parent_entity_id=comment.parent_entity_id,
            origin_post_id=comment.origin_post_id,
        )
    if post.is_propagation:
        return PropagationCommentScope(
            parent_entity_type="propagation",
            parent_entity_id=post.post_id,
            origin_post_id=post.origin_post_id or post.post_id,
        )
    return PropagationCommentScope(
        parent_entity_type="post",
        parent_entity_id=post.post_id,
        origin_post_id=post.post_id,
    )


def build_propagation_record(post: PostSnapshot, run_id: str) -> dict[str, Any] | None:
    if not post.is_propagation:
        return None
    return {
        "propagation_id": post.post_id,
        "platform": post.platform,
        "source_id": post.source_id,
        "origin_post_id": post.origin_post_id,
        "origin_external_id": post.origin_external_id,
        "origin_permalink": post.origin_permalink,
        "propagation_kind": post.propagation_kind or "unknown",
        "author_id": post.author.author_id if post.author else None,
        "created_at": post.created_at,
        "message": post.message,
        "permalink": post.permalink,
        "reactions": post.reactions,
        "shares": post.shares,
        "comments_count": post.comments_count,
        "views": post.views,
        "forwards": post.forwards,
        "reply_count": post.reply_count,
        "has_media": post.has_media,
        "media_type": post.media_type,
        "reaction_breakdown_json": post.reaction_breakdown_json,
        "source_collector": post.source_collector,
        "raw_path": post.raw_path,
        "run_id": run_id,
    }


def build_propagation_edge(post: PostSnapshot, run_id: str) -> dict[str, Any] | None:
    if not post.is_propagation:
        return None
    return {
        "propagation_id": post.post_id,
        "origin_post_id": post.origin_post_id,
        "origin_external_id": post.origin_external_id,
        "propagation_kind": post.propagation_kind or "unknown",
        "platform": post.platform,
        "run_id": run_id,
    }
