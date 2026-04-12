from __future__ import annotations

from typing import Any

from social_posts_analysis.contracts import CollectionManifest
from social_posts_analysis.propagation import build_propagation_edge, build_propagation_record, resolve_comment_scope


def build_table_records(manifest: CollectionManifest, source_run_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    posts_records: list[dict[str, Any]] = []
    propagation_records: list[dict[str, Any]] = []
    propagation_edges: list[dict[str, Any]] = []
    comments_records: list[dict[str, Any]] = []
    comment_edges: list[dict[str, Any]] = []
    authors: list[dict[str, Any]] = []
    media_refs: list[dict[str, Any]] = []

    if manifest.source.source_id:
        authors.append(
            {
                "author_id": manifest.source.source_id,
                "name": manifest.source.source_name,
                "profile_url": manifest.source.source_url,
                "source_collector": manifest.source.source_collector,
                "run_id": manifest.run_id,
            }
        )

    for post in manifest.posts:
        posts_records.append(
            {
                "post_id": post.post_id,
                "platform": post.platform,
                "source_id": post.source_id,
                "origin_post_id": post.origin_post_id or None,
                "origin_external_id": post.origin_external_id,
                "origin_permalink": post.origin_permalink,
                "propagation_kind": post.propagation_kind,
                "is_propagation": post.is_propagation,
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
                "run_id": manifest.run_id,
            }
        )

        propagation_record = build_propagation_record(post, manifest.run_id)
        if propagation_record is not None:
            propagation_records.append(propagation_record)
        propagation_edge = build_propagation_edge(post, manifest.run_id)
        if propagation_edge is not None:
            propagation_edges.append(propagation_edge)

        if post.author and post.author.author_id:
            authors.append(
                {
                    "author_id": post.author.author_id,
                    "name": post.author.name,
                    "profile_url": post.author.profile_url,
                    "source_collector": post.source_collector,
                    "run_id": manifest.run_id,
                }
            )

        for media in post.media_refs:
            media_refs.append(
                {
                    "media_id": media.media_id,
                    "owner_post_id": media.owner_post_id,
                    "media_type": media.media_type,
                    "title": media.title,
                    "url": media.url,
                    "preview_url": media.preview_url,
                    "run_id": manifest.run_id,
                }
            )

        for comment in post.comments:
            scope = resolve_comment_scope(post, comment)
            comments_records.append(
                {
                    "comment_id": comment.comment_id,
                    "platform": comment.platform,
                    "parent_post_id": comment.parent_post_id,
                    "parent_entity_type": scope.parent_entity_type,
                    "parent_entity_id": scope.parent_entity_id,
                    "parent_comment_id": comment.parent_comment_id,
                    "reply_to_message_id": comment.reply_to_message_id,
                    "thread_root_post_id": comment.thread_root_post_id,
                    "origin_post_id": scope.origin_post_id,
                    "author_id": comment.author.author_id if comment.author else None,
                    "created_at": comment.created_at,
                    "message": comment.message,
                    "depth": comment.depth,
                    "permalink": comment.permalink,
                    "reactions": comment.reactions,
                    "reaction_breakdown_json": comment.reaction_breakdown_json,
                    "source_collector": comment.source_collector,
                    "raw_path": comment.raw_path,
                    "run_id": manifest.run_id,
                }
            )
            comment_edges.append(
                {
                    "comment_id": comment.comment_id,
                    "parent_post_id": comment.parent_post_id,
                    "parent_entity_type": scope.parent_entity_type,
                    "parent_entity_id": scope.parent_entity_id,
                    "parent_comment_id": comment.parent_comment_id,
                    "reply_to_message_id": comment.reply_to_message_id,
                    "thread_root_post_id": comment.thread_root_post_id,
                    "origin_post_id": scope.origin_post_id,
                    "depth": comment.depth,
                    "run_id": manifest.run_id,
                }
            )
            if comment.author and comment.author.author_id:
                authors.append(
                    {
                        "author_id": comment.author.author_id,
                        "name": comment.author.name,
                        "profile_url": comment.author.profile_url,
                        "source_collector": comment.source_collector,
                        "run_id": manifest.run_id,
                    }
                )

    collection_runs = [
        {
            "run_id": manifest.run_id,
            "collected_at": manifest.collected_at,
            "collector": manifest.collector,
            "mode": manifest.mode,
            "status": manifest.status,
            "fallback_used": manifest.fallback_used,
            "warning_count": len(manifest.warnings),
            "post_count": len([record for record in posts_records if not record.get("is_propagation")]),
            "propagation_count": len(propagation_records),
            "comment_count": len(comments_records),
            "platform": manifest.source.platform,
            "source_id": manifest.source.source_id,
            "source_name": manifest.source.source_name,
            "source_type": manifest.source.source_type,
            "discussion_linked": manifest.source.discussion_linked,
            "filtered_service_message_count": manifest.source.filtered_service_message_count,
            "source_run_count": len(source_run_ids),
            "source_run_ids": source_run_ids,
        }
    ]

    return {
        "posts": posts_records,
        "propagations": propagation_records,
        "propagation_edges": propagation_edges,
        "comments": comments_records,
        "comment_edges": comment_edges,
        "authors": authors,
        "media_refs": media_refs,
        "collection_runs": collection_runs,
    }

