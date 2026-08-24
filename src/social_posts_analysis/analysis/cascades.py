from __future__ import annotations

from collections import deque
from typing import Any

import polars as pl

CASCADE_METRICS_SCHEMA: dict[str, Any] = {
    "cascade_type": pl.String,
    "scope_id": pl.String,
    "node_count": pl.Int64,
    "max_depth": pl.Int64,
    "max_breadth": pl.Int64,
    "structural_virality": pl.Float64,
    "run_id": pl.String,
}

# Pairwise-distance computation is quadratic; skip structural virality for
# very large components instead of stalling the pipeline.
_MAX_VIRALITY_NODES = 2000


def compute_cascade_metrics(
    comment_edges: pl.DataFrame,
    propagation_edges: pl.DataFrame,
    run_id: str,
) -> pl.DataFrame:
    """Compute per-cascade shape metrics for discussion trees and propagation stars.

    Comment cascades are grouped by ``parent_post_id``; the tree is formed by
    ``parent_comment_id -> comment_id`` links. Propagation cascades are grouped
    by ``origin_post_id`` as a star around the origin post.

    Metrics per cascade:
    - ``node_count``: number of comments (or propagations)
    - ``max_depth``: longest chain from the root (top-level comments / origin)
    - ``max_breadth``: widest single level of the tree
    - ``structural_virality``: mean pairwise shortest-path distance across all
      nodes including the root (Goel et al., 2015); ``None`` when fewer than 2
      nodes or above the size cap
    """
    rows = [*comment_cascade_rows(comment_edges), *propagation_cascade_rows(propagation_edges)]
    if not rows:
        return pl.DataFrame(schema=CASCADE_METRICS_SCHEMA)
    return pl.DataFrame(rows, schema=CASCADE_METRICS_SCHEMA)


def comment_cascade_rows(comment_edges: pl.DataFrame) -> list[dict[str, Any]]:
    if comment_edges.is_empty() or "parent_post_id" not in comment_edges.columns:
        return []
    rows: list[dict[str, Any]] = []
    for keys, group in _partitioned(comment_edges, ["run_id", "parent_post_id"]):
        scope_id = keys[-1]
        if not scope_id:
            continue
        origin_node = f"o:{scope_id}"
        child_ids = [f"c:{cid}" for cid in group["comment_id"].to_list()]
        parent_ids = group["parent_comment_id"].to_list()
        # Top-level comments hang off the origin post so the cascade stays a
        # single connected tree (and structural virality is well-defined).
        edges = [
            (origin_node, f"c:{cid}")
            for cid, parent in zip(group["comment_id"].to_list(), parent_ids, strict=False)
            if not parent
        ]
        edges += [(f"c:{parent}", child) for parent, child in zip(parent_ids, child_ids, strict=False) if parent]
        shape = forest_shape(
            nodes=[origin_node, *child_ids],
            edges=edges,
            roots=[origin_node],
            exclude_roots_from_count=True,
        )
        rows.append({"cascade_type": "comment_tree", "scope_id": scope_id, **shape, "run_id": keys[0]})
    return rows


def propagation_cascade_rows(propagation_edges: pl.DataFrame) -> list[dict[str, Any]]:
    if propagation_edges.is_empty() or "origin_post_id" not in propagation_edges.columns:
        return []
    rows: list[dict[str, Any]] = []
    for keys, group in _partitioned(propagation_edges, ["run_id", "origin_post_id"]):
        scope_id = keys[-1]
        if not scope_id:
            continue
        nodes = [f"p:{pid}" for pid in group["propagation_id"].to_list()]
        origin_node = f"o:{scope_id}"
        shape = forest_shape(
            nodes=[origin_node, *nodes],
            edges=[(origin_node, node) for node in nodes],
            roots=[origin_node],
            exclude_roots_from_count=True,
        )
        rows.append({"cascade_type": "propagation_tree", "scope_id": scope_id, **shape, "run_id": keys[0]})
    return rows


def _partitioned(frame: pl.DataFrame, keys: list[str]) -> Any:
    return frame.partition_by(keys, as_dict=True).items()


def forest_shape(
    *,
    nodes: list[str],
    edges: list[tuple[str, str]],
    roots: list[str],
    exclude_roots_from_count: bool = False,
) -> dict[str, Any]:
    """Depth/breadth/virality for one cascade given its nodes and directed edges."""
    children: dict[str, list[str]] = {}
    child_set: set[str] = set()
    for parent, child in edges:
        children.setdefault(parent, []).append(child)
        child_set.add(child)

    start_nodes = [root for root in roots if root] or list(set(nodes) - child_set)

    max_depth = 0
    queue: deque[tuple[str, int]] = deque((start, 0) for start in start_nodes)
    visited: set[str] = set(start_nodes)
    level_counts: dict[int, int] = {}
    while queue:
        node, depth = queue.popleft()
        max_depth = max(max_depth, depth)
        level_counts[depth] = level_counts.get(depth, 0) + 1
        for child in children.get(node, []):
            if child not in visited:
                visited.add(child)
                queue.append((child, depth + 1))

    virality = None
    if len(nodes) >= 2 and len(nodes) <= _MAX_VIRALITY_NODES:
        virality = structural_virality(nodes=nodes, children=children)

    return {
        "node_count": len(nodes) - len(start_nodes) if exclude_roots_from_count else len(nodes),
        "max_depth": max_depth,
        "max_breadth": max(level_counts.values()) if level_counts else 0,
        "structural_virality": virality,
    }


def structural_virality(*, nodes: list[str], children: dict[str, list[str]]) -> float | None:
    """Mean pairwise shortest-path distance over all ordered node pairs.

    Distances run on the undirected version of the cascade tree, matching the
    definition used in Goel, Anderson, Hofman, Watts (2015).
    """
    adjacency: dict[str, list[str]] = {node: [] for node in nodes}
    for parent, kids in children.items():
        for child in kids:
            if parent in adjacency and child in adjacency:
                adjacency[parent].append(child)
                adjacency[child].append(parent)

    total_distance = 0
    for start in nodes:
        distance = {start: 0}
        queue = deque([start])
        while queue:
            current = queue.popleft()
            for neighbour in adjacency[current]:
                if neighbour not in distance:
                    distance[neighbour] = distance[current] + 1
                    total_distance += distance[neighbour]
                    queue.append(neighbour)
    pair_count = len(nodes) * (len(nodes) - 1)
    if pair_count == 0:
        return None
    return total_distance / pair_count
