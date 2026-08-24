-- Analysis queries over the DuckDB projection of the processed parquet tables.
-- Usage:
--   duckdb -c ".read examples/analysis_queries.sql" data/processed/social_posts_analysis.duckdb
-- or from Python: duckdb.connect(path).execute(query)
--
-- Tables referenced here are synced by `social-posts-analysis analyze` /
-- `export-tables`: posts, propagations, comments, comment_edges,
-- propagation_edges, stance_labels, support_metrics, cascade_metrics,
-- near_duplicates, detected_languages, narrative_clusters.

-- 1. Support balance per side, with the Wilson interval the pipeline stores.
SELECT side_id,
       support_count,
       oppose_count,
       neutral_count,
       round(support_ratio, 3)                          AS support_ratio,
       round(support_ratio_low, 3)                      AS ratio_low_95,
       round(support_ratio_high, 3)                     AS ratio_high_95
FROM support_metrics
WHERE scope_type = 'global'
ORDER BY support_ratio DESC;

-- 2. Most discussed origin posts by extracted comment volume.
SELECT p.post_id,
       p.message,
       count(c.comment_id)                              AS comments_extracted,
       max(p.comments_count)                            AS comments_visible
FROM posts p
LEFT JOIN comments c ON c.parent_post_id = p.post_id
WHERE NOT p.is_propagation
GROUP BY p.post_id, p.message
ORDER BY comments_extracted DESC
LIMIT 20;

-- 3. Propagation overview: which origins spread most and through which channels.
SELECT e.origin_post_id,
       e.propagation_kind,
       count(*)                                         AS propagations,
       sum(coalesce(pr.reactions, 0))                   AS reactions_on_copies
FROM propagation_edges e
LEFT JOIN propagations pr USING (propagation_id)
GROUP BY e.origin_post_id, e.propagation_kind
ORDER BY propagations DESC;

-- 4. Deepest discussion trees: where conversation actually develops.
SELECT scope_id,
       node_count,
       max_depth,
       max_breadth,
       round(structural_virality, 3)                    AS virality
FROM cascade_metrics
WHERE cascade_type = 'comment_tree'
ORDER BY max_depth DESC, node_count DESC
LIMIT 15;

-- 5. Near-duplicate pairs joined back to their texts (copypasta audit).
SELECT nd.similarity,
       nd.item_type_a                                   AS type_a,
       nd.item_id_a                                     AS id_a,
       nd.item_type_b                                   AS type_b,
       nd.item_id_b                                     AS id_b,
       left(a.message, 120)                             AS text_a,
       left(b.message, 120)                             AS text_b
FROM near_duplicates nd
JOIN posts a ON a.post_id = nd.item_id_a
JOIN posts b ON b.post_id = nd.item_id_b
ORDER BY nd.similarity DESC;

-- 6. Stance of commenters inside the largest narrative clusters.
SELECT nc.label                                          AS narrative,
       sl.side_id,
       sl.label                                          AS stance,
       count(*)                                          AS items
FROM stance_labels sl
JOIN cluster_memberships cm
  ON cm.run_id = sl.run_id AND cm.item_type = sl.item_type AND cm.item_id = sl.item_id
JOIN narrative_clusters nc
  ON nc.run_id = cm.run_id AND nc.item_type = cm.item_type AND nc.cluster_id = cm.cluster_id
GROUP BY nc.label, sl.side_id, sl.label
ORDER BY narrative, sl.side_id, items DESC;

-- 7. Language mix of the discussion under each origin post.
SELECT c.parent_post_id                                  AS post_id,
       dl.language,
       count(*)                                          AS comments
FROM comments c
JOIN detected_languages dl
  ON dl.run_id = c.run_id AND dl.item_type = 'comment' AND dl.item_id = c.comment_id
GROUP BY c.parent_post_id, dl.language
ORDER BY post_id, comments DESC;
