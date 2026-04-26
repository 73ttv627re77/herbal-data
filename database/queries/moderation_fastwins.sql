-- =============================================================================
-- 2) FAST WINS QUEUE
-- High confidence, high visibility, unreviewed.
-- Easy approvals — helps build quality coverage quickly.
-- =============================================================================

WITH claim_recency AS (
    SELECT
        cs.claim_id,
        MAX(sc.posted_at) AS most_recent_source_utc,
        MAX(sc.score)     AS max_source_score
    FROM claim_sources cs
    JOIN source_comments sc ON sc.source_comment_id = cs.source_comment_id
    GROUP BY cs.claim_id
)
SELECT
    c.claim_id,
    c.remedy_id,
    c.condition_id,
    c.claim_summary,
    c.polarity,
    COALESCE(c.confidence_score_curated, c.confidence_score, c.confidence_score_machine, 0.0)
        AS effective_confidence,
    c.mod_status,
    c.reviewed_at,
    cr.most_recent_source_utc,
    cr.max_source_score
FROM claims c
JOIN claim_recency cr ON cr.claim_id = c.claim_id
WHERE c.soft_deleted_at IS NULL
  AND c.is_hidden        = FALSE
  AND c.mod_status      IN ('draft', 'pending_review')
  AND c.reviewed_at     IS NULL
  AND COALESCE(c.confidence_score_curated, c.confidence_score, c.confidence_score_machine, 0.0) >= 0.80
  AND COALESCE(cr.max_source_score, 0) >= 25
ORDER BY cr.max_source_score DESC, cr.most_recent_source_utc DESC
LIMIT 200;
