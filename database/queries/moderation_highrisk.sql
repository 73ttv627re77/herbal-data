-- =============================================================================
-- 3) HIGH RISK QUEUE
-- Flagged / low confidence / safety-related claims.
-- Prioritise anything reported as misinformation or dangerous advice.
-- =============================================================================

WITH active_flags AS (
    SELECT
        ef.entity_id::uuid  AS claim_id,
        COUNT(*) FILTER (WHERE ef.is_active = TRUE)                         AS active_flag_count,
        SUM(
            CASE ef.severity
                WHEN 'high'   THEN 5
                WHEN 'warning' THEN 2
                ELSE               1
            END
        ) FILTER (WHERE ef.is_active = TRUE) AS flag_weight,
        ARRAY_AGG(DISTINCT ef.reason) FILTER (WHERE ef.is_active = TRUE) AS active_flag_reasons,
        MAX(ef.created_at) FILTER (WHERE ef.is_active = TRUE)           AS last_flagged_at
    FROM entity_flags ef
    WHERE ef.entity_type = 'claim'
      AND ef.is_active   = TRUE
    GROUP BY ef.entity_id
),
claim_recency AS (
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
    af.active_flag_count,
    af.flag_weight,
    af.active_flag_reasons,
    af.last_flagged_at,
    cr.most_recent_source_utc,
    cr.max_source_score
FROM claims c
LEFT JOIN active_flags  af ON af.claim_id = c.claim_id
LEFT JOIN claim_recency cr ON cr.claim_id = c.claim_id
WHERE c.soft_deleted_at IS NULL
  AND c.is_hidden        = FALSE
  AND (
        c.mod_status = 'flagged'
     OR COALESCE(c.confidence_score_curated, c.confidence_score, c.confidence_score_machine, 0.0) < 0.35
     OR (
            af.active_flag_count > 0
        AND (
                'unsafe'    = ANY(af.active_flag_reasons)
             OR 'medical_claim' = ANY(af.active_flag_reasons)
             OR 'dangerous_advice' = ANY(af.active_flag_reasons)
            )
       )
     )
ORDER BY
    -- Flagged + severity first
    COALESCE(af.flag_weight, 0)        DESC,
    -- Then newest flags
    af.last_flagged_at                  DESC NULLS LAST,
    -- Then lowest confidence
    effective_confidence                 ASC,
    -- Then recency
    cr.most_recent_source_utc           DESC NULLS LAST
LIMIT 200;
