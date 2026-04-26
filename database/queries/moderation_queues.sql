-- =============================================================================
-- Moderation Queue Queries
-- herbal-data / database/queries/moderation_queues.sql
--
-- Three triage queues for the moderation workflow:
--   1) Unified queue with computed priority score
--   2) Fast wins (high confidence, high visibility, easy approvals)
--   3) High risk (flagged, low confidence, safety-related)
--
-- Prerequisites (in schema):
--   claims with mod_status, confidence_score, curated fields
--   claim_sources → source_comments (for provenance)
--   entity_flags (for flag-based queues)
--   v_remedy_condition_conflict_rollup (optional, for conflict scoring)
-- =============================================================================


-- =============================================================================
-- 1) UNIFIED MODERATION QUEUE (ranked by priority score)
--
-- Priority = flag_weight*10 + confidence_penalty + visibility_boost + recency + conflict + status
-- Run this to populate the admin Review Queue list.
-- =============================================================================

WITH active_flags AS (
    SELECT
        ef.entity_id::uuid AS claim_id,
        COUNT(*) FILTER (WHERE ef.is_active = TRUE) AS active_flag_count,
        SUM(
            CASE ef.severity
                WHEN 'high'   THEN 5
                WHEN 'warning' THEN 2
                ELSE               1
            END
        ) FILTER (WHERE ef.is_active = TRUE) AS flag_weight,
        ARRAY_AGG(DISTINCT ef.reason) FILTER (WHERE ef.is_active = TRUE) AS active_flag_reasons
    FROM entity_flags ef
    WHERE ef.entity_type = 'claim'
      AND ef.is_active = TRUE
    GROUP BY ef.entity_id
),
claim_recency AS (
    SELECT
        cs.claim_id,
        MAX(sc.posted_at) AS most_recent_source_utc,
        MAX(sc.score)     AS max_source_score,
        COUNT(*)          AS source_count
    FROM claim_sources cs
    JOIN source_comments sc ON sc.source_comment_id = cs.source_comment_id
    GROUP BY cs.claim_id
),
conflict AS (
    -- Optional: remove this CTE if v_remedy_condition_conflict_rollup does not exist.
    SELECT
        remedy_id,
        condition_id,
        controversy_score,
        is_conflicted
    FROM v_remedy_condition_conflict_rollup
),
base AS (
    SELECT
        c.claim_id,
        c.remedy_id,
        c.condition_id,
        c.mod_status,
        c.is_hidden,
        c.soft_deleted_at,
        c.claim_type,
        c.claim_summary,
        c.polarity,
        c.negation,
        c.certainty,
        COALESCE(c.confidence_score_curated, c.confidence_score, c.confidence_score_machine, 0.0)
            AS effective_confidence,
        c.reviewed_by,
        c.reviewed_at,
        c.curated_by,
        c.curated_at,
        cr.most_recent_source_utc,
        cr.max_source_score,
        cr.source_count,
        COALESCE(af.active_flag_count, 0)         AS active_flag_count,
        COALESCE(af.flag_weight,    0)          AS flag_weight,
        COALESCE(af.active_flag_reasons, ARRAY[]::text[]) AS active_flag_reasons,
        COALESCE(cf.controversy_score, 0.0)    AS controversy_score,
        COALESCE(cf.is_conflicted, FALSE)      AS is_conflicted
    FROM claims c
    LEFT JOIN active_flags  af ON af.claim_id    = c.claim_id
    LEFT JOIN claim_recency cr ON cr.claim_id    = c.claim_id
    LEFT JOIN conflict      cf ON cf.remedy_id   = c.remedy_id
                            AND cf.condition_id = c.condition_id
    WHERE c.soft_deleted_at IS NULL
      AND c.is_hidden       = FALSE
      AND c.mod_status IN ('draft', 'pending_review', 'flagged')
)
SELECT
    b.*,

    -- Priority score: higher = review sooner
    (
        -- Flags matter most (severity-weighted)
        (b.flag_weight * 10)

        -- Low confidence extraction needs review
      + CASE
            WHEN b.effective_confidence < 0.30 THEN 30
            WHEN b.effective_confidence < 0.50 THEN 15
            WHEN b.effective_confidence < 0.70 THEN  5
            ELSE                                    0
        END

        -- High visibility content (upvoted / widely seen)
      + CASE
            WHEN COALESCE(b.max_source_score, 0) >= 500 THEN 15
            WHEN COALESCE(b.max_source_score, 0) >= 100 THEN  8
            WHEN COALESCE(b.max_source_score, 0) >=  25 THEN  4
            ELSE                                            0
        END

        -- Recency: new claims reviewed sooner
      + CASE
            WHEN b.most_recent_source_utc >= now() - interval '2 days' THEN 10
            WHEN b.most_recent_source_utc >= now() - interval '7 days' THEN  5
            ELSE                                                          0
        END

        -- Conflicted pairs may need cluster explanations
      + CASE WHEN b.is_conflicted THEN 5 ELSE 0 END

        -- Status boost
      + CASE b.mod_status
            WHEN 'pending_review' THEN  8
            WHEN 'flagged'       THEN  6
            ELSE                        0
        END
    )::INTEGER AS priority_score

FROM base b
ORDER BY priority_score DESC, b.most_recent_source_utc DESC NULLS LAST
LIMIT 200;
