-- =============================================================================
-- herbal-data / database/migrations/002_moderation_queue.sql
--
-- Adds supporting structures for the moderation queue system:
--   - entity_flags   : user/community flagging on claims/clusters
--   - v_remedy_condition_conflict_rollup : controversy scoring view
--   - moderation_locks : claim-level work-assignment for multi-mod teams
--   - trg_claims_auto_route : auto-set mod_status based on confidence/flags/score
--
-- Idempotent: all CREATE TABLE / CREATE INDEX use IF NOT EXISTS
-- Run after main schema.sql (001_initial_schema.sql)
-- =============================================================================

BEGIN;

-- =============================================================================
-- 1) ENTITY FLAGS
-- User or system-initiated flags on claims / clusters.
-- Active flags feed the moderation queue priority score.
-- =============================================================================

CREATE TABLE IF NOT EXISTS entity_flags (
    entity_flag_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type      TEXT NOT NULL,           -- 'claim', 'cluster', 'collection'
    entity_id        UUID NOT NULL,
    reason           flag_reason NOT NULL,
    severity         moderation_severity NOT NULL DEFAULT 'info',
    is_active        BOOLEAN NOT NULL DEFAULT TRUE,
    flagged_by       UUID REFERENCES app_users(user_id) ON DELETE SET NULL,
    flagged_by_display TEXT,                   -- for anonymous/user flags
    note             TEXT,
    resolved_by      UUID REFERENCES app_users(user_id) ON DELETE SET NULL,
    resolved_at      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ux_entity_flags_unique
        UNIQUE (entity_type, entity_id, reason, flagged_by, is_active)
);

CREATE INDEX IF NOT EXISTS ix_entity_flags_entity
    ON entity_flags (entity_type, entity_id, is_active);
CREATE INDEX IF NOT EXISTS ix_entity_flags_reason
    ON entity_flags (reason, is_active);
CREATE INDEX IF NOT EXISTS ix_entity_flags_severity
    ON entity_flags (severity, is_active);

-- =============================================================================
-- 2) V_REMEDY_CONDITION_CONFLICT_ROLLUP
-- Precomputed controversy scoring per remedy+condition pair.
-- is_conflicted = mixed+negative both > 0 (i.e., genuine disagreement in community)
-- controversy_score = |positive - negative| / total  (0 = unanimous, 1 = perfectly split)
-- Rebuild this view on a schedule or after bulk ingestion.
-- =============================================================================

CREATE OR REPLACE VIEW v_remedy_condition_conflict_rollup AS
SELECT
    c.remedy_id,
    c.condition_id,
    COUNT(*)                                          AS total_claims,
    SUM(CASE WHEN c.polarity = 'positive' THEN 1 ELSE 0 END)  AS positive_count,
    SUM(CASE WHEN c.polarity = 'negative' THEN 1 ELSE 0 END)  AS negative_count,
    SUM(CASE WHEN c.polarity = 'mixed'    THEN 1 ELSE 0 END)  AS mixed_count,
    SUM(CASE WHEN c.polarity = 'neutral'  THEN 1 ELSE 0 END) AS neutral_count,
    AVG(c.confidence_score)                            AS avg_confidence,
    -- Controversy: ratio of the minority position to total (0=unanimous, 0.5=split)
    CASE
        WHEN COUNT(*) > 0 THEN
            GREATEST(
                SUM(CASE WHEN c.polarity = 'positive' THEN 1 ELSE 0 END),
                SUM(CASE WHEN c.polarity = 'negative' THEN 1 ELSE 0 END),
                SUM(CASE WHEN c.polarity = 'mixed'    THEN 1 ELSE 0 END)
            )::FLOAT / COUNT(*)::FLOAT
        ELSE 0.0
    END AS controversy_score,
    -- True conflict: both positive AND negative present in meaningful numbers
    (
        SUM(CASE WHEN c.polarity = 'positive' THEN 1 ELSE 0 END) > 0
    AND SUM(CASE WHEN c.polarity = 'negative' THEN 1 ELSE 0 END) > 0
    AND SUM(CASE WHEN c.polarity = 'positive' THEN 1 ELSE 0 END)
        + SUM(CASE WHEN c.polarity = 'negative' THEN 1 ELSE 0 END)
        >= 3   -- require at least 3 opposing claims to flag as conflicted
    ) AS is_conflicted,
    MAX(sc.posted_at) AS most_recent_posted_at
FROM claims c
JOIN claim_sources cs ON cs.claim_id = c.claim_id
JOIN source_comments sc ON sc.source_comment_id = cs.source_comment_id
WHERE c.is_atomic = TRUE
  AND c.soft_deleted_at IS NULL
  AND c.is_hidden = FALSE
GROUP BY c.remedy_id, c.condition_id;

-- =============================================================================
-- 3) MODERATION LOCKS
-- Work-assignment table for multi-moderator environments.
-- Prevents two mods editing the same claim simultaneously.
-- Lock timeout: 15 minutes (configurable). Auto-expire via cleanup job.
-- =============================================================================

CREATE TABLE IF NOT EXISTS moderation_locks (
    lock_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type     TEXT NOT NULL,
    entity_id       UUID NOT NULL,
    locked_by       UUID NOT NULL REFERENCES app_users(user_id) ON DELETE CASCADE,
    locked_by_display TEXT,               -- snapshot of moderator name at lock time
    locked_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '15 minutes'),
    CONSTRAINT ux_moderation_locks_entity UNIQUE (entity_type, entity_id)
);

CREATE INDEX IF NOT EXISTS ix_moderation_locks_expires
    ON moderation_locks (expires_at);

-- Helper: acquire a lock (returns lock_id if acquired, NULL if already locked)
CREATE OR REPLACE FUNCTION acquire_moderation_lock(
    p_entity_type   TEXT,
    p_entity_id     UUID,
    p_locked_by     UUID,
    p_locked_by_display TEXT
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
    v_lock_id UUID;
BEGIN
    -- Clean up expired locks first
    DELETE FROM moderation_locks WHERE expires_at < now();

    -- Try to acquire
    INSERT INTO moderation_locks (entity_type, entity_id, locked_by, locked_by_display)
    VALUES (p_entity_type, p_entity_id, p_locked_by, p_locked_by_display)
    ON CONFLICT (entity_type, entity_id) DO NOTHING
    RETURNING lock_id INTO v_lock_id;

    RETURN v_lock_id;  -- NULL if lock was already held
END;
$$;

-- Helper: release a lock
CREATE OR REPLACE FUNCTION release_moderation_lock(
    p_entity_type   TEXT,
    p_entity_id     UUID,
    p_locked_by     UUID
) RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM moderation_locks
    WHERE entity_type = p_entity_type
      AND entity_id   = p_entity_id
      AND locked_by   = p_locked_by;
    RETURN FOUND;
END;
$$;

-- Helper: extend a lock (reset expiry to now + 15 min)
CREATE OR REPLACE FUNCTION extend_moderation_lock(
    p_entity_type   TEXT,
    p_entity_id     UUID,
    p_locked_by     UUID
) RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE moderation_locks
    SET expires_at = now() + interval '15 minutes'
    WHERE entity_type = p_entity_type
      AND entity_id   = p_entity_id
      AND locked_by    = p_locked_by;
    RETURN FOUND;
END;
$$;

-- =============================================================================
-- 4) AUTO-ROUTE TRIGGER
-- Automatically sets mod_status = 'pending_review' when:
--   - extraction confidence < 0.6  OR
--   - any active entity_flags exist  OR
--   - max source score > 100 (high visibility)
-- All other cases remain 'draft'.
-- Set in production once NLP quality is trusted.
-- =============================================================================

CREATE OR REPLACE FUNCTION claims_auto_route()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Only act on INSERT of new atomic claims
    IF TG_OP = 'INSERT' AND NEW.is_atomic = TRUE THEN
        -- Check if any active flags exist for this claim
        IF EXISTS (
            SELECT 1 FROM entity_flags ef
            WHERE ef.entity_type = 'claim'
              AND ef.entity_id   = NEW.claim_id
              AND ef.is_active   = TRUE
        ) THEN
            NEW.mod_status := 'pending_review';

        -- Low confidence
        ELSIF COALESCE(NEW.confidence_score_curated, NEW.confidence_score, NEW.confidence_score_machine, 0.0) < 0.6 THEN
            NEW.mod_status := 'pending_review';

        -- High visibility (source score > 100) — check via claim_sources join
        -- We can't easily check this in a row-level trigger without a subquery,
        -- so we check it in application code. Here we just set draft.
        ELSE
            NEW.mod_status := 'draft';
        END IF;
    END IF;

    RETURN NEW;
END;
$$;

-- Commented out by default — enable once NLP extraction quality is validated.
-- Uncomment the line below to activate:
-- CREATE TRIGGER trg_claims_auto_route
--     BEFORE INSERT ON claims
--     FOR EACH ROW
--     EXECUTE FUNCTION claims_auto_route();

COMMENT ON TRIGGER trg_claims_auto_route ON claims IS
    'Disabled by default. Uncomment the CREATE TRIGGER to activate auto-routing.
     Routes new claims to pending_review if: confidence < 0.6 OR any active flags.';

COMMIT;
