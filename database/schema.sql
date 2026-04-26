-- =============================================================================
-- Herbal Data Pipeline — Database Schema
-- PostgreSQL 14+
-- Community-first: raw immutable, structured derived, every claim links to source
-- =============================================================================

BEGIN;

-- ── Extensions ──────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- =============================================================================
-- 1) ENUMERATIONS
-- =============================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'claim_polarity') THEN
        CREATE TYPE claim_polarity AS ENUM ('positive', 'negative', 'mixed', 'neutral', 'unknown');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'claim_type') THEN
        CREATE TYPE claim_type AS ENUM ('anecdotal', 'question', 'advice', 'warning', 'report', 'unknown');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'negation_state') THEN
        CREATE TYPE negation_state AS ENUM ('affirmed', 'negated', 'uncertain');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'certainty_level') THEN
        CREATE TYPE certainty_level AS ENUM ('high', 'medium', 'low', 'unknown');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'extraction_model') THEN
        CREATE TYPE extraction_model AS ENUM ('rules', 'llm', 'hybrid', 'unknown');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'evidence_type') THEN
        CREATE TYPE evidence_type AS ENUM (
            'meta_analysis', 'systematic_review', 'rct', 'observational',
            'case_report', 'mechanistic', 'guideline', 'expert_opinion', 'other'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'evidence_strength') THEN
        CREATE TYPE evidence_strength AS ENUM ('strong', 'moderate', 'limited', 'mixed', 'none', 'unknown');
    END IF;
END $$;

-- =============================================================================
-- 2) CORE TABLES
-- =============================================================================

-- ── remedies ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS remedies (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug                TEXT UNIQUE NOT NULL,
    name                TEXT NOT NULL,
    scientific_name     TEXT,
    category            TEXT CHECK (category IN ('herb', 'spice', 'food', 'supplement', 'mushroom', 'essential_oil')),
    description         TEXT,
    image_url           TEXT,
    mention_count       INT DEFAULT 0,
    evidence_level      TEXT DEFAULT 'anecdotal' CHECK (evidence_level IN ('clinical', 'traditional', 'anecdotal')),
    evidence_strength   evidence_strength DEFAULT 'unknown',
    safety_notes        TEXT,
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);

-- ── conditions ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS conditions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug            TEXT UNIQUE NOT NULL,
    name            TEXT NOT NULL,
    category        TEXT CHECK (category IN (
        'respiratory', 'digestive', 'pain', 'skin', 'immune',
        'mental', 'cardiovascular', 'sleep', 'hormonal', 'detox'
    )),
    description     TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- =============================================================================
-- 3) ENTITY RESOLUTION TABLES
-- =============================================================================

-- ── remedy_aliases ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS remedy_aliases (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    remedy_id   UUID NOT NULL REFERENCES remedies(id) ON DELETE CASCADE,
    alias       TEXT NOT NULL,
    is_primary  BOOLEAN DEFAULT false,
    source      TEXT CHECK (source IN ('user_input', 'nlp', 'manual')),
    created_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE(remedy_id, alias)
);

CREATE INDEX IF NOT EXISTS idx_remedy_aliases_remedy ON remedy_aliases(remedy_id);
CREATE INDEX IF NOT EXISTS idx_remedy_aliases_alias ON remedy_aliases(alias);

-- ── condition_aliases ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS condition_aliases (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    condition_id UUID NOT NULL REFERENCES conditions(id) ON DELETE CASCADE,
    alias       TEXT NOT NULL,
    is_primary  BOOLEAN DEFAULT false,
    source      TEXT CHECK (source IN ('user_input', 'nlp', 'manual')),
    created_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE(condition_id, alias)
);

CREATE INDEX IF NOT EXISTS idx_condition_aliases_condition ON condition_aliases(condition_id);
CREATE INDEX IF NOT EXISTS idx_condition_aliases_alias ON condition_aliases(alias);

-- =============================================================================
-- 4) REMEDY DETAIL TABLES
-- =============================================================================

-- ── preparations ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS preparations (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    remedy_id           UUID NOT NULL REFERENCES remedies(id) ON DELETE CASCADE,
    preparation_type    TEXT CHECK (preparation_type IN (
        'tea', 'tincture', 'capsule', 'topical', 'raw', 'decoction', 'poultice', 'oil', 'syrup'
    )),
    dosage_amount       TEXT,
    dosage_unit         TEXT CHECK (dosage_unit IN ('mg', 'g', 'ml', 'tsp', 'tbsp', 'capsule', 'drop')),
    frequency           TEXT CHECK (frequency IN ('daily', 'twice_daily', 'as_needed', 'weekly')),
    duration            TEXT,
    route               TEXT CHECK (route IN ('oral', 'topical', 'inhaled', 'sublingual')),
    notes               TEXT,
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_preparations_remedy ON preparations(remedy_id);

-- ── contraindications ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS contraindications (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    remedy_id   UUID NOT NULL REFERENCES remedies(id) ON DELETE CASCADE,
    condition   TEXT NOT NULL,
    severity    TEXT NOT NULL CHECK (severity IN ('contraindicated', 'caution', 'monitor')),
    description TEXT,
    source      TEXT,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_contraindications_remedy ON contraindications(remedy_id);

-- ── interactions ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS interactions (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    remedy_id           UUID NOT NULL REFERENCES remedies(id) ON DELETE CASCADE,
    substance           TEXT NOT NULL,
    interaction_type    TEXT CHECK (interaction_type IN ('increases_effect', 'decreases_effect', 'adverse')),
    description         TEXT,
    severity            TEXT CHECK (severity IN ('major', 'moderate', 'minor')),
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_interactions_remedy ON interactions(remedy_id);

-- =============================================================================
-- 5) RAW STORAGE (IMMUTABLE)
-- =============================================================================

-- ── source_posts ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS source_posts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    platform        TEXT NOT NULL,
    external_id     TEXT NOT NULL,
    subreddit       TEXT,
    title           TEXT,
    body            TEXT,
    url             TEXT NOT NULL,
    author_hash     TEXT,
    score           INT,
    comment_count   INT,
    posted_at       TIMESTAMPTZ,
    raw_json        JSONB NOT NULL,
    ingested_at     TIMESTAMPTZ DEFAULT now(),
    UNIQUE(platform, external_id)
);

CREATE INDEX IF NOT EXISTS idx_source_posts_platform ON source_posts(platform);
CREATE INDEX IF NOT EXISTS idx_source_posts_url ON source_posts(url);
CREATE INDEX IF NOT EXISTS idx_source_posts_raw_json ON source_posts USING gin(raw_json);

-- ── source_comments ─────────────────────────────────────────────────────────
-- IMPORTANT: raw_text and raw_json are immutable. is_deleted/is_removed are tombstone flags.
CREATE TABLE IF NOT EXISTS source_comments (
    source_comment_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    post_id             UUID REFERENCES source_posts(id) ON DELETE CASCADE,
    platform            TEXT NOT NULL,
    external_id         TEXT NOT NULL,
    parent_comment_id   TEXT,
    body                TEXT NOT NULL,
    author_hash         TEXT,
    score               INT,
    posted_at           TIMESTAMPTZ,
    raw_json            JSONB NOT NULL,
    ingested_at         TIMESTAMPTZ DEFAULT now(),
    -- Tombstone flags (deleted/removed content still kept for provenance)
    is_deleted          BOOLEAN NOT NULL DEFAULT FALSE,
    is_removed          BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE(platform, external_id)
);

CREATE INDEX IF NOT EXISTS idx_source_comments_post ON source_comments(post_id);
CREATE INDEX IF NOT EXISTS idx_source_comments_platform ON source_comments(platform);
-- Full-text search index for debugging/discovery
CREATE INDEX IF NOT EXISTS idx_source_comments_fts
    ON source_comments USING GIN (to_tsvector('simple', body));

-- =============================================================================
-- 6) CLAIM MODEL (STRUCTURED EXTRACTION — DERIVED, RE-DERIVABLE)
-- =============================================================================

-- Every claim MUST link to at least one source_comment via claim_sources.
-- No orphan claims. Confidence and unknown states are first-class.
CREATE TABLE IF NOT EXISTS claims (
    claim_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    remedy_id            UUID NOT NULL REFERENCES remedies(id) ON DELETE CASCADE,
    condition_id         UUID NOT NULL REFERENCES conditions(id) ON DELETE CASCADE,

    -- Community framing
    claim_type           claim_type NOT NULL DEFAULT 'anecdotal',

    -- Human-readable canonical summary of the claim
    claim_summary        TEXT NOT NULL,

    -- Exact extracted phrase from source (preserves original wording)
    extracted_span       TEXT,

    -- Polarity / negation / certainty
    polarity             claim_polarity NOT NULL DEFAULT 'unknown',
    negation             negation_state NOT NULL DEFAULT 'uncertain',
    certainty            certainty_level NOT NULL DEFAULT 'unknown',

    -- Confidence score 0..1 — always explicit, even if unknown
    confidence_score     NUMERIC(4,3) NOT NULL DEFAULT 0.000
        CHECK (confidence_score >= 0.0 AND confidence_score <= 1.0),

    -- Optional structured fields (freeform text in MVP, avoid false precision)
    method_text          TEXT,
    dosage_text          TEXT,
    duration_text        TEXT,
    route_text           TEXT,
    culture_tag          TEXT,

    -- Extraction lineage (for reprocessing)
    extractor            extraction_model NOT NULL DEFAULT 'unknown',
    extractor_version    TEXT,
    extraction_run_id    UUID,
    extracted_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Lifecycle
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Common access paths
CREATE INDEX IF NOT EXISTS ix_claims_remedy_condition
    ON claims (remedy_id, condition_id);
CREATE INDEX IF NOT EXISTS ix_claims_polarity
    ON claims (polarity);
CREATE INDEX IF NOT EXISTS ix_claims_confidence
    ON claims (confidence_score DESC);
-- Full-text search over claim summaries for discovery/debugging
CREATE INDEX IF NOT EXISTS ix_claims_fts
    ON claims USING GIN (to_tsvector('simple', claim_summary));

-- ── claim_sources (provenance — enforces every claim links to ≥1 source) ───
CREATE TABLE IF NOT EXISTS claim_sources (
    claim_source_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    claim_id             UUID NOT NULL REFERENCES claims(claim_id) ON DELETE CASCADE,
    source_comment_id    UUID NOT NULL REFERENCES source_comments(source_comment_id) ON DELETE RESTRICT,

    support_weight       NUMERIC(4,3) NOT NULL DEFAULT 1.000
        CHECK (support_weight >= 0.0 AND support_weight <= 1.0),

    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ux_claim_sources_unique UNIQUE (claim_id, source_comment_id)
);

CREATE INDEX IF NOT EXISTS ix_claim_sources_claim
    ON claim_sources (claim_id);
CREATE INDEX IF NOT EXISTS ix_claim_sources_source_comment
    ON claim_sources (source_comment_id);

-- ── Enforce at least one source per claim (deferrable trigger) ─────────────
-- Use DEFERRABLE so claim + claim_sources can be inserted in same transaction.
-- Only atomic claims must have a source. Clustered claims skip this.
CREATE OR REPLACE FUNCTION enforce_claim_has_source()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.is_atomic = TRUE AND NOT EXISTS (
        SELECT 1 FROM claim_sources cs
        WHERE cs.claim_id = NEW.claim_id
    ) THEN
        RAISE EXCEPTION 'Atomic claim % must have at least one source_comment in claim_sources',
            NEW.claim_id;
    END IF;
    RETURN NEW;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'trg_claims_must_have_source'
    ) THEN
        CREATE CONSTRAINT TRIGGER trg_claims_must_have_source
            AFTER INSERT OR UPDATE ON claims
            DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW
            EXECUTE FUNCTION enforce_claim_has_source();
    ELSE
        -- Recreate trigger with updated logic
        DROP TRIGGER IF EXISTS trg_claims_must_have_source ON claims;
        CREATE CONSTRAINT TRIGGER trg_claims_must_have_source
            AFTER INSERT OR UPDATE ON claims
            DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW
            EXECUTE FUNCTION enforce_claim_has_source();
    END IF;
END $$;

-- =============================================================================
-- 7) EVIDENCE MODEL
-- =============================================================================

-- Evidence items are standalone scientific citations that can be linked to
-- multiple remedy+condition pairs. Evidence annotates pairs, never overrides
-- community claims. Linked via remedy_condition_evidence.
CREATE TABLE IF NOT EXISTS evidence_items (
    evidence_item_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Evidence classification
    evidence_type        evidence_type NOT NULL DEFAULT 'other',
    strength             evidence_strength NOT NULL DEFAULT 'unknown',

    -- Citation metadata
    title                TEXT NOT NULL,
    authors              TEXT,
    year_published       INTEGER,
    journal              TEXT,

    doi                  TEXT,
    pmid                 TEXT,
    url                  TEXT,

    abstract_text        TEXT,
    notes                TEXT,

    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ux_evidence_doi UNIQUE (doi),
    CONSTRAINT ux_evidence_pmid UNIQUE (pmid)
);

-- Evidence links to remedy+condition PAIRS (not individual claims)
-- This is the many-to-many join: one evidence item can support multiple pairs.
-- Named remedy_condition_evidence per Yura's schema design.
CREATE TABLE IF NOT EXISTS remedy_condition_evidence (
    remedy_condition_evidence_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    remedy_id                    UUID NOT NULL REFERENCES remedies(id) ON DELETE CASCADE,
    condition_id                 UUID NOT NULL REFERENCES conditions(id) ON DELETE CASCADE,
    evidence_item_id             UUID NOT NULL REFERENCES evidence_items(evidence_item_id) ON DELETE CASCADE,

    -- Weighting per pairing (for evidence quality/strength at this specific pair)
    weight                       NUMERIC(4,3) NOT NULL DEFAULT 1.000
        CHECK (weight >= 0.0 AND weight <= 1.0),

    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ux_rce_unique UNIQUE (remedy_id, condition_id, evidence_item_id)
);

CREATE INDEX IF NOT EXISTS ix_rce_remedy_condition
    ON remedy_condition_evidence (remedy_id, condition_id);
CREATE INDEX IF NOT EXISTS ix_rce_evidence
    ON remedy_condition_evidence (evidence_item_id);

-- =============================================================================
-- 8) SAFETY FLAGS
-- =============================================================================

-- Keep separate from claims to avoid "advice masquerading as extraction".
-- Scoped at remedy level or remedy+condition level.
CREATE TABLE IF NOT EXISTS safety_flags (
    safety_flag_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    remedy_id            UUID NOT NULL REFERENCES remedies(id) ON DELETE CASCADE,
    condition_id         UUID REFERENCES conditions(id) ON DELETE SET NULL,

    flag                 TEXT NOT NULL,
    severity             TEXT NOT NULL DEFAULT 'info',
    reference_url        TEXT,

    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_safety_flags_remedy
    ON safety_flags (remedy_id);
CREATE INDEX IF NOT EXISTS ix_safety_flags_remedy_condition
    ON safety_flags (remedy_id, condition_id);

-- =============================================================================
-- 8a) CLAIM CLUSTERS (machine-generated grouping of atomic claims)
-- =============================================================================

CREATE TABLE IF NOT EXISTS claim_clusters (
    claim_cluster_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    remedy_id            UUID NOT NULL REFERENCES remedies(id) ON DELETE CASCADE,
    condition_id         UUID NOT NULL REFERENCES conditions(id) ON DELETE CASCADE,
    cluster_label        TEXT,
    clusterer            TEXT NOT NULL DEFAULT 'unknown',
    clusterer_version    TEXT,
    clustering_run_id    UUID,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_claim_clusters_remedy_condition
    ON claim_clusters (remedy_id, condition_id);

CREATE TABLE IF NOT EXISTS claim_cluster_members (
    claim_cluster_member_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    claim_cluster_id         UUID NOT NULL REFERENCES claim_clusters(claim_cluster_id) ON DELETE CASCADE,
    claim_id                 UUID NOT NULL REFERENCES claims(claim_id) ON DELETE CASCADE,
    membership_score         NUMERIC(4,3) NOT NULL DEFAULT 1.000
        CHECK (membership_score >= 0.0 AND membership_score <= 1.0),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ux_cluster_member_unique UNIQUE (claim_cluster_id, claim_id)
);

CREATE INDEX IF NOT EXISTS ix_claim_cluster_members_cluster
    ON claim_cluster_members (claim_cluster_id);
CREATE INDEX IF NOT EXISTS ix_claim_cluster_members_claim
    ON claim_cluster_members (claim_id);

-- =============================================================================
-- 8b) CLAIM COLLECTIONS (human-curated / editorial lists)
-- =============================================================================

CREATE TABLE IF NOT EXISTS claim_collections (
    claim_collection_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                 TEXT NOT NULL,
    description          TEXT,
    created_by           TEXT NOT NULL DEFAULT 'system',
    is_public            BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS claim_collection_items (
    claim_collection_item_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    claim_collection_id       UUID NOT NULL REFERENCES claim_collections(claim_collection_id) ON DELETE CASCADE,
    claim_id                 UUID REFERENCES claims(claim_id) ON DELETE CASCADE,
    claim_cluster_id         UUID REFERENCES claim_clusters(claim_cluster_id) ON DELETE CASCADE,
    position                 INTEGER NOT NULL DEFAULT 0,
    notes                    TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_collection_item_one_ref CHECK (
        (claim_id IS NOT NULL AND claim_cluster_id IS NULL)
        OR
        (claim_id IS NULL AND claim_cluster_id IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS ix_collection_items_collection
    ON claim_collection_items (claim_collection_id, position);

-- =============================================================================
-- 8c) ROLLUP VIEW (precomputed stats for fast API)
-- =============================================================================

CREATE OR REPLACE VIEW v_remedy_condition_rollup AS
SELECT
    c.remedy_id,
    c.condition_id,
    COUNT(*) AS claim_count,
    SUM(CASE WHEN c.polarity = 'positive' THEN 1 ELSE 0 END) AS positive_count,
    SUM(CASE WHEN c.polarity = 'negative' THEN 1 ELSE 0 END) AS negative_count,
    SUM(CASE WHEN c.polarity = 'mixed' THEN 1 ELSE 0 END) AS mixed_count,
    AVG(c.confidence_score) AS avg_confidence,
    MAX(sc.posted_at) AS most_recent_posted_at
FROM claims c
JOIN claim_sources cs ON cs.claim_id = c.claim_id
JOIN source_comments sc ON sc.source_comment_id = cs.source_comment_id
WHERE c.is_atomic = TRUE
GROUP BY c.remedy_id, c.condition_id;

-- =============================================================================
-- 9) USER CONTENT
-- =============================================================================

CREATE TABLE IF NOT EXISTS user_testimonials (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID,
    remedy_id   UUID NOT NULL REFERENCES remedies(id) ON DELETE CASCADE,
    condition_id UUID NOT NULL REFERENCES conditions(id) ON DELETE CASCADE,
    story       TEXT NOT NULL,
    method      TEXT,
    outcome     TEXT CHECK (outcome IN ('worked', 'partial', 'didn_t_work')),
    rating      INT CHECK (rating >= 1 AND rating <= 5),
    verified    BOOLEAN DEFAULT false,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_testimonials_remedy ON user_testimonials(remedy_id);
CREATE INDEX IF NOT EXISTS idx_testimonials_condition ON user_testimonials(condition_id);

-- =============================================================================
-- 10) TRIGGERS
-- =============================================================================

-- Update remedies.mention_count when claims are inserted or deleted
CREATE OR REPLACE FUNCTION update_remedy_mention_count()
RETURNS TRIGGER AS $$
DECLARE
    target_id UUID;
BEGIN
    IF TG_OP = 'INSERT' THEN
        target_id := NEW.remedy_id;
    ELSIF TG_OP = 'DELETE' THEN
        target_id := OLD.remedy_id;
    END IF;

    IF target_id IS NOT NULL THEN
        UPDATE remedies
        SET mention_count = (
            SELECT COUNT(*) FROM claims WHERE remedy_id = target_id
        ),
        updated_at = now()
        WHERE id = target_id;
    END IF;

    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_claims_mention_count ON claims;
CREATE TRIGGER trg_claims_mention_count
    AFTER INSERT OR DELETE ON claims
    FOR EACH ROW EXECUTE FUNCTION update_remedy_mention_count();

-- Update remedies.updated_at when claims change
CREATE OR REPLACE FUNCTION update_remedy_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE remedies SET updated_at = now() WHERE id = NEW.remedy_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_claims_updated_at ON claims;
CREATE TRIGGER trg_claims_updated_at
    AFTER UPDATE ON claims
    FOR EACH ROW EXECUTE FUNCTION update_remedy_timestamp();

COMMIT;
