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
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
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
CREATE INDEX IF NOT EXISTS idx_source_comments_community ON source_comments(post_id);
-- Full-text search index for debugging/discovery
CREATE INDEX IF NOT EXISTS idx_source_comments_fts
    ON source_comments USING GIN (to_tsvector('simple', body));

-- =============================================================================
-- 6) CLAIM MODEL (STRUCTURED EXTRACTION — DERIVED, RE-DERIVABLE)
-- =============================================================================

-- Every claim MUST link to at least one source_comment via claim_sources.
-- No orphan claims. Confidence and unknown states are first-class.
CREATE TABLE IF NOT EXISTS claims (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    remedy_id       UUID NOT NULL REFERENCES remedies(id) ON DELETE CASCADE,
    condition_id    UUID NOT NULL REFERENCES conditions(id) ON DELETE CASCADE,

    -- Community framing
    claim_type      claim_type NOT NULL DEFAULT 'anecdotal',

    -- Human-readable canonical summary of the claim
    claim_summary   TEXT NOT NULL,

    -- Exact extracted phrase from source (preserves original wording)
    extracted_span  TEXT,

    -- Polarity / negation / certainty
    polarity        claim_polarity NOT NULL DEFAULT 'unknown',
    negation        negation_state NOT NULL DEFAULT 'uncertain',
    certainty       certainty_level NOT NULL DEFAULT 'unknown',

    -- Confidence score 0..1 — always explicit, even if unknown
    confidence_score NUMERIC(4,3) NOT NULL DEFAULT 0.000
        CHECK (confidence_score >= 0.0 AND confidence_score <= 1.0),

    -- Optional structured fields (freeform text in MVP, avoid false precision)
    method_text     TEXT,
    dosage_text     TEXT,
    duration_text   TEXT,
    route_text      TEXT,
    culture_tag     TEXT,

    -- Provenance
    extracted_by    extraction_model DEFAULT 'unknown',
    extracted_at    TIMESTAMPTZ DEFAULT now(),
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_claims_remedy ON claims(remedy_id);
CREATE INDEX IF NOT EXISTS idx_claims_condition ON claims(condition_id);
CREATE INDEX IF NOT EXISTS idx_claims_confidence ON claims(confidence_score DESC);
CREATE INDEX IF NOT EXISTS idx_claims_polarity ON claims(polarity);

-- ── claim_sources (join: claim ↔ source_comment) ────────────────────────────
-- Every claim must have at least one source. This is the enforce rule.
CREATE TABLE IF NOT EXISTS claim_sources (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    claim_id        UUID NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
    comment_id      UUID NOT NULL REFERENCES source_comments(id) ON DELETE CASCADE,
    relevance_score FLOAT DEFAULT 1.0 CHECK (relevance_score >= 0.0 AND relevance_score <= 1.0),
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE(claim_id, comment_id)
);

CREATE INDEX IF NOT EXISTS idx_claim_sources_claim ON claim_sources(claim_id);
CREATE INDEX IF NOT EXISTS idx_claim_sources_comment ON claim_sources(comment_id);

-- =============================================================================
-- 7) EVIDENCE MODEL
-- =============================================================================

-- Evidence links at remedy+condition pair level — never overrides community claims.
-- Evidence annotates, never justifies.
CREATE TABLE IF NOT EXISTS evidence_items (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    remedy_id       UUID NOT NULL REFERENCES remedies(id) ON DELETE CASCADE,
    condition_id    UUID NOT NULL REFERENCES conditions(id) ON DELETE CASCADE,

    -- Evidence classification
    evidence_type   evidence_type,
    quality_score   INT CHECK (quality_score >= 1 AND quality_score <= 5),
    strength        evidence_strength DEFAULT 'unknown',

    -- Citation
    title           TEXT,
    authors         TEXT,
    pubmed_id       TEXT,
    doi             TEXT,
    url             TEXT,
    year            INT,

    -- Finding
    finding         TEXT CHECK (finding IN ('effective', 'inconclusive', 'none', 'adverse')),
    summary         TEXT,

    ingested_at     TIMESTAMPTZ DEFAULT now(),
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_evidence_items_remedy ON evidence_items(remedy_id);
CREATE INDEX IF NOT EXISTS idx_evidence_items_condition ON evidence_items(condition_id);

-- =============================================================================
-- 8) USER CONTENT
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
-- 9) TRIGGERS
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
