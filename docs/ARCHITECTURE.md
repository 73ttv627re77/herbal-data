# Herbal Data Pipeline — Architecture

## Overview

A community-driven natural remedy knowledge base that aggregates folk health experiences from social platforms, structures them for search/discovery, and serves them via API to a Flutter mobile app.

**Positioning:** A structured map of community experiences with herbal and supplement use, enriched (but not led) by external evidence.

```
Reddit (raw) → Scraper → source_posts / source_comments (raw, immutable)
                          ↓
                       NLP Extract → claims (derived, re-derivable)
                          ↓
                     API Server → Flutter App
                          ↑
              PubMed Enrichment (evidence_items)
```

## Principles

1. **Raw is immutable** — raw JSON stored in `source_posts`/`source_comments` never changes
2. **Structured is derived** — claims, aliases, preparations are all computed from raw data
3. **Every claim links to ≥1 source_comment** via `claim_sources` (never orphan claims)
4. **This is not a recommendation engine** — it surfaces experiences, not treatments
5. **Unknown/low-confidence are first-class** — absence of data ≠ absence of effect

## Data Model

### Core Entities

```
remedies ←→ remedy_aliases  (one-to-many, normalization)
conditions ←→ condition_aliases  (one-to-many, normalization)
remedies ←→ preparations  (one-to-many, dosage/route/duration)
remedies ←→ contraindications  (safety)
remedies ←→ interactions  (drug interactions)
```

### Provenance Layer (immutable raw)

```
source_posts  (platform, external_id, raw_json JSONB)
source_comments  (post_id FK, body, raw_json JSONB)
```

### Structured Layer (derived)

```
claims  (remedy_id, condition_id, claim_text, directionality, confidence, negation, hedging)
  ↑ claim_sources ↑ (claim_id, comment_id)
```

### Evidence Layer

```
evidence_items  (remedy_id, condition_id, type, quality_score, pubmed_id, finding)
```

## Reddit Scraper Flow

```
for each subreddit in list:
    search_posts(keywords, last_run_timestamp)  → posts
    for each post:
        save to source_posts (raw_json = full Reddit payload)
        fetch top-level comments (top 20 by score)
        for each comment:
            save to source_comments
    save batch to raw/reddit/{subreddit}/{date}.json
    update state/last_run.json
```

## NLP Pipeline

```
source_comments (unprocessed) →
    batch (10-20 per API call) →
        GPT-4o-mini extraction →
            entity resolution (via aliases) →
                insert claim + claim_sources
```

### Entity Resolution

- Extract raw remedy/condition text
- Check `remedy_aliases` / `condition_aliases` for match
- If no match: create new entity + add alias
- Confidence: NLP-reported confidence + heuristic adjustment

### Claim Fields

| Field | Type | Notes |
|---|---|---|
| directionality | improves/worsens/neutral/unclear | |
| confidence | 0.0–1.0 | NLP-reported, includes unknown |
| negation | bool | true if "didn't work" |
| hedging | bool | true if "might", "seems", "YMMV" |
| sentiment | positive/mixed/negative | |

## API Contract

Every remedy–condition response includes:

- `representative_claims[]` — verbatim claim snippets with source attribution
- `source_count` + `date_range` (oldest to newest source)
- `confidence_distribution` — histogram of confidence scores
- `evidence_label` — "anecdotal" | "supported" | "well-supported"
- `safety_disclaimer` — structured disclaimer object

```
GET /remedies/{slug}/claims?condition_id=...
→ {
    claims: [...],
    provenance: { source_count, date_range, confidence_distribution },
    evidence_label: "anecdotal",
    safety: { disclaimer, contraindications, interactions }
  }
```

## Scoring (NOT effectiveness ranking)

Do NOT rank by "effectiveness." Expose:

- **Volume** — number of experiences
- **Sentiment spread** — positive/mixed/negative ratio
- **Recency** — date of latest claim
- **Confidence median** — median confidence across claims

Let users interpret. This is not a recommendation engine.

## Safety Model

```
contraindications: { remedy_id, condition, severity, description }
interactions: { remedy_id, substance, type, severity }
```

Every API response includes `safety` struct with:
- Contraindication list
- Known drug interactions
- Evidence level
- "Not medical advice" disclaimer

## Evidence Integration

Evidence **annotates** remedy+condition pairs, never overrides community claims.

Evidence label mapping:
- `strong` — multiple RCTs + meta-analyses
- `moderate` — RCTs or strong observational
- `limited` — case reports, weak observational
- `mixed` — conflicting studies
- `none` — no evidence found
- `anecdotal` — community data only, no external evidence

**Rule:** Community experience shown first. Evidence shown as annotation below.

## Facebook Scraper — NOT ACTIVE

Intentionally excluded from MVP due to:
- Reliability issues (checkpoint blocks, auth unstable)
- ToS risk (scraping Facebook violates ToS)
- Maintenance cost (constant cat-and-mouse)
- Better data quality from Reddit (public, API-accessible, structured)

Documented in `facebook_scraper.py` for potential future activation with proper API access.

## Technology Stack

| Layer | Technology |
|---|---|
| Database | PostgreSQL 14+ |
| Reddit API | PRAW |
| NLP | GPT-4o-mini (OpenAI) |
| API | FastAPI |
| DB Client | psycopg2 |
| Scrape (Facebook) | Playwright |
| App | Flutter |

## Configuration

All via environment variables — see `config.py`.

```
REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT
DATABASE_URL
OPENAI_API_KEY
SUBREDDITS, SEARCH_KEYWORDS
RAW_DATA_DIR
```

## Future Enhancements

- PubMed enrichment (auto-link evidence_items via PMID/DOI)
- Semantic search (pgvector embeddings on claims)
- User auth + testimonials submission
- Preparation/dosage structured extraction
- Multilingual tagging