# Herbal Data Pipeline — Build Task Brief (v2)

## Objective
Build a complete backend for a natural remedies app that crowd-sources folk remedy data from social media (Reddit primary, Facebook secondary). Designed for data longevity, entity resolution, evidence quality, and compliance.

## Core Principles
- **"Raw is immutable; structured is derived"** — store raw JSON, re-derive structured data when NLP improves
- **Every structured claim must link to a source_comment row**
- **Provenance by default** — top sources, extraction confidence, exact claim text with attribution
- **Safety rails** — contraindication hooks, "not medical advice" flags, evidence grade
- **Entity resolution first** — controlled vocabularies, alias mapping, confidence scoring
- **No Facebook scraping in MVP** — documented but not active

## Project Structure
```
herbal-data/
├── database/
│   └── schema.sql
├── scraper/
│   ├── reddit_scraper.py
│   └── facebook_scraper.py  (documentation only, not active)
├── nlp/
│   └── extract.py
├── api/
│   └── server.py
├── config.py
├── requirements.txt
├── docs/
│   ├── ARCHITECTURE.md
│   └── DATA_ETHICS.md
└── README.md
```

## 1. Database Schema (database/schema.sql)

PostgreSQL. All tables use UUID PRIMARY KEY DEFAULT gen_random_uuid().
All tables have created_at TIMESTAMPTZ DEFAULT now().

### Entity Resolution Tables

#### remedy_aliases
- id UUID PK
- remedy_id UUID FK → remedies
- alias TEXT NOT NULL (e.g., "St John's wort", "Hypericum perforatum", "SJW")
- is_primary BOOLEAN DEFAULT false
- source TEXT (user_input, nlp, manual)
- UNIQUE(remedy_id, alias)

#### condition_aliases
- id UUID PK
- condition_id UUID FK → conditions
- alias TEXT NOT NULL (e.g., "GERD", "acid reflux", "heartburn")
- is_primary BOOLEAN DEFAULT false
- source TEXT
- UNIQUE(condition_id, alias)

### Core Tables

#### remedies
- id UUID PK
- slug TEXT UNIQUE NOT NULL
- name TEXT NOT NULL (primary display name)
- category TEXT (herb, spice, food, supplement, mushroom, essential_oil)
- description TEXT
- image_url TEXT
- mention_count INT DEFAULT 0 (denormalized, trigger-updated)
- evidence_level TEXT DEFAULT 'anecdotal' (clinical, traditional, anecdotal)
- safety_notes TEXT (general safety information)
- updated_at TIMESTAMPTZ DEFAULT now()

#### conditions
- id UUID PK
- slug TEXT UNIQUE NOT NULL
- name TEXT NOT NULL
- category TEXT (respiratory, digestive, pain, skin, immune, mental, cardiovascular, sleep, hormonal, detox)
- description TEXT

#### preparations
- id UUID PK
- remedy_id UUID FK → remedies
- preparation_type TEXT (tea, tincture, capsule, topical, raw, decoction, poultice, oil, syrup)
- dosage_amount TEXT (e.g., "1 teaspoon", "500mg")
- dosage_unit TEXT (mg, g, ml, tsp, tbsp, capsule, drop)
- frequency TEXT (daily, twice_daily, as_needed, weekly)
- duration TEXT (e.g., "2 weeks", "ongoing")
- route TEXT (oral, topical, inhaled, sublingual)
- notes TEXT

#### contraindications
- id UUID PK
- remedy_id UUID FK → remedies
- condition TEXT NOT NULL (e.g., "pregnancy", "liver disease", "anticoagulant use")
- severity TEXT (contraindicated, caution, monitor)
- description TEXT
- source TEXT

#### interactions
- id UUID PK
- remedy_id UUID FK → remedies
- substance TEXT NOT NULL (drug name or class)
- interaction_type TEXT (increases_effect, decreases_effect, adverse)
- description TEXT
- severity TEXT (major, moderate, minor)

### Raw Storage (IMMUTABLE)

#### source_posts
- id UUID PK
- platform TEXT NOT NULL (reddit, facebook, tiktok)
- external_id TEXT NOT NULL (reddit post id)
- subreddit TEXT
- title TEXT
- body TEXT
- url TEXT NOT NULL
- author_hash TEXT (SHA256 of username — anonymized)
- score INT
- comment_count INT
- posted_at TIMESTAMPTZ
- raw_json JSONB NOT NULL (full raw payload)
- ingested_at TIMESTAMPTZ DEFAULT now()
- UNIQUE(platform, external_id)

#### source_comments
- id UUID PK
- post_id UUID FK → source_posts
- platform TEXT NOT NULL
- external_id TEXT NOT NULL
- parent_id TEXT (for threading)
- body TEXT NOT NULL
- author_hash TEXT
- score INT
- posted_at TIMESTAMPTZ
- raw_json JSONB NOT NULL
- ingested_at TIMESTAMPTZ DEFAULT now()
- UNIQUE(platform, external_id)

### Claim Model (structured extraction)

#### claims
- id UUID PK
- remedy_id UUID FK → remedies
- condition_id UUID FK → conditions
- claim_text TEXT NOT NULL (the exact claim as extracted)
- directionality TEXT (improves, worsens, neutral, unclear)
- confidence FLOAT DEFAULT 0.0 (0.0-1.0, from NLP)
- negation BOOLEAN DEFAULT false (true if "X didn't work")
- hedging BOOLEAN DEFAULT false (true if "might help", "seems to")
- method TEXT (how remedy was used)
- dosage TEXT
- cultural_tag TEXT
- sentiment TEXT (positive, mixed, negative)
- extracted_by TEXT (gpt-4o-mini, manual, rule-based)
- extracted_at TIMESTAMPTZ DEFAULT now()

#### claim_sources (join: claim ↔ source_comment)
- id UUID PK
- claim_id UUID FK → claims
- comment_id UUID FK → source_comments
- relevance_score FLOAT DEFAULT 1.0

### Evidence Model

#### evidence_items
- id UUID PK
- remedy_id UUID FK → remedies
- condition_id UUID FK → conditions
- evidence_type TEXT (rct, meta_analysis, observational, case_report, mechanistic, anecdote)
- quality_score INT (1-5 scale)
- title TEXT
- authors TEXT
- pubmed_id TEXT
- doi TEXT
- url TEXT
- year INT
- finding TEXT (effective, inconclusive, none, adverse)
- summary TEXT
- ingested_at TIMESTAMPTZ DEFAULT now()

### User Content (v2)

#### user_testimonials
- id UUID PK
- user_id UUID
- remedy_id UUID FK → remedies
- condition_id UUID FK → conditions
- story TEXT NOT NULL
- method TEXT
- outcome TEXT (worked, partial, didn_t_work)
- rating INT (1-5)
- verified BOOLEAN DEFAULT false

### Indexes
- idx_remedy_aliases_remedy ON remedy_aliases(remedy_id)
- idx_remedy_aliases_alias ON remedy_aliases(alias)
- idx_condition_aliases_condition ON condition_aliases(condition_id)
- idx_condition_aliases_alias ON condition_aliases(alias)
- idx_claims_remedy ON claims(remedy_id)
- idx_claims_condition ON claims(condition_id)
- idx_claims_confidence ON claims(confidence DESC)
- idx_claim_sources_claim ON claim_sources(claim_id)
- idx_claim_sources_comment ON claim_sources(comment_id)
- idx_source_posts_platform ON source_posts(platform)
- idx_source_posts_url ON source_posts(url)
- idx_source_comments_post ON source_comments(post_id)
- idx_evidence_items_remedy ON evidence_items(remedy_id)
- idx_remedies_mention ON remedies(mention_count DESC)

### Triggers
- After INSERT/DELETE on claims: update remedies.mention_count

## 2. Reddit Scraper (scraper/reddit_scraper.py)

Use PRAW. Save raw JSON to source_posts + source_comments tables AND to /raw/reddit/{subreddit}/{date}.json files.

### Configuration
- Subreddits: herbalism, Supplements, AlternativeHealth, nutrition, HomeRemedies, AskDocs, ChronicPain
- Search keywords: remedy, cure, herbal, natural, tea, tincture, supplement, helped with, worked for, folk remedy, grandmother, traditional
- Rate limit: respect Reddit API
- Batch: 100 posts per subreddit per run
- Incremental: only pull posts since last_run_timestamp (stored in a state file)

### Logic
1. Connect to Reddit API
2. For each subreddit: search posts matching keywords (past month, sorted by relevance)
3. For each post: extract title, selftext, score, num_comments, created_utc, url
4. Store in source_posts (raw_json = full Reddit API response)
5. Fetch top-level comments (top 20 by score)
6. Store each in source_comments (raw_json = full comment object)
7. Save batch JSON to /raw/reddit/{subreddit}/{date}.json
8. Track last_run_timestamp for incremental updates
9. Handle deleted/removed content: store tombstone state

### Deduplication
- Check source_posts(platform, external_id) UNIQUE constraint
- Skip if already exists

## 3. NLP Pipeline (nlp/extract.py)

Process source_comments → claims table.

### Approach
- GPT-4o-mini via OpenAI API
- Batch: 10-20 comments per API call
- Extract per comment:
  - remedy_name, condition_name, method, dosage
  - directionality (improves/worsens/neutral)
  - confidence (0.0-1.0)
  - negation (true if "didn't work")
  - hedging (true if "might help")
  - sentiment, cultural_tag

### Entity Resolution (CRITICAL)
- After extraction, resolve remedy/condition names against aliases tables
- If no match: create new remedy/condition + add extracted name as alias
- If match: use existing entity
- Track resolution confidence separately
- "unknown" state for unclear extractions

### Prompt Template
```
Extract natural remedy information from these social media comments.
For each comment, output a JSON array. If no remedy is mentioned, output [].

Fields per claim:
- remedy: remedy name (exact text from comment)
- condition: what it treats
- method: how used (tea, capsule, topical, etc.)
- dosage: amount if mentioned
- directionality: "improves" | "worsens" | "neutral" | "unclear"
- confidence: 0.0-1.0 (how confident you are this is a real remedy claim)
- negation: true if the comment says it DIDN'T work
- hedging: true if "might", "seems", "YMMV"
- sentiment: "positive" | "mixed" | "negative"
- cultural_tag: cultural origin if mentioned, else null

Comments:
{batch}
```

### Processing
1. Read unprocessed source_comments (WHERE id NOT IN claim_sources.comment_id)
2. Batch (10-20 per API call)
3. Parse JSON responses
4. For each extracted claim:
   - Resolve remedy name → remedy_id (via aliases)
   - Resolve condition name → condition_id (via aliases)
   - Insert claim record
   - Insert claim_sources link
5. Output: processed count, created count, skipped count

## 4. Facebook Scraper (scraper/facebook_scraper.py) — DOCUMENTATION ONLY

NOT ACTIVE. Playwright-based. Documented for future use.

### Status
- Blocked by Facebook security checkpoint
- NOT in MVP scope
- Script included as reference only
- Reddit is primary data source

### Architecture (when activated)
- Login via Playwright
- Navigate to target groups
- Scroll and extract posts/comments
- Save to source_posts/source_comments with platform='facebook'

## 5. API Server (api/server.py)

FastAPI. CORS enabled. Pagination (limit/offset).

### Endpoints

#### Remedies
- GET /remedies — list sorted by mention_count, pagination
- GET /remedies/{slug} — single remedy with aliases, preparations, contraindications
- GET /remedies/{slug}/claims — all claims for this remedy, with source provenance
- GET /remedies/{slug}/evidence — evidence items

#### Conditions
- GET /conditions — list all
- GET /conditions/{slug} — single condition with remedies

#### Search
- GET /search?q={query} — search across remedies, conditions, aliases

#### Claims
- GET /claims — all claims with filters (remedy_id, condition_id, min_confidence, directionality)
- Each claim includes: claim_text, directionality, confidence, negation, hedging, source provenance

#### Provenance (every remedy↔condition pair includes)
- top_sources (subreddit, post ids, timestamps)
- extraction_confidence
- whether anecdotal vs supported by external evidence
- exact claim text with attribution

#### Safety
- GET /remedies/{slug}/safety — contraindications + interactions
- Every response includes evidence_level field

#### Health
- GET /health
- GET /docs (OpenAPI auto-generated)

## 6. Configuration (config.py)

```python
import os

# Reddit
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = "HerbalDataBot/1.0"

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost:5432/herbal_data")

# OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = "gpt-4o-mini"

# Subreddits
SUBREDDITS = ["herbalism", "Supplements", "AlternativeHealth", "nutrition", "HomeRemedies", "AskDocs", "ChronicPain"]
SEARCH_KEYWORDS = ["remedy", "cure", "herbal", "natural", "tea", "tincture", "supplement", "helped with", "worked for", "folk remedy", "grandmother", "traditional"]

# Paths
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "./raw")
STATE_FILE = "./state/last_run.json"
```

## 7. Documentation

### docs/ARCHITECTURE.md
- Architecture diagram (ASCII)
- Database schema explanation
- Data flow: Reddit → raw JSON + source_posts → NLP → claims → API → Flutter
- Entity resolution strategy
- Claim model explanation
- Setup instructions

### docs/DATA_ETHICS.md
- Data retention policy (raw text stored indefinitely for reprocessing)
- Anonymization approach (SHA256 username hashing)
- Takedown process (remove from source_posts, cascade to claims)
- Sources & licensing / Reddit ToS considerations
- Medical misinformation prevention (evidence grading, safety rails, "not medical advice" flags)

## 8. README.md
Quick start: install, setup DB, configure keys, run scraper, run NLP, start API.

## 9. Evaluation Harness (nlp/evaluate.py)
- Sample 300 comments, manually label expected extraction
- Compare NLP output vs manual labels
- Report precision/recall per field (remedy, condition, method, sentiment)
- Run before scaling extraction to full dataset

## Acceptance Criteria
- [ ] schema.sql creates ALL tables including aliases, preparations, contraindications, interactions, source_posts, source_comments, claims, claim_sources, evidence_items
- [ ] reddit_scraper.py saves to both DB (source_posts/source_comments) and raw JSON files
- [ ] extract.py resolves entities via aliases table, handles negation/hedging
- [ ] facebook_scraper.py is documented, marked "NOT ACTIVE"
- [ ] server.py serves claims with full provenance
- [ ] DATA_ETHICS.md exists
- [ ] evaluate.py exists with sample labeling framework
- [ ] All config via environment variables
- [ ] No TODOs or placeholders

## Constraints
- Python 3.10+
- PRAW for Reddit
- FastAPI for API
- psycopg2 for PostgreSQL
- Minimal dependencies
- No hardcoded secrets
- Raw is immutable
- Every claim links to source
