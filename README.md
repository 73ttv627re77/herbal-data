# Herbal Data Pipeline

A structured knowledge base of community experiences with herbal and supplement remedies, enriched by external evidence.

**Positioning:** A structured map of community experiences with herbal and supplement use, enriched (but not led) by external evidence.

> ⚠️ This is not a recommendation engine. It surfaces community experiences, not treatments. Always include safety disclaimers.

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set Up PostgreSQL

```bash
psql -c "CREATE DATABASE herbal_data;"
psql -d herbal_data -f database/schema.sql
```

### 3. Configure Environment

```bash
# Copy and edit .env
cp .env.example .env  # (create .env with content below)

# Required:
export REDDIT_CLIENT_ID="your_reddit_client_id"
export REDDIT_CLIENT_SECRET="your_reddit_client_secret"
export OPENAI_API_KEY="your_openai_key"
export DATABASE_URL="postgresql://localhost:5432/herbal_data"
```

**Reddit API Key:** Apply at https://www.reddit.com/prefs/apps — create a script app.

**OpenAI Key:** Get from https://platform.openai.com/api-keys

### 4. Run Reddit Scraper

```bash
python -m scraper.reddit_scraper
# First run: ingests posts from last 30 days
# Subsequent runs: incremental (only new posts since last run)
```

Raw data saved to `raw/reddit/{subreddit}/{YYYY-MM-DD}.json` and to `source_posts`/`source_comments` tables.

### 5. Run NLP Extraction

```bash
python -m nlp.extract
# Reads unprocessed source_comments
# Extracts claims → claims + claim_sources tables
# Handles entity resolution via aliases
```

### 6. Start API Server

```bash
python -m api.server
# → http://localhost:8000
# → http://localhost:8000/docs (Swagger UI)
```

### 7. (Optional) Run Evaluation

```bash
python -m nlp.evaluate
# Requires manually labeled sample set (300 comments)
# Reports precision/recall per field
```

## Project Structure

```
herbal-data/
├── database/
│   └── schema.sql          # PostgreSQL schema (all tables, indexes, triggers)
├── scraper/
│   ├── reddit_scraper.py   # PRAW-based Reddit scraper
│   └── facebook_scraper.py # ⚠️ NOT ACTIVE — documented only
├── nlp/
│   ├── extract.py          # GPT-4o-mini claim extraction
│   └── evaluate.py         # Evaluation harness
├── api/
│   └── server.py           # FastAPI REST API
├── docs/
│   ├── ARCHITECTURE.md     # System design
│   └── DATA_ETHICS.md      # Compliance & ethics
├── config.py               # Environment variable config
└── requirements.txt       # Python dependencies
```

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /remedies` | List all remedies (sorted by mention_count) |
| `GET /remedies/{slug}` | Single remedy with aliases, preparations, safety |
| `GET /remedies/{slug}/claims` | Claims for this remedy (with provenance) |
| `GET /remedies/{slug}/safety` | Contraindications + interactions |
| `GET /conditions` | List all conditions |
| `GET /conditions/{slug}` | Single condition with linked remedies |
| `GET /search?q=` | Search across remedies + conditions + aliases |
| `GET /claims` | All claims (filter by remedy_id, condition_id, min_confidence) |
| `GET /health` | Health check |

## Key Design Decisions

### Raw is Immutable
`source_posts` and `source_comments` tables store raw JSON. Structured data (claims) is always derived and re-derivable. If NLP logic improves, re-run extraction on the same raw data.

### Every Claim Links to Source
No orphan claims — `claim_sources` join table ensures every claim traces back to at least one `source_comment`.

### Confidence + Negation as First-Class Fields
- `confidence: 0.0–1.0` — NLP-reported uncertainty
- `negation: true` — comment says "X didn't work"
- `hedging: true` — comment says "might", "seems", "YMMV"
- Unknown/low-confidence states are surfaced, not hidden

### Evidence Annotates, Never Leads
Community experience shown first. External evidence shown as: "Scientific evidence: limited / mixed / moderate / strong"

## Data Model

```
remedies ← remedy_aliases (normalization)
        ← preparations (dosage/route/duration)
        ← contraindications (safety)
        ← interactions (drug interactions)

conditions ← condition_aliases (normalization)

source_posts ← source_comments ← claims ← claim_sources
                                     ↑
                               evidence_items (external)
```

## Scoring (Not Effectiveness Ranking)

The API exposes:
- **volume** — number of experiences
- **sentiment spread** — positive/mixed/negative ratio
- **recency** — date of latest claim
- **confidence median** — median confidence across claims

Users interpret. We don't rank by "effectiveness."

## Facebook Scraper — NOT ACTIVE

Intentionally excluded from MVP. See `scraper/facebook_scraper.py` for documentation when/if activated in future.

## License

- Reddit content: CC BY-SA 4.0 (Reddit's license)
- Derived claims: MIT
- Evidence items: public domain (PubMed/open access)