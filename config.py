# =============================================================================
# Herbal Data Pipeline — Configuration
# =============================================================================
# All settings are loaded from environment variables.
# Copy .env.example to .env and fill in your values.

import os

# ── Reddit API ──────────────────────────────────────────────────────────────
REDDIT_CLIENT_ID: str = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET: str = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT: str = os.getenv("REDDIT_USER_AGENT", "HerbalDataBot/1.0")

# ── Database ────────────────────────────────────────────────────────────────
DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://localhost:5432/herbal_data")

# ── OpenAI ──────────────────────────────────────────────────────────────────
OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# ── Paths ───────────────────────────────────────────────────────────────────
RAW_DATA_DIR: str = os.getenv("RAW_DATA_DIR", "./raw")
STATE_FILE: str = os.getenv("STATE_FILE", "./state/last_run.json")

# ── Scraping targets ────────────────────────────────────────────────────────
SUBREDDITS: list[str] = [
    "herbalism",
    "Supplements",
    "AlternativeHealth",
    "nutrition",
    "HomeRemedies",
    "AskDocs",
    "ChronicPain",
]

SEARCH_KEYWORDS: list[str] = [
    "remedy", "cure", "herbal", "natural", "tea", "tincture",
    "supplement", "helped with", "worked for", "folk remedy",
    "grandmother", "traditional",
]

# ── API ─────────────────────────────────────────────────────────────────────
API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
API_PORT: int = int(os.getenv("API_PORT", "8000"))
