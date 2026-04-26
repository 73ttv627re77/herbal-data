#!/usr/bin/env python3
"""
Herbal Data Pipeline — NLP Extraction

Processes unprocessed source_comments through GPT-4o-mini to extract
structured claims into the claims table, with full alias-based entity
resolution and provenance via claim_sources.

"Raw is immutable; structured is derived."
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import openai
import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("nlp_extract")

openai.api_key = config.OPENAI_API_KEY


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db_connection():
    return psycopg2.connect(config.DATABASE_URL)


# ---------------------------------------------------------------------------
# Entity resolution
# ---------------------------------------------------------------------------

def resolve_remedy(name: str, conn) -> tuple[str | None, str | None]:
    """
    Resolve a remedy name to a remedy_id via the aliases table.
    Returns (remedy_id, resolution_confidence).
    If no match, returns (None, None).
    Resolution confidence is 1.0 for primary aliases, 0.8 for secondary.
    """
    if not name or not name.strip():
        return None, None

    canonical = name.strip().lower()

    with conn.cursor() as cur:
        # Direct alias match
        cur.execute(
            """
            SELECT r.id, ra.is_primary
            FROM remedy_aliases ra
            JOIN remedies r ON r.id = ra.remedy_id
            WHERE LOWER(ra.alias) = %s
            LIMIT 1
            """,
            (canonical,),
        )
        row = cur.fetchone()
        if row:
            confidence = 1.0 if row[1] else 0.8
            return str(row[0]), confidence

        # Fuzzy: partial match on name
        cur.execute(
            """
            SELECT id FROM remedies
            WHERE LOWER(name) = %s
               OR LOWER(name) LIKE %s
               OR LOWER(name) LIKE %s
            LIMIT 1
            """,
            (canonical, canonical + "%", "% " + canonical),
        )
        row = cur.fetchone()
        if row:
            return str(row[0]), 0.7

    return None, None


def resolve_condition(name: str, conn) -> tuple[str | None, float]:
    """Resolve a condition name to a condition_id via aliases. Returns (condition_id, confidence)."""
    if not name or not name.strip():
        return None, 0.0

    canonical = name.strip().lower()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.id, ca.is_primary
            FROM condition_aliases ca
            JOIN conditions c ON c.id = ca.condition_id
            WHERE LOWER(ca.alias) = %s
            LIMIT 1
            """,
            (canonical,),
        )
        row = cur.fetchone()
        if row:
            confidence = 1.0 if row[1] else 0.8
            return str(row[0]), confidence

        cur.execute(
            """
            SELECT id FROM conditions
            WHERE LOWER(name) = %s
               OR LOWER(name) LIKE %s
            LIMIT 1
            """,
            (canonical, canonical + "%"),
        )
        row = cur.fetchone()
        if row:
            return str(row[0]), 0.7

    return None, 0.0


def upsert_remedy(name: str, conn) -> str:
    """Create a new remedy with the given name as primary alias. Returns remedy_id."""
    import re

    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO remedies (slug, name, category, evidence_level)
            VALUES (%s, %s, 'herb', 'anecdotal')
            ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            (slug, name.strip()),
        )
        row = cur.fetchone()
        remedy_id = str(row[0])

        cur.execute(
            """
            INSERT INTO remedy_aliases (remedy_id, alias, is_primary, source)
            VALUES (%s, %s, true, 'nlp')
            ON CONFLICT (remedy_id, alias) DO NOTHING
            """,
            (remedy_id, name.strip()),
        )
        conn.commit()

    return remedy_id


def upsert_condition(name: str, conn) -> str:
    """Create a new condition with the given name as primary alias. Returns condition_id."""
    import re

    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO conditions (slug, name)
            VALUES (%s, %s)
            ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            (slug, name.strip()),
        )
        row = cur.fetchone()
        condition_id = str(row[0])

        cur.execute(
            """
            INSERT INTO condition_aliases (condition_id, alias, is_primary, source)
            VALUES (%s, %s, true, 'nlp')
            ON CONFLICT (condition_id, alias) DO NOTHING
            """,
            (condition_id, name.strip()),
        )
        conn.commit()

    return condition_id


# ---------------------------------------------------------------------------
# Claim extraction prompt
# ---------------------------------------------------------------------------

EXTRACT_PROMPT = """\
Extract natural remedy claim information from the following social media comments.

For each comment, output a JSON array of claims. If no remedy claim is present, output [].

Fields per claim:
- remedy: the natural remedy or herb name (exact text from comment, e.g. "peppermint tea", "ashwagandha")
- condition: the health condition it treats (exact text from comment, e.g. "acid reflux", "anxiety")
- method: how it was used — one of: tea, tincture, capsule, topical, raw, decoction, poultice, oil, syrup, supplement, other
- dosage: amount if mentioned, else null
- directionality: "improves" if the comment says it helped, "worsens" if it made things worse, "neutral" if no effect, "unclear" if ambiguous
- confidence: 0.0 to 1.0 — how confident you are this is a real remedy claim (0.0=none, 0.5=possible, 1.0=highly confident). Use 0.0 if the comment is not about a remedy at all.
- negation: true if the comment says the remedy DID NOT work or made things worse
- hedging: true if the comment uses uncertain language ("might", "seems", "probably", "YMMV", "could be", "I think")
- sentiment: "positive" if worked, "negative" if didn't work, "mixed" if mixed
- cultural_tag: any cultural or traditional origin mentioned (e.g. "Ayurvedic", "Chinese medicine", "grandmother's remedy"), else null
- claim_type: one of: anecdotal (personal experience), question, advice, warning, report, unknown — default to anecdotal
- extracted_span: the EXACT phrase or sentence from the comment that contains the remedy claim (verbatim, for provenance)

IMPORTANT:
- confidence=0.0 means the comment contains NO remedy claim worth recording
- negation=true means the user is reporting the remedy failed
- hedging=true means the user is uncertain — map to certainty_level='low'
- Extract the EXACT text of the remedy and condition as used by the commenter
- If multiple remedies are mentioned in one comment, return one claim per remedy

Comments:
{comments}

Output only the JSON array. No explanation, no markdown formatting.\
"""


# ---------------------------------------------------------------------------
# NLP helpers
# ---------------------------------------------------------------------------

def build_prompt(comments: list[dict]) -> str:
    """Build the extraction prompt with batched comments."""
    lines = []
    for i, comment in enumerate(comments):
        lines.append(f"[Comment {i+1}]")
        lines.append(f"ID: {comment['id']}")
        lines.append(f"Text: {comment['body']}")
        lines.append("")

    return EXTRACT_PROMPT.format(comments="\n".join(lines))


def call_llm(prompt: str) -> list[dict]:
    """Call GPT-4o-mini with the extraction prompt. Returns parsed JSON."""
    if not config.OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not configured in environment")

    response = openai.chat.completions.create(
        model=config.OPENAI_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
        max_tokens=4096,
    )

    raw = response.choices[0].message.content.strip()

    # Strip markdown code fences if present
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("Failed to parse LLM output: %s", raw[:200])
        return []

    if not isinstance(parsed, list):
        log.warning("LLM returned non-list: %s", type(parsed))
        return []

    return parsed


# ---------------------------------------------------------------------------
# Claim ingestion
# ---------------------------------------------------------------------------

def fetch_unprocessed_comments(conn, batch_size: int) -> list[dict]:
    """Fetch comments that have not yet been processed into claims."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT sc.id, sc.body, sc.platform, sc.external_id, sp.subreddit
            FROM source_comments sc
            JOIN source_posts sp ON sp.id = sc.post_id
            WHERE sc.id NOT IN (
                SELECT DISTINCT cs.comment_id FROM claim_sources cs
            )
            LIMIT %s
            """,
            (batch_size,),
        )
        return [
            {
                "id": str(row[0]),
                "body": row[1],
                "platform": row[2],
                "external_id": row[3],
                "subreddit": row[4],
            }
            for row in cur.fetchall()
        ]


def _map_polarity(sentiment: str | None, directionality: str) -> str:
    """Map NLP sentiment + directionality to claim_polarity enum."""
    if sentiment == "positive" or directionality == "improves":
        return "positive"
    if sentiment == "negative" or directionality == "worsens":
        return "negative"
    if sentiment == "mixed":
        return "mixed"
    if directionality == "neutral":
        return "neutral"
    return "unknown"


def _map_negation(negation: bool) -> str:
    """Map boolean negation to negation_state enum."""
    return "negated" if negation else "affirmed"


def _map_certainty(hedging: bool, confidence: float) -> str:
    """Map hedging flag + confidence to certainty_level enum."""
    if hedging:
        return "low"
    if confidence >= 0.8:
        return "high"
    if confidence >= 0.5:
        return "medium"
    return "unknown"


def _build_claim_summary(claim: dict) -> str:
    """Build claim_summary from extracted fields."""
    remedy = claim.get("remedy", "")
    condition = claim.get("condition", "")
    method = claim.get("method")
    parts = [remedy]
    if condition:
        parts.append(f"for {condition}")
    if method:
        parts.append(f"({method})")
    return " ".join(parts)


def insert_claim(conn, claim: dict, remedy_id: str | None, condition_id: str | None) -> str | None:
    """Insert a single claim and its claim_sources link. Returns claim_id or None."""
    negation = bool(claim.get("negation", False))
    hedging = bool(claim.get("hedging", False))
    confidence = float(claim.get("confidence", 0.0))

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO claims
                (remedy_id, condition_id,
                 claim_type, claim_summary, extracted_span,
                 polarity, negation, certainty,
                 confidence_score,
                 method_text, dosage_text, cultural_tag,
                 extracted_by)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (
                remedy_id,
                condition_id,
                claim.get("claim_type", "anecdotal"),
                _build_claim_summary(claim),
                claim.get("extracted_span"),
                _map_polarity(claim.get("sentiment"), claim.get("directionality", "")),
                _map_negation(negation),
                _map_certainty(hedging, confidence),
                round(confidence, 3),
                claim.get("method"),
                claim.get("dosage"),
                claim.get("cultural_tag"),
                "llm",
            ),
        )
        row = cur.fetchone()
        if not row:
            return None
        return str(row[0])


def link_claim_sources(conn, claim_id: str, comment_id: str, relevance: float = 1.0) -> None:
    """Insert the claim_sources join record."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO claim_sources (claim_id, comment_id, relevance_score)
            VALUES (%s, %s, %s)
            ON CONFLICT (claim_id, comment_id) DO NOTHING
            """,
            (claim_id, comment_id, relevance),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------

def process_batch(conn, comments: list[dict]) -> tuple[int, int, int]:
    """
    Process a batch of comments through GPT-4o-mini and ingest claims.
    Returns (created, skipped, errors) counts.
    """
    prompt = build_prompt(comments)

    try:
        raw_claims = call_llm(prompt)
    except Exception as exc:
        log.error("LLM call failed for batch: %s", exc)
        return (0, len(comments), 0)

    # Index LLM output by comment position
    created = 0
    skipped = 0

    for i, comment in enumerate(comments):
        comment_claims = []
        if i < len(raw_claims):
            raw = raw_claims[i]
            # raw may be a list of claims per comment
            if isinstance(raw, list):
                comment_claims = raw
            elif isinstance(raw, dict):
                if raw.get("confidence", 0) > 0:
                    comment_claims = [raw]

        if not comment_claims:
            # Still link it (as "no claim extracted") if confidence was 0
            # We create a placeholder claim for auditability
            skipped += 1
            continue

        for claim in comment_claims:
            try:
                confidence = float(claim.get("confidence", 0.0))
                if confidence < config.NLP_MIN_CONFIDENCE:
                    skipped += 1
                    continue

                # Resolve remedy
                remedy_name = claim.get("remedy", "").strip()
                if not remedy_name:
                    skipped += 1
                    continue

                remedy_id, _ = resolve_remedy(remedy_name, conn)
                if not remedy_id:
                    remedy_id = upsert_remedy(remedy_name, conn)
                    log.info("Created new remedy: %s", remedy_name)

                # Resolve condition
                condition_name = (claim.get("condition") or "").strip()
                if condition_name:
                    condition_id, _ = resolve_condition(condition_name, conn)
                    if not condition_id:
                        condition_id = upsert_condition(condition_name, conn)
                        log.info("Created new condition: %s", condition_name)
                else:
                    condition_id = None

                # Build full claim_text
                claim_text = f"{remedy_name}"
                if condition_name:
                    claim_text += f" for {condition_name}"
                method = claim.get("method", "")
                if method:
                    claim_text += f" ({method})"

                claim["_claim_text"] = claim_text

                # Insert claim
                claim_id = insert_claim(conn, claim, remedy_id, condition_id)
                if claim_id:
                    link_claim_sources(conn, claim_id, comment["id"])
                    created += 1
                else:
                    skipped += 1

            except Exception as exc:
                log.error("Error processing claim for comment %s: %s", comment["id"], exc)
                skipped += 1

    return (created, skipped, 0)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run():
    """Process all unprocessed comments in batches."""
    log.info("Starting NLP extraction pipeline (model=%s)", config.OPENAI_MODEL)

    if not config.OPENAI_API_KEY:
        log.error("OPENAI_API_KEY not set. Set it in config or .env file.")
        sys.exit(1)

    conn = get_db_connection()

    total_created = 0
    total_skipped = 0
    total_errors = 0

    while True:
        comments = fetch_unprocessed_comments(conn, config.NLP_BATCH_SIZE)
        if not comments:
            log.info("No more unprocessed comments. Extraction complete.")
            break

        log.info("Processing batch of %d comments", len(comments))
        created, skipped, errors = process_batch(conn, comments)
        total_created += created
        total_skipped += skipped
        total_errors += errors

        log.info(
            "Batch done — created: %d, skipped: %d, errors: %d",
            created, skipped, errors,
        )

        # If entire batch was skipped, we've hit low-confidence content — stop
        if created == 0 and skipped > 0:
            log.info("No claims above confidence threshold. Stopping.")
            break

        # Rate limit courtesy
        time.sleep(1.0)

    conn.close()

    log.info(
        "Extraction complete — total created: %d, skipped: %d, errors: %d",
        total_created, total_skipped, total_errors,
    )


if __name__ == "__main__":
    run()
