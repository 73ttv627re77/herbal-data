#!/usr/bin/env python3
"""
Herbal Data Pipeline — FastAPI Server

Serves structured remedy/condition/claim data with full provenance,
safety endpoints, CORS, pagination, and OpenAPI docs.
"""

from __future__ import annotations

import logging
import math
import os
import sys
from contextlib import asynccontextmanager
from typing import Any

import psycopg2
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("api_server")

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_db():
    return psycopg2.connect(config.DATABASE_URL)


# ---------------------------------------------------------------------------
# Pagination helpers
# ---------------------------------------------------------------------------

def paginate_query(
    conn,
    base_sql: str,
    params: list,
    count_sql: str,
    limit: int,
    offset: int,
) -> tuple[list[dict], int]:
    """Execute a paginated query. Returns (rows, total_count)."""
    with conn.cursor() as cur:
        cur.execute(count_sql, params)
        total = cur.fetchone()[0] or 0

        cur.execute(base_sql + " LIMIT %s OFFSET %s", params + [limit, offset])
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]

    return rows, total


def build_page_meta(total: int, limit: int, offset: int) -> dict:
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "page": math.floor(offset / limit) + 1 if limit > 0 else 1,
        "pages": math.ceil(total / limit) if limit > 0 else 1,
        "has_next": (offset + limit) < total,
        "has_prev": offset > 0,
    }


# ---------------------------------------------------------------------------
# Response models (plain dict schemas for documentation)
# ---------------------------------------------------------------------------

REMEDY_SUMMARY = {
    "id": "uuid",
    "slug": "string",
    "name": "string",
    "category": "string",
    "description": "string|null",
    "mention_count": "int",
    "evidence_level": "string",
}

REMEDY_DETAIL = {
    **REMEDY_SUMMARY,
    "image_url": "string|null",
    "safety_notes": "string|null",
    "aliases": [{"alias": "string", "is_primary": "bool"}],
    "preparations": [{
        "id": "uuid",
        "preparation_type": "string",
        "dosage_amount": "string|null",
        "dosage_unit": "string|null",
        "frequency": "string|null",
        "route": "string|null",
        "notes": "string|null",
    }],
}

CONDITION_SUMMARY = {
    "id": "uuid",
    "slug": "string",
    "name": "string",
    "category": "string|null",
    "description": "string|null",
}

CLAIM_SUMMARY = {
    "id": "uuid",
    "claim_summary": "string",
    "extracted_span": "string | null",
    "claim_type": "string",
    "polarity": "string",
    "negation": "string",
    "certainty": "string",
    "confidence_score": "float",
    "method": "string|null",
    "dosage": "string|null",
    "sentiment": "string|null",
    "cultural_tag": "string|null",
    "remedy": REMEDY_SUMMARY,
    "condition": CONDITION_SUMMARY,
    "sources": [{
        "comment_id": "uuid",
        "platform": "string",
        "external_id": "string",
        "body": "string",
        "subreddit": "string|null",
        "posted_at": "datetime",
        "relevance_score": "float",
    }],
    "extracted_by": "string",
    "extracted_at": "datetime",
}

SAFETY_RESPONSE = {
    "remedy": REMEDY_SUMMARY,
    "contraindications": [{
        "id": "uuid",
        "condition": "string",
        "severity": "string",
        "description": "string|null",
        "source": "string|null",
    }],
    "interactions": [{
        "id": "uuid",
        "substance": "string",
        "interaction_type": "string",
        "description": "string|null",
        "severity": "string|null",
    }],
    "safety_notes": "string|null",
    "evidence_level": "string",
    "medical_disclaimer": (
        "This content is crowd-sourced and not verified by medical professionals. "
        "Do not use this information for self-diagnosis or treatment."
    ),
}


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Starting Herbal Data API server")
    log.info("Database: %s", config.DATABASE_URL.split("@")[1] if "@" in config.DATABASE_URL else "localhost")
    yield
    log.info("Shutting down Herbal Data API server")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Herbal Data API",
    description=(
        "Structured folk remedy data crowd-sourced from Reddit. "
        "Provides remedies, conditions, claims with provenance, "
        "safety information, and evidence grades."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS if config.CORS_ORIGINS != ["*"] else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def remedy_row_to_dict(row: dict) -> dict:
    return dict(row)


def condition_row_to_dict(row: dict) -> dict:
    return dict(row)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health", tags=["health"])
async def health():
    """Basic health check endpoint."""
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        conn.close()
        return {"status": "ok", "database": "connected"}
    except Exception as exc:
        log.error("Health check failed: %s", exc)
        return JSONResponse({"status": "degraded", "database": "disconnected"}, status_code=503)


# ---------------------------------------------------------------------------
# Remedies
# ---------------------------------------------------------------------------

@app.get("/remedies", tags=["remedies"], response_model=dict)
async def list_remedies(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    category: str | None = None,
    evidence_level: str | None = None,
):
    """
    List all remedies, sorted by mention_count descending (most discussed first).
    Supports pagination and optional filters.
    """
    conn = get_db()
    try:
        where_clauses = []
        params: list = []

        if category:
            where_clauses.append("category = %s")
            params.append(category)

        if evidence_level:
            where_clauses.append("evidence_level = %s")
            params.append(evidence_level)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        base_sql = f"""
            SELECT id, slug, name, category, description, mention_count, evidence_level,
                   image_url, safety_notes, created_at, updated_at
            FROM remedies
            {where_sql}
            ORDER BY mention_count DESC
        """
        count_sql = f"SELECT COUNT(*) FROM remedies {where_sql}"

        rows, total = paginate_query(conn, base_sql, params, count_sql, limit, offset)

        return {
            "data": [remedy_row_to_dict(r) for r in rows],
            "pagination": build_page_meta(total, limit, offset),
        }
    finally:
        conn.close()


@app.get("/remedies/{slug}", tags=["remedies"], response_model=dict)
async def get_remedy(slug: str):
    """Get a single remedy with its aliases, preparations, and summary stats."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, slug, name, category, description, image_url,
                       mention_count, evidence_level, safety_notes,
                       created_at, updated_at
                FROM remedies WHERE slug = %s
                """,
                (slug,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Remedy not found")

            columns = [desc[0] for desc in cur.description]
            remedy = dict(zip(columns, row))

            # Aliases
            cur.execute(
                "SELECT alias, is_primary FROM remedy_aliases WHERE remedy_id = %s",
                (remedy["id"],),
            )
            remedy["aliases"] = [{"alias": r[0], "is_primary": r[1]} for r in cur.fetchall()]

            # Preparations
            cur.execute(
                """
                SELECT id, preparation_type, dosage_amount, dosage_unit,
                       frequency, duration, route, notes
                FROM preparations WHERE remedy_id = %s
                """,
                (remedy["id"],),
            )
            cols = [desc[0] for desc in cur.description]
            remedy["preparations"] = [dict(zip(cols, r)) for r in cur.fetchall()]

            return remedy
    finally:
        conn.close()


@app.get("/remedies/{slug}/claims", tags=["remedies"], response_model=dict)
async def get_remedy_claims(
    slug: str,
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    min_confidence: float = Query(default=0.0, ge=0.0, le=1.0),
    polarity: str | None = None,
):
    """
    Get all claims for a remedy with full source provenance.
    Includes the exact claim text, polarity, confidence, and
    links back to the source Reddit comment.
    """
    conn = get_db()
    try:
        # Resolve remedy_id from slug
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM remedies WHERE slug = %s", (slug,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Remedy not found")
            remedy_id = str(row[0])

        # Build claims query
        where_clauses = ["c.remedy_id = %s"]
        params = [remedy_id]

        if min_confidence > 0:
            where_clauses.append("c.confidence_score >= %s")
            params.append(min_confidence)

        if polarity:
            where_clauses.append("c.polarity = %s")
            params.append(polarity)

        where_sql = "WHERE " + " AND ".join(where_clauses)

        base_sql = f"""
            SELECT c.id, c.claim_summary, c.claim_type, c.polarity,
                   c.negation, c.certainty, c.confidence_score,
                   c.method_text, c.dosage_text,
                   c.cultural_tag, c.extracted_span, c.extracted_by, c.extracted_at,
                   r.id as remedy_id, r.slug as remedy_slug, r.name as remedy_name,
                   r.category as remedy_category, r.evidence_level,
                   co.id as condition_id, co.slug as condition_slug,
                   co.name as condition_name, co.category as condition_category
            FROM claims c
            JOIN remedies r ON r.id = c.remedy_id
            LEFT JOIN conditions co ON co.id = c.condition_id
            {where_sql}
            ORDER BY c.confidence_score DESC, c.extracted_at DESC
        """
        count_sql = f"SELECT COUNT(*) FROM claims c {where_sql}"

        rows, total = paginate_query(conn, base_sql, params, count_sql, limit, offset)

        # Attach sources to each claim
        claims = []
        for row in rows:
            claim = {
                "id": str(row["id"]),
                "claim_summary": row["claim_summary"],
                "polarity": row["polarity"],
                "confidence_score": float(row["confidence_score"]),
                "negation": row["negation"],
                "certainty": row["certainty"],
                "method_text": row["method_text"],
                "dosage_text": row["dosage_text"],
                "cultural_tag": row["cultural_tag"],
                "claim_type": row["claim_type"],
                "extracted_span": row["extracted_span"],
                "extracted_by": row["extracted_by"],
                "extracted_at": row["extracted_at"],
                "remedy": {
                    "id": str(row["remedy_id"]),
                    "slug": row["remedy_slug"],
                    "name": row["remedy_name"],
                    "category": row["remedy_category"],
                    "evidence_level": row["evidence_level"],
                },
                "condition": None,
                "sources": [],
            }

            if row["condition_id"]:
                claim["condition"] = {
                    "id": str(row["condition_id"]),
                    "slug": row["condition_slug"],
                    "name": row["condition_name"],
                    "category": row["condition_category"],
                }

            claims.append(claim)

        # Batch-fetch all sources for these claims
        if claims:
            claim_ids = [c["id"] for c in claims]
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT cs.claim_id, cs.relevance_score,
                           sc.id as comment_id, sc.platform, sc.external_id,
                           sc.body, sc.author_hash, sc.score, sc.posted_at,
                           sp.subreddit
                    FROM claim_sources cs
                    JOIN source_comments sc ON sc.id = cs.comment_id
                    JOIN source_posts sp ON sp.id = sc.post_id
                    WHERE cs.claim_id = ANY(%s)
                    ORDER BY cs.relevance_score DESC
                    """,
                    (claim_ids,),
                )
                for src_row in cur.fetchall():
                    src_claim_id = str(src_row[0])
                    for claim in claims:
                        if claim["id"] == src_claim_id:
                            claim["sources"].append({
                                "comment_id": str(src_row[2]),
                                "platform": src_row[3],
                                "external_id": src_row[4],
                                "body": src_row[5],
                                "author_hash": src_row[6],
                                "score": src_row[7],
                                "posted_at": src_row[8],
                                "subreddit": src_row[9],
                                "relevance_score": float(src_row[1]) if src_row[1] else 1.0,
                            })
                            break

        return {
            "data": claims,
            "pagination": build_page_meta(total, limit, offset),
        }
    finally:
        conn.close()


@app.get("/remedies/{slug}/evidence", tags=["remedies"], response_model=dict)
async def get_remedy_evidence(
    slug: str,
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
):
    """Get all evidence items (RCTs, meta-analyses, etc.) for a remedy."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM remedies WHERE slug = %s", (slug,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Remedy not found")
            remedy_id = str(row[0])

        base_sql = """
            SELECT id, evidence_type, quality_score, title, authors,
                   pubmed_id, doi, url, year, finding, summary, ingested_at
            FROM evidence_items
            WHERE remedy_id = %s
            ORDER BY quality_score DESC, year DESC
        """
        count_sql = "SELECT COUNT(*) FROM evidence_items WHERE remedy_id = %s"

        rows, total = paginate_query(conn, base_sql, [remedy_id], count_sql, limit, offset)

        return {
            "data": [dict(r) for r in rows],
            "pagination": build_page_meta(total, limit, offset),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Safety
# ---------------------------------------------------------------------------

@app.get("/remedies/{slug}/safety", tags=["safety"], response_model=dict)
async def get_remedy_safety(slug: str):
    """
    Get safety profile for a remedy: contraindications, drug interactions,
    and a medical disclaimer.
    """
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, slug, name, safety_notes, evidence_level FROM remedies WHERE slug = %s",
                (slug,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Remedy not found")

            columns = [desc[0] for desc in cur.description]
            remedy = dict(zip(columns, row))

            # Contraindications
            cur.execute(
                """
                SELECT id, condition, severity, description, source
                FROM contraindications WHERE remedy_id = %s
                ORDER BY
                    CASE severity
                        WHEN 'contraindicated' THEN 1
                        WHEN 'caution' THEN 2
                        ELSE 3 END
                """,
                (remedy["id"],),
            )
            cols = [desc[0] for desc in cur.description]
            contraindications = [dict(zip(cols, r)) for r in cur.fetchall()]

            # Interactions
            cur.execute(
                """
                SELECT id, substance, interaction_type, description, severity
                FROM interactions WHERE remedy_id = %s
                ORDER BY
                    CASE severity
                        WHEN 'major' THEN 1 WHEN 'moderate' THEN 2 ELSE 3 END
                """,
                (remedy["id"],),
            )
            cols = [desc[0] for desc in cur.description]
            interactions = [dict(zip(cols, r)) for r in cur.fetchall()]

        return {
            "remedy": {
                "id": str(remedy["id"]),
                "slug": remedy["slug"],
                "name": remedy["name"],
                "evidence_level": remedy["evidence_level"],
            },
            "contraindications": contraindications,
            "interactions": interactions,
            "safety_notes": remedy["safety_notes"],
            "evidence_level": remedy["evidence_level"],
            "medical_disclaimer": (
                "This content is crowd-sourced from public social media posts "
                "and has not been verified by medical professionals. "
                "Do not use this information for self-diagnosis, treatment, "
                "or as a substitute for professional medical advice."
            ),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Conditions
# ---------------------------------------------------------------------------

@app.get("/conditions", tags=["conditions"], response_model=dict)
async def list_conditions(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
):
    """List all conditions."""
    conn = get_db()
    try:
        base_sql = """
            SELECT id, slug, name, category, description, created_at
            FROM conditions ORDER BY name ASC
        """
        count_sql = "SELECT COUNT(*) FROM conditions"
        rows, total = paginate_query(conn, base_sql, [], count_sql, limit, offset)
        return {
            "data": [condition_row_to_dict(r) for r in rows],
            "pagination": build_page_meta(total, limit, offset),
        }
    finally:
        conn.close()


@app.get("/conditions/{slug}", tags=["conditions"], response_model=dict)
async def get_condition(slug: str):
    """Get a single condition with associated remedies and claim counts."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, slug, name, category, description FROM conditions WHERE slug = %s",
                (slug,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Condition not found")

            columns = [desc[0] for desc in cur.description]
            condition = dict(zip(columns, row))

            # Remedies associated with this condition
            cur.execute(
                """
                SELECT DISTINCT r.id, r.slug, r.name, r.category, r.evidence_level,
                       COUNT(c.id) as claim_count
                FROM claims c
                JOIN remedies r ON r.id = c.remedy_id
                WHERE c.condition_id = %s
                GROUP BY r.id, r.slug, r.name, r.category, r.evidence_level
                ORDER BY claim_count DESC
                """,
                (condition["id"],),
            )
            cols = [desc[0] for desc in cur.description]
            condition["remedies"] = [dict(zip(cols, r)) for r in cur.fetchall()]

            return condition
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Claims (global)
# ---------------------------------------------------------------------------

@app.get("/claims", tags=["claims"], response_model=dict)
async def list_claims(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    remedy_id: str | None = None,
    condition_id: str | None = None,
    min_confidence: float = Query(default=0.0, ge=0.0, le=1.0),
    polarity: str | None = None,
    negation: str | None = None,
    certainty: str | None = None,
):
    """
    List all claims with optional filters.
    Each claim includes full source provenance.
    """
    conn = get_db()
    try:
        where_clauses = []
        params: list = []

        if remedy_id:
            where_clauses.append("c.remedy_id = %s")
            params.append(remedy_id)

        if condition_id:
            where_clauses.append("c.condition_id = %s")
            params.append(condition_id)

        if min_confidence > 0:
            where_clauses.append("c.confidence_score >= %s")
            params.append(min_confidence)

        if polarity:
            where_clauses.append("c.polarity = %s")
            params.append(polarity)

        if negation is not None:
            where_clauses.append("c.negation = %s")
            params.append(negation)

        if certainty:
            where_clauses.append("c.certainty = %s")
            params.append(certainty)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        base_sql = f"""
            SELECT c.id, c.claim_summary, c.claim_type, c.polarity,
                   c.negation, c.certainty, c.confidence_score,
                   c.method_text, c.dosage_text,
                   c.cultural_tag, c.extracted_span, c.extracted_by, c.extracted_at,
                   r.id as remedy_id, r.slug as remedy_slug, r.name as remedy_name,
                   r.evidence_level,
                   co.id as condition_id, co.slug as condition_slug,
                   co.name as condition_name
            FROM claims c
            JOIN remedies r ON r.id = c.remedy_id
            LEFT JOIN conditions co ON co.id = c.condition_id
            {where_sql}
            ORDER BY c.confidence_score DESC
        """
        count_sql = f"SELECT COUNT(*) FROM claims c {where_sql}"

        rows, total = paginate_query(conn, base_sql, params, count_sql, limit, offset)

        # Attach sources
        claims = []
        for row in rows:
            claim = {
                "id": str(row["id"]),
                "claim_summary": row["claim_summary"],
                "polarity": row["polarity"],
                "confidence_score": float(row["confidence_score"]),
                "negation": row["negation"],
                "certainty": row["certainty"],
                "method_text": row["method_text"],
                "dosage_text": row["dosage_text"],
                "cultural_tag": row["cultural_tag"],
                "claim_type": row["claim_type"],
                "extracted_span": row["extracted_span"],
                "extracted_by": row["extracted_by"],
                "extracted_at": row["extracted_at"],
                "remedy": {
                    "id": str(row["remedy_id"]),
                    "slug": row["remedy_slug"],
                    "name": row["remedy_name"],
                    "evidence_level": row["evidence_level"],
                },
                "condition": None,
                "sources": [],
            }
            if row["condition_id"]:
                claim["condition"] = {
                    "id": str(row["condition_id"]),
                    "slug": row["condition_slug"],
                    "name": row["condition_name"],
                }
            claims.append(claim)

        if claims:
            claim_ids = [c["id"] for c in claims]
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT cs.claim_id, cs.relevance_score,
                           sc.id, sc.platform, sc.external_id, sc.body,
                           sc.author_hash, sc.score, sc.posted_at, sp.subreddit
                    FROM claim_sources cs
                    JOIN source_comments sc ON sc.id = cs.comment_id
                    JOIN source_posts sp ON sp.id = sc.post_id
                    WHERE cs.claim_id = ANY(%s)
                    """,
                    (claim_ids,),
                )
                for src_row in cur.fetchall():
                    src_claim_id = str(src_row[0])
                    for claim in claims:
                        if claim["id"] == src_claim_id:
                            claim["sources"].append({
                                "comment_id": str(src_row[2]),
                                "platform": src_row[3],
                                "external_id": src_row[4],
                                "body": src_row[5],
                                "author_hash": src_row[6],
                                "score": src_row[7],
                                "posted_at": src_row[8],
                                "subreddit": src_row[9],
                                "relevance_score": float(src_row[1]) if src_row[1] else 1.0,
                            })

        return {
            "data": claims,
            "pagination": build_page_meta(total, limit, offset),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

@app.get("/search", tags=["search"], response_model=dict)
async def search(
    q: str = Query(..., min_length=1),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
):
    """
    Search across remedies, conditions, and aliases simultaneously.
    Returns remedies and conditions matching the query.
    """
    conn = get_db()
    try:
        pattern = f"%{q}%"

        # Remedies via aliases
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT r.id, r.slug, r.name, r.category,
                       r.mention_count, r.evidence_level, 'remedy' as result_type
                FROM remedies r
                LEFT JOIN remedy_aliases ra ON ra.remedy_id = r.id
                WHERE r.name ILIKE %s
                   OR ra.alias ILIKE %s
                   OR r.slug ILIKE %s
                ORDER BY r.mention_count DESC
                LIMIT %s OFFSET %s
                """,
                (pattern, pattern, pattern, limit, offset),
            )
            cols = [desc[0] for desc in cur.description]
            remedy_results = [dict(zip(cols, r)) for r in cur.fetchall()]

            cur.execute(
                """
                SELECT COUNT(DISTINCT r.id)
                FROM remedies r
                LEFT JOIN remedy_aliases ra ON ra.remedy_id = r.id
                WHERE r.name ILIKE %s
                   OR ra.alias ILIKE %s
                   OR r.slug ILIKE %s
                """,
                (pattern, pattern, pattern),
            )
            remedy_total = cur.fetchone()[0] or 0

        # Conditions via aliases
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT c.id, c.slug, c.name, c.category, c.description,
                       'condition' as result_type
                FROM conditions c
                LEFT JOIN condition_aliases ca ON ca.condition_id = c.id
                WHERE c.name ILIKE %s
                   OR ca.alias ILIKE %s
                   OR c.slug ILIKE %s
                ORDER BY c.name ASC
                LIMIT %s OFFSET %s
                """,
                (pattern, pattern, pattern, limit, offset),
            )
            cols = [desc[0] for desc in cur.description]
            condition_results = [dict(zip(cols, r)) for r in cur.fetchall()]

            cur.execute(
                """
                SELECT COUNT(DISTINCT c.id)
                FROM conditions c
                LEFT JOIN condition_aliases ca ON ca.condition_id = c.id
                WHERE c.name ILIKE %s
                   OR ca.alias ILIKE %s
                   OR c.slug ILIKE %s
                """,
                (pattern, pattern, pattern),
            )
            condition_total = cur.fetchone()[0] or 0

        return {
            "query": q,
            "remedies": {
                "data": remedy_results,
                "total": remedy_total,
            },
            "conditions": {
                "data": condition_results,
                "total": condition_total,
            },
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "server:app",
        host=config.API_HOST,
        port=config.API_PORT,
        reload=False,
    )
