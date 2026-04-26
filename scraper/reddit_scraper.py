#!/usr/bin/env python3
"""
Herbal Data Pipeline — Reddit Scraper
Fetches posts and comments from herbalism-related subreddits.
Saves raw JSON to both the database (source_posts / source_comments) and
flat files under raw/reddit/{subreddit}/{date}.json.

Incremental: only pulls content newer than last_run_timestamp.
"""

import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import praw
import psycopg2
from psycopg2.extras import execute_values

# Ensure project root is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("reddit_scraper")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sha256(text: str) -> str:
    """Hash a string with SHA-256 for author anonymisation."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def utc_from_epoch(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def load_state() -> dict:
    """Load incremental-scrape state from disk."""
    os.makedirs(os.path.dirname(config.STATE_FILE), exist_ok=True)
    if os.path.exists(config.STATE_FILE):
        with open(config.STATE_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {}


def save_state(state: dict) -> None:
    os.makedirs(os.path.dirname(config.STATE_FILE), exist_ok=True)
    with open(config.STATE_FILE, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, default=str)


def get_db_connection():
    return psycopg2.connect(config.DATABASE_URL)


def get_reddit_client() -> praw.Reddit:
    return praw.Reddit(
        client_id=config.REDDIT_CLIENT_ID,
        client_secret=config.REDDIT_CLIENT_SECRET,
        user_agent=config.REDDIT_USER_AGENT,
    )


# ---------------------------------------------------------------------------
# Database upserts
# ---------------------------------------------------------------------------

def upsert_post(conn, post_data: dict) -> str | None:
    """Insert a post into source_posts. Returns the row UUID or None if skipped."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO source_posts
                (platform, external_id, subreddit, title, body, url,
                 author_hash, score, comment_count, posted_at, raw_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (platform, external_id) DO NOTHING
            RETURNING id
            """,
            (
                "reddit",
                post_data["external_id"],
                post_data["subreddit"],
                post_data["title"],
                post_data["body"],
                post_data["url"],
                post_data["author_hash"],
                post_data["score"],
                post_data["comment_count"],
                post_data["posted_at"],
                json.dumps(post_data["raw_json"]),
            ),
        )
        row = cur.fetchone()
        if row:
            conn.commit()
            return str(row[0])
    conn.commit()
    return None


def upsert_comments(conn, post_uuid: str, comments: list[dict]) -> int:
    """Batch-insert comments into source_comments. Returns count inserted."""
    if not comments:
        return 0

    values = []
    for c in comments:
        values.append((
            post_uuid,
            "reddit",
            c["external_id"],
            c["parent_id"],
            c["body"],
            c["author_hash"],
            c["score"],
            c["posted_at"],
            json.dumps(c["raw_json"]),
        ))

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO source_comments
                (post_id, platform, external_id, parent_id, body,
                 author_hash, score, posted_at, raw_json)
            VALUES %s
            ON CONFLICT (platform, external_id) DO NOTHING
            """,
            values,
        )
        inserted = cur.rowcount
        conn.commit()
        return inserted


# ---------------------------------------------------------------------------
# Raw file persistence
# ---------------------------------------------------------------------------

def save_raw_file(subreddit: str, posts: list[dict], comments_map: dict[str, list[dict]]) -> str:
    """Write a batch of posts + comments to a dated JSON file. Returns path."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dirpath = os.path.join(config.RAW_DATA_DIR, "reddit", subreddit)
    os.makedirs(dirpath, exist_ok=True)
    filepath = os.path.join(dirpath, f"{today}.json")

    # Append if file exists from same day
    existing = []
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as fh:
            existing = json.load(fh)

    seen_ids = {p["external_id"] for p in existing}
    for post in posts:
        if post["external_id"] not in seen_ids:
            entry = {
                "post": post["raw_json"],
                "comments": [c["raw_json"] for c in comments_map.get(post["external_id"], [])],
            }
            existing.append(entry)

    with open(filepath, "w", encoding="utf-8") as fh:
        json.dump(existing, fh, indent=2, ensure_ascii=False)

    return filepath


# ---------------------------------------------------------------------------
# Core scraping logic
# ---------------------------------------------------------------------------

def scrape_subreddit(reddit: praw.Reddit, conn, subreddit_name: str, since_epoch: float) -> dict:
    """
    Scrape a single subreddit. Returns stats dict.
    """
    subreddit = reddit.subreddit(subreddit_name)
    keyword_query = " OR ".join(config.SEARCH_KEYWORDS)

    posts_data: list[dict] = []
    comments_map: dict[str, list[dict]] = {}
    stats = {"posts_fetched": 0, "posts_new": 0, "comments_new": 0}

    log.info("Searching r/%s for: %s", subreddit_name, keyword_query)

    try:
        search_results = subreddit.search(
            keyword_query,
            sort="new",
            time_filter="month",
            limit=100,
        )
    except Exception as exc:
        log.error("Search failed for r/%s: %s", subreddit_name, exc)
        return stats

    for submission in search_results:
        created = utc_from_epoch(submission.created_utc)

        # Skip posts older than our last run
        if submission.created_utc <= since_epoch:
            continue

        stats["posts_fetched"] += 1

        author_name = str(submission.author) if submission.author else "[deleted]"
        author_hash = sha256(author_name) if author_name != "[deleted]" else None

        post_data = {
            "external_id": submission.id,
            "subreddit": subreddit_name,
            "title": submission.title,
            "body": submission.selftext or "",
            "url": f"https://reddit.com{submission.permalink}",
            "author_hash": author_hash,
            "score": submission.score,
            "comment_count": submission.num_comments,
            "posted_at": created,
            "raw_json": {
                "id": submission.id,
                "title": submission.title,
                "selftext": submission.selftext,
                "author": author_name,
                "score": submission.score,
                "num_comments": submission.num_comments,
                "created_utc": submission.created_utc,
                "permalink": submission.permalink,
                "url": submission.url,
                "subreddit": subreddit_name,
                "is_self": submission.is_self,
                "link_flair_text": submission.link_flair_text,
            },
        }

        post_uuid = upsert_post(conn, post_data)
        if post_uuid:
            stats["posts_new"] += 1
            posts_data.append(post_data)

        # Fetch comments regardless of whether post was new (comments might be new)
        post_uuid_for_comments = post_uuid
        if not post_uuid_for_comments:
            # Post already existed — fetch its UUID
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM source_posts WHERE platform='reddit' AND external_id=%s",
                    (submission.id,),
                )
                row = cur.fetchone()
                if row:
                    post_uuid_for_comments = str(row[0])

        if not post_uuid_for_comments:
            continue

        comments_list: list[dict] = []
        submission.comments.replace_more(limit=0)
        top_comments = sorted(submission.comments, key=lambda c: c.score, reverse=True)[:20]

        for comment in top_comments:
            c_author = str(comment.author) if comment.author else "[deleted]"
            c_hash = sha256(c_author) if c_author != "[deleted]" else None

            comment_data = {
                "external_id": comment.id,
                "parent_id": comment.parent_id,
                "body": comment.body,
                "author_hash": c_hash,
                "score": comment.score,
                "posted_at": utc_from_epoch(comment.created_utc),
                "raw_json": {
                    "id": comment.id,
                    "parent_id": comment.parent_id,
                    "body": comment.body,
                    "author": c_author,
                    "score": comment.score,
                    "created_utc": comment.created_utc,
                    "permalink": comment.permalink,
                    "submission_id": submission.id,
                    "subreddit": subreddit_name,
                    "is_submitter": comment.is_submitter,
                },
            }
            comments_list.append(comment_data)

        inserted = upsert_comments(conn, post_uuid_for_comments, comments_list)
        stats["comments_new"] += inserted
        comments_map[submission.id] = comments_list

        # Rate-limit courtesy
        time.sleep(0.1)

    # Save raw file for this batch
    if posts_data:
        filepath = save_raw_file(subreddit_name, posts_data, comments_map)
        log.info("Raw data saved to %s", filepath)

    return stats


def run():
    """Main entry point."""
    log.info("Starting Reddit scraper")

    if not config.REDDIT_CLIENT_ID or not config.REDDIT_CLIENT_SECRET:
        log.error("Reddit API credentials not configured. Set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET.")
        sys.exit(1)

    reddit = get_reddit_client()
    conn = get_db_connection()

    state = load_state()
    total = {"posts_fetched": 0, "posts_new": 0, "comments_new": 0}

    for subreddit_name in config.SUBREDDITS:
        since_key = f"reddit_{subreddit_name}"
        since_epoch = state.get(since_key, 0.0)

        try:
            stats = scrape_subreddit(reddit, conn, subreddit_name, since_epoch)
            for k in total:
                total[k] += stats[k]

            # Update state with current time
            state[since_key] = datetime.now(timezone.utc).timestamp()
            save_state(state)

        except Exception as exc:
            log.error("Error scraping r/%s: %s", subreddit_name, exc)
            conn.rollback()

    conn.close()

    log.info(
        "Scraper complete — fetched %d posts, %d new posts, %d new comments",
        total["posts_fetched"],
        total["posts_new"],
        total["comments_new"],
    )


if __name__ == "__main__":
    run()
