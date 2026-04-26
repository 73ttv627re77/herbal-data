#!/usr/bin/env python3
"""
Herbal Data Pipeline — Facebook Scraper

NOT ACTIVE — blocked by Facebook checkpoint.
============================================

This module is documented reference only. It is NOT executed in the MVP.

Facebook's automated systems detect and block browser automation tools,
including Playwright, Selenium, and similar frameworks. Login is blocked
by a checkpoint that requires phone number / email verification that cannot
be bypassed programmatically.

If social media data from Facebook groups is required in a future version,
the recommended path is:

  1. Facebook Graph API (official) — requires Facebook app review and
     appropriate permissions (pages_manage_metadata, groups_access_member_info).
     You must demonstrate legitimate use to Meta's review team.

  2. Manual data contribution — users opt-in and paste/share content
     directly into the app rather than automated scraping.

Current primary data source: Reddit (see scraper/reddit_scraper.py).

---
Architecture (for future reference when/if activated):
-------------------------------------------------
- Login via Playwright (profile-based cookie session)
- Navigate to target groups by URL
- Scroll pagination to load older content
- Extract: post text, author (anonymised), timestamp, reaction counts
- Extract: top-level comments with threading
- Save to source_posts / source_comments with platform='facebook'
- Save raw JSON to raw/facebook/{group_id}/{date}.json
- Incremental via scroll position tracking in state file
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Configuration (for documentation)
# ---------------------------------------------------------------------------
# FACEBOOK_EMAIL=       Facebook account email
# FACEBOOK_PASSWORD=   Facebook account password
# FACEBOOK_GROUP_IDS=  comma-separated list of group IDs to scrape
# RAW_DATA_DIR=        base directory for raw JSON files
# STATE_FILE=          path for incremental scroll state
#
# Example .env entry:
#   FACEBOOK_EMAIL=your_email@example.com
#   FACEBOOK_PASSWORD=your_app_password
#   FACEBOOK_GROUP_IDS=123456789,987654321

# ---------------------------------------------------------------------------
# NOT ACTIVE — code below is reference only
# ---------------------------------------------------------------------------

# Try to import playwright; fail gracefully if not installed
try:
    from playwright.sync_api import sync_playwright
    _PLAYWRIGHT_AVAILABLE = True
except ImportError:
    _PLAYWRIGHT_AVAILABLE = False

log = logging.getLogger("facebook_scraper")


def sha256(text: str) -> str:
    """Anonymise author by hashing username."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class FacebookScraper:
    """
    Reference implementation for Facebook group scraping.
    NOT ACTIVE — requires Facebook checkpoint bypass which is not possible.
    """

    def __init__(
        self,
        email: str,
        password: str,
        group_ids: list[str],
        raw_data_dir: str = "./raw",
        state_file: str = "./state/facebook_state.json",
    ):
        self.email = email
        self.password = password
        self.group_ids = group_ids
        self.raw_data_dir = raw_data_dir
        self.state_file = state_file
        self.browser = None
        self.context = None

    def _load_state(self) -> dict:
        if os.path.exists(self.state_file):
            with open(self.state_file, "r", encoding="utf-8") as fh:
                return json.load(fh)
        return {}

    def _save_state(self, state: dict) -> None:
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        with open(self.state_file, "w", encoding="utf-8") as fh:
            json.dump(state, fh, indent=2)

    def _extract_post(self, post_element) -> dict[str, Any]:
        """
        Extract fields from a Facebook post DOM element.
        Returns a dict mirroring the structure used for Reddit in reddit_scraper.py.
        """
        raise NotImplementedError("Facebook scraper is not active")

    def _extract_comments(self, post_element) -> list[dict[str, Any]]:
        """Extract top-level comments from a post element."""
        raise NotImplementedError("Facebook scraper is not active")

    def login(self) -> None:
        """
        Attempt login via Playwright.
        FAILS at checkpoint — this is expected behaviour.
        """
        if not _PLAYWRIGHT_AVAILABLE:
            log.error("Playwright is not installed. Install with: pip install playwright")
            return

        log.warning("Facebook login attempted — this WILL fail at checkpoint")

        pw = sync_playwright().start()
        self.browser = pw.chromium.launch(headless=True)
        self.context = self.browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        self.page = self.context.new_page()

        self.page.goto("https://www.facebook.com/login")
        self.page.fill('input[name="email"]', self.email)
        self.page.fill('input[name="pass"]', self.password)
        self.page.click('button[name="login"]')
        time.sleep(3)

        # Facebook checkpoint detection
        if "checkpoint" in self.page.url.lower() or "login" in self.page.url.lower():
            log.error(
                "Facebook checkpoint triggered. "
                "Automated Facebook scraping is blocked. "
                "Use the official Facebook Graph API instead."
            )
            self.browser.close()
            raise RuntimeError("Facebook checkpoint blocked — scraper not active")

    def scrape_group(self, group_id: str) -> dict:
        """
        Scrape a single Facebook group.
        Returns stats dict (posts_fetched, posts_new, comments_new).
        """
        state = self._load_state()
        group_state = state.get(group_id, {})
        scroll_position = group_state.get("scroll_position", 0)

        stats = {"posts_fetched": 0, "posts_new": 0, "comments_new": 0}

        # Navigate to group page
        self.page.goto(f"https://www.facebook.com/groups/{group_id}", timeout=30000)
        time.sleep(2)

        # Scroll to load posts
        for scroll_round in range(10):
            self.page.evaluate(f"window.scrollTo(0, {scroll_round * 2000})")
            time.sleep(1.5)

        log.info("Scrolled %d rounds in group %s", scroll_round + 1, group_id)

        # Update state
        group_state["scroll_position"] = (scroll_round + 1) * 2000
        group_state["last_scrape"] = datetime.now(timezone.utc).isoformat()
        state[group_id] = group_state
        self._save_state(state)

        return stats

    def save_raw_batch(self, group_id: str, posts: list[dict]) -> str:
        """Save a batch of posts + comments to a dated JSON file."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        dirpath = os.path.join(self.raw_data_dir, "facebook", group_id)
        os.makedirs(dirpath, exist_ok=True)
        filepath = os.path.join(dirpath, f"{today}.json")

        existing = []
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as fh:
                existing = json.load(fh)

        existing.extend(posts)

        with open(filepath, "w", encoding="utf-8") as fh:
            json.dump(existing, fh, indent=2, ensure_ascii=False)

        return filepath

    def close(self) -> None:
        if self.browser:
            self.browser.close()


def run():
    """
    Entry point — NOT ACTIVE.

    Raises RuntimeError immediately to prevent accidental execution.
    """
    raise RuntimeError(
        "Facebook scraper is NOT ACTIVE. "
        "Facebook blocks automated browser tools at the login checkpoint. "
        "Reddit is the primary data source (see scraper/reddit_scraper.py). "
        "For Facebook data, consider the official Facebook Graph API."
    )


if __name__ == "__main__":
    run()
