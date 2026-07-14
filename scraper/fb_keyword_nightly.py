#!/usr/bin/env python3
"""
Facebook Keyword-Search Nightly Scraper

Searches Facebook for content matching a keyword query (default: "toe nail fungal infection natural remedies"),
discovers candidate public video/reel/post URLs, scrapes comments where possible, and saves traceable JSON outputs.

Uses Chrome remote debugging at http://127.0.0.1:18800 with the openclaw browser profile.
Chrome must be started with --remote-debugging-port=18800 and --remote-allow-origins=*.
If CDP is not reachable, attempts to start Chrome from the known profile.

Supports --dry-run (discover only) and --query / --max-candidates / --max-scrape overrides.
"""

import argparse
import json
import os
import random
import re
import sys
import time
import hashlib
from typing import Callable, Optional
import websocket
import urllib.request
import urllib.parse
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Paths ────────────────────────────────────────────────────────────────────
REPO = Path("/Users/openclaw/.openclaw/workspace/herbal-data")
RAW_BASE = REPO / "raw" / "facebook_keyword"
STATE_DIR = REPO / "state"
STATE_QUERIES = STATE_DIR / "fb_keyword_queries.json"
STATE_LATEST = STATE_DIR / "fb_keyword_latest.json"
LOG_DIR = REPO / "logs"

DEFAULT_QUERY = "toe nail fungal infection natural remedies"
CHROME_CDP = "http://127.0.0.1:18800"
DEFAULT_MAX_CANDIDATES = 10
DEFAULT_MAX_SCRAPE = 3
DEFAULT_TARGETS_FILE = REPO / "state" / "fb_reel_targets.json"
DEFAULT_FRESH_PER_SOURCE = 1
STATE_SOURCE = STATE_DIR / "fb_source_crawl_state.json"
DEFAULT_MAX_DISCOVER_PER_SOURCE = 5
RUN_INVOCATION_ENV_VAR = "FB_KEYWORD_RUN_INVOCATION_ID"
RUN_INVOCATION_ID_MAX_LEN = 96
CURRENT_POST_WINDOW_DAYS = 14
REVISIT_INTERVAL_DAYS_RECENT = 7
REVISIT_INTERVAL_DAYS_OLDER = 30

DEFAULT_MAX_RUNTIME_SECONDS = 45 * 60
DEFAULT_MAX_FB_NAVIGATIONS = 80
DEFAULT_MIN_INTER_REEL_PAUSE_SECONDS = 8.0
DEFAULT_MAX_INTER_REEL_PAUSE_SECONDS = 16.0
DEFAULT_MIN_SOURCE_SWITCH_PAUSE_SECONDS = 20.0
DEFAULT_MAX_SOURCE_SWITCH_PAUSE_SECONDS = 40.0

SAFETY_REASON_LOGIN = "facebook-login-required"
SAFETY_REASON_AUTH_CHALLENGE = "facebook-auth-challenge"
SAFETY_REASON_CHECKPOINT = "facebook-checkpoint"
SAFETY_REASON_CAPTCHA = "facebook-captcha"
SAFETY_REASON_ACTION_BLOCK = "facebook-action-blocked"
SAFETY_REASON_NAVIGATION_LIMIT = "facebook-navigation-limit-exceeded"
SAFETY_REASON_RUNTIME_LIMIT = "facebook-runtime-limit-exceeded"
SAFETY_REASON_NAVIGATION_VERIFY_TIMEOUT = "facebook-navigation-verify-timeout"

DEFAULT_NAVIGATION_VERIFY_TIMEOUT_SECONDS = 8.0
DEFAULT_NAVIGATION_VERIFY_POLL_SECONDS = 0.25


# ── CDP client ────────────────────────────────────────────────────────────────

def _safe_float(value: str, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def parse_facebook_safety_reason(page_state: dict) -> Optional[str]:
    """Return a safety-stop reason if page markers indicate a warning/fail state."""
    if not isinstance(page_state, dict):
        return None

    url = str(page_state.get("url", "")).lower()
    title = str(page_state.get("title", "")).lower()
    body = str(page_state.get("body_text", "")).lower()
    combined = f"{title} {body}"

    if (
        "/checkpoint/" in url
        or "account checkpoint" in combined
        or "this checkpoint" in combined
        or "checkpoint required" in combined
    ):
        return SAFETY_REASON_CHECKPOINT

    if (
        "captcha" in combined and ("verify" in combined or "security" in combined)
        or "please complete this security check" in combined
        or "verify you are not a robot" in combined
    ):
        return SAFETY_REASON_CAPTCHA

    if (
        "action blocked" in combined
        or "temporarily blocked" in combined and "facebook" in combined
        or "you can't do this right now" in combined
        or "you can not do this right now" in combined
    ):
        return SAFETY_REASON_ACTION_BLOCK

    if (
        "security check" in combined
        and ("unusual" in combined or "review" in combined or "verification" in combined)
        or "verify your identity" in combined
    ):
        return SAFETY_REASON_AUTH_CHALLENGE

    if (
        ("log in" in combined or "login" in combined)
        and "facebook" in combined
        and ("account" in combined or "continue" in combined or "password" in combined)
    ):
        return SAFETY_REASON_LOGIN

    return None


def select_variable_pause(
    min_seconds: float,
    max_seconds: float,
    rng: Optional[random.Random] = None,
) -> float:
    """Return a bounded random pause, with testable injection."""
    effective_min = _safe_float(str(min_seconds), 0.0)
    effective_max = _safe_float(str(max_seconds), effective_min)
    if effective_max < effective_min:
        effective_max = effective_min
    selector = rng or random
    return float(selector.uniform(effective_min, effective_max))


def inter_reel_pause_seconds(rng: Optional[random.Random] = None) -> float:
    return select_variable_pause(
        DEFAULT_MIN_INTER_REEL_PAUSE_SECONDS,
        DEFAULT_MAX_INTER_REEL_PAUSE_SECONDS,
        rng=rng,
    )


def source_switch_pause_seconds(rng: Optional[random.Random] = None) -> float:
    return select_variable_pause(
        DEFAULT_MIN_SOURCE_SWITCH_PAUSE_SECONDS,
        DEFAULT_MAX_SOURCE_SWITCH_PAUSE_SECONDS,
        rng=rng,
    )


def new_fb_safety_state(
    max_runtime_seconds: int,
    max_navigations: int,
    now_ts: Optional[float] = None,
) -> dict:
    start_ts = float(now_ts) if now_ts is not None else time.time()
    return {
        "run_started_at": start_ts,
        "max_runtime_seconds": int(max_runtime_seconds),
        "max_navigations": int(max_navigations),
        "navigation_count": 0,
        "stopped": False,
        "stop_reason": "",
        "stop_at": "",
    }


def _mark_safety_stop(state: dict, reason: str, now_ts: float) -> None:
    state["stopped"] = True
    state["stop_reason"] = reason
    state["stop_at"] = _now_str(datetime.fromtimestamp(now_ts))


def _check_navigation_limits(state: dict, now_ts: float) -> Optional[str]:
    if bool(state.get("stopped")):
        return str(state.get("stop_reason") or "")

    if int(state.get("navigation_count", 0)) >= int(state.get("max_navigations", 0)):
        return SAFETY_REASON_NAVIGATION_LIMIT

    started = float(state.get("run_started_at", now_ts))
    if (float(now_ts) - started) >= float(state.get("max_runtime_seconds", 0)):
        return SAFETY_REASON_RUNTIME_LIMIT

    return None


def _safety_page_state(cdp: "CDP") -> dict:
    expr = (
        "(function() {"
        "  return {"
        "    url: (window.location.href || ''),"
        "    title: (document.title || ''),"
        "    ready_state: (document.readyState || ''),"
        "    body_text: ((document.body && document.body.innerText) ?"
        "      document.body.innerText.slice(0, 5000) : '')"
        "  };"
        "})();"
    )
    r = cdp.send("Runtime.evaluate", {"expression": expr, "returnByValue": True})
    result = r.get("result", {}).get("value", {})
    if not isinstance(result, dict):
        return {"url": "", "title": "", "body_text": ""}
    return {
        "url": str(result.get("url", "")),
        "title": str(result.get("title", "")),
        "body_text": str(result.get("body_text", "")),
    }


def _normalize_navigation_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(str(url).strip())
    if not parsed.scheme and not parsed.netloc:
        return str(url).strip().lower()
    return urllib.parse.urlunsplit(
        (
            (parsed.scheme or "https").lower(),
            parsed.netloc.lower(),
            (parsed.path or "/").rstrip("/") or "/",
            parsed.query,
            "",
        )
    ).lower()


def _safe_invocation_id(raw_invocation_id: str) -> str:
    token = str(raw_invocation_id).strip()
    if not token:
        return ""
    safe = re.sub(r"[^0-9A-Za-z_.-]+", "-", token)
    safe = safe.strip("._-")
    if not safe:
        return ""
    return safe[:RUN_INVOCATION_ID_MAX_LEN]


def _is_navigation_destination_reached(page_state: dict, target_url: str) -> bool:
    target = _normalize_navigation_url(target_url)
    current = _normalize_navigation_url(str(page_state.get("url", "")))
    if not target or not current:
        return False
    ready_state = str(page_state.get("ready_state", "")).strip().lower()
    if ready_state not in {"interactive", "complete"}:
        return False
    parsed_target = urllib.parse.urlsplit(target)
    parsed_current = urllib.parse.urlsplit(current)
    target_netloc = parsed_target.netloc.lower()
    if not target_netloc:
        return target == current
    if parsed_current.netloc.lower() != target_netloc:
        return False

    target_path = (parsed_target.path or "/").rstrip("/") or "/"
    current_path = (parsed_current.path or "/").rstrip("/") or "/"

    if target_path == "/":
        if parsed_target.query:
            return False
        return current_path == "/"

    target_parts = target_path.split("/")
    current_parts = current_path.split("/")
    if len(current_parts) < len(target_parts):
        return False
    if current_parts[: len(target_parts)] != target_parts:
        return False

    target_query = urllib.parse.parse_qs(parsed_target.query)
    if target_query:
        current_query = urllib.parse.parse_qs(parsed_current.query)
        for key, target_values in target_query.items():
            if not target_values:
                continue
            if current_query.get(key, [None])[0] != target_values[0]:
                return False
    return True


def _wait_for_navigation_verified(
    cdp: "CDP",
    target_url: str,
    state: dict,
    now_fn: Callable[[], float],
    sleep_fn: Callable[[float], None],
    page_state_fn: Callable[["CDP"], dict],
    *,
    timeout_seconds: float = DEFAULT_NAVIGATION_VERIFY_TIMEOUT_SECONDS,
    poll_interval_seconds: float = DEFAULT_NAVIGATION_VERIFY_POLL_SECONDS,
) -> bool:
    timeout_seconds = float(max(0.0, timeout_seconds))
    deadline = float(now_fn()) + timeout_seconds
    interval = float(max(0.0, poll_interval_seconds))
    if interval > 0:
        max_iteration_count = max(1, int(timeout_seconds / interval) + 2)
    else:
        max_iteration_count = 1

    iteration_count = 0
    while True:
        iteration_count += 1
        if iteration_count > max_iteration_count:
            _mark_safety_stop(
                state, SAFETY_REASON_NAVIGATION_VERIFY_TIMEOUT, float(now_fn())
            )
            return False

        now_ts = float(now_fn())
        reason = None
        if bool(state.get("stopped")):
            reason = str(state.get("stop_reason") or "")
        elif (
            float(now_ts)
            - float(state.get("run_started_at", now_ts))
        ) >= float(state.get("max_runtime_seconds", 0)):
            reason = SAFETY_REASON_RUNTIME_LIMIT
        if reason:
            _mark_safety_stop(state, reason, now_ts)
            return False

        page_state = page_state_fn(cdp)
        reason = parse_facebook_safety_reason(page_state)
        if reason:
            _mark_safety_stop(state, reason, now_ts)
            return False

        if _is_navigation_destination_reached(page_state, target_url):
            return True

        if now_ts >= deadline:
            _mark_safety_stop(state, SAFETY_REASON_NAVIGATION_VERIFY_TIMEOUT, now_ts)
            return False

        sleep_fn(interval)


def _check_navigation_safety(cdp: "CDP", state: dict, now_fn=time.time) -> bool:
    """Check current page for warning banners and record safety stop if matched."""
    if bool(state.get("stopped")):
        return True
    state_now = float(now_fn())
    reason = parse_facebook_safety_reason(_safety_page_state(cdp))
    if reason:
        _mark_safety_stop(state, reason, state_now)
        return True
    return False


def _claim_navigation_slot(state: dict, now_fn=time.time) -> Optional[str]:
    """Return a stop reason when another navigation is not allowed."""
    now_ts = float(now_fn())
    reason = _check_navigation_limits(state, now_ts)
    if reason:
        _mark_safety_stop(state, reason, now_ts)
        return reason
    return None


def navigate_with_safety(
    cdp: "CDP",
    target_url: str,
    state: Optional[dict] = None,
    now_fn=time.time,
    *,
    detect_after_navigation: bool = True,
    sleep_fn: Callable[[float], None] = time.sleep,
    page_state_fn: Optional[Callable[["CDP"], dict]] = None,
    timeout_seconds: float = DEFAULT_NAVIGATION_VERIFY_TIMEOUT_SECONDS,
    poll_interval_seconds: float = DEFAULT_NAVIGATION_VERIFY_POLL_SECONDS,
) -> bool:
    """
    Navigate via CDP only if run safety limits are not exceeded.
    Returns True on successful navigation.
    """
    if state is None:
        state = new_fb_safety_state(
            DEFAULT_MAX_RUNTIME_SECONDS,
            DEFAULT_MAX_FB_NAVIGATIONS,
            now_ts=now_fn(),
        )
    if not target_url:
        return False
    if bool(state.get("stopped")):
        return False

    reason = _claim_navigation_slot(state, now_fn=now_fn)
    if reason:
        return False

    cdp.send("Page.navigate", {"url": target_url})
    state["navigation_count"] = int(state.get("navigation_count", 0)) + 1
    state["last_navigation_url"] = str(target_url)
    state["last_navigation_at"] = float(now_fn())
    if not detect_after_navigation:
        return True
    return _wait_for_navigation_verified(
        cdp,
        target_url,
        state,
        now_fn=now_fn,
        sleep_fn=sleep_fn,
        page_state_fn=page_state_fn or _safety_page_state,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )

class CDP:
    def __init__(self, ws_url: str):
        self.ws = websocket.WebSocket()
        self.ws.connect(ws_url, timeout=15, suppress_origin=True)
        self.msg_id = 0
        self._events = []

        def receiver():
            while True:
                try:
                    msg = self.ws.recv()
                    if msg:
                        self._events.append(json.loads(msg))
                except Exception:
                    break

        self._rt = threading.Thread(target=receiver, daemon=True)
        self._rt.start()

    def send(self, method: str, params=None) -> dict:
        self.msg_id += 1
        self.ws.send(json.dumps({"id": self.msg_id, "method": method, "params": params or {}}))
        for _ in range(600):
            for i, ev in enumerate(self._events):
                if ev.get("id") == self.msg_id and ("result" in ev or "error" in ev):
                    self._events.pop(i)
                    return ev.get("result", ev)
            time.sleep(0.02)
        return {"error": "CDP timeout"}

    def close(self):
        try:
            self.ws.close()
        except Exception:
            pass


def list_cdp_targets() -> list[dict]:
    try:
        req = urllib.request.Request(CHROME_CDP + "/json")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return []


def find_target(url_contains: str = "") -> tuple[str, str]:
    """Find a CDP target matching url_contains. Returns (ws_url, page_url)."""
    for t in list_cdp_targets():
        if t.get("type") == "page":
            u = t.get("url", "")
            if not url_contains or url_contains in u:
                return t.get("webSocketDebuggerUrl", ""), u
    return "", ""


def ensure_browser() -> bool:
    """Return True if a CDP target is already available."""
    return bool(list_cdp_targets())


def create_new_tab(target_url: str = "https://www.facebook.com") -> tuple[str, str, str]:
    """
    Create a new Chrome tab via Target.createTarget CDP command.
    Returns (ws_url, tab_id, page_url) for the new tab.
    """
    # Get existing WS URL to send the command
    targets = list_cdp_targets()
    main_page = next((t for t in targets if t.get("type") == "page"), None)
    if not main_page:
        return "", "", ""
    ws_url = main_page["webSocketDebuggerUrl"]


    ws = websocket.WebSocket()
    ws.connect(ws_url, timeout=15, suppress_origin=True)
    msg_id = 0
    events = []

    def receiver():
        while True:
            try:
                msg = ws.recv()
                if msg:
                    events.append(json.loads(msg))
            except Exception:
                break

    rt = threading.Thread(target=receiver, daemon=True)
    rt.start()

    def send_cmd(method, params=None) -> dict:
        nonlocal msg_id
        msg_id += 1
        ws.send(json.dumps({"id": msg_id, "method": method, "params": params or {}}))
        for _ in range(300):
            for i, ev in enumerate(events):
                if ev.get("id") == msg_id and ("result" in ev or "error" in ev):
                    events.pop(i)
                    return ev.get("result", ev)
            time.sleep(0.02)
        return {"error": "timeout"}


    result = send_cmd("Target.createTarget", {"url": target_url})
    ws.close()


    target_id = result.get("targetId", "")
    if not target_id:
        return "", "", ""

    # Get the new tab's info from the target list
    time.sleep(0.3)
    targets2 = list_cdp_targets()
    new_tab = next((t for t in targets2 if t.get("id") == target_id), None)
    if not new_tab:
        return "", "", ""
    return new_tab.get("webSocketDebuggerUrl", ""), target_id, new_tab.get("url", "")


# ── Navigation helpers ────────────────────────────────────────────────────────

def navigate_to_search(cdp: CDP, query: str, safety_state: Optional[dict] = None) -> bool:
    """Navigate Chrome to the Facebook search results page for the given query."""
    search_url = f"https://www.facebook.com/search/videos/?q={urllib.parse.quote(query)}"
    if safety_state is None:
        safety_state = new_fb_safety_state(
            DEFAULT_MAX_RUNTIME_SECONDS,
            DEFAULT_MAX_FB_NAVIGATIONS,
        )
    if not navigate_with_safety(cdp, search_url, safety_state):
        return False
    time.sleep(3)
    # Scroll once to load results
    cdp.send("Runtime.evaluate", {"expression": "window.scrollBy(0, 600)"})
    time.sleep(2)
    return True


def extract_candidate_links(cdp: CDP) -> list[dict]:
    """
    Extract candidate video/reel/post links from the current page.
    Returns list of {id, url, type} where type is 'reel', 'video', or 'post'.
    """
    js = """
    (function() {
        var results = [];
        var seen = new Set();

        // Reels: links matching /reel/ or /video/
        var links = document.querySelectorAll('a[href*="/reel/"], a[href*="/video/"]');
        for (var i = 0; i < links.length; i++) {
            var href = links[i].getAttribute('href');
            if (!href) continue;

            // Skip small UI links (comment counts etc.)
            var text = links[i].innerText ? links[i].innerText.trim() : '';
            if (text.length < 10 && !href.includes('/reel/')) continue;

            // Extract numeric ID
            var m = href.match(/\\/(\\d+)/);
            if (!m) continue;
            var id = m[1];
            if (seen.has(id)) continue;
            seen.add(id);

            // Build full URL
            var url = href;
            if (!url.startsWith('http')) {
                url = 'https://www.facebook.com' + href;
            }

            var type = href.includes('/reel/') ? 'reel' : 'video';
            results.push({id: id, url: url, type: type});
            if (results.length >= 100) break;
        }

        // Also try video ADF links (Facebook video player URLs)
        var dclinks = document.querySelectorAll('a[href*="video_id="]');
        for (var i = 0; i < dclinks.length; i++) {
            var href = dclinks[i].getAttribute('href');
            if (!href) continue;
            var m = href.match(/video_id=(\\d+)/);
            if (!m) continue;
            var id = m[1];
            if (seen.has(id)) continue;
            seen.add(id);
            var url = 'https://www.facebook.com/video/video/' + id;
            results.push({id: id, url: url, type: 'video'});
            if (results.length >= 100) break;
        }

        return results;
    })()
    """
    r = cdp.send("Runtime.evaluate", {"expression": js, "returnByValue": True})
    val = r.get("result", {}).get("value", "[]")
    try:
        items = json.loads(val) if isinstance(val, str) else val
    except Exception:
        items = []
    return items


def canonicalize_source_url(url: str) -> str:
    """Normalize a Facebook URL for deterministic dedupe/output usage."""
    if not url:
        return ""
    normalized = url.strip()
    if normalized.startswith("//"):
        normalized = "https:" + normalized
    if normalized.startswith("/"):
        normalized = "https://www.facebook.com" + normalized
    parsed = urllib.parse.urlsplit(normalized)
    if not parsed.scheme:
        parsed = urllib.parse.urlsplit("https://www.facebook.com/" + normalized.lstrip("/"))
    if not parsed.netloc:
        return normalized

    scheme = parsed.scheme or "https"
    netloc = parsed.netloc
    path = (parsed.path or "/").rstrip("/")
    query = parsed.query or ""

    if path.startswith("/watch"):
        q = urllib.parse.parse_qs(parsed.query)
        if "v" in q and q["v"]:
            query = urllib.parse.urlencode({"v": q["v"][0]})
        else:
            query = ""
    elif path.startswith("/reel/") or "/videos/" in path or "/video/" in path:
        query = ""
    elif path.startswith("/share/"):
        query = parsed.query

    return urllib.parse.urlunsplit((scheme, netloc, path if path else "/", query, ""))


def stable_source_key(url: str) -> str:
    """Stable key for deduping historical sources."""
    normalized = canonicalize_source_url(url)
    if not normalized:
        return ""

    parsed = urllib.parse.urlsplit(normalized)
    path = parsed.path or ""
    m = re.search(r"/reel/([0-9]+)", path)
    if m:
        return f"reel:{m.group(1)}"
    m = re.search(r"/videos?/([0-9]+)", path)
    if m:
        return f"video:{m.group(1)}"
    if path.startswith("/watch"):
        q = urllib.parse.parse_qs(parsed.query)
        if "v" in q and q["v"]:
            return f"watch:{q['v'][0]}"
    if normalized.startswith("https://www.facebook.com/share/"):
        return f"share:{normalized}"
    return f"url:{normalized}"


def is_direct_reel_video_target(url: str) -> bool:
    """Return True for explicit reel/video/watch/share-r URLs."""
    normalized = canonicalize_source_url(url)
    if not normalized:
        return False
    parsed = urllib.parse.urlsplit(normalized)
    path = parsed.path or ""
    query = urllib.parse.parse_qs(parsed.query)
    share_reel_token = path[len("/share/r/"):] if path.startswith("/share/r/") else ""
    return bool(
        re.search(r"/(?:reel|video|videos?)/", path) or
        (path.startswith("/watch") and query.get("v") and query["v"][0]) or
        (path.startswith("/share/r/") and bool(share_reel_token.strip("/")))
    )


def load_raw_source_keys(raw_base: Path = RAW_BASE) -> set[str]:
    """Load stable source keys from historical raw outputs."""
    keys: set[str] = set()
    if not raw_base.exists():
        return keys
    for path in raw_base.glob("*/*.json"):
        if path.name == "discovery.json":
            continue
        try:
            payload = json.loads(path.read_text())
        except Exception:
            continue
        source_url = payload.get("source_url", "")
        key = stable_source_key(str(source_url))
        if key:
            keys.add(key)
    return keys


def _now_str(now: Optional[datetime] = None) -> str:
    return (now or datetime.now()).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")


def _parse_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def _is_current_post(post_state: dict, now: datetime) -> bool:
    last_seen = _parse_ts(str(post_state.get("last_seen")))
    if not last_seen:
        return False
    return (now - last_seen).days <= CURRENT_POST_WINDOW_DAYS


def _is_revisit_due(post_state: dict, now: datetime) -> bool:
    revisit = _parse_ts(post_state.get("next_revisit_at"))
    return revisit is not None and revisit <= now


def _revisit_delay_days(post_state: dict, now: datetime) -> int:
    first_seen = _parse_ts(post_state.get("first_seen")) or now
    age_days = max(0, (now - first_seen).days)
    if age_days <= CURRENT_POST_WINDOW_DAYS:
        return REVISIT_INTERVAL_DAYS_RECENT
    return REVISIT_INTERVAL_DAYS_OLDER


def load_source_crawl_state(path: Path = STATE_SOURCE) -> dict:
    if not path.exists():
        return {
            "version": 1,
            "updated_at": "",
            "sources": {},
        }
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {
            "version": 1,
            "updated_at": "",
            "sources": {},
        }
    if not isinstance(payload, dict):
        return {
            "version": 1,
            "updated_at": "",
            "sources": {},
        }
    payload.setdefault("version", 1)
    payload.setdefault("updated_at", "")
    payload.setdefault("sources", {})
    if not isinstance(payload.get("sources"), dict):
        payload["sources"] = {}
    return payload


def save_source_crawl_state(state: dict, path: Path = STATE_SOURCE) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(state, f, indent=2)


def load_targets_file(path: Path) -> list[dict[str, str]]:
    """Load target records from a JSON list/object or legacy string entries."""
    records: list[dict[str, str]] = []
    if not path.exists():
        return records
    try:
        data = json.loads(path.read_text())
    except Exception:
        return records

    items: list = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = data.get("targets") or data.get("urls") or []

    for item in items if isinstance(items, list) else []:
        if isinstance(item, str):
            url = canonicalize_source_url(item)
            if url:
                records.append({"url": url})
            continue
        if not isinstance(item, dict):
            continue
        url = canonicalize_source_url(str(item.get("url", "")).strip())
        if not url:
            continue
        records.append({
            "url": url,
            "label": str(item.get("label", "")) if item.get("label") is not None else "",
            "platform": str(item.get("platform", "facebook")).lower(),
            "crawl_mode": str(item.get("crawl_mode", item.get("mode", "")).lower()),
            "status": str(item.get("status", item.get("state", "active")).lower()),
        })
    return records


def split_target_records(records: list[dict[str, str]]) -> tuple[list[str], list[dict[str, str]]]:
    """Split loaded targets into explicit URLs and source records."""
    explicit: list[str] = []
    sources: list[dict[str, str]] = []

    for item in records:
        url = canonicalize_source_url(str(item.get("url", "")))
        if not url:
            continue
        if item.get("platform", "facebook") != "facebook":
            continue
        item["url"] = url
        mode = item.get("crawl_mode") or ""
        if mode == "source":
            sources.append(item)
            continue
        if mode == "explicit":
            explicit.append(url)
            continue
        if is_direct_reel_video_target(url):
            explicit.append(url)
        else:
            sources.append(item)
    return explicit, sources


def discover_source_reels(
    source_records: list[dict[str, str]],
    max_per_source: int = DEFAULT_MAX_DISCOVER_PER_SOURCE,
    max_total: Optional[int] = None,
    safety_state: Optional[dict] = None,
) -> tuple[dict[str, list[str]], list[str]]:
    """Discover reel URLs from each source poster page."""
    discovered_by_source: dict[str, list[str]] = {}
    errors: list[str] = []
    if not source_records:
        return discovered_by_source, errors
    if max_total is not None and max_total <= 0:
        return discovered_by_source, errors

    if not ensure_browser():
        errors.append("Chrome CDP not reachable for source poster expansion")
        return discovered_by_source, errors

    try:
        scrape_ws_url, _, _ = create_new_tab()
    except Exception as e:
        errors.append(f"Could not open source expansion tab: {e}")
        return discovered_by_source, errors
    if not scrape_ws_url:
        errors.append("Could not get WebSocket URL for source expansion tab")
        return discovered_by_source, errors

    cdp = CDP(scrape_ws_url)
    total_discovered = 0
    try:
        for idx, record in enumerate(source_records):
            source_url = record.get("url", "")
            if not source_url:
                continue
            source_key = stable_source_key(source_url)
            if not source_key:
                continue
            if safety_state is not None and safety_state.get("stopped"):
                break

            if max_total is not None and total_discovered >= max_total:
                break

            candidates, discover_errors = discover_reels_from_source_page(
                cdp,
                source_url,
                safety_state=safety_state,
            )
            if discover_errors:
                errors.extend(discover_errors)

            if not isinstance(candidates, list) or not candidates:
                if safety_state is not None and safety_state.get("stopped"):
                    break
                if idx + 1 < len(source_records):
                    time.sleep(source_switch_pause_seconds())
                continue

            seen_for_source: set[str] = set()
            out_urls: list[str] = []
            for candidate in candidates[: max_per_source + 10]:
                url = canonicalize_source_url(str(candidate.get("url", "")))
                if not url:
                    continue
                post_key = stable_source_key(url)
                if post_key in seen_for_source:
                    continue
                out_urls.append(url)
                seen_for_source.add(post_key)
                total_discovered += 1
                if len(out_urls) >= max_per_source:
                    break
                if max_total is not None and total_discovered >= max_total:
                    break
            if out_urls:
                discovered_by_source[source_key] = out_urls

            if safety_state is not None and safety_state.get("stopped"):
                break

            if idx + 1 < len(source_records):
                time.sleep(source_switch_pause_seconds())

    finally:
        cdp.close()

    return discovered_by_source, errors


def plan_source_scrape_tasks(
    explicit_urls: list[str],
    source_records: list[dict[str, str]],
    source_state: dict,
    discovered_by_source: dict[str, list[str]],
    new_post_keys_by_source: dict[str, set[str]],
    max_scrape: int,
    now: Optional[datetime] = None,
) -> tuple[list[dict[str, str]], dict]:
    """Create a deterministic scrape plan with reason annotations."""
    now_dt = now or datetime.now()
    now_str = _now_str(now_dt)
    source_records = list(source_records or [])

    source_by_key: dict[str, dict[str, str]] = {}
    for idx, rec in enumerate(source_records):
        source_url = canonicalize_source_url(str(rec.get("url", "")))
        if not source_url:
            continue
        source_key = stable_source_key(source_url)
        if not source_key:
            continue
        source = dict(rec)
        source["url"] = source_url
        source["source_key"] = source_key
        source["source_order"] = str(idx)
        source_by_key[source_key] = source

    budget = max_scrape if max_scrape > 0 else 0
    tasks: list[dict[str, str]] = []
    selected_by_reason: dict[str, int] = {"explicit": 0, "latest": 0, "backfill": 0, "revisit": 0}
    planned_urls: set[str] = set()
    summary = {
        "discovered_count": 0,
        "new_count": 0,
        "revisited_count": 0,
        "skipped_current": 0,
        "explicit_count": 0,
        "source_count": len(source_records),
        "selected_count": 0,
        "selected_by_reason": selected_by_reason,
    }

    source_state.setdefault("sources", {})
    sources_blob = source_state.get("sources", {})
    if not isinstance(sources_blob, dict):
        source_state["sources"] = {}
        sources_blob = source_state["sources"]

    def add_task(
        url: str,
        reason: str,
        source_key: Optional[str] = None,
        post_key: Optional[str] = None,
        is_new: bool = False,
    ) -> bool:
        nonlocal budget
        if budget <= 0 or not url:
            return False
        if url in planned_urls:
            return False
        if reason == "backfill" and is_new:
            is_new = False
        task = {
            "url": url,
            "reason": reason,
            "source_key": source_key or "",
            "post_key": post_key or "",
            "is_new": bool(is_new),
        }
        tasks.append(task)
        planned_urls.add(url)
        selected_by_reason[reason] = selected_by_reason.get(reason, 0) + 1
        budget -= 1
        summary["selected_count"] += 1
        if reason == "explicit":
            summary["explicit_count"] += 1
        if reason == "revisit":
            summary["revisited_count"] += 1
        if reason in {"latest", "backfill", "revisit"} and is_new:
            summary["new_count"] += 1
        return True

    # Explicit URLs are always highest priority and should always run first.
    for target_url in explicit_urls:
        if not add_task(target_url, "explicit"):
            break

    # Update source/post metadata first so that discovered/backlog planning can read it.
    new_post_total = 0
    for source_key, source_info in source_by_key.items():
        source_obj = sources_blob.setdefault(
            source_key,
            {
                "source_key": source_key,
                "source_url": source_info.get("url", ""),
                "label": source_info.get("label", ""),
                "platform": "facebook",
                "status": source_info.get("status", "active"),
                "first_seen": now_str,
                "last_seen": now_str,
                "created_at": source_info.get("created_at", now_str),
                "updated_at": now_str,
                "posts": {},
                "errors": [],
            },
        )
        source_obj.setdefault("posts", {})
        source_obj.setdefault("label", source_info.get("label", ""))
        source_obj.setdefault("platform", "facebook")
        source_obj["updated_at"] = now_str
        source_obj["last_seen"] = now_str

        discovered = discovered_by_source.get(source_key, []) or []
        new_keys = new_post_keys_by_source.get(source_key, set())
        for idx, post_url in enumerate(discovered):
            post_key = stable_source_key(post_url)
            if not post_key:
                continue

            post_obj = source_obj.setdefault("posts", {}).setdefault(
                post_key,
                {
                    "post_key": post_key,
                    "url": post_url,
                    "first_seen": now_str,
                    "last_seen": now_str,
                    "last_scraped_at": "",
                    "last_comment_count": 0,
                    "scrape_count": 0,
                    "status": "current",
                    "current": True,
                    "next_revisit_at": "",
                },
            )
            if post_obj.get("post_key") != post_key:
                post_obj["post_key"] = post_key
            if post_key in new_keys:
                new_post_total += 1
                summary["discovered_count"] += 1
                add_task(post_url, "latest", source_key, post_key, is_new=True)
            post_obj["url"] = post_url
            post_obj["last_seen"] = now_str
            post_obj["current"] = True
            post_obj["status"] = "current"

    summary["discovered_count"] = max(summary["discovered_count"], new_post_total)

    # Backfill all known posts that have never been scraped.
    backfill_candidates: list[tuple[datetime, int, str, str]] = []
    # Revisit all due old posts.
    revisit_candidates: list[tuple[datetime, int, str, str]] = []

    for source_key, source_info in source_by_key.items():
        source_obj = sources_blob.get(source_key, {})
        posts = source_obj.get("posts") if isinstance(source_obj, dict) else {}
        if not isinstance(posts, dict):
            continue

        for post_key, post_obj in posts.items():
            if not isinstance(post_obj, dict):
                continue
            url = post_obj.get("url", "")
            post_key = str(post_key)
            if url in planned_urls:
                continue

            last_seen = _parse_ts(post_obj.get("last_seen")) or _parse_ts(post_obj.get("first_seen"))
            sort_seen = last_seen or datetime(1970, 1, 1)
            is_current = _is_current_post(post_obj, now_dt)
            post_obj["current"] = bool(is_current)
            post_obj["status"] = "current" if is_current else "stale"

            scrape_count = int(post_obj.get("scrape_count") or 0)
            is_due = _is_revisit_due(post_obj, now_dt)
            is_new_post = not bool(post_obj.get("last_scraped_at"))

            if is_due:
                revisit_at = _parse_ts(post_obj.get("next_revisit_at")) or (last_seen or now_dt)
                revisit_candidates.append((revisit_at, len(tasks), source_key, post_key))
            elif is_new_post:
                backfill_candidates.append((sort_seen, source_info.get("source_order", 0), source_key, post_key))
            else:
                if is_current:
                    summary["skipped_current"] += 1

    # Deterministic ordering: older posts first for backfill, older next_revisit_at first for revisit.
    for _, _, source_key, post_key in sorted(backfill_candidates, key=lambda item: (item[0], int(item[1]))):
        source_obj = sources_blob.get(source_key, {})
        post_obj = source_obj.get("posts", {}).get(post_key, {})
        if not isinstance(post_obj, dict):
            continue
        add_task(post_obj.get("url", ""), "backfill", source_key, post_key, is_new=not bool(post_obj.get("last_scraped_at")))

    for _, _, source_key, post_key in sorted(revisit_candidates, key=lambda item: (item[0], item[1])):
        source_obj = sources_blob.get(source_key, {})
        post_obj = source_obj.get("posts", {}).get(post_key, {})
        if not isinstance(post_obj, dict):
            continue
        add_task(post_obj.get("url", ""), "revisit", source_key, post_key)

    source_state["updated_at"] = now_str
    return tasks, summary


def mark_task_result(source_state: dict, task: dict, result: dict, now: datetime) -> None:
    source_key = task.get("source_key", "")
    post_key = task.get("post_key", "")
    if not source_key or not post_key:
        return

    sources = source_state.get("sources")
    if not isinstance(sources, dict):
        return
    source_obj = sources.get(source_key)
    if not isinstance(source_obj, dict):
        return
    posts = source_obj.get("posts")
    if not isinstance(posts, dict):
        return
    post_obj = posts.get(post_key)
    if not isinstance(post_obj, dict):
        return

    now_str = _now_str(now)
    comment_count = int(result.get("total_comments", 0) or 0)
    prev_comment_count = int(post_obj.get("last_comment_count") or 0)

    post_obj["last_scraped_at"] = now_str
    post_obj["last_comment_count"] = max(prev_comment_count, comment_count)
    post_obj["scrape_count"] = int(post_obj.get("scrape_count") or 0) + 1
    post_obj["last_seen"] = now_str
    post_obj["current"] = True
    post_obj["status"] = "current"
    delay_days = _revisit_delay_days(post_obj, now)
    if delay_days <= 0:
        delay_days = REVISIT_INTERVAL_DAYS_RECENT
    next_revisit_at = now + timedelta(days=delay_days)
    post_obj["next_revisit_at"] = _now_str(next_revisit_at)

    source_obj["last_scraped_at"] = now_str
    source_obj["status"] = "active"
    source_obj["updated_at"] = now_str


def discover_reels_from_source_page(
    cdp: CDP,
    source_url: str,
    safety_state: Optional[dict] = None,
) -> tuple[list[dict], list[str]]:
    """Discover `/reel/<id>` links from a source poster stream page."""
    errors: list[str] = []
    try:
        if "http" not in source_url:
            errors.append(f"{source_url}: invalid source url")
            return [], errors

        if safety_state is None:
            safety_state = new_fb_safety_state(
                DEFAULT_MAX_RUNTIME_SECONDS,
                DEFAULT_MAX_FB_NAVIGATIONS,
            )
        if not navigate_with_safety(cdp, source_url, safety_state):
            reason = str((safety_state or {}).get("stop_reason", "facebook safety stop"))
            errors.append(f"{source_url}: safety stop ({reason})")
            return [], errors
        time.sleep(4)

        for _ in range(3):
            cdp.send("Runtime.evaluate", {"expression": "window.scrollBy(0, 900)"})
            time.sleep(1)

        js = """
        (function() {
            var anchors = document.querySelectorAll('a[href]');
            var seen = new Set();
            var out = [];
            for (var i = 0; i < anchors.length; i++) {
                var href = anchors[i].getAttribute('href');
                if (!href) continue;
                if (href.indexOf('/reel/') === -1) continue;
                if (href.startsWith('//')) href = 'https:' + href;
                if (href.startsWith('/')) href = 'https://www.facebook.com' + href;
                var m = href.match(/\\/reel\\/(\\d+)/);
                if (!m) continue;
                var id = m[1];
                if (seen.has(id)) continue;
                seen.add(id);
                out.push({id: id, url: href});
                if (out.length >= 25) break;
            }
            return out;
        })()
        """
        response = cdp.send("Runtime.evaluate", {"expression": js, "returnByValue": True})
        val = response.get("result", {}).get("value", "[]")
        try:
            items = json.loads(val) if isinstance(val, str) else val
        except Exception:
            items = []
        if not isinstance(items, list):
            items = []
        return [
            {
                "id": item.get("id"),
                "url": canonicalize_source_url(str(item.get("url", ""))),
            }
            for item in items
            if isinstance(item, dict) and item.get("id") and item.get("url")
        ], errors
    except Exception as exc:
        return [], [f"{source_url}: {exc}"]


def resolve_source_targets(
    source_urls: list[str],
    historical_keys: set[str],
    max_per_source: int = DEFAULT_FRESH_PER_SOURCE,
    max_total: Optional[int] = None,
) -> tuple[list[str], list[str]]:
    """Compatibility shim for old callers: expand source URLs into discovered reels."""
    source_records = [
        {"url": canonicalize_source_url(url)}
        for url in source_urls
        if canonicalize_source_url(url)
    ]
    discovered_by_source, errors = discover_source_reels(
        source_records=source_records,
        max_per_source=max_per_source,
        max_total=max_total,
    )
    resolved = []
    for url_list in discovered_by_source.values():
        resolved.extend(url_list)
    return resolved, errors


# ── Comment extraction ────────────────────────────────────────────────────────

def parse_author_time(label: str) -> tuple[str, str]:
    """Parse 'Comment by {Author} {N} {units} ago' label."""
    label = label.replace("Comment by ", "")
    m = re.search(r"^(.+?)\\s+(\\d+\\s+\\w+\\s+ago)$", label)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return label.strip(), ""


def extract_comments_from_ax_tree(tree: list) -> list[dict]:
    """Extract comments from Chrome accessibility tree."""
    nodes_by_id = {str(n.get("nodeId")): n for n in tree}

    articles = [
        n for n in tree
        if n.get("role", {}).get("value") == "article"
        and "Comment by" in n.get("name", {}).get("value", "")
    ]

    def get_text(node_id: str, depth: int = 0) -> str:
        if depth > 30:
            return ""
        node = nodes_by_id.get(str(node_id))
        if not node:
            return ""
        parts = []
        role = node.get("role", {}).get("value", "")
        name_val = node.get("name", {}).get("value", "")
        if name_val and name_val.strip() and role not in ("none", "generic"):
            parts.append(name_val.strip())
        for child_id in node.get("childIds", []):
            t = get_text(child_id, depth + 1)
            if t:
                parts.append(t)
        return " ".join(parts)

    comments = []
    for a in articles:
        full_label = a.get("name", {}).get("value", "")
        author, time_ago = parse_author_time(full_label)
        text = get_text(a.get("nodeId"))
        text = text.replace("Comment by " + author + " " + time_ago, "").strip()
        for _ in range(4):
            text = text.replace(author + " " + author, author)
        text = text.replace(author + " ", "").strip()
        cid = "fb_" + hashlib.md5(text[:80].encode()).hexdigest()[:12]
        comments.append({
            "cid": cid,
            "author": author,
            "time": time_ago,
            "text": text[:1500],
        })
    return comments


def wait_for_ax_tree(cdp: CDP, timeout: float = 8, check_interval: float = 1) -> list:
    """Wait for comments to appear in the accessibility tree."""
    cdp.send("Accessibility.enable")
    time.sleep(0.5)
    for _ in range(int(timeout / check_interval)):
        r = cdp.send("Accessibility.getFullAXTree")
        tree = r.get("nodes", [])
        articles = [
            n for n in tree
            if n.get("role", {}).get("value") == "article"
            and "Comment by" in n.get("name", {}).get("value", "")
        ]
        if articles:
            return tree
        time.sleep(check_interval)
    r = cdp.send("Accessibility.getFullAXTree")
    return r.get("nodes", [])


def open_comments_sidebar(cdp: CDP) -> bool:
    """Open the comments sidebar for the current page. Returns True on success."""
    cdp.send("Runtime.evaluate", {"expression": "window.scrollTo(0, 0)"})
    time.sleep(0.3)

    # Wait for Comment button
    for _ in range(80):
        r = cdp.send("Runtime.evaluate", {
            "expression": "document.querySelectorAll('[aria-label=\"Comment\"]').length"
        })
        if r.get("result", {}).get("value", 0) > 0:
            break
        time.sleep(0.2)
    else:
        return False

    time.sleep(1)

    r = cdp.send("Runtime.evaluate", {"expression": (
        "(function() {"
        "var btns = document.querySelectorAll('[aria-label=\"Comment\"]');"
        "var minY = Infinity, btn = null;"
        "for (var i = 0; i < btns.length; i++) {"
        "var rect = btns[i].getBoundingClientRect();"
        "if (rect.y < minY && rect.width > 0 && rect.height > 0) {"
        "minY = rect.y; btn = btns[i];"
        "}"
        "}"
        "if (!btn) return 'not_found';"
        "btn.click();"
        "return 'clicked';"
        "})()"
    )})
    if r.get("result", {}).get("value") == "not_found":
        return False

    time.sleep(3)
    r = cdp.send("Runtime.evaluate", {"expression": "!!document.querySelector('[role=\"complementary\"]')"})
    return bool(r.get("result", {}).get("value"))


def click_view_more_comments(cdp: CDP) -> str:
    """Click 'View more comments' button. Returns 'clicked' or reason."""
    r = cdp.send("Runtime.evaluate", {"expression": (
        "(function() {"
        "var all = document.querySelectorAll('div[role=button], span');"
        "for (var i = 0; i < all.length; i++) {"
        "var txt = all[i].innerText ? all[i].innerText.trim() : '';"
        "if (txt === 'View more comments') {"
        "var rect = all[i].getBoundingClientRect();"
        "if (rect.width > 0 && rect.height > 0) {"
        "all[i].click();"
        "return 'clicked';"
        "}"
        "}"
        "}"
        "return 'not found';"
        "})()"
    )})
    return r.get("result", {}).get("value", "no_value")


def scrape_comments_for_url(
    cdp_ws_url: str,
    source_url: str,
    max_rounds: int = 6,
    safety_state: Optional[dict] = None,
) -> dict:
    """
    Connect to an existing Chrome tab (via cdp_ws_url), navigate to source_url,
    extract comments using accessibility tree. Returns result dict.
    """
    cdp = CDP(cdp_ws_url)
    result = {
        "source_url": source_url,
        "method": "cdp_accessibility_tree",
        "total_comments": 0,
        "comments": [],
        "errors": [],
    }

    try:
        if safety_state is None:
            safety_state = new_fb_safety_state(
                DEFAULT_MAX_RUNTIME_SECONDS,
                DEFAULT_MAX_FB_NAVIGATIONS,
            )
        # Verify current page or navigate
        r = cdp.send("Runtime.evaluate", {"expression": "window.location.href"})
        current = r.get("result", {}).get("value", "")

        if source_url not in current and current != source_url:
            if navigate_with_safety(cdp, source_url, safety_state):
                time.sleep(4)
            else:
                result["errors"].append(
                    f"Safety stop before opening source: {safety_state.get('stop_reason', 'unknown')}"
                )
                cdp.close()
                return result

        # Scroll to top
        cdp.send("Runtime.evaluate", {"expression": "window.scrollTo(0, 0)"})
        time.sleep(1)

        # Click Comment button
        if not open_comments_sidebar(cdp):
            result["errors"].append("Could not open comments sidebar")
            cdp.close()
            return result

        # Initial load
        time.sleep(2)
        tree = wait_for_ax_tree(cdp, timeout=8)
        comments = extract_comments_from_ax_tree(tree)
        seen_ids = set(c["cid"] for c in comments)

        # Load more
        for _ in range(max_rounds):
            time.sleep(1)
            res = click_view_more_comments(cdp)
            if res != "clicked":
                break
            time.sleep(3)
            tree = wait_for_ax_tree(cdp, timeout=6)
            new_comments = extract_comments_from_ax_tree(tree)
            new_count = sum(1 for c in new_comments if c["cid"] not in seen_ids)
            if new_count == 0:
                break
            for c in new_comments:
                if c["cid"] not in seen_ids:
                    seen_ids.add(c["cid"])
                    comments.append(c)

        result["total_comments"] = len(comments)
        result["comments"] = comments

    except Exception as e:
        result["errors"].append(str(e))
    finally:
        cdp.close()

    return result


# ── Main pipeline ──────────────────────────────────────────────────────────────

def run_keyword_search(
    query: str,
    max_candidates: int,
    max_scrape: int,
    dry_run: bool,
    out_dir: Path,
    safety_state: Optional[dict] = None,
) -> dict:
    """
    Run the keyword search pipeline.
    Returns a summary dict.
    """
    ts = datetime.now()
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = {
        "query": query,
        "started_at": ts.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "dry_run": dry_run,
        "candidates": [],
        "scraped": [],
        "total_candidates": 0,
        "total_scraped": 0,
        "total_comments": 0,
        "errors": [],
        "output_dir": str(out_dir),
    }
    safety_state = safety_state or new_fb_safety_state(
        DEFAULT_MAX_RUNTIME_SECONDS,
        DEFAULT_MAX_FB_NAVIGATIONS,
    )
    candidates: list[dict] = []

    # Check CDP
    if not ensure_browser():
        summary["errors"].append("Chrome CDP not reachable at " + CHROME_CDP)
        return summary

    # Open a fresh tab for search via Target.createTarget CDP command
    try:
        search_ws_url, _, _ = create_new_tab()
    except Exception as e:
        summary["errors"].append(f"Could not open new Chrome tab: {e}")
        return summary
    if not search_ws_url:
        summary["errors"].append("Could not get WebSocket URL for new tab")
        return summary

    # Connect CDP client to search tab
    cdp = CDP(search_ws_url)
    try:
        # Navigate to search
        print(f"[1] Navigating to FB search: {query[:50]}...")
        if not navigate_to_search(cdp, query, safety_state=safety_state):
            summary["errors"].append(
                f"Safety stop before search navigation: {safety_state.get('stop_reason', 'unknown')}"
            )
            return summary
        time.sleep(2)

        # Scroll to load more results
        for _ in range(4):
            cdp.send("Runtime.evaluate", {"expression": "window.scrollBy(0, 800)"})
            time.sleep(1.5)

        # Extract candidates
        print("[2] Extracting candidate links...")
        candidates = extract_candidate_links(cdp)
        # Dedupe by id
        seen_ids = set()
        unique = []
        for c in candidates:
            if c["id"] not in seen_ids:
                seen_ids.add(c["id"])
                unique.append(c)
        candidates = unique[:max_candidates]
        print(f"    Found {len(candidates)} candidates")

        # Save discovery artifact
        discovery = {
            "query": query,
            "discovered_at": ts.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "candidate_count": len(candidates),
            "candidates": candidates,
            "errors": [],
        }
        disc_path = out_dir / "discovery.json"
        with open(disc_path, "w") as f:
            json.dump(discovery, f, indent=2, ensure_ascii=False)
        print(f"    Discovery saved -> {disc_path}")

        summary["candidates"] = [c["id"] for c in candidates]
        summary["total_candidates"] = len(candidates)

    except Exception as e:
        summary["errors"].append(f"Discovery phase error: {e}")
        return summary
    finally:
        cdp.close()

    if dry_run:
        print("[DRY RUN] Skipping comment scraping")
        return summary

    # ── Comment scraping phase ────────────────────────────────────────────────
    if max_scrape > 0 and candidates:
        try:
            scrape_ws_url, _, _ = create_new_tab()
        except Exception as e:
            summary["errors"].append(f"Could not open scrape tab: {e}")
            return summary
        if not scrape_ws_url:
            summary["errors"].append("Could not get WebSocket URL for scrape tab")
            return summary

        scrape_cdp = CDP(scrape_ws_url)
        try:
            for i, cand in enumerate(candidates[:max_scrape]):
                print(f"[3.{i+1}] Scraping {cand['type']} {cand['id']} -> {cand['url'][:60]}...")
                time.sleep(inter_reel_pause_seconds())

                if safety_state.get("stopped"):
                    summary["errors"].append(
                        f"Safety stop before candidate scrape: {safety_state.get('stop_reason', 'unknown')}"
                    )
                    break

                if not navigate_with_safety(scrape_cdp, cand["url"], safety_state):
                    summary["errors"].append(
                        f"Safety stop before candidate navigation: {safety_state.get('stop_reason', 'unknown')}"
                    )
                    break
                time.sleep(4)

                result = {"source_url": cand["url"], "method": "cdp_accessibility_tree", "total_comments": 0, "comments": [], "errors": []}
                comments = []
                if not open_comments_sidebar(scrape_cdp):
                    err = f"No comment button for {cand['id']}"
                    print(f"    WARNING: {err}")
                    summary["errors"].append(err)
                    result["errors"].append(err)
                else:
                    time.sleep(2)
                    tree = wait_for_ax_tree(scrape_cdp, timeout=8)
                    comments = extract_comments_from_ax_tree(tree)
                    seen_ids = set(c["cid"] for c in comments)

                    for rnd in range(5):
                        time.sleep(1)
                        res = click_view_more_comments(scrape_cdp)
                        if res != "clicked":
                            break
                        time.sleep(3)
                        tree = wait_for_ax_tree(scrape_cdp, timeout=6)
                        new_comments = extract_comments_from_ax_tree(tree)
                        new_count = sum(1 for c in new_comments if c["cid"] not in seen_ids)
                        if new_count == 0:
                            break
                        for c in new_comments:
                            if c["cid"] not in seen_ids:
                                seen_ids.add(c["cid"])
                                comments.append(c)

                    result["total_comments"] = len(comments)
                    result["comments"] = comments

                # Save per-candidate result
                slug = cand["id"]
                cand_path = out_dir / f"{slug}.json"
                with open(cand_path, "w") as f:
                    json.dump(result, f, indent=2, ensure_ascii=False)
                print(f"    Saved {result['total_comments']} comments -> {cand_path}")

                summary["scraped"].append(cand["id"])
                summary["total_comments"] += len(comments)
                summary["total_scraped"] += 1

                time.sleep(inter_reel_pause_seconds())

                if safety_state.get("stopped"):
                    break
        except Exception as e:
            summary["errors"].append(f"Scrape phase error: {e}")
        finally:
            scrape_cdp.close()

    return summary


def target_slug(url: str) -> str:
    """Return a stable artifact filename slug for a target URL."""
    match = re.search(r"/(?:reel|videos?|watch)/([0-9A-Za-z_.-]+)", url)
    if match:
        return re.sub(r"[^0-9A-Za-z_.-]+", "_", match.group(1))[:80]
    return hashlib.md5(url.encode("utf-8")).hexdigest()[:16]


def run_target_urls(
    target_urls: list[str],
    out_dir: Path,
    mode_query: Optional[str] = None,
    additional_errors: Optional[list[str]] = None,
    task_meta: Optional[list[dict[str, str]]] = None,
    source_state: Optional[dict] = None,
    safety_state: Optional[dict] = None,
) -> dict:
    """Scrape comments from planned Facebook URLs with optional reason metadata."""
    ts = datetime.now()
    out_dir.mkdir(parents=True, exist_ok=True)

    normalized_targets = []
    seen_targets = set()
    raw_target_urls = list(target_urls or [])
    for target_url in raw_target_urls:
        canonical_url = canonicalize_source_url(target_url)
        if not canonical_url:
            continue
        if not is_direct_reel_video_target(canonical_url):
            continue
        if canonical_url in seen_targets:
            continue
        seen_targets.add(canonical_url)
        normalized_targets.append(canonical_url)

    if len(normalized_targets) != len(raw_target_urls):
        skipped = len(raw_target_urls) - len(normalized_targets)
        if skipped > 0:
            print(f"[target] Dropped {skipped} invalid/uncanonicalizable target URL(s)")
    tasks_by_url = {}
    for task in task_meta or []:
        task_url = canonicalize_source_url(str(task.get("url", "")))
        if task_url:
            tasks_by_url[task_url] = task

    selected_by_reason = {"explicit": 0, "latest": 0, "backfill": 0, "revisit": 0}
    summary = {
        "query": mode_query or "explicit-target-urls",
        "started_at": ts.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "dry_run": False,
        "candidates": [target_slug(url) for url in normalized_targets],
        "scraped": [],
        "tasks": [],
        "total_candidates": len(normalized_targets),
        "total_scraped": 0,
        "total_comments": 0,
        "errors": [],
        "output_dir": str(out_dir),
        "target_urls": normalized_targets,
        "selected_by_reason": selected_by_reason,
        "discovered_count": 0,
        "new_count": 0,
        "revisited_count": 0,
        "skipped_current": 0,
        "selected_count": 0,
        "explicit_count": 0,
        "source_count": 0,
    }
    if additional_errors:
        summary["errors"].extend(additional_errors)
    safety_state = safety_state or new_fb_safety_state(
        DEFAULT_MAX_RUNTIME_SECONDS,
        DEFAULT_MAX_FB_NAVIGATIONS,
    )

    if not normalized_targets:
        summary["errors"].append("No valid target URLs supplied")
        return summary

    if not ensure_browser():
        summary["errors"].append("Chrome CDP not reachable at " + CHROME_CDP)
        return summary
    try:
        scrape_ws_url, _, _ = create_new_tab()
    except Exception as e:
        summary["errors"].append(f"Could not open scrape tab: {e}")
        return summary
    if not scrape_ws_url:
        summary["errors"].append("Could not get WebSocket URL for scrape tab")
        return summary

    for idx, url in enumerate(normalized_targets):
        if safety_state.get("stopped"):
            summary["errors"].append(
                f"Safety stop before target scrape: {safety_state.get('stop_reason', 'unknown')}"
            )
            break
        summary["total_candidates"] = len(normalized_targets)
        slug = target_slug(url)
        print(f"[target] Scraping {url}")
        result = scrape_comments_for_url(scrape_ws_url, url, safety_state=safety_state)
        path = out_dir / f"{slug}.json"
        with open(path, "w") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        summary["tasks"].append(url)
        summary["scraped"].append(slug)
        summary["total_scraped"] += 1
        summary["total_comments"] += int(result.get("total_comments", 0))
        for err in result.get("errors", []):
            summary["errors"].append(f"{slug}: {err}")

        task = tasks_by_url.get(url, {})
        reason = str(task.get("reason", "explicit"))
        summary["selected_by_reason"][reason] = summary["selected_by_reason"].get(reason, 0) + 1
        if reason == "revisit":
            summary["revisited_count"] += 1
        if bool(task.get("is_new")):
            summary["new_count"] += 1

        if source_state is not None and task:
            mark_task_result(source_state, task, result, datetime.now())

        if idx + 1 < len(target_urls):
            time.sleep(inter_reel_pause_seconds())

    summary["selected_count"] = summary["total_scraped"]
    summary["discovered_count"] = summary.get("discovered_count", 0)
    summary["source_count"] = summary.get("source_count", 0)
    summary["explicit_count"] = summary.get("explicit_count", 0)
    return summary


def attach_safety_summary(
    summary: dict,
    safety_state: dict,
    *,
    end_ts: Optional[float] = None,
) -> dict:
    """Attach safety-stop metadata to a run summary."""
    finished_at = float(end_ts if end_ts is not None else time.time())
    started_at = float(safety_state.get("run_started_at", finished_at))
    runtime_seconds = max(0.0, finished_at - started_at)
    summary["safety_stop"] = bool(safety_state.get("stopped"))
    summary["safety_stop_reason"] = safety_state.get("stop_reason", "")
    summary["safety_stop_at"] = safety_state.get("stop_at", "")
    summary["navigation_count"] = int(safety_state.get("navigation_count", 0))
    summary["navigation_limit"] = int(safety_state.get("max_navigations", 0))
    summary["runtime_limit_seconds"] = int(safety_state.get("max_runtime_seconds", 0))
    summary["runtime_seconds"] = round(runtime_seconds, 3)

    if safety_state.get("stopped"):
        stop_reason = str(safety_state.get("stop_reason", ""))
        if stop_reason and all(
            stop_reason not in str(error) for error in summary.get("errors", [])
        ):
            summary["errors"].append(f"Safety stop: {stop_reason}")

    return summary


def save_latest(summary: dict) -> None:
    """Save the latest-run summary to state/fb_keyword_latest.json."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    latest = {
        "last_run": summary.get("started_at", ""),
        "query": summary.get("query", ""),
        "dry_run": summary.get("dry_run", False),
        "safety_stop": bool(summary.get("safety_stop", False)),
        "safety_stop_reason": summary.get("safety_stop_reason", ""),
        "safety_stop_at": summary.get("safety_stop_at", ""),
        "candidate_count": summary.get("total_candidates", 0),
        "scraped_count": summary.get("total_scraped", 0),
        "total_comments": summary.get("total_comments", 0),
        "runtime_seconds": summary.get("runtime_seconds", 0.0),
        "navigation_count": summary.get("navigation_count", 0),
        "navigation_limit": summary.get("navigation_limit", 0),
        "runtime_limit_seconds": summary.get("runtime_limit_seconds", 0),
        "discovered_count": summary.get("discovered_count", 0),
        "new_count": summary.get("new_count", 0),
        "revisited_count": summary.get("revisited_count", 0),
        "skipped_current": summary.get("skipped_current", 0),
        "source_count": summary.get("source_count", 0),
        "explicit_count": summary.get("explicit_count", 0),
        "selected_count": summary.get("selected_count", summary.get("total_scraped", 0)),
        "selected_by_reason": summary.get("selected_by_reason", {}),
        "run_invocation_id": summary.get("run_invocation_id", ""),
        "output_dir": summary.get("output_dir", ""),
        "errors": summary.get("errors", []),
    }
    with open(STATE_LATEST, "w") as f:
        json.dump(latest, f, indent=2)
    print(f"Latest summary -> {STATE_LATEST}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="Facebook Keyword Nightly Scraper")
    parser.add_argument("--query", default=DEFAULT_QUERY,
                        help=f"Search query (default: {DEFAULT_QUERY})")
    parser.add_argument("--max-candidates", type=int, default=DEFAULT_MAX_CANDIDATES,
                        help=f"Max candidates to discover (default: {DEFAULT_MAX_CANDIDATES})")
    parser.add_argument("--max-scrape", type=int, default=DEFAULT_MAX_SCRAPE,
                        help=f"Max candidates to scrape for comments (default: {DEFAULT_MAX_SCRAPE})")
    parser.add_argument("--max-discover-per-source", type=int, default=DEFAULT_MAX_DISCOVER_PER_SOURCE,
                        help=f"Max discoveries per source per run (default: {DEFAULT_MAX_DISCOVER_PER_SOURCE})")
    parser.add_argument("--max-runtime-seconds", type=int, default=DEFAULT_MAX_RUNTIME_SECONDS,
                        help=f"Maximum FB navigation runtime per run in seconds (default: {DEFAULT_MAX_RUNTIME_SECONDS})")
    parser.add_argument("--max-navigations", type=int, default=DEFAULT_MAX_FB_NAVIGATIONS,
                        help=f"Maximum FB navigations per run (default: {DEFAULT_MAX_FB_NAVIGATIONS})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Discover candidates only, skip comment scraping")
    parser.add_argument("--target-url", action="append", default=[],
                        help="Explicit Facebook reel/post URL to scrape; repeatable")
    parser.add_argument("--targets-file", default=str(DEFAULT_TARGETS_FILE),
                        help=f"JSON target URL file (default: {DEFAULT_TARGETS_FILE})")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    RAW_BASE.mkdir(parents=True, exist_ok=True)

    run_invocation_id = _safe_invocation_id(
        os.environ.get(RUN_INVOCATION_ENV_VAR, "")
    )
    ts = datetime.now()
    ts_str = ts.strftime("%Y%m%d_%H%M%S")
    if run_invocation_id:
        ts_str = run_invocation_id
    out_dir = RAW_BASE / ts_str

    cli_records: list[dict[str, str]] = []
    for target_url in args.target_url or []:
        url = canonicalize_source_url(target_url)
        if not url:
            continue
        cli_records.append({"url": url, "crawl_mode": "explicit", "platform": "facebook"})

    target_records = list(load_targets_file(Path(args.targets_file)))
    target_records.extend(cli_records)
    target_records_dedupe: list[dict[str, str]] = []
    seen_urls = set()
    for record in target_records:
        url = canonicalize_source_url(str(record.get("url", "")))
        if not url:
            continue
        if url in seen_urls:
            continue
        seen_urls.add(url)
        record["url"] = url
        target_records_dedupe.append(record)

    explicit_urls, source_records = split_target_records(target_records_dedupe)

    source_records_by_key = {}
    for record in source_records:
        source_key = stable_source_key(record.get("url", ""))
        if not source_key or source_key in source_records_by_key:
            continue
        source_records_by_key[source_key] = record
    source_records = list(source_records_by_key.values())

    source_state = load_source_crawl_state()
    discovery_errors: list[str] = []
    discovered_by_source: dict[str, list[str]] = {}
    new_post_keys_by_source: dict[str, set[str]] = {}
    safety_state = new_fb_safety_state(
        max_runtime_seconds=max(args.max_runtime_seconds, 1),
        max_navigations=max(args.max_navigations, 1),
    )

    if source_records:
        discovered_by_source, discovery_errors = discover_source_reels(
            source_records=source_records,
            max_per_source=args.max_discover_per_source,
            max_total=None,
            safety_state=safety_state,
        )

        existing_sources = source_state.get("sources", {})
        if not isinstance(existing_sources, dict):
            existing_sources = {}
            source_state["sources"] = existing_sources

        for source_key, urls in discovered_by_source.items():
            state_source = existing_sources.get(source_key, {})
            state_posts = state_source.get("posts") if isinstance(state_source, dict) else {}
            if not isinstance(state_posts, dict):
                state_posts = {}
            new_keys = set()
            for url in urls:
                post_key = stable_source_key(url)
                if post_key and post_key not in state_posts:
                    new_keys.add(post_key)
            new_post_keys_by_source[source_key] = new_keys

    scheduled_tasks, plan_summary = plan_source_scrape_tasks(
        explicit_urls=explicit_urls,
        source_records=source_records,
        source_state=source_state,
        discovered_by_source=discovered_by_source,
        new_post_keys_by_source=new_post_keys_by_source,
        max_scrape=args.max_scrape,
        now=ts,
    )
    if not scheduled_tasks and (explicit_urls or source_records):
        if args.max_scrape <= 0:
            print("[planner] max_scrape is set to 0; no tasks scheduled")
        else:
            print("[planner] No source-crawl tasks were scheduled (all monitored sources are current and no revisit is due)")

    print(f"=== FB Keyword Nightly ===")
    print(f"Query:    {args.query}")
    print(f"Targets:  {len(explicit_urls) + len(source_records)}")
    print(f"Planned:  {len(scheduled_tasks)}")
    print(f"Output:   {out_dir}")
    print(f"Dry run:  {args.dry_run}")
    print()

    if explicit_urls or source_records:
        if not args.dry_run and safety_state.get("stopped"):
            summary = {
                "query": "source-crawl",
                "started_at": ts.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "dry_run": False,
                "candidates": [],
                "scraped": [],
                "tasks": [],
                "total_candidates": 0,
                "total_scraped": 0,
                "total_comments": 0,
                "errors": list(discovery_errors),
                "output_dir": str(out_dir),
                "target_urls": [],
                "selected_by_reason": {
                    "explicit": 0,
                    "latest": 0,
                    "backfill": 0,
                    "revisit": 0,
                },
                "discovered_count": 0,
                "new_count": 0,
                "revisited_count": 0,
                "skipped_current": 0,
                "selected_count": 0,
                "explicit_count": 0,
                "source_count": len(source_records),
            }
            summary.update(plan_summary)
            summary["errors"].append(
                f"Safety stop before scraping: {safety_state.get('stop_reason', 'unknown')}"
            )
        else:
            if args.dry_run:
                summary = {
                    "query": "source-crawl-plan",
                    "started_at": ts.strftime("%Y-%m-%dT%H:%M:%S%z"),
                    "dry_run": True,
                    "candidates": [target_slug(task["url"]) for task in scheduled_tasks],
                    "scraped": [],
                    "tasks": [task.get("url") for task in scheduled_tasks],
                    "total_candidates": len(scheduled_tasks),
                    "total_scraped": 0,
                    "total_comments": 0,
                    "errors": [],
                    "output_dir": str(out_dir),
                    "target_urls": [task.get("url") for task in scheduled_tasks],
                }
                summary.update(plan_summary)
                summary["errors"].extend(discovery_errors)
            elif scheduled_tasks:
                summary = run_target_urls(
                    target_urls=[task.get("url", "") for task in scheduled_tasks],
                    out_dir=out_dir,
                    mode_query="source-crawl",
                    additional_errors=discovery_errors,
                    task_meta=scheduled_tasks,
                    source_state=source_state,
                    safety_state=safety_state,
                )
            else:
                summary = {
                    "query": "source-crawl",
                    "started_at": ts.strftime("%Y-%m-%dT%H:%M:%S%z"),
                    "dry_run": False,
                    "candidates": [],
                    "scraped": [],
                    "tasks": [],
                    "total_candidates": 0,
                    "total_scraped": 0,
                    "total_comments": 0,
                    "errors": list(discovery_errors),
                    "output_dir": str(out_dir),
                    "target_urls": [],
                    "selected_by_reason": {
                        "explicit": 0,
                        "latest": 0,
                        "backfill": 0,
                        "revisit": 0,
                    },
                    "discovered_count": 0,
                    "new_count": 0,
                    "revisited_count": 0,
                    "skipped_current": 0,
                    "selected_count": 0,
                    "explicit_count": 0,
                    "source_count": len(source_records),
                }
                summary["errors"].extend(discovery_errors)
                summary.update(plan_summary)
                summary["query"] = "source-crawl"
            if not args.dry_run:
                summary["query"] = summary.get("query", "source-crawl")

        summary.update(plan_summary)
        summary["query"] = "source-crawl"
        save_source_crawl_state(source_state)

    else:
        summary = run_keyword_search(
            query=args.query,
            max_candidates=args.max_candidates,
            max_scrape=args.max_scrape,
            dry_run=args.dry_run,
            out_dir=out_dir,
            safety_state=safety_state,
        )

    if args.dry_run and (explicit_urls or source_records):
        print(f"Dry-run summary: {summary['total_candidates']} discovered/scheduled")
        if summary["total_candidates"] == 0:
            save_source_crawl_state(source_state)

    summary["run_invocation_id"] = run_invocation_id
    attach_safety_summary(summary, safety_state)

    save_latest(summary)

    print()
    print(f"=== Done ===")
    print(f"Candidates: {summary['total_candidates']}")
    print(f"Scraped:    {summary['total_scraped']}")
    print(f"Comments:   {summary['total_comments']}")
    if summary["errors"]:
        print(f"Errors:     {summary['errors']}")

    if summary["errors"] and not summary["total_candidates"]:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
