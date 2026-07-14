#!/usr/bin/env python3
"""Durable wrapper for the nightly Facebook reel scraper cron."""

from __future__ import annotations

import json
import ipaddress
import hashlib
import os
import smtplib
import subprocess
import sys
import socket
import time
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Optional

REPO = Path("/Users/openclaw/.openclaw/workspace/herbal-data")
SCRIPT = REPO / "scraper" / "fb_keyword_nightly.py"
INGEST_SCRIPT = REPO / "scraper" / "ingest_facebook.py"
LATEST = REPO / "state" / "fb_keyword_latest.json"
TARGETS = REPO / "state" / "fb_reel_targets.json"
EMAIL_ENV = Path("/Users/openclaw/.openclaw-credentials/email.env")
EMAIL_TO = "yurazaicev@gmail.com"
RUN_INVOCATION_ENV_VAR = "FB_KEYWORD_RUN_INVOCATION_ID"


def run(
    cmd: list[str],
    check: bool = False,
    env: Optional[dict[str, str]] = None,
) -> subprocess.CompletedProcess[str]:
    print("$ " + " ".join(cmd), flush=True)
    return subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        check=check,
        env=env if env is not None else os.environ,
    )


def ensure_browser() -> None:
    status = run(["openclaw", "browser", "status", "--json"])
    running = False
    try:
        payload = json.loads(status.stdout)
        running = bool(payload.get("running") and payload.get("cdpReady"))
    except Exception:
        running = False
    if running:
        return
    started = run(["openclaw", "browser", "start"])
    if started.returncode != 0:
        print(started.stdout)
        print(started.stderr, file=sys.stderr)
    time.sleep(4)


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def load_target_urls() -> list[str]:
    if not TARGETS.exists():
        return []
    try:
        payload = json.loads(TARGETS.read_text())
    except Exception:
        return []
    urls: list[str] = []
    for target in payload.get("targets", []):
        if isinstance(target, str):
            urls.append(target)
        elif isinstance(target, dict) and target.get("url"):
            urls.append(str(target["url"]))
    return urls


def output_files(output_dir: Optional[str]) -> list[Path]:
    if not output_dir:
        return []
    path = Path(output_dir)
    if not path.exists():
        return []
    return sorted(path.glob("*.json"))


def current_run_id() -> str:
    return f"nightly-{int(time.time() * 1_000_000)}-{os.getpid()}"


def latest_state_snapshot() -> dict:
    if not LATEST.exists():
        return {
            "exists": False,
            "mtime_ns": 0,
            "size": 0,
            "sha256": "",
        }
    try:
        text = LATEST.read_bytes()
        stat = LATEST.stat()
        return {
            "exists": True,
            "mtime_ns": int(stat.st_mtime_ns),
            "size": int(stat.st_size),
            "sha256": hashlib.sha256(text).hexdigest(),
        }
    except Exception:
        return {
            "exists": True,
            "mtime_ns": -1,
            "size": -1,
            "sha256": "",
            "error": "snapshot-error",
        }


def latest_state_is_fresh(before: dict, after: dict) -> bool:
    if not before.get("exists"):
        return bool(after.get("exists"))
    if not after.get("exists"):
        return False
    if before.get("mtime_ns") != after.get("mtime_ns"):
        return True
    if before.get("size") != after.get("size"):
        return True
    if before.get("sha256") != after.get("sha256"):
        return True
    return False


def _current_run_latest_payload(
    before: dict,
    invocation_id: str,
    run_started_at: float,
) -> tuple[dict, Optional[str]]:
    after = latest_state_snapshot()
    if not after.get("exists"):
        return {}, "current-run validation failed: latest state file missing after invocation."
    if not latest_state_is_fresh(before, after):
        return {}, "current-run validation failed: latest state did not change for this invocation."

    try:
        payload = json.loads(LATEST.read_text())
    except Exception as exc:
        return {}, f"current-run validation failed: latest state is unreadable ({exc})."
    if not isinstance(payload, dict):
        return {}, "current-run validation failed: latest state is not a JSON object."

    expected_id = str(invocation_id).strip()
    if expected_id:
        actual_id = str(payload.get("run_invocation_id", "")).strip()
        if actual_id != expected_id:
            return payload, (
                f"current-run validation failed: latest run_invocation_id ({actual_id or '<missing>'}) "
                f"does not match this invocation ({expected_id})."
            )

    output_dir = str(payload.get("output_dir", "")).strip()
    if expected_id and expected_id not in output_dir:
        return payload, (
            "current-run validation failed: latest output_dir does not belong to this invocation."
        )

    try:
        last_run = str(payload.get("last_run", ""))
        if last_run and run_started_at:
            parsed_last = datetime.fromisoformat(last_run)
            if parsed_last.timestamp() < run_started_at - 300:
                return payload, (
                    "current-run validation failed: latest last_run is before invocation start."
                )
    except Exception:
        return payload, "current-run validation failed: latest last_run timestamp is invalid."

    return payload, None


def current_run_comment_ids(files: list[Path]) -> set[str]:
    ids: set[str] = set()
    for path in files:
        try:
            payload = json.loads(path.read_text())
        except Exception:
            continue
        for comment in payload.get("comments", []):
            cid = comment.get("cid")
            if cid:
                ids.add(str(cid))
    return ids


def historical_comment_ids(exclude_dir: Optional[str]) -> set[str]:
    ids: set[str] = set()
    exclude = Path(exclude_dir).resolve() if exclude_dir else None
    for path in (REPO / "raw" / "facebook_keyword").glob("*/*.json"):
        if exclude and exclude in path.resolve().parents:
            continue
        try:
            payload = json.loads(path.read_text())
        except Exception:
            continue
        for comment in payload.get("comments", []):
            cid = comment.get("cid")
            if cid:
                ids.add(str(cid))
    return ids


def _normalize_output_dir(output_dir: Optional[str]) -> Optional[str]:
    if not output_dir:
        return None
    path = Path(output_dir)
    if not path.is_absolute():
        return str(REPO / path)
    return str(path)


def run_keyword_ingest(output_dir: Optional[str]) -> subprocess.CompletedProcess[str]:
    if not output_dir:
        return subprocess.CompletedProcess(
            args=[sys.executable, str(INGEST_SCRIPT)],
            returncode=1,
            stdout="",
            stderr="No keyword output_dir provided; skipping ingest.\n",
        )
    cmd = [
        sys.executable,
        str(INGEST_SCRIPT),
        "--skip-facebook-posts",
        "--facebook-keyword-output-dir",
        _normalize_output_dir(output_dir),
    ]
    return run(cmd)


def _load_frontend_env() -> dict[str, str]:
    merged = dict(os.environ)
    merged.update(load_env(EMAIL_ENV))
    return merged


def _discover_frontend_host() -> str:
    def _is_private_lan_host(host: str) -> bool:
        try:
            addr = ipaddress.ip_address(host)
        except ValueError:
            return False
        if addr.version != 4 or addr.is_loopback or not addr.is_private:
            return False
        return not host.startswith("100.64.")

    def _host_rank(host: str) -> tuple[int, str]:
        if host.startswith("192.168."):
            return (0, host)
        if host.startswith("10."):
            return (1, host)
        try:
            addr = ipaddress.ip_address(host)
            if addr in ipaddress.ip_network("172.16.0.0/12"):
                return (2, host)
        except ValueError:
            return (99, host)
        return (3, host)

    def _local_ipv4_candidates() -> list[str]:
        candidates: list[str] = []

        try:
            infos = socket.getaddrinfo(
                socket.gethostname(),
                None,
                family=socket.AF_INET,
                type=socket.SOCK_DGRAM,
                flags=socket.AI_ADDRCONFIG,
            )
            for _, _, _, _, sockaddr in infos:
                candidates.append(sockaddr[0])
        except Exception:
            pass

        if os.uname().sysname == "Darwin":
            try:
                result = subprocess.run(
                    ["ifconfig", "-a"],
                    text=True,
                    capture_output=True,
                    check=False,
                )
                if result.returncode == 0:
                    for line in result.stdout.splitlines():
                        line = line.strip()
                        if not line.startswith("inet "):
                            continue
                        parts = line.split()
                        if len(parts) >= 2:
                            candidates.append(parts[1])
            except Exception:
                pass

        return [ip for ip in dict.fromkeys(candidates) if _is_private_lan_host(ip)]

    for host in sorted(_local_ipv4_candidates(), key=_host_rank):
        return host

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("100.64.0.1", 1))
        return sock.getsockname()[0]
    except Exception:
        return ""
    finally:
        sock.close()


def _frontend_port(env: dict[str, str]) -> str:
    port = env.get("HERBAL_FRONTEND_PORT", "").strip()
    if not port:
        return "5173"
    try:
        int(port)
        return port
    except ValueError:
        return "5173"


def _frontend_host(env: dict[str, str]) -> str:
    host = env.get("HERBAL_FRONTEND_HOST", "").strip()
    if host:
        return host
    return _discover_frontend_host() or "localhost"


def _frontend_url() -> str:
    env = _load_frontend_env()
    explicit = env.get("HERBAL_FRONTEND_URL", "").strip()
    if explicit:
        return explicit
    host = _frontend_host(env)
    port = _frontend_port(env)
    return f"http://{host}:{port}/"


def build_email_body(
    latest: dict,
    returncode: int,
    stdout: str,
    stderr: str,
    ingest_result: Optional[subprocess.CompletedProcess[str]] = None,
) -> str:
    files = output_files(latest.get("output_dir"))
    run_ids = current_run_comment_ids(files)
    historical_ids = historical_comment_ids(latest.get("output_dir"))
    new_ids = run_ids - historical_ids
    target_urls = load_target_urls()
    errors = latest.get("errors") or []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    safety_stop = bool(latest.get("safety_stop", False))
    safety_reason = str(latest.get("safety_stop_reason", "")).strip()
    selected_by_reason = latest.get("selected_by_reason", {}) or {}
    if not isinstance(selected_by_reason, dict):
        selected_by_reason = {}

    lines = [
        f"Facebook scraper nightly report ({now} Europe/London)",
        "",
        f"Status: {'needs attention' if safety_stop or returncode != 0 or errors else 'ok'}",
        f"Return code: {returncode}",
        f"Safety stop: {'yes' if safety_stop else 'no'}",
        f"Safety stop reason: {safety_reason or 'none'}",
        f"Runtime seconds: {latest.get('runtime_seconds', 0)} / {latest.get('runtime_limit_seconds', 0)}",
        f"FB navigations: {latest.get('navigation_count', 0)} / {latest.get('navigation_limit', 0)}",
        f"Query/mode: {latest.get('query', 'unknown')}",
        f"Targets configured: {len(target_urls)}",
        f"Candidates found: {latest.get('candidate_count', 0)}",
        f"Items scraped: {latest.get('scraped_count', 0)}",
        f"Comments extracted: {latest.get('total_comments', 0)}",
        f"Discovered posts: {latest.get('discovered_count', 0)}",
        f"New posts scheduled: {latest.get('new_count', 0)}",
        f"Revisited posts: {latest.get('revisited_count', 0)}",
        f"Skipped current-known posts: {latest.get('skipped_current', 0)}",
        f"New unique comment IDs vs prior runs: {len(new_ids)}",
        f"Selected by reason: explicit={selected_by_reason.get('explicit', 0)} "
        f"latest={selected_by_reason.get('latest', 0)} "
        f"backfill={selected_by_reason.get('backfill', 0)} "
        f"revisit={selected_by_reason.get('revisit', 0)}",
        f"Output directory: {latest.get('output_dir', 'none')}",
        f"Frontend URL: {_frontend_url()}",
        f"Errors: {len(errors)}",
    ]
    if target_urls:
        lines.append("")
        lines.append("Targets:")
        lines.extend(f"- {url}" for url in target_urls)
    if errors:
        lines.append("")
        lines.append("Error detail:")
        lines.extend(f"- {error}" for error in errors)
    if stderr.strip():
        lines.append("")
        lines.append("stderr tail:")
        lines.append(stderr.strip()[-2000:])
    if stdout.strip():
        lines.append("")
        lines.append("stdout tail:")
        lines.append(stdout.strip()[-2000:])
    if ingest_result is not None:
        lines.append("")
        lines.append("Ingest status: "
                     f"{'ok' if ingest_result.returncode == 0 else 'needs attention'}")
        lines.append(f"Ingest return code: {ingest_result.returncode}")
        if ingest_result.stdout:
            lines.append("Ingest stdout tail:")
            lines.append(ingest_result.stdout.strip()[-2000:])
        if ingest_result.stderr:
            lines.append("Ingest stderr tail:")
            lines.append(ingest_result.stderr.strip()[-2000:])
    return "\n".join(lines)


def send_report(
    latest: dict,
    returncode: int,
    stdout: str,
    stderr: str,
    ingest_result: Optional[subprocess.CompletedProcess[str]] = None,
) -> bool:
    env = {**os.environ, **load_env(EMAIL_ENV)}
    required = ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS", "EMAIL_FROM"]
    missing = [key for key in required if not env.get(key)]
    if missing:
        print(f"email_report=skipped missing={','.join(missing)}", file=sys.stderr)
        return False

    subject_date = datetime.now().strftime("%Y-%m-%d")
    ingest_ok = not ingest_result or ingest_result.returncode == 0
    subject_status = (
        "OK"
        if returncode == 0 and not latest.get("safety_stop") and not latest.get("errors") and ingest_ok
        else "ATTENTION"
    )
    msg = EmailMessage()
    msg["From"] = env["EMAIL_FROM"]
    msg["To"] = EMAIL_TO
    msg["Subject"] = f"Facebook scraper overnight report {subject_date} [{subject_status}]"
    msg.set_content(
        build_email_body(
            latest, returncode, stdout, stderr, ingest_result
        )
    )

    with smtplib.SMTP(env["SMTP_HOST"], int(env["SMTP_PORT"]), timeout=30) as smtp:
        smtp.starttls()
        smtp.login(env["SMTP_USER"], env["SMTP_PASS"])
        smtp.send_message(msg)
    print(f"email_report=sent to={EMAIL_TO}")
    return True


def main() -> int:
    ensure_browser()
    run_invocation_id = current_run_id()
    cmd = [
        sys.executable,
        str(SCRIPT),
        "--max-candidates",
        "10",
        "--max-scrape",
        "3",
    ]
    run_env = dict(os.environ)
    run_env[RUN_INVOCATION_ENV_VAR] = run_invocation_id
    pre_latest = latest_state_snapshot()
    invocation_started_at = time.time()
    result = run(cmd, env=run_env)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    ingest_result = None
    latest, validation_error = _current_run_latest_payload(
        pre_latest,
        run_invocation_id,
        invocation_started_at,
    )
    if latest:
        try:
            print("latest=" + json.dumps(latest, sort_keys=True))
        except Exception:
            pass
    if validation_error:
        latest = {
            "query": "unknown",
            "candidate_count": 0,
            "scraped_count": 0,
            "total_comments": 0,
            "output_dir": "",
            "errors": [validation_error],
            "run_invocation_id": run_invocation_id,
        }
    if not latest:
        latest = {
            "query": "unknown",
            "candidate_count": 0,
            "scraped_count": 0,
            "total_comments": 0,
            "output_dir": "",
            "errors": ["No latest scraper state was written."],
            "run_invocation_id": run_invocation_id,
        }
    if not latest:
        latest = {}

    if validation_error:
        latest["errors"] = list(dict.fromkeys(
            [validation_error] + latest.get("errors", [])
        ))

    valid_latest_for_ingest = (
        not validation_error
        and bool(latest.get("output_dir"))
    )
    try:
        if valid_latest_for_ingest:
            ingest_result = run_keyword_ingest(latest.get("output_dir"))
            print("ingest_stdout=" + ingest_result.stdout)
            if ingest_result.stderr:
                print(f"ingest_stderr={ingest_result.stderr}", file=sys.stderr)
        else:
            ingest_result = subprocess.CompletedProcess(
                args=[sys.executable, str(INGEST_SCRIPT)],
                returncode=1,
                stdout="",
                stderr="No eligible current invocation output to ingest.",
            )
    except Exception as exc:
        ingest_result = subprocess.CompletedProcess(
            args=[sys.executable, str(INGEST_SCRIPT)],
            returncode=1,
            stdout="",
            stderr=f"Ingest run failed before execute: {exc}",
        )
    scraper_ok = result.returncode == 0 and not bool(latest.get("errors"))
    ingest_ok = bool(ingest_result and ingest_result.returncode == 0)
    exit_code = 0 if scraper_ok and ingest_ok else 1
    try:
        send_report(latest, exit_code, result.stdout, result.stderr, ingest_result)
    except Exception as exc:
        print(f"email_report=failed error={exc}", file=sys.stderr)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
