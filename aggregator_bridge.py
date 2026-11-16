"""
Pure-Ingestion Daemon & Multi-Vertical Collector (`aggregator_bridge.py`)

Fundamentally transforms `ai-hardware-bot` into the dedicated 24/7 Ingestion &
Time-Series Persistence Layer for `tech-trend-researcher`:

1. Bypasses `app.py`'s inline LLM batch filter (`classify_batch` / `flush_queue`) when
   `PURE_AGGREGATOR_MODE=true`. Previously, `app.py` only inserted items into SQLite
   *after* an OpenRouter LLM call and dropped anything below a static single-item score,
   starving downstream trend discovery of early-stage baseline mentions.
2. Reuses `app.py`'s existing fetchers (`fetch_all_sources`) AND expands ingestion across
   the required multi-vertical sources:
   - Specialized Subreddits (`r/semiconductors`, `r/hardware`, `r/LocalLLaMA`, `r/biotech`,
     `r/robotics`, `r/QuantumComputing`) with real upvote & comment counts.
   - Multi-Vertical RSS/Atom (`bioRxiv`, `FierceBiotech`, `Nature Biotechnology`,
     `IEEE Spectrum Robotics`, `The Robot Report`, `Quantum Computing Report`,
     `Physics World Quantum`, `TechCrunch Venture`, `Crunchbase News`).
   - GitHub API active hardware/kernel/robotics/quantum repositories with star & issue counts.
3. Extracts and persists numerical `engagement_points` and `comments_count` on every cycle,
   writing both to `items` and to the longitudinal `item_observations` table in SQLite WAL mode.
"""

from __future__ import annotations

import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import feedparser
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

import app as core_app

load_dotenv()
log = logging.getLogger("ai-hardware-bot.aggregator")

POINTS_RE = re.compile(r"Points:\s*(\d+)", re.IGNORECASE)
COMMENTS_RE = re.compile(r"Comments:\s*(\d+)", re.IGNORECASE)


def upgrade_aggregator_db_schema(db_path: str) -> None:
    """Enable WAL mode and add longitudinal observation columns/tables."""
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30.0)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        existing_cols = {
            row[1] for row in conn.execute("PRAGMA table_info(items)").fetchall()
        }
        if existing_cols:
            migrations = [
                ("summary", "TEXT DEFAULT ''"),
                ("engagement_points", "REAL DEFAULT 0.0"),
                ("comments_count", "REAL DEFAULT 0.0"),
                ("ingested_at", "TEXT DEFAULT ''"),
            ]
            for col_name, col_def in migrations:
                if col_name not in existing_cols:
                    conn.execute(f"ALTER TABLE items ADD COLUMN {col_name} {col_def};")

        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS item_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                observed_at_utc TEXT NOT NULL,
                engagement_points REAL NOT NULL DEFAULT 0.0,
                comments_count REAL NOT NULL DEFAULT 0.0
            );
            CREATE INDEX IF NOT EXISTS idx_item_obs_url_time
                ON item_observations(url, observed_at_utc);
            CREATE INDEX IF NOT EXISTS idx_items_published_at
                ON items(published_at);
            """
        )
        conn.commit()
    finally:
        conn.close()


def _extract_engagement(item: dict[str, Any]) -> tuple[float, float]:
    """Extract numerical points/likes and comments from raw item fields or HN summary text."""
    pts = float(item.get("engagement_points") or item.get("points") or item.get("score") or 0.0)
    coms = float(item.get("comments_count") or item.get("num_comments") or 0.0)
    raw_sum = str(item.get("raw_summary") or item.get("summary") or "")
    if pts == 0.0:
        m_pts = POINTS_RE.search(raw_sum)
        if m_pts:
            pts = float(m_pts.group(1))
    if coms == 0.0:
        m_com = COMMENTS_RE.search(raw_sum)
        if m_com:
            coms = float(m_com.group(1))
    return pts, coms


def fetch_reddit_communities() -> list[dict[str, Any]]:
    if not core_app.get_bool("ENABLE_REDDIT_INGESTION", True):
        return []
    subs_raw = os.getenv("REDDIT_SUBREDDITS", "semiconductors,hardware,LocalLLaMA,biotech,robotics,QuantumComputing")
    subs = [s.strip() for s in subs_raw.split(",") if s.strip()]
    limit = core_app.get_int("REDDIT_LIMIT_PER_SUB", 15)
    collected: list[dict[str, Any]] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for sub in subs:
        try:
            resp = requests.get(
                f"https://www.reddit.com/r/{sub}/hot.json?limit={limit}",
                headers={"User-Agent": "AI-Hardware-Trend-Bot/2.3"},
                timeout=12,
            )
            if not resp.ok:
                continue
            for child in resp.json().get("data", {}).get("children", []):
                post = child.get("data", {})
                title = str(post.get("title") or "").strip()
                permalink = str(post.get("permalink") or "")
                url = str(post.get("url") or (f"https://www.reddit.com{permalink}" if permalink else "")).strip()
                if not title or not url:
                    continue
                created_utc = post.get("created_utc")
                pub_iso = (
                    datetime.fromtimestamp(float(created_utc), tz=timezone.utc).isoformat()
                    if created_utc
                    else now_iso
                )
                pts = float(post.get("score") or post.get("ups") or 0.0)
                coms = float(post.get("num_comments") or 0.0)
                selftext = str(post.get("selftext") or "")[:1500]
                collected.append(
                    {
                        "url": url,
                        "title": title,
                        "source": f"r/{sub}",
                        "source_type": "reddit",
                        "published_at": pub_iso,
                        "category": sub.lower(),
                        "raw_summary": f"Points: {int(pts)}. Comments: {int(coms)}. {selftext}".strip(),
                        "engagement_points": pts,
                        "comments_count": coms,
                    }
                )
        except Exception as exc:
            log.warning("fetch_reddit r/%s failed: %s", sub, exc)
    return collected


def fetch_multi_vertical_rss() -> list[dict[str, Any]]:
    if not core_app.get_bool("ENABLE_MULTI_VERTICAL_FEEDS", True):
        return []
    raw_specs = os.getenv("MULTI_VERTICAL_RSS_FEEDS", "")
    if not raw_specs.strip():
        return []
    collected: list[dict[str, Any]] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for pair in raw_specs.split("|"):
        if "=" not in pair:
            continue
        label, feed_url = pair.split("=", 1)
        label, feed_url = label.strip(), feed_url.strip()
        if not feed_url:
            continue
        try:
            parsed = feedparser.parse(feed_url, request_headers={"User-Agent": "AI-Hardware-Trend-Bot/2.3"})
            for entry in parsed.entries[:15]:
                title = str(getattr(entry, "title", "") or "").strip()
                link = str(getattr(entry, "link", "") or "").strip()
                if not title or not link:
                    continue
                pub = getattr(entry, "published", None) or getattr(entry, "updated", None) or now_iso
                summary = core_app.clean_text(str(getattr(entry, "summary", "") or ""))[:1800]
                collected.append(
                    {
                        "url": link,
                        "title": title,
                        "source": label,
                        "source_type": "biorxiv" if "biorxiv" in feed_url.lower() else "custom_rss",
                        "published_at": str(pub),
                        "category": label.lower(),
                        "raw_summary": summary,
                        "engagement_points": 10.0,
                        "comments_count": 0.0,
                    }
                )
        except Exception as exc:
            log.warning("fetch_multi_vertical_rss %s failed: %s", label, exc)
    return collected


def fetch_github_signals() -> list[dict[str, Any]]:
    if not core_app.get_bool("ENABLE_GITHUB_INGESTION", True):
        return []
    topics_raw = os.getenv("GITHUB_SEARCH_TOPICS", "cuda,rocm,riscv,verilog,quantum,robotics")
    topics = [t.strip() for t in topics_raw.split(",") if t.strip()]
    query = " OR ".join(f"topic:{t}" for t in topics[:5])
    collected: list[dict[str, Any]] = []
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        resp = requests.get(
            "https://api.github.com/search/repositories",
            params={"q": query, "sort": "updated", "order": "desc", "per_page": 15},
            headers={"User-Agent": "AI-Hardware-Trend-Bot/2.3", "Accept": "application/vnd.github+json"},
            timeout=12,
        )
        if resp.ok:
            for repo in resp.json().get("items", []):
                url = str(repo.get("html_url") or "").strip()
                name = str(repo.get("full_name") or "").strip()
                desc = str(repo.get("description") or "").strip()
                if not url or not name:
                    continue
                stars = float(repo.get("stargazers_count") or 0.0)
                issues = float(repo.get("open_issues_count") or 0.0)
                collected.append(
                    {
                        "url": url,
                        "title": f"{name}: {desc}"[:240],
                        "source": "GitHub",
                        "source_type": "github",
                        "published_at": str(repo.get("pushed_at") or now_iso),
                        "category": "github",
                        "raw_summary": f"Points: {int(stars)}. Comments: {int(issues)}. {desc}",
                        "engagement_points": stars,
                        "comments_count": issues,
                    }
                )
    except Exception as exc:
        log.warning("fetch_github_signals failed: %s", exc)
    return collected


def ingest_cycle_pure() -> int:
    """
    Execute a full multi-vertical ingestion cycle and persist all deduplicated items
    and time-series engagement observations directly to SQLite WAL without LLM gating.
    """
    raw_items = core_app.fetch_all_sources()
    raw_items.extend(fetch_reddit_communities())
    raw_items.extend(fetch_multi_vertical_rss())
    raw_items.extend(fetch_github_signals())

    now_iso = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(core_app.DB_PATH, timeout=30.0)
    inserted = 0
    obs_recorded = 0
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        for item in raw_items:
            url = core_app.normalize_url(str(item.get("url") or ""))
            fp = core_app.item_fingerprint(item)
            title = str(item.get("title") or "").strip()
            if not url or not title:
                continue

            pts, coms = _extract_engagement(item)
            raw_sum = str(item.get("raw_summary") or item.get("summary") or "")[:2200]
            source = str(item.get("source") or "unknown")
            source_type = str(item.get("source_type") or "rss")
            pub_at = str(item.get("published_at") or now_iso)
            cat = str(item.get("category") or "")

            cur = conn.execute(
                """
                INSERT INTO items (
                    url, content_fingerprint, title, source, source_type,
                    published_at, category, raw_summary, summary,
                    engagement_points, comments_count, ingested_at, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    engagement_points = MAX(items.engagement_points, excluded.engagement_points),
                    comments_count = MAX(items.comments_count, excluded.comments_count)
                """,
                (url, fp, title, source, source_type, pub_at, cat, raw_sum, raw_sum, pts, coms, now_iso, now_iso),
            )
            inserted += cur.rowcount

            conn.execute(
                """
                INSERT OR IGNORE INTO processed_urls (url, content_fingerprint, source, source_type, status, processed_at)
                VALUES (?, ?, ?, ?, 'ingested_raw', ?)
                """,
                (url, fp, source, source_type, now_iso),
            )

            conn.execute(
                """
                INSERT INTO item_observations (url, observed_at_utc, engagement_points, comments_count)
                VALUES (?, ?, ?, ?)
                """,
                (url, now_iso, pts, coms),
            )
            obs_recorded += 1

        conn.commit()
    finally:
        conn.close()

    # Touch heartbeat file for Docker HEALTHCHECK
    hb_path = Path(core_app.HEALTHCHECK_FILE)
    try:
        hb_path.parent.mkdir(parents=True, exist_ok=True)
        hb_path.write_text(now_iso, encoding="utf-8")
    except Exception as exc:
        log.warning("Could not touch heartbeat file %s: %s", hb_path, exc)

    log.info(
        "Pure ingestion cycle complete: %d fetched, %d upserted to items, %d time-series observations logged.",
        len(raw_items),
        inserted,
        obs_recorded,
    )
    return inserted


def is_pure_aggregator_mode() -> bool:
    return os.getenv("PURE_AGGREGATOR_MODE", "true").strip().lower() in ("1", "true", "yes")


def main() -> None:
    core_app.init_db()
    upgrade_aggregator_db_schema(core_app.DB_PATH)

    if not is_pure_aggregator_mode():
        log.info("PURE_AGGREGATOR_MODE=false; delegating to legacy app.main()")
        core_app.main()
        return

    fetch_minutes = max(1, core_app.get_int("FETCH_MINUTES", 5))
    log.info(
        "Starting ai-hardware-bot in PURE_AGGREGATOR_MODE (interval=%dm, db=%s)",
        fetch_minutes,
        core_app.DB_PATH,
    )
    ingest_cycle_pure()

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(ingest_cycle_pure, "interval", minutes=fetch_minutes, id="pure_ingest_job", max_instances=1)
    scheduler.start()
    try:
        while True:
            time.sleep(30)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == "__main__":
    main()
