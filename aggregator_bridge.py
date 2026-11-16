"""
Aggregator Bridge & Pure-Ingestion Mode (`aggregator_bridge.py`)

Separates `ai-hardware-bot` (Data Ingestion & Persistence Layer) from
`tech-trend-researcher` (Longitudinal Trend & Editorial Intelligence Layer):

1. Configures `agi_hardware_trends.db` in SQLite WAL (`PRAGMA journal_mode=WAL`)
   so `tech-trend-researcher` can read historical time-series snapshots concurrently
   without locking the ingestion daemon.
2. Ensures rich metadata columns (`summary`, `engagement_points`, `comments_count`,
   `ingested_at`) are present on `items` and records time-series engagement observations
   in `item_observations` so downstream trend analysis can compute true velocity/acceleration
   trajectories without re-scraping the web.
3. Provides `PURE_AGGREGATOR_MODE=true` support so `ai-hardware-bot` can run purely as a
   high-reliability multi-source collector without duplicating downstream LLM synthesis.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from typing import Any


def upgrade_aggregator_db_schema(db_path: str) -> None:
    """Enable WAL mode and add non-breaking columns/tables required by tech-trend-researcher."""
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

        # Time-series observation table for longitudinal trend velocity & burst detection
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


def record_item_observation(
    db_path: str,
    url: str,
    engagement_points: float = 0.0,
    comments_count: float = 0.0,
) -> None:
    """Append a point-in-time engagement snapshot for longitudinal velocity tracking."""
    now_iso = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path, timeout=30.0)
    try:
        conn.execute(
            """
            INSERT INTO item_observations (url, observed_at_utc, engagement_points, comments_count)
            VALUES (?, ?, ?, ?)
            """,
            (url, now_iso, float(engagement_points), float(comments_count)),
        )
        conn.commit()
    finally:
        conn.close()


def is_pure_aggregator_mode() -> bool:
    """Return True when inline LLM flushing is disabled in favor of tech-trend-researcher."""
    return os.getenv("PURE_AGGREGATOR_MODE", "true").strip().lower() in ("1", "true", "yes")
