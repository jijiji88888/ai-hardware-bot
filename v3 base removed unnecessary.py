import os
import time
import json
import html
import sqlite3
import datetime as dt
import hashlib
import re
import logging
import threading
from collections import defaultdict, deque
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl, urlencode as encode_query

import requests
import feedparser
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler


load_dotenv()

# =========================
# Logging
# =========================
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").strip().upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ai_hardware_bot")

# =========================
# Environment helpers
# =========================


def get_int(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def get_float(name, default):
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def get_bool(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ["1", "true", "yes", "y", "on"]


def get_csv(name, default_values=None):
    raw = os.getenv(name, "")
    if not raw.strip():
        return list(default_values or [])
    for separator in ["|", ";"]:
        raw = raw.replace(separator, ",")
    return [part.strip() for part in raw.split(",") if part.strip()]


def get_feed_specs(name, default_specs=None):
    """Parse feed/page specs from .env.
    Format:
    SOURCE_NAME=https://feed.url|OTHER_SOURCE=https://other.url

    The pipe separator is used because RSS URLs can contain commas or query strings.
    """
    raw = os.getenv(name, "")
    if not raw.strip():
        return list(default_specs or [])
    specs = []
    for part in raw.split("|"):
        part = part.strip()
        if not part:
            continue
        if "=" in part:
            label, url = part.split("=", 1)
        elif "::" in part:
            label, url = part.split("::", 1)
        else:
            label, url = part, part
        label = clean_text(label) if "clean_text" in globals() else label.strip()
        url = url.strip()
        if label and url:
            specs.append((label, url))
    return specs or list(default_specs or [])


# =========================
# Configuration
# =========================

DB_PATH = os.getenv("DB_PATH", "/app/data/agi_hardware_trends2.db").strip()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "").strip()
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "deepseek/deepseek-v4-flash").strip()

# Safer default: fetch every 10 minutes, not every minute.
FETCH_MINUTES = get_int("FETCH_MINUTES", 10)
DIGEST_MINUTES = get_int("DIGEST_MINUTES", 360)

# Caps are intentionally broad enough to capture the latest items from each source
# over a 10-minute window, while preventing unlimited token/API pressure.
MAX_CANDIDATES_PER_RUN = get_int("MAX_CANDIDATES_PER_RUN", 180)
MAX_CANDIDATES_PER_FLUSH = get_int("MAX_CANDIDATES_PER_FLUSH", 96)
CANDIDATE_QUEUE_MAX = get_int("CANDIDATE_QUEUE_MAX", 360)

LLM_BATCH_SIZE = max(1, get_int("LLM_BATCH_SIZE", 8))
LLM_MIN_SCORE = get_float("LLM_MIN_SCORE", 7.0)
ALERT_THRESHOLD = get_float("ALERT_THRESHOLD", 8.5)

ARXIV_RESULTS_PER_CATEGORY = get_int("ARXIV_RESULTS_PER_CATEGORY", 6)
HN_NEW_STORIES_PER_RUN = get_int("HN_NEW_STORIES_PER_RUN", 80)
TECHMEME_MAX_ITEMS = get_int("TECHMEME_MAX_ITEMS", 40)

MAX_ALERTS_PER_FLUSH = get_int("MAX_ALERTS_PER_FLUSH", 8)
DIGEST_MAX_ITEMS = get_int("DIGEST_MAX_ITEMS", 20)
SEND_EMPTY_DIGEST = get_bool("SEND_EMPTY_DIGEST", False)
INCLUDE_ALERTS_IN_DIGEST = get_bool("INCLUDE_ALERTS_IN_DIGEST", False)
SEND_RAW_FETCH_REPORT = get_bool("SEND_RAW_FETCH_REPORT", False)

ENABLE_ARXIV = get_bool("ENABLE_ARXIV", True)
ENABLE_HACKERNEWS = get_bool("ENABLE_HACKERNEWS", True)
ENABLE_TECHMEME = get_bool("ENABLE_TECHMEME", True)

DEBUG_LLM = get_bool("DEBUG_LLM", False)
SEND_STARTUP_MESSAGE = get_bool("SEND_STARTUP_MESSAGE", True)

QUEUE_FLUSH_LIMIT = get_int("QUEUE_FLUSH_LIMIT", 32)
CHECK_QUEUE_SECONDS = get_int("CHECK_QUEUE_SECONDS", 60)
FORCE_FLUSH_MINUTES = get_int("FORCE_FLUSH_MINUTES", 30)

MAX_ITEM_RETRIES = get_int("MAX_ITEM_RETRIES", 5)

FEED_FAILURE_THRESHOLD = get_int("FEED_FAILURE_THRESHOLD", 4)
FEED_COOLDOWN_MINUTES = get_int("FEED_COOLDOWN_MINUTES", 180)

HN_FETCH_WORKERS = get_int("HN_FETCH_WORKERS", 8)
ARXIV_FETCH_WORKERS = get_int("ARXIV_FETCH_WORKERS", 4)

HEALTHCHECK_FILE = os.getenv("HEALTHCHECK_FILE", "/app/data/heartbeat.txt").strip()

LLM_TEXT_FIELD_MAX_CHARS = get_int("LLM_TEXT_FIELD_MAX_CHARS", 500)

DEFAULT_ARXIV_CATEGORIES = [
    "cs.AI", "cs.LG", "cs.CL", "cs.CV", "cs.RO", "cs.DC", "cs.AR", "cs.ET",
    "eess.SY", "eess.SP", "eess.IV",
]
ARXIV_CATEGORIES = get_csv("ARXIV_CATEGORIES", DEFAULT_ARXIV_CATEGORIES)

SEND_RAW_FETCH_REPORT = get_bool("SEND_RAW_FETCH_REPORT", False)
SEND_RAW_FETCH_JSON = get_bool("SEND_RAW_FETCH_JSON", False)


# =========================
# Expanded high-signal sources
# =========================

# Step 1: specialist sources
ENABLE_SPECIALIST_RSS = get_bool("ENABLE_SPECIALIST_RSS", True)
SPECIALIST_RSS_MAX_ITEMS_PER_FEED = get_int("SPECIALIST_RSS_MAX_ITEMS_PER_FEED", 12)
DEFAULT_SPECIALIST_RSS_FEEDS = [
    ("Semiconductor Engineering", "https://semiengineering.com/feed/"),
    ("The Register / HPC", "https://api.theregister.com/api/v1/article?limit=25&orderBy=published&query=tag%3Ahpc&remapper=rss&site_id=2"),
    ("ServeTheHome", "https://www.servethehome.com/feed/"),
    ("OCP Blog", "https://www.opencompute.org/blog/rss;;https://news.google.com/rss/search?q=%22Open+Compute+Project%22&hl=en-US&gl=US&ceid=US:en"),
]

# Step 2: Asia/supply-chain layer
ENABLE_SUPPLY_CHAIN_RSS = get_bool("ENABLE_SUPPLY_CHAIN_RSS", True)
SUPPLY_CHAIN_RSS_MAX_ITEMS_PER_FEED = get_int("SUPPLY_CHAIN_RSS_MAX_ITEMS_PER_FEED", 12)
DEFAULT_SUPPLY_CHAIN_RSS_FEEDS = [
    ("DIGITIMES Asia", "https://www.digitimes.com/rss/daily.xml"),
    ("TrendForce / Semiconductors", "https://www.trendforce.com/feed/Semiconductors.html"),
]

# Step 3: SEC EDGAR polling — broad feed across filers
ENABLE_SEC_EDGAR = get_bool("ENABLE_SEC_EDGAR", True)
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "AI-Hardware-Trend-Bot/2.3 contact@example.com").strip()
SEC_FORMS = get_csv("SEC_FORMS", ["8-K", "10-Q", "10-K", "S-1", "F-1", "20-F", "6-K"])
SEC_MAX_ITEMS_PER_FETCH = get_int("SEC_MAX_ITEMS_PER_FETCH", 100)

# Step 4: OpenReview
ENABLE_OPENREVIEW = get_bool("ENABLE_OPENREVIEW", True)
OPENREVIEW_API_BASE = os.getenv("OPENREVIEW_API_BASE", "https://api2.openreview.net").strip().rstrip("/")
OPENREVIEW_MAX_NOTES_PER_INVITATION = get_int("OPENREVIEW_MAX_NOTES_PER_INVITATION", 12)
OPENREVIEW_INVITATIONS = get_csv("OPENREVIEW_INVITATIONS", [
    "ICLR.cc/2026/Conference/-/Submission",
    "NeurIPS.cc/2025/Conference/-/Submission",
    "ICML.cc/2026/Conference/-/Submission",
])

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, text/html, application/json;q=0.9, */*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


# ==========================================
# Volatile staging queue
# ==========================================

CANDIDATE_QUEUE: list = []
SEEN_URLS_CACHE: dict = {}
SEEN_FINGERPRINTS_CACHE: dict = {}
_queue_lock = Lock()
_last_flush_time: dt.datetime = None  # type: ignore[assignment]

FEED_FAILURE_STATE: dict = {}
FEED_CACHE_HEADERS: dict = {}
_feed_state_lock = Lock()

_thread_local = threading.local()


# =========================
# Small utilities
# =========================


def utc_now():
    return dt.datetime.now(dt.timezone.utc)


_last_flush_time = utc_now()


def clean_text(value):
    if not value:
        return ""
    text = html.unescape(str(value))
    return " ".join(text.replace("\n", " ").replace("\t", " ").split())


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def safe_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ["true", "1", "yes", "y", "on"]
    return bool(value)


def html_escape(value):
    return html.escape(clean_text(value), quote=False)


def normalize_url(url):
    """Reduce duplicate URLs caused by tracking parameters."""
    if not url:
        return ""
    try:
        parts = urlsplit(url.strip())
        tracking_prefixes = ("utm_",)
        tracking_names = {"fbclid", "gclid", "mc_cid", "mc_eid", "igshid"}
        query_pairs = []
        for key, value in parse_qsl(parts.query, keep_blank_values=True):
            key_lower = key.lower()
            if key_lower.startswith(tracking_prefixes) or key_lower in tracking_names:
                continue
            query_pairs.append((key, value))
        clean_query = encode_query(query_pairs, doseq=True)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, clean_query, ""))
    except Exception:
        return url.strip()


def title_fingerprint(title):
    """Stable article fingerprint for cross-source duplicate protection."""
    cleaned = clean_text(title).lower()
    cleaned = re.sub(r"https?://\S+", " ", cleaned)
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
    cleaned = re.sub(r"\b(the|a|an|to|of|and|or|for|in|on|with|by|from|at|as|is|are)\b", " ", cleaned)
    cleaned = " ".join(cleaned.split())[:220]
    if not cleaned:
        return ""
    return hashlib.sha256(cleaned.encode("utf-8")).hexdigest()


def item_fingerprint(item):
    if item.get("content_fingerprint"):
        return item["content_fingerprint"]
    title = item.get("title", "")
    if item.get("source_type") == "sec_filing":
        base = title_fingerprint(title)
        url_part = hashlib.sha256(item.get("url", "").encode("utf-8")).hexdigest()[:16]
        return f"{base}:{url_part}"
    return title_fingerprint(title)


def feed_should_skip_live(feed_url):
    with _feed_state_lock:
        state = FEED_FAILURE_STATE.get(feed_url)
        cooldown_until = state.get("cooldown_until") if state else None
    return bool(cooldown_until and utc_now() < cooldown_until)


def feed_record_result(feed_url, success):
    with _feed_state_lock:
        state = FEED_FAILURE_STATE.setdefault(feed_url, {"consecutive_failures": 0, "cooldown_until": None})
        if success:
            state["consecutive_failures"] = 0
            state["cooldown_until"] = None
        else:
            state["consecutive_failures"] += 1
            if state["consecutive_failures"] >= FEED_FAILURE_THRESHOLD:
                state["cooldown_until"] = utc_now() + dt.timedelta(minutes=FEED_COOLDOWN_MINUTES)


def ensure_db_directory():
    directory = os.path.dirname(DB_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)


def chunk_list(values, size):
    for start in range(0, len(values), size):
        yield values[start:start + size]


def fair_cap_items(items, max_items):
    """Apply a global cap without letting one source dominate the run."""
    if max_items <= 0 or len(items) <= max_items:
        return items

    grouped = defaultdict(deque)
    source_order = []
    for item in items:
        source = item.get("source", "unknown")
        if source not in grouped:
            source_order.append(source)
        grouped[source].append(item)

    result = []
    while len(result) < max_items and source_order:
        next_order = []
        for source in source_order:
            if grouped[source] and len(result) < max_items:
                result.append(grouped[source].popleft())
            if grouped[source]:
                next_order.append(source)
        source_order = next_order
    return result


# =========================
# Database
# =========================


def connect_db():
    conn = getattr(_thread_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.row_factory = sqlite3.Row
        _thread_local.conn = conn
    return conn


def init_db():
    ensure_db_directory()
    conn = connect_db()
    cur = conn.cursor()

    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA synchronous=NORMAL")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            content_fingerprint TEXT,
            title TEXT,
            source TEXT,
            source_type TEXT,
            published_at TEXT,
            category TEXT,
            hardware_domain TEXT,
            bottleneck_addressed TEXT,
            specific_tech TEXT,
            relevance_score REAL,
            why_it_matters TEXT,
            ai_revolution_thesis TEXT,
            telegram_summary TEXT,
            raw_summary TEXT,
            raw_llm_json TEXT,
            sent_alert INTEGER DEFAULT 0,
            digest_sent INTEGER DEFAULT 0,
            created_at TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_urls (
            url TEXT PRIMARY KEY,
            content_fingerprint TEXT,
            title TEXT,
            source TEXT,
            source_type TEXT,
            status TEXT,
            first_seen_at TEXT,
            last_seen_at TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_fingerprints (
            content_fingerprint TEXT PRIMARY KEY,
            title TEXT,
            status TEXT,
            first_seen_at TEXT,
            last_seen_at TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )

    cur.execute("PRAGMA table_info(items)")
    existing_columns = {row[1] for row in cur.fetchall()}
    wanted_columns = {
        "content_fingerprint": "TEXT",
        "hardware_domain": "TEXT",
        "bottleneck_addressed": "TEXT",
        "ai_revolution_thesis": "TEXT",
    }
    for column_name, column_type in wanted_columns.items():
        if column_name not in existing_columns:
            cur.execute(f"ALTER TABLE items ADD COLUMN {column_name} {column_type}")

    cur.execute("PRAGMA table_info(processed_urls)")
    existing_processed_columns = {row[1] for row in cur.fetchall()}
    if "content_fingerprint" not in existing_processed_columns:
        cur.execute("ALTER TABLE processed_urls ADD COLUMN content_fingerprint TEXT")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_items_fingerprint ON items(content_fingerprint)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_processed_urls_fingerprint ON processed_urls(content_fingerprint)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_items_alert ON items(sent_alert, relevance_score, created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_items_digest ON items(digest_sent, relevance_score, created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_items_created ON items(created_at)")
    conn.commit()


def get_state(key, default=None):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT value FROM bot_state WHERE key = ?", (key,))
    row = cur.fetchone()
    return row["value"] if row else default


def set_state(key, value):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO bot_state (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, str(value)),
    )
    conn.commit()


def get_hn_high_water_mark():
    try:
        return int(get_state("hn_high_water_mark", "0"))
    except (TypeError, ValueError):
        return 0


def set_hn_high_water_mark(value):
    set_state("hn_high_water_mark", value)


def processed_url_exists(url):
    if not url:
        return True
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM processed_urls WHERE url = ? LIMIT 1", (url,))
    return cur.fetchone() is not None


def processed_fingerprint_exists(content_fingerprint):
    if not content_fingerprint:
        return False
    conn = connect_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM processed_fingerprints WHERE content_fingerprint = ? LIMIT 1",
        (content_fingerprint,),
    )
    return cur.fetchone() is not None


def mark_url_processed(item, status):
    url = normalize_url(item.get("url", ""))
    fp = item_fingerprint(item)
    if not url and not fp:
        return
    now = utc_now().isoformat()
    conn = connect_db()
    cur = conn.cursor()

    if url:
        cur.execute(
            """
            INSERT INTO processed_urls (url, content_fingerprint, title, source, source_type, status, first_seen_at, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                content_fingerprint = COALESCE(excluded.content_fingerprint, processed_urls.content_fingerprint),
                title = excluded.title,
                source = excluded.source,
                source_type = excluded.source_type,
                status = excluded.status,
                last_seen_at = excluded.last_seen_at
            """,
            (
                url,
                fp,
                item.get("title", ""),
                item.get("source", ""),
                item.get("source_type", ""),
                status,
                now,
                now,
            ),
        )

    if fp:
        cur.execute(
            """
            INSERT INTO processed_fingerprints (content_fingerprint, title, status, first_seen_at, last_seen_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(content_fingerprint) DO UPDATE SET
                title = excluded.title,
                status = excluded.status,
                last_seen_at = excluded.last_seen_at
            """,
            (fp, item.get("title", ""), status, now, now),
        )

    conn.commit()


def insert_item(item, sent_alert=0, digest_sent=0):
    conn = connect_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO items (
                url, content_fingerprint, title, source, source_type, published_at, category,
                hardware_domain, bottleneck_addressed, specific_tech,
                relevance_score, why_it_matters, ai_revolution_thesis,
                telegram_summary, raw_summary, raw_llm_json,
                sent_alert, digest_sent, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalize_url(item.get("url", "")),
                item_fingerprint(item),
                item.get("title", ""),
                item.get("source", ""),
                item.get("source_type", ""),
                item.get("published_at", utc_now().isoformat()),
                item.get("category", ""),
                item.get("hardware_domain", ""),
                item.get("bottleneck_addressed", ""),
                item.get("specific_tech", ""),
                safe_float(item.get("relevance_score", 0)),
                item.get("why_it_matters", ""),
                item.get("ai_revolution_thesis", ""),
                item.get("telegram_summary", ""),
                item.get("raw_summary", ""),
                item.get("raw_llm_json", ""),
                sent_alert,
                digest_sent,
                utc_now().isoformat(),
            ),
        )
        conn.commit()
        inserted = True
    except sqlite3.IntegrityError:
        conn.rollback()
        inserted = False
    return inserted

# =========================
# Telegram
# =========================


def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram disabled: missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID.")
        return False

    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    text = message.strip()
    chunks = []

    current = ""
    for raw_line in text.splitlines():
        line = raw_line[:3800]
        proposed = f"{current}\n{line}" if current else line
        if len(proposed) > 3800:
            if current:
                chunks.append(current)
            current = line
        else:
            current = proposed
    if current:
        chunks.append(current)

    success = True
    for chunk in chunks:
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": chunk,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        }
        try:
            response = requests.post(api_url, json=payload, timeout=30)
            if response.status_code != 200:
                log.warning(f"Telegram sending issue: {response.status_code} {response.text[:500]}")
                success = False
        except Exception as exc:
            log.warning(f"Telegram sending exception: {repr(exc)}")
            success = False
        time.sleep(1)

    return success

def send_telegram_document(filename, text_content):
    """Sends a text string as a downloadable file to Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False

    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    
    files = {
        "document": (filename, text_content.encode('utf-8'))
    }
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "caption": "📊 Raw fetched titles (pre-deduplication)"
    }
    
    try:
        response = requests.post(api_url, data=payload, files=files, timeout=30)
        if response.status_code != 200:
            log.warning(f"Telegram document sending issue: {response.status_code} {response.text[:500]}")
            return False
        return True
    except Exception as exc:
        log.warning(f"Telegram document sending exception: {repr(exc)}")
        return False

def format_item_html(row, include_score=True):
    title = html_escape(row["title"])
    url = html_escape(row["url"])
    source = html_escape(row["source"])
    score = safe_float(row["relevance_score"], 0)
    tech = html_escape(row["specific_tech"] or row["category"])
    domain = html_escape(row["hardware_domain"])
    bottleneck = html_escape(row["bottleneck_addressed"])
    why = html_escape(row["why_it_matters"])
    thesis = html_escape(row["ai_revolution_thesis"])
    summary = html_escape(row["telegram_summary"])

    lines = []
    if include_score:
        lines.append(f"<b>{score:.1f}/10</b> · {tech}")
    else:
        lines.append(f"<b>{tech}</b>")
    lines.append(f"<a href=\"{url}\">{title}</a>")
    lines.append(f"Source: {source}")
    if domain or bottleneck:
        lines.append(f"Layer: {domain} · Bottleneck: {bottleneck}")
    if summary:
        lines.append(summary)
    if why:
        lines.append(f"Why it matters: {why}")
    if thesis:
        lines.append(f"AI thesis: {thesis}")
    return "\n".join(lines)


def send_immediate_alerts(alert_items):
    if not alert_items:
        return []

    items_to_send = alert_items[:MAX_ALERTS_PER_FLUSH]

    blocks = ["🚨 <b>High-signal AI hardware alert</b>"]
    for item in items_to_send:
        blocks.append(format_item_html(item, include_score=True))

    message = "\n\n".join(blocks)

    if not send_telegram(message):
        log.warning("Immediate alert send failed; leaving these items for the next digest.")
        return []

    log.info(f"Sent {len(items_to_send)} immediate alert items.")
    return [item["url"] for item in items_to_send]


def send_digest():
    conn = connect_db()
    cur = conn.cursor()
    
    time_window = (utc_now() - dt.timedelta(hours=24)).isoformat()
    
    cur.execute(
        """
        SELECT * FROM items
        WHERE digest_sent = 0 
          AND relevance_score >= ? 
          AND created_at >= ?
        ORDER BY relevance_score DESC, created_at DESC
        LIMIT ?
        """,
        (LLM_MIN_SCORE, time_window, DIGEST_MAX_ITEMS),
    )
    rows = cur.fetchall()

    if not rows:
        if SEND_EMPTY_DIGEST:
            send_telegram("🧠 <b>AI hardware digest</b>\nNo new relevant items passed the strict filter.")
        else:
            log.info("No new digest items.")
        return 0

    header = f"🧠 <b>AI hardware digest</b> · {len(rows)} new signal(s)"
    blocks = [header]
    for row in rows:
        blocks.append(format_item_html(row, include_score=True))
    message = "\n\n".join(blocks)

    if not send_telegram(message):
        log.warning("Digest send failed; items remain unsent.")
        return 0

    ids = [row["id"] for row in rows]
    conn = connect_db()
    cur = conn.cursor()
    cur.executemany("UPDATE items SET digest_sent = 1 WHERE id = ?", [(item_id,) for item_id in ids])
    conn.commit()
    log.info(f"Sent {len(ids)} digest items.")
    return len(ids)

def send_raw_json_dump(raw_items):
    """Sends all raw fetched items as an uncleaned JSON file to Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    
    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    filename = f"raw_fetch_dump_{utc_now().strftime('%Y%m%d_%H%M%S')}.json"
    
    try:
        json_content = json.dumps(raw_items, indent=2, ensure_ascii=False)
    except Exception as exc:
        log.warning(f"Failed to serialize raw JSON dump: {repr(exc)}")
        return False

    files = {
        "document": (filename, json_content.encode("utf-8"), "application/json")
    }
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "caption": f"📁 <b>Raw JSON Fetch Dump</b>\nTotal items: {len(raw_items)} (Pre-cleaning & pre-slicing)",
        "parse_mode": "HTML"
    }
    
    try:
        response = requests.post(api_url, data=payload, files=files, timeout=45)
        if response.status_code != 200:
            log.warning(f"Telegram raw JSON sending issue: {response.status_code} {response.text[:500]}")
            return False
        log.info(f"Sent raw JSON dump ({len(raw_items)} items) to Telegram.")
        return True
    except Exception as exc:
        log.warning(f"Telegram raw JSON sending exception: {repr(exc)}")
        return False


# =========================
# Source fetchers
# =========================


ARXIV_HEADERS = {
    "User-Agent": "ai-hardware-trend-bot/1.0 (contact: your-real-email@example.com)"
}


def _fetch_arxiv_atom(url, max_retries=3, timeout=15):
    """Direct fetch + parse of the arXiv Atom API, bypassing fetch_standard_rss
    so category tags survive (fetch_standard_rss normalizes entries down to
    title/url/summary and drops <category> tags, which is what caused every
    bucket to come back 0/20 the first time this was tried). Minimal
    retry/backoff since we lose fetch_standard_rss's shared resilience
    wrapper (Wayback fallback, cooldown, conditional GET) for this source."""
    last_exc = None
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=ARXIV_HEADERS, timeout=timeout)
            resp.raise_for_status()
            parsed = feedparser.parse(resp.content)
            if parsed.bozo and not parsed.entries:
                raise ValueError(f"feedparser bozo with no entries: {parsed.bozo_exception}")
            return parsed.entries
        except Exception as exc:
            last_exc = exc
            log.warning(f"arXiv fetch attempt {attempt + 1}/{max_retries} failed: {repr(exc)}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # 1s, 2s, 4s
    log.error(f"arXiv combined fetch failed after {max_retries} attempts: {repr(last_exc)}")
    return []


def _parse_arxiv_entry(entry, category):
    """Field names here MUST match what fetch_and_enqueue()/insert_item()
    expect (url, source, source_type, published_at, raw_summary) — NOT the
    raw feedparser/Atom names (link, published, summary). fetch_and_enqueue()
    silently drops any item with an empty "url" (line ~1963), so a field-name
    mismatch here doesn't error, it just makes arXiv items vanish before
    dedup ever sees them."""
    return {
        "title": clean_text(entry.get("title", "")),
        "url": normalize_url(entry.get("link", "")),
        "source": f"arXiv / {category}",
        "source_type": "research",
        "published_at": entry.get("published", entry.get("updated", utc_now().isoformat())),
        "raw_summary": clean_text(entry.get("summary", ""))[:2200],
        "categories": [t.get("term") for t in entry.get("tags", []) if t.get("term")],
    }


def fetch_arxiv():
    """Fetch ARXIV_RESULTS_PER_CATEGORY items for each configured category
    in a single combined API call. Bypasses fetch_standard_rss so category
    tags are preserved for bucketing. One request total avoids the
    concurrent-request pattern arXiv's API terms ask clients not to use,
    and sidesteps the 429s the per-category ThreadPoolExecutor version
    could trigger."""
    items = []
    if not ENABLE_ARXIV or not ARXIV_CATEGORIES:
        return items

    combined_query = "(" + " OR ".join(f"cat:{c}" for c in ARXIV_CATEGORIES) + ")"
    max_results = ARXIV_RESULTS_PER_CATEGORY * len(ARXIV_CATEGORIES) * 10

    params = {
        "search_query": combined_query,
        "start": 0,
        "max_results": max_results,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }
    url = "https://export.arxiv.org/api/query?" + urlencode(params)

    raw_entries = _fetch_arxiv_atom(url)
    if not raw_entries:
        return items

    # Bucket by category, capped at ARXIV_RESULTS_PER_CATEGORY per bucket.
    # An entry can belong to multiple ARXIV_CATEGORIES (arXiv cross-lists);
    # it's counted toward every matching bucket it's still short on, not
    # just the first, so overlapping categories don't starve each other.
    buckets = {cat: [] for cat in ARXIV_CATEGORIES}
    for entry in raw_entries:
        entry_categories = [t.get("term") for t in entry.get("tags", []) if t.get("term")]
        for cat in ARXIV_CATEGORIES:
            if cat in entry_categories and len(buckets[cat]) < ARXIV_RESULTS_PER_CATEGORY:
                buckets[cat].append(_parse_arxiv_entry(entry, cat))

    for cat, bucketed in buckets.items():
        if len(bucketed) < ARXIV_RESULTS_PER_CATEGORY:
            log.warning(
                f"arXiv / {cat}: only {len(bucketed)} of "
                f"{ARXIV_RESULTS_PER_CATEGORY} requested items found in combined fetch"
            )
        items.extend(bucketed)

    return items


def _fetch_hn_item(session, story_id):
    try:
        response = session.get(
            f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json",
            headers=REQUEST_HEADERS,
            timeout=20,
        )
        if response.status_code != 200:
            return None
        hit = response.json() or {}
        if hit.get("type") != "story":
            return None

        title = clean_text(hit.get("title", ""))
        if not title:
            return None

        link = normalize_url(hit.get("url") or f"https://news.ycombinator.com/item?id={story_id}")
        points = hit.get("score") or 0
        comments = hit.get("descendants") or 0
        created_at = dt.datetime.fromtimestamp(hit.get("time", time.time()), tz=dt.timezone.utc).isoformat()
        text = clean_text(hit.get("text", ""))

        return {
            "title": title,
            "url": link,
            "source": "Hacker News / newest",
            "source_type": "community",
            "published_at": created_at,
            "raw_summary": f"Newest Hacker News story. Points: {points}. Comments: {comments}. Text: {text[:1200]}",
        }
    except Exception as exc:
        log.error(f"HN item {story_id} exception: {repr(exc)}")
        return None


def fetch_hackernews():
    items = []
    if not ENABLE_HACKERNEWS:
        return items

    try:
        ids_response = requests.get(
            "https://hacker-news.firebaseio.com/v0/newstories.json",
            headers=REQUEST_HEADERS,
            timeout=30,
        )
        if ids_response.status_code != 200:
            log.warning(f"HN newest IDs failure: {ids_response.status_code} {ids_response.text[:300]}")
            return items
        story_ids = ids_response.json()[:HN_NEW_STORIES_PER_RUN]
    except Exception as exc:
        log.error(f"HN newest IDs exception: {repr(exc)}")
        return items

    last_seen_id = get_hn_high_water_mark()
    fresh_ids = [sid for sid in story_ids if sid > last_seen_id] if last_seen_id else story_ids
    skipped = len(story_ids) - len(fresh_ids)
    if skipped:
        log.info(f"HN: skipping {skipped} id(s) at or below high-water mark {last_seen_id}.")
    if not fresh_ids:
        return items

    session = requests.Session()
    workers = max(1, min(HN_FETCH_WORKERS, len(fresh_ids)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_fetch_hn_item, session, sid) for sid in fresh_ids]
        for future in as_completed(futures):
            result = future.result()
            if result:
                items.append(result)

    set_hn_high_water_mark(max(fresh_ids))
    return items


def fetch_techmeme():
    if not ENABLE_TECHMEME:
        return []
    return fetch_standard_rss(
        "https://www.techmeme.com/feed.xml",
        source_label="Techmeme",
        source_type="technology_news",
        max_items=TECHMEME_MAX_ITEMS,
    )


def fetch_standard_rss(feed_url, source_label=None, source_type="rss", max_items=25):
    """Fetch an RSS/Atom feed with browser headers, conditional GET, a
    per-feed cooldown after repeated failures, and a Wayback Machine
    fallback."""
    items = []
    max_items = max_items or 25
    feed = None
    raw_content = None
    live_status = None
    not_modified = False

    request_headers = dict(REQUEST_HEADERS)
    cached_headers = FEED_CACHE_HEADERS.get(feed_url) or {}
    if cached_headers.get("etag"):
        request_headers["If-None-Match"] = cached_headers["etag"]
    if cached_headers.get("last_modified"):
        request_headers["If-Modified-Since"] = cached_headers["last_modified"]

    skip_live = feed_should_skip_live(feed_url)
    if skip_live:
        log.info(f"RSS feed on cooldown, skipping live attempt: {source_label or feed_url}")

    if not skip_live:
        try:
            response = requests.get(feed_url, headers=request_headers, timeout=35)
            live_status = response.status_code
            if response.status_code == 304:
                not_modified = True
            elif response.status_code == 200:
                raw_content = response.content
                if response.headers.get("ETag") or response.headers.get("Last-Modified"):
                    FEED_CACHE_HEADERS[feed_url] = {
                        "etag": response.headers.get("ETag", cached_headers.get("etag")),
                        "last_modified": response.headers.get("Last-Modified", cached_headers.get("last_modified")),
                    }
            else:
                log.info(f"RSS live fetch failed for {source_label or feed_url}: {response.status_code}")
        except requests.exceptions.Timeout:
            log.info(f"RSS timeout for {source_label or feed_url}.")
        except Exception as exc:
            log.info(f"RSS requests issue for {source_label or feed_url}: {repr(exc)}")

    if not_modified:
        feed_record_result(feed_url, success=True)
        return items

    if raw_content:
        try:
            feed = feedparser.parse(raw_content)
        except Exception as exc:
            log.info(f"RSS feedparser parsing issue for {feed_url}: {repr(exc)}")
            feed = None

    live_entries_ok = bool(feed and getattr(feed, "entries", None))
    feed_record_result(feed_url, success=live_entries_ok)

    if not live_entries_ok:
        reason = "blocked/challenge page or malformed feed" if raw_content is not None else f"status {live_status or 'request failed'}"
        log.info(f"RSS live path yielded no entries for {source_label or feed_url} ({reason}). Trying Wayback Machine...")
        try:
            wb_api_url = f"https://archive.org/wayback/available?url={feed_url}"
            wb_resp = requests.get(wb_api_url, timeout=15)
            if wb_resp.status_code == 200:
                snapshots = wb_resp.json().get("archived_snapshots", {})
                if "closest" in snapshots and snapshots["closest"].get("available"):
                    snapshot_url = snapshots["closest"]["url"].replace("http://", "https://")
                    log.info(f"Wayback snapshot found for {source_label or feed_url}. Fetching...")
                    snap_resp = requests.get(snapshot_url, headers=REQUEST_HEADERS, timeout=30)
                    if snap_resp.status_code == 200:
                        wb_feed = feedparser.parse(snap_resp.content)
                        if getattr(wb_feed, "entries", None):
                            feed = wb_feed
        except Exception as wb_exc:
            log.info(f"Wayback Machine RSS fallback failed for {feed_url}: {repr(wb_exc)}")

    if not feed or not getattr(feed, "entries", None):
        try:
            feed = feedparser.parse(feed_url)
        except Exception:
            return items

    if not feed or not getattr(feed, "entries", None):
        return items

    source_name = source_label or (clean_text(feed.feed.get("title", feed_url)) if getattr(feed, "feed", None) else feed_url)
    for entry in getattr(feed, "entries", [])[:max_items]:
        title = clean_text(entry.get("title", ""))
        link = normalize_url(entry.get("link", ""))
        summary = clean_text(entry.get("summary", entry.get("description", "")))

        if not title or not link:
            continue

        items.append({
            "title": title,
            "url": link,
            "source": f"RSS / {source_name}",
            "source_type": source_type,
            "published_at": entry.get("published", entry.get("updated", utc_now().isoformat())),
            "raw_summary": summary[:2200],
        })

    return items

def fetch_standard_rss_2(feed_url, source_label=None, source_type="custom_rss", max_items=None):
    """Enriched RSS/Atom fetcher: extracts <content:encoded>, tags/categories, author, and handles stubs."""
    items = []
    max_items = max_items or CUSTOM_RSS_MAX_ITEMS_PER_FEED
    feed = None
    raw_content = None
    live_status = None
    not_modified = False

    request_headers = dict(REQUEST_HEADERS)
    cached_headers = FEED_CACHE_HEADERS.get(feed_url) or {}
    if cached_headers.get("etag"):
        request_headers["If-None-Match"] = cached_headers["etag"]
    if cached_headers.get("last_modified"):
        request_headers["If-Modified-Since"] = cached_headers["last_modified"]

    skip_live = feed_should_skip_live(feed_url)
    if skip_live:
        log.info(f"RSS feed on cooldown, skipping live attempt: {source_label or feed_url}")

    if not skip_live:
        try:
            response = requests.get(feed_url, headers=request_headers, timeout=35)
            live_status = response.status_code
            if response.status_code == 304:
                not_modified = True
            elif response.status_code == 200:
                raw_content = response.content
                if response.headers.get("ETag") or response.headers.get("Last-Modified"):
                    FEED_CACHE_HEADERS[feed_url] = {
                        "etag": response.headers.get("ETag", cached_headers.get("etag")),
                        "last_modified": response.headers.get("Last-Modified", cached_headers.get("last_modified")),
                    }
            else:
                log.info(f"RSS live fetch failed for {source_label or feed_url}: {response.status_code}")
        except requests.exceptions.Timeout:
            log.info(f"RSS timeout for {source_label or feed_url}.")
        except Exception as exc:
            log.info(f"RSS requests issue for {source_label or feed_url}: {repr(exc)}")

    if not_modified:
        feed_record_result(feed_url, success=True)
        return items

    if raw_content:
        try:
            feed = feedparser.parse(raw_content)
        except Exception as exc:
            log.info(f"RSS feedparser parsing issue for {feed_url}: {repr(exc)}")
            feed = None

    live_entries_ok = bool(feed and getattr(feed, "entries", None))
    feed_record_result(feed_url, success=live_entries_ok)

    if not live_entries_ok:
        reason = "blocked/challenge page or malformed feed" if raw_content is not None else f"status {live_status or 'request failed'}"
        log.info(f"RSS live path yielded no entries for {source_label or feed_url} ({reason}). Trying Wayback Machine...")
        try:
            wb_api_url = f"https://archive.org/wayback/available?url={feed_url}"
            wb_resp = requests.get(wb_api_url, timeout=15)
            if wb_resp.status_code == 200:
                snapshots = wb_resp.json().get("archived_snapshots", {})
                if "closest" in snapshots and snapshots["closest"].get("available"):
                    snapshot_url = snapshots["closest"]["url"].replace("http://", "https://")
                    log.info(f"Wayback snapshot found for {source_label or feed_url}. Fetching...")
                    snap_resp = requests.get(snapshot_url, headers=REQUEST_HEADERS, timeout=30)
                    if snap_resp.status_code == 200:
                        wb_feed = feedparser.parse(snap_resp.content)
                        if getattr(wb_feed, "entries", None):
                            feed = wb_feed
        except Exception as wb_exc:
            log.info(f"Wayback Machine RSS fallback failed for {feed_url}: {repr(wb_exc)}")

    if not feed or not getattr(feed, "entries", None):
        try:
            feed = feedparser.parse(feed_url)
        except Exception:
            return items

    if not feed or not getattr(feed, "entries", None):
        return items

    source_name = source_label or (clean_text(feed.feed.get("title", feed_url)) if getattr(feed, "feed", None) else feed_url)
    session = requests.Session()

    for entry in getattr(feed, "entries", [])[:max_items]:
        title = clean_text(entry.get("title", ""))
        link = normalize_url(entry.get("link", ""))
        if not title or not link:
            continue

        # 1. Prefer content:encoded or entry.content if available
        body = ""
        if "content" in entry and entry.content and isinstance(entry.content, list):
            body = clean_text(re.sub(r"<[^>]+>", " ", entry.content[0].get("value", "")))
        if not body:
            body = clean_text(re.sub(r"<[^>]+>", " ", entry.get("summary", entry.get("description", ""))))

        # 2. Extract tags / categories
        tags = []
        if "tags" in entry and entry.tags:
            for t in entry.tags:
                term = clean_text(t.get("term", ""))
                if term and term not in tags:
                    tags.append(term)

        # 3. Extract author
        author = clean_text(entry.get("author", entry.get("dc_creator", "")))

        # 4. If body is very short (< 120 chars), attempt fast OpenGraph scrape
        if len(body) < 120 and link:
            meta_snippet = fetch_webpage_summary_snippet(session, link, timeout=6)
            if meta_snippet and len(meta_snippet) > len(body):
                body = f"{body} (Lead: {meta_snippet})"

        substance = []
        if tags:
            substance.append(f"Tags: {', '.join(tags[:6])}")
        if author:
            substance.append(f"Author: {author}")
        if body:
            substance.append(f"Summary: {body[:2200]}")

        items.append({
            "title": title,
            "url": link,
            "source": f"RSS / {source_name}",
            "source_type": source_type,
            "published_at": entry.get("published", entry.get("updated", utc_now().isoformat())),
            "raw_summary": " | ".join(substance) if substance else title,
        })

    return items


# =========================
# Expanded source fetchers
# =========================


def split_fallback_urls(url_value):
    raw = str(url_value or "")
    return [part.strip() for part in raw.split(";;") if part.strip()]


def fetch_feed_specs(specs, source_type, max_items_per_feed):
    items = []
    for label, feed_url in specs:
        total_for_label = 0
        tried_any = False
        for candidate_url in split_fallback_urls(feed_url):
            tried_any = True
            try:
                fetched = fetch_standard_rss(
                    candidate_url,
                    source_label=label,
                    source_type=source_type,
                    max_items=max_items_per_feed,
                )
                log.info(f"{source_type} feed: {len(fetched)} items from {label}")
                items.extend(fetched)
                total_for_label += len(fetched)
                if fetched:
                    break
            except Exception as exc:
                log.error(f"{source_type} issue for {label} / {candidate_url}: {repr(exc)}")
            time.sleep(1)
        if tried_any and total_for_label == 0:
            log.warning(f"{source_type}: no items from {label} after fallback URLs.")
    return items


def fetch_feed_specs_2(specs, source_type, max_items_per_feed):
    items = []
    for label, feed_url in specs:
        total_for_label = 0
        tried_any = False
        for candidate_url in split_fallback_urls(feed_url):
            tried_any = True
            try:
                fetched = fetch_standard_rss_2(
                    candidate_url,
                    source_label=label,
                    source_type=source_type,
                    max_items=max_items_per_feed,
                )
                log.info(f"{source_type} feed: {len(fetched)} items from {label}")
                items.extend(fetched)
                total_for_label += len(fetched)
                if fetched:
                    break
            except Exception as exc:
                log.error(f"{source_type} issue for {label} / {candidate_url}: {repr(exc)}")
            time.sleep(1)
        if tried_any and total_for_label == 0:
            log.warning(f"{source_type}: no items from {label} after fallback URLs.")
    return items

def fetch_webpage_summary_snippet(session, url, timeout=6):
    """Fallback scraper to grab OpenGraph or meta description for short stubs."""
    try:
        resp = session.get(url, headers=REQUEST_HEADERS, timeout=timeout)
        if resp.status_code != 200:
            return ""
        html_text = resp.text
        # Fast regex extraction for og:description or description meta tags
        og_match = re.search(
            r'<meta\s+[^>]*property=[\'"]og:description[\'"][^>]*content=[\'"]([^\'"]+)[\'"]',
            html_text,
            re.IGNORECASE,
        ) or re.search(
            r'<meta\s+[^>]*content=[\'"]([^\'"]+)[\'"][^>]*property=[\'"]og:description[\'"]',
            html_text,
            re.IGNORECASE,
        )
        if og_match:
            return clean_text(og_match.group(1))

        meta_match = re.search(
            r'<meta\s+[^>]*name=[\'"]description[\'"][^>]*content=[\'"]([^\'"]+)[\'"]',
            html_text,
            re.IGNORECASE,
        ) or re.search(
            r'<meta\s+[^>]*content=[\'"]([^\'"]+)[\'"][^>]*name=[\'"]description[\'"]',
            html_text,
            re.IGNORECASE,
        )
        if meta_match:
            return clean_text(meta_match.group(1))
    except Exception:
        pass
    return ""


def fetch_specialist_rss():
    if not ENABLE_SPECIALIST_RSS:
        return []
    specs = get_feed_specs("SPECIALIST_RSS_FEEDS", DEFAULT_SPECIALIST_RSS_FEEDS)
    return fetch_feed_specs_2(specs, "specialist_media", SPECIALIST_RSS_MAX_ITEMS_PER_FEED)


def fetch_supply_chain_rss():
    if not ENABLE_SUPPLY_CHAIN_RSS:
        return []
    specs = get_feed_specs("SUPPLY_CHAIN_RSS_FEEDS", DEFAULT_SUPPLY_CHAIN_RSS_FEEDS)
    return fetch_feed_specs(specs, "supply_chain_media", SUPPLY_CHAIN_RSS_MAX_ITEMS_PER_FEED)


def fetch_sec_edgar():
    """Fetch SEC EDGAR via the broad 'Latest Filings' feed across every filer."""
    items = []
    if not ENABLE_SEC_EDGAR:
        return items

    headers = {**REQUEST_HEADERS, "User-Agent": SEC_USER_AGENT}
    url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=&company=&dateb=&owner=include&start=0&count={SEC_MAX_ITEMS_PER_FETCH}&output=atom"

    try:
        response = requests.get(url, headers=headers, timeout=35)
        if response.status_code != 200:
            log.warning(f"SEC EDGAR Daily RSS issue: {response.status_code} {response.text[:160]}")
        else:
            feed = feedparser.parse(response.content)
            kept = 0
            for entry in getattr(feed, "entries", []):
                title = clean_text(entry.get("title", ""))
                link = normalize_url(entry.get("link", ""))
                summary = clean_text(entry.get("summary", entry.get("description", "")))

                if not title or not link:
                    continue

                form_match = re.match(r"^([A-Z0-9-]+)\s+-", title)
                if form_match and form_match.group(1).strip() not in SEC_FORMS:
                    continue

                items.append({
                    "title": title,
                    "url": link,
                    "source": "SEC EDGAR / Daily Feed",
                    "source_type": "sec_filing",
                    "published_at": entry.get("updated", entry.get("published", utc_now().isoformat())),
                    "raw_summary": summary[:2200],
                })
                kept += 1

            log.info(f"sec_edgar: {kept} relevant form filings extracted from broad feed")
    except Exception as exc:
        log.error(f"SEC EDGAR exception: {repr(exc)}")

    return items


def openreview_content_value(content, key):
    if not isinstance(content, dict):
        return ""
    value = content.get(key, "")
    if isinstance(value, dict) and "value" in value:
        value = value.get("value")
    if isinstance(value, list):
        return clean_text(", ".join(str(part) for part in value))
    return clean_text(value)


def openreview_timestamp_to_iso(value):
    try:
        value = float(value)
        if value > 10_000_000_000:
            value = value / 1000
        return dt.datetime.fromtimestamp(value, tz=dt.timezone.utc).isoformat()
    except Exception:
        return utc_now().isoformat()


def parse_openreview_notes(data):
    if isinstance(data, dict):
        notes = data.get("notes") or data.get("results") or data.get("items") or []
        return notes if isinstance(notes, list) else []
    return []


def fetch_openreview_invitation(invitation):
    url = f"{OPENREVIEW_API_BASE}/notes"
    params = {
        "invitation": invitation,
        "limit": OPENREVIEW_MAX_NOTES_PER_INVITATION,
        "sort": "tmdate:desc",
    }
    response = requests.get(url, params=params, headers=REQUEST_HEADERS, timeout=45)
    if response.status_code != 200:
        log.warning(f"OpenReview issue for {invitation}: {response.status_code} {response.text[:180]}")
        return []
    return parse_openreview_notes(response.json())


def fetch_openreview():
    items = []
    if not ENABLE_OPENREVIEW:
        return items

    for invitation in OPENREVIEW_INVITATIONS:
        try:
            notes = fetch_openreview_invitation(invitation)
            kept = 0
            for note in notes[:OPENREVIEW_MAX_NOTES_PER_INVITATION]:
                content = note.get("content", {}) if isinstance(note, dict) else {}
                title = openreview_content_value(content, "title")
                abstract = openreview_content_value(content, "abstract")
                keywords = openreview_content_value(content, "keywords")
                tldr = openreview_content_value(content, "TLDR") or openreview_content_value(content, "tldr")
                note_id = note.get("id") or note.get("forum") or hashlib.sha256(title.encode("utf-8")).hexdigest()[:16]
                if not title:
                    continue
                items.append({
                    "title": title,
                    "url": normalize_url(f"https://openreview.net/forum?id={note_id}"),
                    "source": f"OpenReview / {invitation}",
                    "source_type": "research_openreview",
                    "published_at": openreview_timestamp_to_iso(note.get("tmdate") or note.get("tcdate")),
                    "raw_summary": clean_text(
                        f"OpenReview submission. Invitation: {invitation}. Keywords: {keywords}. TLDR: {tldr}. Abstract: {abstract[:1800]}"
                    ),
                })
                kept += 1
            log.info(f"openreview: {kept} notes from {invitation}")
        except Exception as exc:
            log.error(f"OpenReview exception for {invitation}: {repr(exc)}")
        time.sleep(1)

    return items


# =========================
# Gemini classification
# =========================


def parse_llm_json(text):
    if not text:
        raise ValueError("Empty response text.")

    text = text.strip()
    if text.startswith("```"):
        text = text.replace("```json", "").replace("```", "").strip()

    parsed = None
    try:
        parsed = json.loads(text)
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            parsed = json.loads(text[start:end + 1])

    if not isinstance(parsed, dict):
        raise ValueError("OpenRouter response was not a JSON object.")
    if "results" not in parsed or not isinstance(parsed["results"], list):
        raise ValueError("OpenRouter response did not include a results list.")
    return parsed


def classify_batch(candidates):
    if not OPENROUTER_API_KEY:
        log.error(
            "Missing OPENROUTER_API_KEY. Classification skipped; batch will be requeued "
            "(capped by MAX_ITEM_RETRIES, currently %s)." % MAX_ITEM_RETRIES
        )
        return None

    compact_items = []
    for index, item in enumerate(candidates):
        compact_items.append(
            {
                "index": index,
                "title": item.get("title", "")[:450],
                "source": item.get("source", "")[:120],
                "source_type": item.get("source_type", "")[:80],
                "summary": item.get("raw_summary", "")[:2200],
                "url": item.get("url", "")[:400],
                "published_at": item.get("published_at", "")[:80],
            }
        )

    system_prompt = """
You are a strict AI hardware revolution filter for a monitoring bot.
Goal:
Find only hardware, semiconductor, robotics embodiment, energy, datacenter, or physical infrastructure technologies that could materially improve the AI scaling curve, AI training, AI inference, robotics, or AI deployment.

Relevant ONLY when the item has concrete technical substance about at least one physical/hardware layer:
- AI accelerators: GPU, TPU, NPU, ASIC, wafer-scale engine, inference chip, training chip.
- Memory: HBM, DRAM, SRAM, MRAM/ReRAM, CXL memory, memory bandwidth, memory wall, near-memory or in-memory compute.
- Interconnect: NVLink-like fabrics, Ethernet/InfiniBand hardware, optical interconnect, silicon photonics, CPO, pluggable optics, transceivers, SerDes, switches.
- Advanced packaging: chiplets, 2.5D/3D packaging, CoWoS-like packaging, interposers, glass substrate, TGV, hybrid bonding, RDL.
- Semiconductor manufacturing: lithography, deposition, etch, metrology, advanced nodes, compound semiconductors, wafer capacity, yield, EDA or physical design that directly affects chips.
- Power and thermal: datacenter power delivery, liquid cooling, immersion cooling, rack-scale power, energy efficiency for AI compute.
- Robotics hardware: sensors, actuators, robot hands, embodied AI hardware, embedded compute, edge AI hardware, physical autonomy systems.
- Quantum/neuromorphic/photonic compute only if there is a clear AI compute or AI infrastructure angle.
- SEC-filing and OpenReview items are relevant only if the title/summary clearly identifies a physical hardware, semiconductor, robotics, datacenter, power, cooling, packaging, memory, or interconnect signal.

Positive source-quality signals:
- SEC filing metadata, supply-chain media, or specialist semiconductor/HPC publication.
- Production ramp, shipment, qualification, capacity expansion, standards adoption, or named technical bottleneck.

Reject / mark irrelevant:
- Software-only LLMs, agents, prompts, applications, SaaS, consumer AI products, app launches.
- Generic company earnings, stock moves, crypto, regulation, marketing partnerships, layoffs, funding rounds, or SEC filings without specific hardware technology.
- General academic AI papers with no hardware, robotics embodiment, infrastructure, or physical compute contribution.
- Hype phrases like "AI-powered" unless the item explains the underlying hardware/infrastructure.

Scoring:
0-3 = not AI hardware/infrastructure.
4-6 = some hardware mention but weak, vague, or not clearly AI-revolution relevant.
7-8 = useful AI hardware/infrastructure signal with a clear bottleneck addressed.
9-10 = high-signal breakthrough, production ramp, major technical shift, or ecosystem change likely to matter for AI scaling.

Strict rule:
Set relevant=true only when score >= 7 and the item clearly maps to a hardware/infrastructure bottleneck for AI.

Return only valid JSON using this schema:
{
  "results": [
    {
      "index": 0,
      "relevant": true,
      "score": 8,
      "category": "AI accelerator",
      "hardware_domain": "GPU / ASIC / memory / interconnect / packaging / manufacturing / power cooling / robotics hardware / other",
      "bottleneck_addressed": "compute / memory bandwidth / interconnect / power / thermal / manufacturing capacity / robotics embodiment / other",
      "specific_tech": "Short concrete technology name",
      "why_it_matters": "One or two factual sentences explaining the hardware reason.",
      "ai_revolution_thesis": "One sentence on how this could improve AI capability, cost, latency, scale, or deployment.",
      "telegram_summary": "Short Telegram-ready summary."
    }
  ]
}
"""

    user_prompt = {
        "instruction": "Classify every item. Be strict. Do not rescue software-only news. Only output JSON.",
        "items": compact_items,
    }

    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt.strip()},
            {"role": "user", "content": json.dumps(user_prompt, ensure_ascii=False)}
        ],
        "temperature": 0.05,
        "response_format": {"type": "json_object"}
    }

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json"
    }

    url = "https://openrouter.ai/api/v1/chat/completions"

    for attempt in range(3):
        try:
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=90,
            )

            if response.status_code == 200:
                data = response.json()
                text = data.get("choices", [{}])[0].get("message", {}).get("content", "")

                if DEBUG_LLM:
                    log.debug(f"OpenRouter raw response:\n{text[:2500]}")

                parsed = parse_llm_json(text)
                return parsed.get("results", [])

            if response.status_code in [429, 500, 502, 503, 504]:
                wait_seconds = 10 * (attempt + 1)
                log.warning(f"OpenRouter temporary error {response.status_code}. Retrying in {wait_seconds}s. {response.text[:500]}")
                time.sleep(wait_seconds)
                continue

            log.error(f"OpenRouter error: {response.status_code} {response.text[:1000]}")
            return None

        except Exception as exc:
            wait_seconds = 10 * (attempt + 1)
            log.warning(f"OpenRouter exception: {repr(exc)}. Retrying in {wait_seconds}s.")
            time.sleep(wait_seconds)

    return None


def classify_and_insert(candidates):
    inserted_count = 0
    processed_count = 0
    failed_items = []
    new_alerts = []

    # Convert to list so we can count total batches
    batches = list(chunk_list(candidates, LLM_BATCH_SIZE))
    total_batches = len(batches)

    for i, batch in enumerate(batches, 1):
        log.info(f"Processing batch {i}/{total_batches} with OpenRouter ({len(batch)} items)...")
        
        results = classify_batch(batch)
        if results is None:
            failed_items.extend(batch)
            continue

        result_by_index = {}
        for result in results:
            try:
                index = int(result.get("index"))
            except Exception:
                continue
            if 0 <= index < len(batch):
                result_by_index[index] = result

        for index, original in enumerate(batch):
            result = result_by_index.get(index)

            if result is None:
                mark_url_processed(original, "rejected")
                processed_count += 1
                continue

            score = safe_float(result.get("score", 0))
            relevant = safe_bool(result.get("relevant"))
            processed_count += 1

            if not relevant or score < LLM_MIN_SCORE:
                mark_url_processed(original, "rejected")
                continue

            cap = LLM_TEXT_FIELD_MAX_CHARS
            item = {
                **original,
                "category": clean_text(result.get("category", ""))[:cap],
                "hardware_domain": clean_text(result.get("hardware_domain", ""))[:cap],
                "bottleneck_addressed": clean_text(result.get("bottleneck_addressed", ""))[:cap],
                "specific_tech": clean_text(result.get("specific_tech", ""))[:cap],
                "relevance_score": score,
                "why_it_matters": clean_text(result.get("why_it_matters", ""))[:cap],
                "ai_revolution_thesis": clean_text(result.get("ai_revolution_thesis", ""))[:cap],
                "telegram_summary": clean_text(result.get("telegram_summary", ""))[:cap],
                "raw_llm_json": json.dumps(result, ensure_ascii=False),
            }

            is_alert = score >= ALERT_THRESHOLD

            if insert_item(item, sent_alert=0, digest_sent=0):
                inserted_count += 1
                if is_alert:
                    new_alerts.append(item)

            mark_url_processed(original, "accepted")

        time.sleep(2)

    if new_alerts:
        new_alerts.sort(key=lambda x: safe_float(x.get("relevance_score", 0)), reverse=True)
        sent_urls = send_immediate_alerts(new_alerts)
        if sent_urls:
            conn = connect_db()
            cur = conn.cursor()
            digest_flag = 0 if INCLUDE_ALERTS_IN_DIGEST else 1
            cur.executemany(
                "UPDATE items SET sent_alert = 1, digest_sent = ? WHERE url = ?",
                [(digest_flag, url) for url in sent_urls],
            )
            conn.commit()
        skipped = len(new_alerts) - len(sent_urls)
        if skipped > 0:
            log.info(f"{skipped} alert-tier item(s) not sent as immediate alerts this flush; they remain eligible for the next digest.")

    return {
        "inserted": inserted_count,
        "processed": processed_count,
        "failed_items": failed_items,
    }


# =========================
# Source aggregation
# =========================


def fetch_all_sources() -> list:
    """Run every enabled fetcher and return capped items as a flat list."""
    fetchers = [
        ("arxiv", fetch_arxiv),
        ("hackernews", fetch_hackernews),
        ("techmeme", fetch_techmeme),
        ("specialist_rss", fetch_specialist_rss),
        ("supply_chain_rss", fetch_supply_chain_rss),
        ("sec_edgar", fetch_sec_edgar),
        ("openreview", fetch_openreview),
    ]
    all_items = []
    for name, fetcher in fetchers:
        try:
            items = fetcher()
            log.info(f"fetch_{name}: {len(items)} items")
            all_items.extend(items)
        except Exception as exc:
            log.error(f"fetch_{name} crash: {repr(exc)}")

    if SEND_RAW_FETCH_JSON and all_items:
        send_raw_json_dump(all_items)

    capped = fair_cap_items(all_items, MAX_CANDIDATES_PER_RUN)
    if len(capped) < len(all_items):
        log.info(f"Global cap applied: {len(capped)}/{len(all_items)} items kept this run.")
    return capped


# =========================
# Runtime — two-phase queue pipeline
# =========================


def touch_healthcheck():
    try:
        directory = os.path.dirname(HEALTHCHECK_FILE)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(HEALTHCHECK_FILE, "w") as fh:
            fh.write(utc_now().isoformat())
    except Exception as exc:
        log.warning(f"Could not write healthcheck file {HEALTHCHECK_FILE}: {repr(exc)}")


def fetch_and_enqueue():
    ts = utc_now().strftime("%H:%M:%S")
    log.info(f"[{ts}] Fetching all sources...")
    raw_items = fetch_all_sources()

    if SEND_RAW_FETCH_REPORT and raw_items:
        grouped_titles = defaultdict(list)
        for item in raw_items:
            source = item.get("source", "Unknown Source")
            title = clean_text(item.get("title", "No Title"))
            grouped_titles[source].append(title)

        report_lines = [f"Raw Fetch Report - {utc_now().strftime('%Y-%m-%d %H:%M:%S UTC')}"]
        report_lines.append(f"Total items fetched (pre-deduping): {len(raw_items)}")
        report_lines.append("=" * 60)

        for source, titles in sorted(grouped_titles.items()):
            report_lines.append(f"\n[{source}] ({len(titles)} items):")
            for title in titles:
                report_lines.append(f"  - {title}")

        report_content = "\n".join(report_lines)
        filename = f"raw_fetch_{utc_now().strftime('%Y%m%d_%H%M%S')}.txt"
        send_telegram_document(filename, report_content)

    new_items = []
    for item in raw_items:
        url = normalize_url(item.get("url", ""))
        if not url:
            continue
        item["url"] = url
        fp = item_fingerprint(item)
        item["content_fingerprint"] = fp

        if url in SEEN_URLS_CACHE or fp in SEEN_FINGERPRINTS_CACHE:
            continue
        if processed_url_exists(url) or processed_fingerprint_exists(fp):
            continue
        new_items.append((url, fp, item))

    added = 0
    dropped = 0
    with _queue_lock:
        for url, fp, item in new_items:
            if url in SEEN_URLS_CACHE or fp in SEEN_FINGERPRINTS_CACHE:
                continue
            if len(CANDIDATE_QUEUE) >= CANDIDATE_QUEUE_MAX:
                dropped += 1
                continue

            SEEN_URLS_CACHE[url] = True
            if fp:
                SEEN_FINGERPRINTS_CACHE[fp] = True
            CANDIDATE_QUEUE.append(item)
            added += 1

        if len(SEEN_URLS_CACHE) > 20_000:
            oldest_urls = list(SEEN_URLS_CACHE.keys())[:10000]
            for old_url in oldest_urls:
                del SEEN_URLS_CACHE[old_url]

            oldest_fps = list(SEEN_FINGERPRINTS_CACHE.keys())[:10000]
            for old_fp in oldest_fps:
                del SEEN_FINGERPRINTS_CACHE[old_fp]

            for q_item in CANDIDATE_QUEUE:
                q_url = q_item.get("url", "")
                q_fp = q_item.get("content_fingerprint", "")
                if q_url:
                    SEEN_URLS_CACHE[q_url] = True
                if q_fp:
                    SEEN_FINGERPRINTS_CACHE[q_fp] = True

        depth = len(CANDIDATE_QUEUE)

    if dropped:
        log.warning(f"[{ts}] Queue full. Dropped {dropped} overflow item(s). Increase CANDIDATE_QUEUE_MAX if needed.")
    log.info(f"[{ts}] +{added} new items queued. Depth: {depth}")
    touch_healthcheck()


def requeue_failed_items(items, max_retries=None):
    if not items:
        return 0
    max_retries = MAX_ITEM_RETRIES if max_retries is None else max_retries

    added = 0
    permanently_dropped = []
    with _queue_lock:
        for item in items:
            item["_retry_count"] = item.get("_retry_count", 0) + 1
            if item["_retry_count"] > max_retries:
                permanently_dropped.append(item)
                continue
            if len(CANDIDATE_QUEUE) >= CANDIDATE_QUEUE_MAX:
                break
            CANDIDATE_QUEUE.append(item)
            added += 1

    for item in permanently_dropped:
        mark_url_processed(item, "failed_permanent")
    if permanently_dropped:
        log.error(f"Dropped {len(permanently_dropped)} item(s) after exceeding {max_retries} retries.")

    return added


def flush_queue():
    global _last_flush_time

    with _queue_lock:
        if not CANDIDATE_QUEUE:
            _last_flush_time = utc_now()
            return
        process_count = min(len(CANDIDATE_QUEUE), MAX_CANDIDATES_PER_FLUSH)
        batch = CANDIDATE_QUEUE[:process_count]
        del CANDIDATE_QUEUE[:process_count]
        remaining = len(CANDIDATE_QUEUE)

    ts = utc_now().strftime("%H:%M:%S")
    log.info(f"[{ts}] Flushing {len(batch)} item(s) through OpenRouter. Remaining queued: {remaining}")

    result = classify_and_insert(batch)

    failed_items = result.get("failed_items", [])
    if failed_items:
        requeued = requeue_failed_items(failed_items)
        log.warning(f"Requeued {requeued}/{len(failed_items)} failed item(s) for later retry.")

    inserted = result.get("inserted", 0)
    processed = result.get("processed", 0)
    log.info(f"OpenRouter processed {processed} item(s); inserted {inserted} relevant item(s).")

    _last_flush_time = utc_now()
    touch_healthcheck()


def drain_queue(max_flushes=None):
    if max_flushes is None:
        max_flushes = (CANDIDATE_QUEUE_MAX // max(1, MAX_CANDIDATES_PER_FLUSH)) + 2
    flush_count = 0
    while True:
        with _queue_lock:
            if not CANDIDATE_QUEUE:
                break
        if flush_count >= max_flushes:
            log.warning(f"[{utc_now().strftime('%H:%M:%S')}] Max flush cycles reached this call. Remaining items wait for the next flush check.")
            break
        flush_queue()
        flush_count += 1


def run_pipeline():
    fetch_and_enqueue()
    drain_queue()


def maybe_flush():
    with _queue_lock:
        depth = len(CANDIDATE_QUEUE)
        stale = depth > 0 and (utc_now() - _last_flush_time) >= dt.timedelta(minutes=FORCE_FLUSH_MINUTES)

    if depth == 0:
        return
    if depth >= QUEUE_FLUSH_LIMIT or stale:
        reason = "queue depth" if depth >= QUEUE_FLUSH_LIMIT else "force-flush timeout"
        log.info(f"maybe_flush triggered ({reason}); depth={depth}")
        drain_queue()


def log_flush_heartbeat():
    with _queue_lock:
        depth = len(CANDIDATE_QUEUE)
        last_flush = _last_flush_time.strftime("%Y-%m-%d %H:%M:%S UTC") if _last_flush_time else "Never"
    
    log.info(
        f"⏱️ [FLUSH HEARTBEAT] 1-second background check is active and healthy. "
        f"Current queue depth: {depth}/{CANDIDATE_QUEUE_MAX} | Last flush: {last_flush}"
    )


def print_config():
    log.info("Configuration:")
    log.info(f"DB_PATH: {DB_PATH}")
    log.info(f"OPENROUTER_MODEL: {OPENROUTER_MODEL}")
    log.info(f"FETCH_MINUTES: {FETCH_MINUTES} (fetch and flush now run on independent timers)")
    log.info(f"MAX_CANDIDATES_PER_RUN: {MAX_CANDIDATES_PER_RUN}")
    log.info(f"MAX_CANDIDATES_PER_FLUSH: {MAX_CANDIDATES_PER_FLUSH}")
    log.info(f"CANDIDATE_QUEUE_MAX: {CANDIDATE_QUEUE_MAX}")
    log.info(f"QUEUE_FLUSH_LIMIT: {QUEUE_FLUSH_LIMIT}")
    log.info(f"CHECK_QUEUE_SECONDS: {CHECK_QUEUE_SECONDS}")
    log.info(f"FORCE_FLUSH_MINUTES: {FORCE_FLUSH_MINUTES}")
    log.info(f"MAX_ITEM_RETRIES: {MAX_ITEM_RETRIES}")
    log.info(f"DIGEST_MINUTES: {DIGEST_MINUTES}")
    log.info(f"LLM_BATCH_SIZE: {LLM_BATCH_SIZE}")
    log.info(f"LLM_MIN_SCORE: {LLM_MIN_SCORE}")
    log.info(f"ALERT_THRESHOLD: {ALERT_THRESHOLD}")
    log.info(f"MAX_ALERTS_PER_FLUSH: {MAX_ALERTS_PER_FLUSH}")
    log.info(f"DIGEST_MAX_ITEMS: {DIGEST_MAX_ITEMS}")
    log.info(f"ENABLE_ARXIV: {ENABLE_ARXIV} categories: {ARXIV_CATEGORIES} per category: {ARXIV_RESULTS_PER_CATEGORY} workers: {ARXIV_FETCH_WORKERS}")
    log.info(f"ENABLE_HACKERNEWS: {ENABLE_HACKERNEWS} HN_NEW_STORIES_PER_RUN: {HN_NEW_STORIES_PER_RUN} workers: {HN_FETCH_WORKERS} high_water_mark: {get_hn_high_water_mark()}")
    log.info(f"ENABLE_TECHMEME: {ENABLE_TECHMEME} TECHMEME_MAX_ITEMS: {TECHMEME_MAX_ITEMS}")
    log.info(f"ENABLE_SPECIALIST_RSS: {ENABLE_SPECIALIST_RSS} feeds: {len(get_feed_specs('SPECIALIST_RSS_FEEDS', DEFAULT_SPECIALIST_RSS_FEEDS))} per feed: {SPECIALIST_RSS_MAX_ITEMS_PER_FEED}")
    log.info(f"ENABLE_SUPPLY_CHAIN_RSS: {ENABLE_SUPPLY_CHAIN_RSS} feeds: {len(get_feed_specs('SUPPLY_CHAIN_RSS_FEEDS', DEFAULT_SUPPLY_CHAIN_RSS_FEEDS))} per feed: {SUPPLY_CHAIN_RSS_MAX_ITEMS_PER_FEED}")
    log.info(f"ENABLE_SEC_EDGAR: {ENABLE_SEC_EDGAR} forms: {SEC_FORMS} broad_feed_max: {SEC_MAX_ITEMS_PER_FETCH}")
    log.info(f"ENABLE_OPENREVIEW: {ENABLE_OPENREVIEW} invitations: {len(OPENREVIEW_INVITATIONS)} per invitation: {OPENREVIEW_MAX_NOTES_PER_INVITATION}")
    log.info(f"Telegram token found: {bool(TELEGRAM_BOT_TOKEN)}")
    log.info(f"Telegram chat found: {bool(TELEGRAM_CHAT_ID)}")
    log.info(f"OpenRouter key found: {bool(OPENROUTER_API_KEY)}")


def main():
    init_db()
    print_config()

    if SEND_STARTUP_MESSAGE:
        send_telegram(
            f"✅ AI hardware trend analyzer started.\n"
            f"Fetch every {FETCH_MINUTES} min, flush check every {CHECK_QUEUE_SECONDS}s "
            f"(queue ≥{QUEUE_FLUSH_LIMIT} or {FORCE_FLUSH_MINUTES} min stale triggers a flush)."
        )

    run_pipeline()
    send_digest()

    scheduler = BackgroundScheduler(timezone="UTC")

    scheduler.add_job(
        fetch_and_enqueue,
        "interval",
        minutes=FETCH_MINUTES,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        maybe_flush,
        "interval",
        seconds=CHECK_QUEUE_SECONDS,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        send_digest,
        "interval",
        minutes=DIGEST_MINUTES,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        log_flush_heartbeat,
        "interval",
        hours=1,
        max_instances=1,
        coalesce=True,
    )

    class SilentExecutorsFilter(logging.Filter):
        def filter(self, record):
            msg = record.getMessage()
            if "maybe_flush" in msg and ("Running job" in msg or "executed successfully" in msg):
                return False
            return True

    logging.root.addFilter(SilentExecutorsFilter())
    for handler in logging.root.handlers:
        handler.addFilter(SilentExecutorsFilter())

    for name in ["apscheduler", "apscheduler.scheduler", "apscheduler.executors.default", "apscheduler.executors.pool"]:
        exec_log = logging.getLogger(name)
        exec_log.setLevel(logging.WARNING)
        exec_log.propagate = False
        exec_log.addFilter(SilentExecutorsFilter())

    scheduler.start()
    log.info("Scheduler active. Main thread blocking.")

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        log.info("Scheduler stopped.")

if __name__ == "__main__":
    main()
