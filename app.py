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

from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlsplit

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
    "cs.AR", "cs.DC", "cs.ET", "cs.NI", "cs.RO", "cs.AI", "cs.LG",
    "eess.SY", "eess.SP", "eess.IV",
    "physics.optics", "physics.app-ph", "cond-mat.mtrl-sci", "physics.flu-dyn",
]
ARXIV_CATEGORIES = get_csv("ARXIV_CATEGORIES", DEFAULT_ARXIV_CATEGORIES)
ENABLE_ARXIV_FRONTIER_SEARCH = get_bool("ENABLE_ARXIV_FRONTIER_SEARCH", True)
ARXIV_FRONTIER_MAX_RESULTS_PER_TOPIC = get_int("ARXIV_FRONTIER_MAX_RESULTS_PER_TOPIC", 8)

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
    ("TrendForce News", "https://www.trendforce.com/news/feed"),
    ("Data Center Dynamics", "https://www.datacenterdynamics.com/en/rss/"),
    ("Chips and Cheese", "https://chipsandcheese.com/feed/"),
]

# Step 1b: Targeted Frontier Hardware Topic RSS feeds (6 core physical infrastructure pillars)
ENABLE_FRONTIER_TOPIC_RSS = get_bool("ENABLE_FRONTIER_TOPIC_RSS", True)
FRONTIER_TOPIC_RSS_MAX_ITEMS_PER_FEED = get_int("FRONTIER_TOPIC_RSS_MAX_ITEMS_PER_FEED", 8)
DEFAULT_FRONTIER_TOPIC_RSS_FEEDS = [
    (
        "Frontier / Thermal & Immersion Cooling",
        "https://news.google.com/rss/search?q=(%22direct-to-chip%22+OR+%22microchannel+cold+plate%22+OR+%22immersion+cooling%22+OR+%22coolant+distribution+unit%22+OR+%22microfluidic+cooling%22+OR+%22dielectric+fluid%22)+AND+(AI+OR+datacenter+OR+GPU+OR+semiconductor)&hl=en-US&gl=US&ceid=US:en",
    ),
    (
        "Frontier / Glass Substrates & FOPLP",
        "https://news.google.com/rss/search?q=(%22glass+substrate%22+OR+%22through-glass+via%22+OR+%22TGV%22+OR+%22FOPLP%22+OR+%22panel-level+packaging%22+OR+%22glass+interposer%22+OR+%22ABF+substrate%22)&hl=en-US&gl=US&ceid=US:en",
    ),
    (
        "Frontier / HBM4, CXL 3 & Near-Memory Compute",
        "https://news.google.com/rss/search?q=(%22HBM4%22+OR+%22HBM4e%22+OR+%22CXL+3.0%22+OR+%22CXL+3.1%22+OR+%22memory+pooling%22+OR+%22processing-in-memory%22+OR+%22near-memory+compute%22)&hl=en-US&gl=US&ceid=US:en",
    ),
    (
        "Frontier / Rack HV Power, VPD & GaN/SiC PoL",
        "https://news.google.com/rss/search?q=(%22400V+DC%22+OR+%22800V+DC%22+OR+%22vertical+power+delivery%22+OR+%22integrated+voltage+regulator%22+OR+%22point-of-load%22)+AND+(datacenter+OR+rack+OR+GaN+OR+SiC+OR+AI)&hl=en-US&gl=US&ceid=US:en",
    ),
    (
        "Frontier / UWBG & CVD Diamond Thermal",
        "https://news.google.com/rss/search?q=(%22CVD+diamond%22+OR+%22diamond+heat+spreader%22+OR+%22diamond+substrate%22+OR+%22gallium+oxide%22+OR+%22Ga2O3%22+OR+%22ultra-wide+bandgap%22)+AND+(semiconductor+OR+wafer+OR+thermal+OR+power)&hl=en-US&gl=US&ceid=US:en",
    ),
    (
        "Frontier / All-Optical Switching & CPO",
        "https://news.google.com/rss/search?q=(%22optical+circuit+switch%22+OR+%22co-packaged+optics%22+OR+%2251.2T%22+OR+%22102.4T%22+OR+%22MEMS+mirror%22+OR+%22optical+engine%22)+AND+(switch+OR+ASIC+OR+AI+OR+cluster)&hl=en-US&gl=US&ceid=US:en",
    ),
]

# Step 2: Asia/supply-chain layer
ENABLE_SUPPLY_CHAIN_RSS = get_bool("ENABLE_SUPPLY_CHAIN_RSS", True)
SUPPLY_CHAIN_RSS_MAX_ITEMS_PER_FEED = get_int("SUPPLY_CHAIN_RSS_MAX_ITEMS_PER_FEED", 12)
DEFAULT_SUPPLY_CHAIN_RSS_FEEDS = [
    ("DIGITIMES Asia", "https://www.digitimes.com/rss/daily.xml"),
    ("TrendForce / Semiconductors", "https://www.trendforce.com/feed/Semiconductors.html"),
]

ENABLE_HTML_SCRAPER = get_bool("ENABLE_HTML_SCRAPER", True)
HTML_SCRAPER_MAX_ITEMS_PER_SITE = get_int("HTML_SCRAPER_MAX_ITEMS_PER_SITE", 12)

DEFAULT_HTML_SCRAPER_TARGETS = [
    ("TrendForce News", "https://www.trendforce.com/news/;;/\\d{4}/\\d{2}/\\d{2}/"),
]

# Step 3: SEC EDGAR polling — market-wide full-text search (EFTS)
ENABLE_SEC_EDGAR = get_bool("ENABLE_SEC_EDGAR", True)
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "AI-Hardware-Trend-Bot/2.3 contact@example.com").strip()
SEC_FORMS = get_csv("SEC_FORMS", ["8-K", "10-Q", "10-K", "S-1", "F-1", "20-F", "6-K"])
SEC_LOOKBACK_DAYS = get_int("SEC_LOOKBACK_DAYS", 3)
SEC_MAX_ITEMS_PER_FETCH = get_int("SEC_MAX_ITEMS_PER_FETCH", 40)
ENABLE_SEC_FRONTIER_EXPANSION = get_bool("ENABLE_SEC_FRONTIER_EXPANSION", True)
DEFAULT_SEC_EFTS_QUERY = (
    '"HBM" OR "HBM4" OR "HBM4e" OR "high bandwidth memory" OR "CoWoS" '
    'OR "glass substrate" OR "through-glass via" OR "FOPLP" OR "panel-level packaging" '
    'OR "Compute Express Link" OR "processing-in-memory" OR "near-memory" '
    'OR "silicon photonics" OR "co-packaged optics" OR "optical circuit switch" '
    'OR "liquid cooling" OR "immersion cooling" OR "cold plate" OR "coolant distribution unit" '
    'OR "microfluidic cooling" OR "vertical power delivery" OR "integrated voltage regulator" '
    'OR "CVD diamond" OR "gallium oxide" OR "AI accelerator" OR "GPU cluster"'
)
SEC_EFTS_QUERY = os.getenv("SEC_EFTS_QUERY", DEFAULT_SEC_EFTS_QUERY).strip()

# Topic-scoped SEC EFTS queries executed alongside SEC_EFTS_QUERY when ENABLE_SEC_FRONTIER_EXPANSION=true
FRONTIER_SEC_EFTS_QUERIES = [
    (
        "Advanced Thermal Management & Liquid/Immersion Cooling Systems",
        '"direct-to-chip" OR "cold plate" OR "microchannel" OR "immersion cooling" OR "dielectric fluid" OR "coolant distribution unit" OR "microfluidic cooling"',
    ),
    (
        "Glass Substrates & Panel-Level Packaging (PLP)",
        '"glass substrate" OR "through-glass via" OR "panel-level packaging" OR "FOPLP" OR "glass interposer" OR "ABF substrate"',
    ),
    (
        "Next-Generation Memory Hierarchies & Near-Memory Compute",
        '"HBM4" OR "HBM4e" OR "Compute Express Link" OR "CXL 3.0" OR "CXL 3.1" OR "memory pooling" OR "processing-in-memory" OR "near-memory compute"',
    ),
    (
        "Rack-Scale High-Voltage Power Delivery & Point-of-Load (PoL) Conversion",
        '"400V DC" OR "800V DC" OR "rack busbar" OR "vertical power delivery" OR "integrated voltage regulator" OR "point-of-load"',
    ),
    (
        "Ultra-Wide Bandgap Materials & Synthetic Diamond Thermal Spreaders",
        '"CVD diamond" OR "diamond substrate" OR "diamond heat spreader" OR "gallium oxide" OR "Ga2O3" OR "aluminum nitride" OR "ultra-wide bandgap"',
    ),
    (
        "All-Optical Circuit Switching (OCS) & Co-Packaged Switch Fabrics",
        '"optical circuit switch" OR "MEMS mirror" OR "co-packaged optics" OR "51.2T" OR "102.4T" OR "optical engine"',
    ),
]

# =========================
# 6 Priority Frontier Hardware Domains & Sub-Topic Taxonomy
# =========================
FRONTIER_TOPICS = [
    {
        "id": "thermal_liquid_immersion",
        "title": "Advanced Thermal Management & Liquid/Immersion Cooling Systems",
        "category": "Thermal & Cooling",
        "hardware_domain": "power cooling",
        "bottleneck_addressed": "thermal",
        "why_it_complies": (
            "Compute accelerator TDPs have exceeded 1,000 W per socket and are moving toward 2,000 W+, "
            "rendering air cooling physically unviable. Heat dissipation at the cold plate and facility "
            "loop is now a primary governor of cluster density."
        ),
        "arxiv_query": (
            '("microchannel" OR "cold plate" OR "immersion cooling" OR "dielectric fluid" '
            'OR "coolant distribution" OR "microfluidic cooling" OR "two-phase cooling") '
            'AND (cat:physics.flu-dyn OR cat:physics.app-ph OR cat:cs.AR OR cat:cs.DC OR cat:eess.SY)'
        ),
        "subtopics": [
            (
                "Direct-to-Chip (DLC) microchannel cold plates & manifold hydraulics",
                re.compile(
                    r"\b(direct-to-chip|direct\s+liquid\s+cooling|\bDLC\b.*?(?:cool|cold\s*plate|rack)|microchannel\s+cold\s*plate|cold\s*plate|manifold\s+hydraulics|micro-?channel\s+heat\s+sink)\b",
                    re.IGNORECASE,
                ),
            ),
            (
                "Single-phase & two-phase liquid immersion cooling (synthetic dielectric fluids)",
                re.compile(
                    r"\b(single-phase\s+immersion|two-phase\s+(?:liquid\s+)?immersion|immersion\s+cooling|dielectric\s+fluid|fluorochemical\s+coolant|boiling\s+heat\s+transfer.*?dielectric)\b",
                    re.IGNORECASE,
                ),
            ),
            (
                "Liquid-to-Liquid & Liquid-to-Air Coolant Distribution Units (CDUs)",
                re.compile(
                    r"\b(coolant\s+distribution\s+unit|\bCDU\b.*?(?:liquid|coolant|rack|kW|MW|heat\s+exchanger)|liquid-to-liquid|liquid-to-air\s+CDU|facility\s+water\s+loop)\b",
                    re.IGNORECASE,
                ),
            ),
            (
                "In-silicon microfluidic cooling channels etched into active compute dies",
                re.compile(
                    r"\b(in-silicon\s+microfluidic|microfluidic\s+cooling|backside\s+microfluidic|embedded\s+microchannel\s+cooling|intradie\s+cooling|direct-on-die\s+microfluidic|silicon\s+microchannel)\b",
                    re.IGNORECASE,
                ),
            ),
        ],
    },
    {
        "id": "glass_substrates_plp",
        "title": "Glass Substrates & Panel-Level Packaging (PLP)",
        "category": "Advanced Packaging & Substrates",
        "hardware_domain": "packaging",
        "bottleneck_addressed": "manufacturing capacity",
        "why_it_complies": (
            "As multi-die reticle sizes expand (dual compute dies surrounded by 8-12 HBM stacks), "
            "standard organic Ajinomoto Build-up Film (ABF) substrates suffer from thermal warpage, "
            "trace density limits, and electrical parasitic losses. Glass core substrates represent "
            "the next physical substrate transition."
        ),
        "arxiv_query": (
            '("glass substrate" OR "through-glass via" OR "TGV" OR "panel-level packaging" '
            'OR "FOPLP" OR "glass interposer" OR "ABF substrate") '
            'AND (cat:cond-mat.mtrl-sci OR cat:physics.app-ph OR cat:cs.AR OR cat:cs.ET)'
        ),
        "subtopics": [
            (
                "Through-Glass Vias (TGV) with ultra-high aspect ratios & sub-10 um pitch",
                re.compile(
                    r"\b(through-glass\s+via|\bTGV\b.*?(?:glass|via|substrate|interposer|pitch|aspect\s+ratio|laser)|sub-10\s*(?:µm|um|micron)\s+pitch|glass\s+core\s+substrate|glass\s+substrate)\b",
                    re.IGNORECASE,
                ),
            ),
            (
                "Fan-Out Panel-Level Packaging (FOPLP) & rectangular panel economics",
                re.compile(
                    r"\b(fan-out\s+panel-level\s+packaging|\bFOPLP\b|panel-level\s+packaging|\bPLP\b.*?(?:wafer|packaging|substrate|panel|CoWoS)|rectangular\s+panel\s+packaging)\b",
                    re.IGNORECASE,
                ),
            ),
            (
                "Coefficient of Thermal Expansion (CTE) matching & ABF warpage mitigation",
                re.compile(
                    r"\b(coefficient\s+of\s+thermal\s+expansion|\bCTE\b\s+mismatch|\bCTE\b\s+matching|glass\s+interposer|\bABF\b\s+(?:substrate|warpage)|substrate\s+warpage)\b",
                    re.IGNORECASE,
                ),
            ),
        ],
    },
    {
        "id": "nextgen_memory_near_memory_compute",
        "title": "Next-Generation Memory Hierarchies & Near-Memory Compute",
        "category": "Memory & Near-Memory Compute",
        "hardware_domain": "memory",
        "bottleneck_addressed": "memory bandwidth",
        "why_it_complies": (
            "Frontier LLM inference is fundamentally memory-bandwidth bound (FLOP-to-byte imbalances). "
            "The transition to custom base dies and memory-pooling interconnects is shifting memory "
            "from a passive commodity to active compute silicon."
        ),
        "arxiv_query": (
            '("HBM4" OR "HBM4e" OR "Compute Express Link" OR "CXL 3" OR "memory pooling" '
            'OR "processing-in-memory" OR "near-memory compute" OR "near-memory computing") '
            'AND (cat:cs.AR OR cat:cs.DC OR cat:cs.ET OR cat:eess.SY)'
        ),
        "subtopics": [
            (
                "HBM4 & HBM4e logic base dies (TSMC N4/N3) & 2,048-bit wide interfaces",
                re.compile(
                    r"\b(HBM4e?|2,?048-bit\s+(?:memory|interface|HBM)|custom\s+(?:HBM\s+)?base\s+die|logic\s+base\s+die|hybrid\s+bonding.*?HBM)\b",
                    re.IGNORECASE,
                ),
            ),
            (
                "Compute Express Link (CXL 3.0 / 3.1) memory pooling, fabric switches & disaggregation",
                re.compile(
                    r"\b(CXL\s*3(?:\.[01])?|compute\s+express\s+link|CXL\s+(?:memory\s+pooling|fabric\s+switch|switch|disaggregation|expander)|memory\s+pooling\s+fabric|cache-coherent\s+memory\s+disaggregation)\b",
                    re.IGNORECASE,
                ),
            ),
            (
                "Processing-in-Memory (PIM) & Near-Memory Compute in DRAM banks/buffer dies",
                re.compile(
                    r"\b(processing-in-memory|\bPIM\b.*?(?:DRAM|HBM|memory|bank|buffer\s+die)|near-memory\s+comput(?:e|ing)|in-DRAM\s+compute|HBM-PIM|buffer-die\s+compute)\b",
                    re.IGNORECASE,
                ),
            ),
        ],
    },
    {
        "id": "rack_hv_power_delivery_pol",
        "title": "Rack-Scale High-Voltage Power Delivery & Point-of-Load (PoL) Conversion",
        "category": "Rack Power & PoL Conversion",
        "hardware_domain": "power cooling",
        "bottleneck_addressed": "power",
        "why_it_complies": (
            "Delivering thousands of amperes at sub-1.0V to silicon packages creates unsustainable "
            "resistive (I^2R) heat losses across traditional 12V and 48V copper busbars. Power delivery "
            "network (PDN) design inside the rack is as critical as power delivery to the building."
        ),
        "arxiv_query": (
            '("vertical power delivery" OR "integrated voltage regulator" OR "point-of-load" '
            'OR "400V" OR "800V" OR "backside power delivery" OR "GaN converter") '
            'AND (cat:eess.SY OR cat:physics.app-ph OR cat:cs.AR OR cat:cs.ET)'
        ),
        "subtopics": [
            (
                "400V / 800V DC high-voltage server rack busbar architectures",
                re.compile(
                    r"\b((?:\+/-\s*)?400\s*V(?:\s*DC)?|(?:\+/-\s*)?800\s*V(?:\s*DC)?|high-voltage\s+(?:DC\s+)?rack\s+busbar|rack\s+busbar\s+architecture|ORv3\s+HPR|high\s+power\s+rack\s+busbar)\b",
                    re.IGNORECASE,
                ),
            ),
            (
                "Vertical Power Delivery (VPD) & on-package Integrated Voltage Regulators (IVR)",
                re.compile(
                    r"\b(vertical\s+power\s+delivery|\bVPD\b.*?(?:power|regulator|package|die|substrate)|integrated\s+voltage\s+regulator|\bIVR\b.*?(?:on-package|on-die|power|buck)|backside\s+power\s+delivery|\bBSPDN\b)\b",
                    re.IGNORECASE,
                ),
            ),
            (
                "GaN & SiC point-of-load (PoL) converter stages at multi-MHz switching frequencies",
                re.compile(
                    r"\b(point-of-load|\bPoL\b\s+converter|(?:GaN|gallium\s+nitride|SiC|silicon\s+carbide).*?(?:point-of-load|buck\s+converter|server\s+PSU|power\s+stage|MHz|megahertz)|multi-megahertz\s+switching)\b",
                    re.IGNORECASE,
                ),
            ),
        ],
    },
    {
        "id": "uwbg_materials_cvd_diamond",
        "title": "Ultra-Wide Bandgap Materials & Synthetic Diamond Thermal Spreaders",
        "category": "UWBG & Diamond Materials",
        "hardware_domain": "manufacturing",
        "bottleneck_addressed": "thermal",
        "why_it_complies": (
            "Silicon's natural thermal conductivity (~150 W/m*K) forms a thermal bottleneck at the "
            "junction layer. Advanced materials integrated into the wafer stack allow higher sustained "
            "clock frequencies and current densities without thermal throttling."
        ),
        "arxiv_query": (
            '("CVD diamond" OR "diamond heat spreader" OR "diamond substrate" OR "gallium oxide" '
            'OR "Ga2O3" OR "aluminum nitride" OR "ultra-wide bandgap") '
            'AND (cat:cond-mat.mtrl-sci OR cat:physics.app-ph)'
        ),
        "subtopics": [
            (
                "CVD diamond substrates & heat spreaders (~2,000 W/m*K) bonded directly to silicon",
                re.compile(
                    r"\b(CVD\s+diamond|chemical\s+vapor\s+deposition\s+diamond|diamond\s+(?:heat\s+spreader|substrate|wafer|thermal\s+spreader|interposer)|diamond-on-silicon|synthetic\s+diamond\s+cooling)\b",
                    re.IGNORECASE,
                ),
            ),
            (
                "Gallium Oxide (Ga2O3) & Aluminum Nitride (AlN) ultra-wide bandgap power/thermal electronics",
                re.compile(
                    r"\b(gallium\s+oxide|Ga2O3|beta-Ga2O3|aluminum\s+nitride|aluminium\s+nitride|\bAlN\b\s+(?:substrate|wafer|thin\s+film|interlayer|thermal|power)|ultra-wide\s+bandgap|\bUWBG\b)\b",
                    re.IGNORECASE,
                ),
            ),
        ],
    },
    {
        "id": "all_optical_switching_cpo",
        "title": "All-Optical Circuit Switching (OCS) & Co-Packaged Switch Fabrics",
        "category": "Optical Interconnect & CPO",
        "hardware_domain": "interconnect",
        "bottleneck_addressed": "interconnect",
        "why_it_complies": (
            "Clusters scaling to 100,000+ accelerators spend 15-20% of their power purely on optical "
            "transceivers, electrical SerDes, and retimers. Eliminating electrical-optical-electrical "
            "(E-O-E) conversions in switching fabrics is mandatory to sustain network scaling."
        ),
        "arxiv_query": (
            '("optical circuit switch" OR "MEMS mirror" OR "co-packaged optics" OR "51.2T" '
            'OR "102.4T" OR "optical switch fabric" OR "silicon photonic switch") '
            'AND (cat:physics.optics OR cat:physics.app-ph OR cat:cs.NI OR cat:cs.AR OR cat:cs.DC)'
        ),
        "subtopics": [
            (
                "Direct-drive Optical Circuit Switches (OCS) using MEMS 2D/3D mirror arrays",
                re.compile(
                    r"\b(optical\s+circuit\s+switch(?:ing|es)?|\bOCS\b.*?(?:MEMS|optical|mirror|fabric|cluster|spine)|MEMS\s+(?:2D|3D|mirror\s+array|optical\s+switch)|all-optical\s+switch(?:ing)?|E-O-E\s+conversion)\b",
                    re.IGNORECASE,
                ),
            ),
            (
                "51.2T and 102.4T switch ASICs co-packaged with optical engines (CPO)",
                re.compile(
                    r"\b(co-packaged\s+optics|\bCPO\b.*?(?:switch|ASIC|optical\s+engine|51\.2T|102\.4T|silicon\s+photonics)|(?:51\.2\s*T|102\.4\s*T)\s*(?:bps)?\s*(?:switch|ASIC|CPO|Ethernet)|optical\s+engine\s+co-packaged)\b",
                    re.IGNORECASE,
                ),
            ),
        ],
    },
]

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


def match_frontier_topics(item):
    """Deterministically match an item against the 6 priority frontier hardware domains and sub-topics."""
    haystack = " ".join(
        [
            clean_text(item.get("title", "")),
            clean_text(item.get("raw_summary", "")),
            clean_text(item.get("category", "")),
            clean_text(item.get("source", "")),
        ]
    )
    if not haystack:
        return []

    matches = []
    for topic in FRONTIER_TOPICS:
        matched_subs = []
        for sub_name, pattern in topic["subtopics"]:
            if pattern.search(haystack):
                matched_subs.append(sub_name)
        if matched_subs:
            matches.append(
                {
                    "id": topic["id"],
                    "title": topic["title"],
                    "category": topic["category"],
                    "hardware_domain": topic["hardware_domain"],
                    "bottleneck_addressed": topic["bottleneck_addressed"],
                    "why_it_complies": topic["why_it_complies"],
                    "subtopics": matched_subs,
                }
            )
    return matches


def enrich_item_with_frontier_topics(item):
    """Annotate candidate item in-place with matched frontier topics and sub-topics prior to queueing/LLM."""
    matches = match_frontier_topics(item)
    if not matches:
        return item

    topic_titles = [m["title"] for m in matches]
    subtopic_names = []
    for m in matches:
        for s in m["subtopics"]:
            if s not in subtopic_names:
                subtopic_names.append(s)

    item["frontier_topic"] = " | ".join(topic_titles)
    item["frontier_subtopics"] = " ; ".join(subtopic_names)
    item["frontier_domain_hint"] = matches[0]["hardware_domain"]
    item["frontier_bottleneck_hint"] = matches[0]["bottleneck_addressed"]
    item["frontier_category_hint"] = matches[0]["category"]

    existing_summary = clean_text(item.get("raw_summary", ""))
    frontier_tag = f"[Frontier Hardware Match: {item['frontier_topic']} -> {item['frontier_subtopics']}]"
    if frontier_tag not in existing_summary:
        item["raw_summary"] = f"{frontier_tag} {existing_summary}"[:2200]
    return item


def fair_cap_items(items, max_items):
    """Apply a global cap without letting one source dominate the run, prioritizing Frontier Topic matches within each source."""
    for item in items:
        enrich_item_with_frontier_topics(item)

    if max_items <= 0 or len(items) <= max_items:
        return items

    grouped_priority = defaultdict(list)
    grouped_standard = defaultdict(list)
    source_order = []
    for item in items:
        source = item.get("source", "unknown")
        if source not in grouped_priority and source not in grouped_standard:
            source_order.append(source)
        if item.get("frontier_topic"):
            grouped_priority[source].append(item)
        else:
            grouped_standard[source].append(item)

    grouped = {
        source: deque(grouped_priority[source] + grouped_standard[source])
        for source in source_order
    }

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
            frontier_topic TEXT,
            frontier_subtopics TEXT,
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
        "frontier_topic": "TEXT",
        "frontier_subtopics": "TEXT",
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
                frontier_topic, frontier_subtopics,
                relevance_score, why_it_matters, ai_revolution_thesis,
                telegram_summary, raw_summary, raw_llm_json,
                sent_alert, digest_sent, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                item.get("frontier_topic", ""),
                item.get("frontier_subtopics", ""),
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
    frontier_topic = html_escape(row["frontier_topic"] if "frontier_topic" in row.keys() else "")
    frontier_subs = html_escape(row["frontier_subtopics"] if "frontier_subtopics" in row.keys() else "")
    if frontier_topic:
        lines.append(f"Frontier Pillar: <b>{frontier_topic}</b>" + (f" ({frontier_subs})" if frontier_subs else ""))
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

    if ENABLE_ARXIV_FRONTIER_SEARCH:
        seen_arxiv_urls = {it.get("url") for it in items if it.get("url")}
        frontier_added = 0
        for topic in FRONTIER_TOPICS:
            t_query = topic.get("arxiv_query", "")
            if not t_query:
                continue
            t_params = {
                "search_query": t_query,
                "start": 0,
                "max_results": ARXIV_FRONTIER_MAX_RESULTS_PER_TOPIC,
                "sortBy": "submittedDate",
                "sortOrder": "descending",
            }
            t_url = "https://export.arxiv.org/api/query?" + urlencode(t_params)
            t_entries = _fetch_arxiv_atom(t_url, max_retries=2, timeout=15)
            for entry in t_entries[:ARXIV_FRONTIER_MAX_RESULTS_PER_TOPIC]:
                parsed_item = _parse_arxiv_entry(entry, f"Frontier: {topic['category']}")
                if not parsed_item["url"] or parsed_item["url"] in seen_arxiv_urls:
                    continue
                seen_arxiv_urls.add(parsed_item["url"])
                parsed_item["frontier_topic"] = topic["title"]
                enrich_item_with_frontier_topics(parsed_item)
                items.append(parsed_item)
                frontier_added += 1
            time.sleep(0.5)
        log.info(f"arXiv frontier topic search: +{frontier_added} targeted preprints across 6 frontier domains")

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


def fetch_standard_rss(
    feed_url, source_label=None, source_type="rss", max_items=25
):
  """Fetch an RSS/Atom feed with browser headers, conditional GET,

  cooldown tracking, and a Wayback Machine fallback.
  """
  # 1. Early exit: if on cooldown, do nothing and return immediately
  if feed_should_skip_live(feed_url):
    log.info(
        f"RSS feed on cooldown, skipping attempt: {source_label or feed_url}"
    )
    return []

  items = []
  max_items = max_items or 25
  feed = None
  raw_content = None
  live_status = None
  not_modified = False

  # 2. Build request headers with cached ETag / Last-Modified
  request_headers = dict(REQUEST_HEADERS)
  cached_headers = FEED_CACHE_HEADERS.get(feed_url) or {}
  if cached_headers.get("etag"):
    request_headers["If-None-Match"] = cached_headers["etag"]
  if cached_headers.get("last_modified"):
    request_headers["If-Modified-Since"] = cached_headers["last_modified"]

  # 3. Live HTTP request
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
            "last_modified": response.headers.get(
                "Last-Modified", cached_headers.get("last_modified")
            ),
        }
    else:
      log.info(
          f"RSS live fetch failed for {source_label or feed_url}:"
          f" {response.status_code}"
      )
  except requests.exceptions.Timeout:
    log.info(f"RSS timeout for {source_label or feed_url}.")
  except Exception as exc:
    log.info(f"RSS requests issue for {source_label or feed_url}: {repr(exc)}")

  # 4. Handle 304 Not Modified (success, no new content)
  if not_modified:
    feed_record_result(feed_url, success=True)
    return items

  # 5. Parse live response
  if raw_content:
    try:
      feed = feedparser.parse(raw_content)
    except Exception as exc:
      log.info(f"RSS feedparser parsing issue for {feed_url}: {repr(exc)}")
      feed = None

  # 6. Record live attempt result
  live_entries_ok = bool(feed and getattr(feed, "entries", None))
  feed_record_result(feed_url, success=live_entries_ok)

  # 7. Fallback to Wayback Machine only if live fetch produced no entries
  if not live_entries_ok:
    reason = (
        "blocked/challenge page or malformed feed"
        if raw_content is not None
        else f"status {live_status or 'request failed'}"
    )
    log.info(
        f"RSS live path yielded no entries for {source_label or feed_url}"
        f" ({reason}). Trying Wayback Machine..."
    )
    try:
      wb_api_url = f"https://archive.org/wayback/available?url={feed_url}"
      wb_resp = requests.get(wb_api_url, timeout=15)
      if wb_resp.status_code == 200:
        snapshots = wb_resp.json().get("archived_snapshots", {})
        if "closest" in snapshots and snapshots["closest"].get("available"):
          snapshot_url = snapshots["closest"]["url"].replace(
              "http://", "https://"
          )
          log.info(
              f"Wayback snapshot found for {source_label or feed_url}."
              " Fetching..."
          )
          snap_resp = requests.get(
              snapshot_url, headers=REQUEST_HEADERS, timeout=30
          )
          if snap_resp.status_code == 200:
            wb_feed = feedparser.parse(snap_resp.content)
            if getattr(wb_feed, "entries", None):
              feed = wb_feed
    except Exception as wb_exc:
      log.info(
          f"Wayback Machine RSS fallback failed for {feed_url}: {repr(wb_exc)}"
      )

  if not feed or not getattr(feed, "entries", None):
    return items

  # 8. Extract items
  source_name = source_label or (
      clean_text(feed.feed.get("title", feed_url))
      if getattr(feed, "feed", None)
      else feed_url
  )
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
        "published_at": entry.get(
            "published", entry.get("updated", utc_now().isoformat())
        ),
        "raw_summary": summary[:2200],
    })

  return items

def fetch_standard_rss_2(
    feed_url, source_label=None, source_type="custom_rss", max_items=None
):
  """Enriched RSS/Atom fetcher: extracts <content:encoded>, tags/categories,

  author, and handles stubs.
  """
  # 1. Early exit: if on cooldown, do nothing and return immediately
  if feed_should_skip_live(feed_url):
    log.info(
        f"RSS feed on cooldown, skipping attempt: {source_label or feed_url}"
    )
    return []

  items = []
  max_items = max_items or 25
  feed = None
  raw_content = None
  live_status = None
  not_modified = False

  # 2. Build request headers with cached ETag / Last-Modified
  request_headers = dict(REQUEST_HEADERS)
  cached_headers = FEED_CACHE_HEADERS.get(feed_url) or {}
  if cached_headers.get("etag"):
    request_headers["If-None-Match"] = cached_headers["etag"]
  if cached_headers.get("last_modified"):
    request_headers["If-Modified-Since"] = cached_headers["last_modified"]

  # 3. Live HTTP request
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
            "last_modified": response.headers.get(
                "Last-Modified", cached_headers.get("last_modified")
            ),
        }
    else:
      log.info(
          f"RSS live fetch failed for {source_label or feed_url}:"
          f" {response.status_code}"
      )
  except requests.exceptions.Timeout:
    log.info(f"RSS timeout for {source_label or feed_url}.")
  except Exception as exc:
    log.info(f"RSS requests issue for {source_label or feed_url}: {repr(exc)}")

  # 4. Handle 304 Not Modified
  if not_modified:
    feed_record_result(feed_url, success=True)
    return items

  # 5. Parse live response
  if raw_content:
    try:
      feed = feedparser.parse(raw_content)
    except Exception as exc:
      log.info(f"RSS feedparser parsing issue for {feed_url}: {repr(exc)}")
      feed = None

  # 6. Record live attempt result
  live_entries_ok = bool(feed and getattr(feed, "entries", None))
  feed_record_result(feed_url, success=live_entries_ok)

  # 7. Fallback to Wayback Machine only if live fetch produced no entries
  if not live_entries_ok:
    reason = (
        "blocked/challenge page or malformed feed"
        if raw_content is not None
        else f"status {live_status or 'request failed'}"
    )
    log.info(
        f"RSS live path yielded no entries for {source_label or feed_url}"
        f" ({reason}). Trying Wayback Machine..."
    )
    try:
      wb_api_url = f"https://archive.org/wayback/available?url={feed_url}"
      wb_resp = requests.get(wb_api_url, timeout=15)
      if wb_resp.status_code == 200:
        snapshots = wb_resp.json().get("archived_snapshots", {})
        if "closest" in snapshots and snapshots["closest"].get("available"):
          snapshot_url = snapshots["closest"]["url"].replace(
              "http://", "https://"
          )
          log.info(
              f"Wayback snapshot found for {source_label or feed_url}."
              " Fetching..."
          )
          snap_resp = requests.get(
              snapshot_url, headers=REQUEST_HEADERS, timeout=30
          )
          if snap_resp.status_code == 200:
            wb_feed = feedparser.parse(snap_resp.content)
            if getattr(wb_feed, "entries", None):
              feed = wb_feed
    except Exception as wb_exc:
      log.info(
          f"Wayback Machine RSS fallback failed for {feed_url}: {repr(wb_exc)}"
      )

  if not feed or not getattr(feed, "entries", None):
    return items

  # 8. Extract and enrich items
  source_name = source_label or (
      clean_text(feed.feed.get("title", feed_url))
      if getattr(feed, "feed", None)
      else feed_url
  )
  session = requests.Session()

  for entry in getattr(feed, "entries", [])[:max_items]:
    title = clean_text(entry.get("title", ""))
    link = normalize_url(entry.get("link", ""))
    if not title or not link:
      continue

    # Extract body from content:encoded or description
    body = ""
    if "content" in entry and entry.content and isinstance(entry.content, list):
      body = clean_text(
          re.sub(r"<[^>]+>", " ", entry.content[0].get("value", ""))
      )
    if not body:
      body = clean_text(
          re.sub(
              r"<[^>]+>", " ", entry.get("summary", entry.get("description", ""))
          )
      )

    # Extract tags
    tags = []
    if "tags" in entry and entry.tags:
      for t in entry.tags:
        term = clean_text(t.get("term", ""))
        if term and term not in tags:
          tags.append(term)

    # Extract author
    author = clean_text(entry.get("author", entry.get("dc_creator", "")))

    # Fallback OpenGraph scrape if body is very short
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
        "published_at": entry.get(
            "published", entry.get("updated", utc_now().isoformat())
        ),
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
    candidate_urls = split_fallback_urls(feed_url)

    for candidate_url in candidate_urls:
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
        log.error(
            f"{source_type} issue for {label} / {candidate_url}: {repr(exc)}"
        )
  return items


def fetch_feed_specs_2(specs, source_type, max_items_per_feed):
  items = []
  for label, feed_url in specs:
    total_for_label = 0
    candidate_urls = split_fallback_urls(feed_url)

    for candidate_url in candidate_urls:
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
        log.error(
            f"{source_type} issue for {label} / {candidate_url}: {repr(exc)}"
        )
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


def fetch_frontier_topic_rss():
    """Fetch targeted RSS feeds dedicated to the 6 priority frontier hardware domains."""
    if not ENABLE_FRONTIER_TOPIC_RSS:
        return []
    specs = get_feed_specs("FRONTIER_TOPIC_RSS_FEEDS", DEFAULT_FRONTIER_TOPIC_RSS_FEEDS)
    fetched = fetch_feed_specs(specs, "frontier_specialist_rss", FRONTIER_TOPIC_RSS_MAX_ITEMS_PER_FEED)
    for item in fetched:
        enrich_item_with_frontier_topics(item)
    return fetched


def fetch_supply_chain_rss():
    if not ENABLE_SUPPLY_CHAIN_RSS:
        return []
    specs = get_feed_specs("SUPPLY_CHAIN_RSS_FEEDS", DEFAULT_SUPPLY_CHAIN_RSS_FEEDS)
    return fetch_feed_specs(specs, "supply_chain_media", SUPPLY_CHAIN_RSS_MAX_ITEMS_PER_FEED)

def fetch_universal_html_scrapers():
    """Fetches and parses articles from all sites defined in HTML_SCRAPER_TARGETS."""
    if not ENABLE_HTML_SCRAPER:
        return []

    targets = get_feed_specs("HTML_SCRAPER_TARGETS", DEFAULT_HTML_SCRAPER_TARGETS)
    all_scraped_items = []

    ignore_patterns = re.compile(
        r"/(about|contact|privacy|terms|login|signup|register|tag|category|author|cart|search|feed)/?", 
        re.IGNORECASE
    )

    for label, target in targets:
        if ";;" in target:
            base_url, url_pattern = target.split(";;", 1)
            base_url, url_pattern = base_url.strip(), url_pattern.strip()
        else:
            base_url, url_pattern = target.strip(), None

        site_items = []
        seen_urls = set()
        base_netloc = urlsplit(base_url).netloc.lower()

        try:
            # 20-second timeout per site so one slow site never stalls the pipeline
            resp = requests.get(base_url, headers=REQUEST_HEADERS, timeout=20)
            if resp.status_code != 200:
                log.warning(f"HTML scraper: '{label}' returned HTTP status {resp.status_code}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
                    continue

                full_url = urljoin(base_url, href)
                parsed_url = urlsplit(full_url)

                # Keep on the same domain
                if parsed_url.netloc.lower() != base_netloc:
                    continue

                if url_pattern:
                    if not re.search(url_pattern, full_url):
                        continue
                else:
                    if ignore_patterns.search(parsed_url.path) or len(parsed_url.path.strip("/")) < 4:
                        continue

                norm_url = normalize_url(full_url)
                if norm_url in seen_urls:
                    continue

                title = clean_text(a.get_text())
                if len(title) < 15:
                    heading = a.find(["h1", "h2", "h3", "h4"])
                    if heading:
                        title = clean_text(heading.get_text())
                if len(title) < 15:
                    continue

                seen_urls.add(norm_url)

                # Date extraction
                pub_date = utc_now().isoformat()
                date_match = re.search(r"/(20\d{2})[-/](\d{1,2})[-/](\d{1,2})", full_url)
                if date_match:
                    y, m, d = date_match.groups()
                    pub_date = f"{y}-{int(m):02d}-{int(d):02d}T00:00:00+00:00"

                parent = a.find_parent(["article", "li", "div", "section"])
                summary = ""
                category = "Hardware"
                if parent:
                    if not date_match:
                        time_tag = parent.find("time")
                        if time_tag and time_tag.get("datetime"):
                            pub_date = time_tag["datetime"]

                    p_tag = parent.find("p")
                    if p_tag:
                        summary = clean_text(p_tag.get_text())
                    else:
                        summary = clean_text(parent.get_text()).replace(title, "").strip()[:400]

                    cat_tag = parent.find("a", href=re.compile(r"/category/"))
                    if cat_tag:
                        category = clean_text(cat_tag.get_text())

                site_items.append({
                    "title": title,
                    "url": norm_url,
                    "source": f"Web / {label}",
                    "source_type": "scraped_web",
                    "published_at": pub_date,
                    "category": category,
                    "raw_summary": summary[:2200] if summary else title,
                })

                if len(site_items) >= HTML_SCRAPER_MAX_ITEMS_PER_SITE:
                    break

            site_items.sort(key=lambda x: x["published_at"], reverse=True)
            log.info(f"html_scraper: scraped {len(site_items)} items from '{label}'")
            all_scraped_items.extend(site_items)

        except Exception as exc:
            log.error(f"html_scraper error scraping '{label}': {repr(exc)}")

    return all_scraped_items


def fetch_sec_edgar():
    """Search all SEC EDGAR filings via EFTS full-text search across base and 6 Frontier Hardware Topic queries."""
    items = []
    if not ENABLE_SEC_EDGAR or not SEC_EFTS_QUERY:
        return items

    headers = {
        "User-Agent": SEC_USER_AGENT,
        "Accept": "application/json",
    }
    now_utc = utc_now()
    start_dt = (now_utc - dt.timedelta(days=max(1, SEC_LOOKBACK_DAYS))).strftime("%Y-%m-%d")
    end_dt = now_utc.strftime("%Y-%m-%d")
    allowed_forms = {f.strip().upper() for f in SEC_FORMS if f.strip()}
    url = "https://efts.sec.gov/LATEST/search-index"

    query_plan = [("Base AI Hardware Query", SEC_EFTS_QUERY)]
    if ENABLE_SEC_FRONTIER_EXPANSION:
        for topic_label, topic_q in FRONTIER_SEC_EFTS_QUERIES:
            query_plan.append((topic_label, topic_q))

    seen_accessions = set()

    for query_label, query_str in query_plan:
        if len(items) >= SEC_MAX_ITEMS_PER_FETCH:
            break

        params = {
            "q": query_str,
            "startdt": start_dt,
            "enddt": end_dt,
        }
        if allowed_forms:
            params["forms"] = ",".join(sorted(allowed_forms))

        try:
            response = requests.get(url, params=params, headers=headers, timeout=35)
            if response.status_code != 200:
                log.warning(f"SEC EFTS issue ({query_label}): {response.status_code} {response.text[:160]}")
                continue

            data = response.json() or {}
            if "errorType" in data:
                log.warning(f"SEC EFTS query error ({query_label}): {data.get('errorMessage', data.get('errorType'))}")
                continue

            hits = (data.get("hits") or {}).get("hits") or []

            for hit in hits:
                if len(items) >= SEC_MAX_ITEMS_PER_FETCH:
                    break

                src = hit.get("_source") or {}
                adsh = clean_text(src.get("adsh", ""))
                if not adsh or adsh in seen_accessions:
                    continue

                form = clean_text(src.get("form", "")).upper()
                root_forms = [clean_text(rf).upper() for rf in (src.get("root_forms") or [])]
                if allowed_forms and form not in allowed_forms and not any(rf in allowed_forms for rf in root_forms):
                    continue

                ciks = src.get("ciks") or []
                if not ciks:
                    continue
                cik_int = str(int(re.sub(r"\D", "", str(ciks[0])) or 0))
                adsh_nodash = adsh.replace("-", "")
                hit_id = hit.get("_id", "")
                filename = hit_id.split(":", 1)[1] if ":" in hit_id else ""

                if filename:
                    filing_url = normalize_url(f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{adsh_nodash}/{filename}")
                else:
                    filing_url = normalize_url(f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{adsh_nodash}/{adsh}-index.htm")

                display_names = src.get("display_names") or []
                company_label = clean_text(display_names[0]) if display_names else f"CIK {ciks[0]}"
                file_date = clean_text(src.get("file_date", ""))
                period_ending = clean_text(src.get("period_ending", ""))
                file_desc = clean_text(src.get("file_description", "") or src.get("file_type", ""))
                sics = [clean_text(s) for s in (src.get("sics") or []) if s]
                form_items = [clean_text(it) for it in (src.get("items") or []) if it]

                seen_accessions.add(adsh)

                title_desc = f": {file_desc}" if file_desc and file_desc.upper() != form else ""
                title = f"{company_label} - Form {form}{title_desc} ({file_date})"

                summary_parts = [
                    f"Filer: {company_label}",
                    f"Form: {form}",
                    f"Filed: {file_date}",
                    f"Matched Signal: {query_label}",
                ]
                if period_ending:
                    summary_parts.append(f"Period: {period_ending}")
                if sics:
                    summary_parts.append(f"SIC: {', '.join(sics)}")
                if form_items:
                    summary_parts.append(f"8-K Items: {', '.join(form_items)}")
                if file_desc:
                    summary_parts.append(f"Document: {file_desc}")

                filing_item = {
                    "title": title,
                    "url": filing_url,
                    "source": "SEC EDGAR / Full-Text Search",
                    "source_type": "sec_filing",
                    "published_at": f"{file_date}T00:00:00+00:00" if file_date else utc_now().isoformat(),
                    "raw_summary": " | ".join(summary_parts)[:2200],
                }
                if query_label != "Base AI Hardware Query":
                    filing_item["frontier_topic"] = query_label
                enrich_item_with_frontier_topics(filing_item)
                items.append(filing_item)

        except Exception as exc:
            log.error(f"SEC EDGAR EFTS exception ({query_label}): {repr(exc)}")
        time.sleep(0.15)

    log.info(f"sec_edgar: {len(items)} AI-hardware filings matched via EFTS ({start_dt} to {end_dt})")
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
        enrich_item_with_frontier_topics(item)
        compact_items.append(
            {
                "index": index,
                "title": item.get("title", "")[:450],
                "source": item.get("source", "")[:120],
                "source_type": item.get("source_type", "")[:80],
                "summary": item.get("raw_summary", "")[:2200],
                "url": item.get("url", "")[:400],
                "published_at": item.get("published_at", "")[:80],
                "matched_frontier_topic": item.get("frontier_topic", ""),
                "matched_frontier_subtopics": item.get("frontier_subtopics", ""),
            }
        )

    system_prompt = """
You are a strict AI hardware revolution filter for a monitoring bot.
Goal:
Find only hardware, semiconductor, robotics embodiment, energy, datacenter, or physical infrastructure technologies that could materially improve the AI scaling curve, AI training, AI inference, robotics, or AI deployment.

PRIORITY FRONTIER HARDWARE PILLARS (Mandatory High-Signal Tracking Domains):
Pay special attention to any item addressing the following 6 physical scaling bottlenecks and their core sub-topics:

1. Advanced Thermal Management & Liquid/Immersion Cooling Systems
   - Physical Bottleneck: Compute accelerator TDPs have exceeded 1,000 W per socket and are moving toward 2,000 W+, rendering air cooling physically unviable. Heat dissipation at the cold plate and facility loop is now a primary governor of cluster density.
   - Core Sub-Topics:
     * Direct-to-Chip (DLC) microchannel cold plates and manifold hydraulics.
     * Single-phase and two-phase liquid immersion cooling using synthetic dielectric fluids.
     * Liquid-to-Liquid and Liquid-to-Air Coolant Distribution Units (CDUs).
     * In-silicon microfluidic cooling channels etched directly into the back of active compute dies.

2. Glass Substrates & Panel-Level Packaging (PLP)
   - Physical Bottleneck: As multi-die reticle sizes expand (e.g., dual compute dies surrounded by 8-12 HBM stacks), standard organic Ajinomoto Build-up Film (ABF) substrates suffer from thermal warpage, trace density limits, and electrical parasitic losses. Glass core substrates represent the next physical substrate transition.
   - Core Sub-Topics:
     * Through-Glass Vias (TGV) with ultra-high aspect ratios and sub-10 um pitch.
     * Fan-Out Panel-Level Packaging (FOPLP) moving packaging economics from 300 mm round wafers to large rectangular panels.
     * Coefficient of Thermal Expansion (CTE) matching between silicon dies, glass interposers, and board assemblies.

3. Next-Generation Memory Hierarchies & Near-Memory Compute
   - Physical Bottleneck: Frontier LLM inference is fundamentally memory-bandwidth bound (FLOP-to-byte imbalances). The transition to custom base dies and memory-pooling interconnects is shifting memory from a passive commodity to active compute silicon.
   - Core Sub-Topics:
     * HBM4 & HBM4e: Migration from standard DRAM base dies to advanced logic nodes (such as TSMC N4/N3) for 2,048-bit wide memory interfaces.
     * Compute Express Link (CXL 3.0 / 3.1): Memory pooling, fabric switches, and cache-coherent hardware resource disaggregation across server nodes.
     * Processing-in-Memory (PIM) & Near-Memory Compute: Integrating arithmetic logic directly into DRAM banks or buffer dies to reduce data movement latency.

4. Rack-Scale High-Voltage Power Delivery & Point-of-Load (PoL) Conversion
   - Physical Bottleneck: Delivering thousands of amperes at sub-1.0V to silicon packages creates unsustainable resistive (I^2R) heat losses across traditional 12V and 48V copper busbars. Power delivery network (PDN) design inside the rack is as critical as power delivery to the building.
   - Core Sub-Topics:
     * 400V / 800V DC high-voltage server rack busbar architectures.
     * Vertical Power Delivery (VPD) and on-package Integrated Voltage Regulators (IVR) delivering power directly underneath the die.
     * Gallium Nitride (GaN) and Silicon Carbide (SiC) point-of-load converter stages operating at multi-megahertz switching frequencies.

5. Ultra-Wide Bandgap Materials & Synthetic Diamond Thermal Spreaders
   - Physical Bottleneck: Silicon's natural thermal conductivity (~150 W/m*K) forms a thermal bottleneck at the junction layer. Advanced materials integrated into the wafer stack allow higher sustained clock frequencies and current densities without thermal throttling.
   - Core Sub-Topics:
     * Chemical Vapor Deposition (CVD) diamond substrates and heat spreaders (~2,000 W/m*K) bonded directly to silicon.
     * Gallium Oxide (Ga2O3) and aluminum nitride (AlN) for extreme high-voltage, high-heat power electronics.

6. All-Optical Circuit Switching (OCS) & Co-Packaged Switch Fabrics
   - Physical Bottleneck: Clusters scaling to 100,000+ accelerators spend 15-20% of their power purely on optical transceivers, electrical SerDes, and retimers. Eliminating electrical-optical-electrical (E-O-E) conversions in switching fabrics is mandatory to sustain network scaling.
   - Core Sub-Topics:
     * Direct-drive optical circuit switches (OCS) using micro-electro-mechanical system (MEMS) 2D/3D mirror arrays.
     * 51.2T and 102.4T switch ASICs co-packaged with optical engines (CPO).

Additional Relevant Physical Hardware Layers:
- AI accelerators: GPU, TPU, NPU, ASIC, wafer-scale engine, inference chip, training chip.
- Semiconductor manufacturing: lithography (EUV/High-NA), deposition, etch, metrology, advanced nodes, wafer capacity, yield, EDA or physical design that directly affects chips.
- Robotics hardware: sensors, actuators, robot hands, embodied AI hardware, embedded compute, edge AI hardware, physical autonomy systems.
- Quantum/neuromorphic/photonic compute only if there is a clear AI compute or AI infrastructure angle.
- SEC-filing items are relevant only if the title/summary clearly identifies a physical hardware, semiconductor, robotics, datacenter, power, cooling, packaging, substrate, UWBG/diamond material, memory, or optical interconnect signal.

Positive source-quality signals:
- Concrete substance on any of the 6 Priority Frontier Hardware Pillars above (score 8-10 when accompanied by R&D breakthroughs, qualification, tape-outs, supply-chain adoption, or capacity expansion).
- SEC filing metadata, supply-chain media, or specialist semiconductor/HPC/datacenter publication.
- Production ramp, shipment, qualification, capacity expansion, standards adoption, or named technical bottleneck.

Reject / mark irrelevant:
- Software-only LLMs, agents, prompts, applications, SaaS, consumer AI products, app launches.
- Generic company earnings, stock moves, crypto, regulation, marketing partnerships, layoffs, funding rounds, or SEC filings without specific hardware technology.
- General academic AI papers with no hardware, robotics embodiment, infrastructure, materials, thermal/power, or physical compute contribution.
- Hype phrases like "AI-powered" unless the item explains the underlying hardware/infrastructure.

Scoring:
0-3 = not AI hardware/infrastructure.
4-6 = some hardware mention but weak, vague, or not clearly AI-revolution relevant.
7-8 = useful AI hardware/infrastructure signal with a clear bottleneck addressed.
9-10 = high-signal breakthrough, production ramp, major technical shift, or ecosystem change likely to matter for AI scaling (especially across the 6 Priority Frontier Hardware Pillars).

Strict rule:
Set relevant=true only when score >= 7 and the item clearly maps to a hardware/infrastructure bottleneck for AI.

Return only valid JSON using this schema:
{
  "results": [
    {
      "index": 0,
      "relevant": true,
      "score": 8,
      "category": "Thermal & Cooling / Glass Substrates & PLP / Memory & Near-Memory Compute / Rack Power & PoL / UWBG & Diamond Materials / Optical Interconnect & CPO / AI accelerator / other",
      "frontier_topic": "Exact title of the matching Priority Frontier Hardware Pillar (1-6) if applicable, else empty string",
      "hardware_domain": "GPU / ASIC / memory / interconnect / packaging / manufacturing / power cooling / robotics hardware / other",
      "bottleneck_addressed": "compute / memory bandwidth / interconnect / power / thermal / manufacturing capacity / robotics embodiment / other",
      "specific_tech": "Short concrete technology name (e.g., TGV glass core substrate, HBM4 N3 base die, 800V DC rack busbar, MEMS 3D OCS, CVD diamond heat spreader)",
      "why_it_matters": "One or two factual sentences explaining the physical hardware reason.",
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
            enrich_item_with_frontier_topics(original)
            llm_frontier = clean_text(result.get("frontier_topic", ""))
            resolved_frontier = llm_frontier or original.get("frontier_topic", "")
            resolved_subtopics = original.get("frontier_subtopics", "")
            resolved_domain = clean_text(result.get("hardware_domain", "")) or original.get("frontier_domain_hint", "")
            resolved_bottleneck = clean_text(result.get("bottleneck_addressed", "")) or original.get("frontier_bottleneck_hint", "")
            resolved_category = clean_text(result.get("category", "")) or original.get("frontier_category_hint", "")

            item = {
                **original,
                "category": resolved_category[:cap],
                "hardware_domain": resolved_domain[:cap],
                "bottleneck_addressed": resolved_bottleneck[:cap],
                "specific_tech": clean_text(result.get("specific_tech", ""))[:cap],
                "frontier_topic": resolved_frontier[:cap],
                "frontier_subtopics": resolved_subtopics[:cap],
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
        ("frontier_topic_rss", fetch_frontier_topic_rss),
        ("supply_chain_rss", fetch_supply_chain_rss),
        ("html_scraper", fetch_universal_html_scrapers),
        ("sec_edgar", fetch_sec_edgar),
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
        enrich_item_with_frontier_topics(item)
        fp = item_fingerprint(item)
        item["content_fingerprint"] = fp

        if url in SEEN_URLS_CACHE or fp in SEEN_FINGERPRINTS_CACHE:
            continue
        if processed_url_exists(url) or processed_fingerprint_exists(fp):
            continue
        new_items.append((url, fp, item))

    # Order frontier-matched candidates first so queue caps never drop priority hardware signals
    new_items.sort(key=lambda tup: 0 if tup[2].get("frontier_topic") else 1)

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
    log.info(f"ENABLE_ARXIV_FRONTIER_SEARCH: {ENABLE_ARXIV_FRONTIER_SEARCH} max_per_topic: {ARXIV_FRONTIER_MAX_RESULTS_PER_TOPIC} frontier_pillars: {len(FRONTIER_TOPICS)}")
    log.info(f"ENABLE_SPECIALIST_RSS: {ENABLE_SPECIALIST_RSS} feeds: {len(get_feed_specs('SPECIALIST_RSS_FEEDS', DEFAULT_SPECIALIST_RSS_FEEDS))} per feed: {SPECIALIST_RSS_MAX_ITEMS_PER_FEED}")
    log.info(f"ENABLE_FRONTIER_TOPIC_RSS: {ENABLE_FRONTIER_TOPIC_RSS} feeds: {len(get_feed_specs('FRONTIER_TOPIC_RSS_FEEDS', DEFAULT_FRONTIER_TOPIC_RSS_FEEDS))} per feed: {FRONTIER_TOPIC_RSS_MAX_ITEMS_PER_FEED}")
    log.info(f"ENABLE_SUPPLY_CHAIN_RSS: {ENABLE_SUPPLY_CHAIN_RSS} feeds: {len(get_feed_specs('SUPPLY_CHAIN_RSS_FEEDS', DEFAULT_SUPPLY_CHAIN_RSS_FEEDS))} per feed: {SUPPLY_CHAIN_RSS_MAX_ITEMS_PER_FEED}")
    log.info(f"ENABLE_HTML_SCRAPER: {ENABLE_HTML_SCRAPER} targets: {len(get_feed_specs('HTML_SCRAPER_TARGETS', DEFAULT_HTML_SCRAPER_TARGETS))} per site: {HTML_SCRAPER_MAX_ITEMS_PER_SITE}")
    log.info(f"ENABLE_SEC_EDGAR: {ENABLE_SEC_EDGAR} forms: {SEC_FORMS} lookback_days: {SEC_LOOKBACK_DAYS} max_items: {SEC_MAX_ITEMS_PER_FETCH} frontier_expansion: {ENABLE_SEC_FRONTIER_EXPANSION}")
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
