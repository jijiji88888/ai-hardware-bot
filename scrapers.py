import datetime as dt
import html
import json
import logging
import os
import re
import time
from threading import Lock
from urllib.parse import urlencode, urljoin, urlsplit

import feedparser
import requests
from bs4 import BeautifulSoup

try:
    from curl_cffi import requests as curl_requests
except ImportError:
    curl_requests = None

log = logging.getLogger("ai_hardware_bot")

HTML_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

ARXIV_HEADERS = {
    "User-Agent": "ai-hardware-trend-bot/1.0 (contact: your-real-email@example.com)"
}

# Domains known to return HTTP 403 to plain Python requests TLS fingerprints;
# skip straight to Chrome TLS impersonation (curl_cffi) on these domains.
_TLS_IMPERSONATE_DOMAINS = {
    "datacenterdynamics.com",
    "hpcwire.com",
}
_TLS_IMPERSONATE_LOCK = Lock()


def _utc_now():
    return dt.datetime.now(dt.timezone.utc)


def _clean_text(text):
    if not text:
        return ""
    text = html.unescape(str(text))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize_url(url):
    if not url:
        return ""
    return str(url).strip()


def _normalize_domain(url_str):
    return urlsplit(str(url_str or "")).netloc.lower().lstrip("www.")


def split_pipe_specs(raw):
    """Split 'Label=URL|Label2=URL2' on '|' ONLY outside regex parentheses/brackets.

    Prevents regex alternations such as '(?:news|analysis|opinions)' or
    '(?:20\\d{2}/\\d{2}/\\d{2}|off-the-wire)' inside HTML_SCRAPER_TARGETS from
    being shattered into bogus target entries.
    """
    parts = []
    buf = []
    paren_depth = 0
    bracket_depth = 0
    escaped = False
    for ch in raw:
        if escaped:
            buf.append(ch)
            escaped = False
            continue
        if ch == "\\":
            buf.append(ch)
            escaped = True
            continue
        if ch == "(" and bracket_depth == 0:
            paren_depth += 1
            buf.append(ch)
        elif ch == ")" and bracket_depth == 0 and paren_depth > 0:
            paren_depth -= 1
            buf.append(ch)
        elif ch == "[":
            bracket_depth += 1
            buf.append(ch)
        elif ch == "]" and bracket_depth > 0:
            bracket_depth -= 1
            buf.append(ch)
        elif ch == "|" and paren_depth == 0 and bracket_depth == 0:
            segment = "".join(buf).strip()
            if segment:
                parts.append(segment)
            buf = []
        else:
            buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def decode_response_text(resp):
    """Decode HTTP response bytes cleanly as UTF-8 first to prevent ISO-8859-1 mojibake."""
    raw = getattr(resp, "content", None)
    if not raw:
        return getattr(resp, "text", "") or ""
    if isinstance(raw, str):
        return raw
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        enc = getattr(resp, "apparent_encoding", None) or getattr(resp, "encoding", None) or "utf-8"
        try:
            return raw.decode(enc, errors="replace")
        except Exception:
            return raw.decode("utf-8", errors="replace")


def browser_get(url, headers=None, timeout=25, accept_json=False):
    """Fetch a URL using Chrome TLS impersonation (curl_cffi) when available,
    falling back to requests with browser navigation headers."""
    parsed = urlsplit(url)
    req_headers = dict(HTML_BROWSER_HEADERS)
    if parsed.scheme and parsed.netloc:
        req_headers["Referer"] = f"{parsed.scheme}://{parsed.netloc}/"
    if accept_json:
        req_headers["Accept"] = "application/json,text/plain,*/*"
    if headers:
        req_headers.update(headers)

    if curl_requests is not None:
        return curl_requests.get(
            url,
            headers=req_headers,
            impersonate="chrome",
            timeout=timeout,
            allow_redirects=True,
        )
    return requests.get(url, headers=req_headers, timeout=timeout, allow_redirects=True)


def fetch_rss_with_tls_fallback(feed_url, request_headers, timeout=35):
    """Fetch an RSS/Atom URL, skipping plain requests on known-403 domains and
    auto-escalating HTTP 403 responses to Chrome TLS impersonation."""
    feed_domain = _normalize_domain(feed_url)
    if feed_domain in _TLS_IMPERSONATE_DOMAINS and curl_requests is not None:
        return browser_get(feed_url, headers=request_headers, timeout=timeout)
    response = requests.get(feed_url, headers=request_headers, timeout=timeout)
    if response.status_code == 403 and curl_requests is not None:
        with _TLS_IMPERSONATE_LOCK:
            _TLS_IMPERSONATE_DOMAINS.add(feed_domain)
        response = browser_get(feed_url, headers=request_headers, timeout=timeout)
    return response


# =========================
# Two-Stage arXiv Fetcher (CDN RSS + Polite Chunked Atom API)
# =========================

def _clean_arxiv_summary(raw_summary):
    """Strip rss.arxiv.org 'arXiv:XXXX Announce Type: ... Abstract:' boilerplate."""
    text = _clean_text(raw_summary or "")
    text = re.sub(
        r"^arXiv:\S+\s+Announce\s+Type:\s*\S+\s*Abstract:\s*",
        "",
        text,
        flags=re.IGNORECASE,
    )
    return text.strip()[:2200]


def _extract_arxiv_categories(entry):
    cats = []
    for tag in entry.get("tags", []) or []:
        term = (tag.get("term") or "").strip()
        if term:
            cats.append(term)
    if not cats and entry.get("category"):
        cats.append(str(entry.get("category")).strip())
    return cats


def _fetch_arxiv_rss(categories, timeout=30):
    """Fast primary fetch from arXiv's CDN-cached multi-category RSS feed."""
    if not categories:
        return []
    feed_url = "https://rss.arxiv.org/rss/" + "+".join(categories)
    try:
        resp = requests.get(feed_url, headers=ARXIV_HEADERS, timeout=timeout)
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
        entries = list(getattr(parsed, "entries", []) or [])
        log.info(f"arXiv CDN RSS returned {len(entries)} entries across {len(categories)} categories")
        return entries
    except Exception as exc:
        log.warning(f"arXiv CDN RSS fetch failed ({feed_url}): {repr(exc)}")
        return []


def _fetch_arxiv_atom(url, max_retries=2, timeout=45):
    """Direct fetch + parse of the arXiv Atom API with realistic timeout and backoff."""
    last_exc = None
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=ARXIV_HEADERS, timeout=timeout)
            resp.raise_for_status()
            parsed = feedparser.parse(resp.content)
            if parsed.bozo and not parsed.entries:
                raise ValueError(f"feedparser bozo with no entries: {parsed.bozo_exception}")
            return list(parsed.entries)
        except Exception as exc:
            last_exc = exc
            log.warning(f"arXiv API attempt {attempt + 1}/{max_retries} failed: {repr(exc)}")
            if attempt < max_retries - 1:
                time.sleep(3 * (attempt + 1))
    log.error(f"arXiv API chunk failed after {max_retries} attempts: {repr(last_exc)}")
    return []


def fetch_arxiv_items(
    categories,
    results_per_category=20,
    chunk_size=3,
    api_timeout=45,
    normalize_url_fn=_normalize_url,
    clean_text_fn=_clean_text,
):
    """Fetch arXiv papers via Stage 1 (fast CDN RSS) + Stage 2 (polite 3-category chunked Atom API)."""
    items = []
    if not categories:
        return items

    buckets = {cat: [] for cat in categories}
    seen_per_bucket = {cat: set() for cat in categories}

    def _bucket_entries(entries, target_cats):
        for entry in entries:
            entry_cats = _extract_arxiv_categories(entry)
            link = normalize_url_fn(entry.get("link", ""))
            if not link:
                continue
            for cat in target_cats:
                if len(buckets[cat]) >= results_per_category:
                    continue
                if cat in entry_cats and link not in seen_per_bucket[cat]:
                    seen_per_bucket[cat].add(link)
                    buckets[cat].append({
                        "title": clean_text_fn(entry.get("title", "")),
                        "url": link,
                        "source": f"arXiv / {cat}",
                        "source_type": "research",
                        "published_at": entry.get("published", entry.get("updated", _utc_now().isoformat())),
                        "raw_summary": _clean_arxiv_summary(entry.get("summary", entry.get("description", ""))),
                        "categories": entry_cats,
                    })

    # Stage 1: Fast CDN RSS across all categories
    rss_entries = _fetch_arxiv_rss(categories, timeout=30)
    if rss_entries:
        _bucket_entries(rss_entries, categories)

    # Stage 2: Small-chunk Atom API fallback for categories still short of quota
    missing_cats = [cat for cat in categories if len(buckets[cat]) < results_per_category]
    if missing_cats:
        chunk_size = max(1, int(chunk_size))
        chunks = [missing_cats[i : i + chunk_size] for i in range(0, len(missing_cats), chunk_size)]
        for idx, chunk in enumerate(chunks):
            if idx > 0:
                time.sleep(3.0)  # arXiv API politeness (1 req / 3s)
            chunk_query = "(" + " OR ".join(f"cat:{c}" for c in chunk) + ")"
            chunk_max = min(120, max(30, results_per_category * len(chunk) * 2))
            params = {
                "search_query": chunk_query,
                "start": 0,
                "max_results": chunk_max,
                "sortBy": "submittedDate",
                "sortOrder": "descending",
            }
            url = "https://export.arxiv.org/api/query?" + urlencode(params)
            api_entries = _fetch_arxiv_atom(url, max_retries=2, timeout=api_timeout)
            if api_entries:
                _bucket_entries(api_entries, chunk)

    for cat, bucketed in buckets.items():
        if len(bucketed) < results_per_category:
            log.info(f"arXiv / {cat}: {len(bucketed)}/{results_per_category} items collected")
        items.extend(bucketed)

    return items


# =========================
# Dedicated Per-Source Scrapers & Universal Fallback (Zero Google News)
# =========================

def _unwrap_wayback_url(url_str):
    if not url_str:
        return ""
    return re.sub(r"^https?://web\.archive\.org/web/\d+(?:id_|[a-z_]+)?/", "", url_str.strip())


def clean_scraped_title(raw_title):
    """Unescape HTML entities, strip tags, and collapse whitespace for clean readable titles."""
    return _clean_text(raw_title)


def is_garbage_title(title):
    """Reject navigation labels, boilerplate, or unreadable garbled strings."""
    if not title or len(title) < 15:
        return True
    low = title.lower().strip()
    if low in {
        "read more", "learn more", "view all", "see more", "click here",
        "subscribe", "sign in", "log in", "register", "cookie policy",
        "privacy policy", "terms of service", "all news", "latest news",
    }:
        return True
    if title.count("\ufffd") >= 2:
        return True
    return False


def _parse_rss_bytes_to_scraped_items(raw_bytes, label, max_items, normalize_url_fn=_normalize_url):
    feed = feedparser.parse(raw_bytes)
    entries = getattr(feed, "entries", []) or []
    items = []
    seen = set()
    for entry in entries:
        title = clean_scraped_title(entry.get("title", ""))
        link = normalize_url_fn(entry.get("link", ""))
        if not link or is_garbage_title(title) or link in seen:
            continue
        seen.add(link)
        summary = clean_scraped_title(entry.get("summary", entry.get("description", ""))) or title
        pub_date = _clean_text(entry.get("published", entry.get("updated", _utc_now().isoformat())))
        items.append({
            "title": title,
            "url": link,
            "source": f"Web / {label}",
            "source_type": "scraped_web",
            "published_at": pub_date or _utc_now().isoformat(),
            "category": "Hardware",
            "raw_summary": summary[:2200],
        })
        if len(items) >= max_items:
            break
    return items


def _parse_html_articles(html_text, base_url, label, url_pattern, ignore_patterns, max_items, normalize_url_fn=_normalize_url):
    site_items = []
    seen_urls = set()
    base_netloc = urlsplit(base_url).netloc.lower().lstrip("www.")
    soup = BeautifulSoup(html_text, "html.parser")

    # Pass 1: Structured JSON-LD metadata
    for script_tag in soup.find_all("script", type=re.compile(r"ld\+json", re.I)):
        raw_json = script_tag.string or script_tag.get_text() or ""
        if not raw_json.strip():
            continue
        try:
            ld_data = json.loads(raw_json)
        except Exception:
            continue
        ld_nodes = ld_data if isinstance(ld_data, list) else [ld_data]
        expanded = []
        for node in ld_nodes:
            if isinstance(node, dict):
                expanded.append(node)
                if isinstance(node.get("@graph"), list):
                    expanded.extend(n for n in node["@graph"] if isinstance(n, dict))
                if isinstance(node.get("itemListElement"), list):
                    for el in node["itemListElement"]:
                        if isinstance(el, dict):
                            expanded.append(el.get("item") if isinstance(el.get("item"), dict) else el)
        for node in expanded:
            if not isinstance(node, dict):
                continue
            raw_link = _unwrap_wayback_url(str(node.get("url") or node.get("@id") or ""))
            title = clean_scraped_title(node.get("headline") or node.get("name") or "")
            if not raw_link or is_garbage_title(title):
                continue
            full_url = urljoin(base_url, raw_link)
            parsed_url = urlsplit(full_url)
            if parsed_url.netloc.lower().lstrip("www.") != base_netloc:
                continue
            if url_pattern and not re.search(url_pattern, full_url):
                continue
            if not url_pattern and (ignore_patterns.search(parsed_url.path) or len(parsed_url.path.strip("/")) < 4):
                continue
            norm_url = normalize_url_fn(full_url)
            if norm_url in seen_urls:
                continue
            seen_urls.add(norm_url)
            pub_date = _clean_text(node.get("datePublished") or node.get("dateModified") or _utc_now().isoformat())
            summary = clean_scraped_title(node.get("description") or title)
            site_items.append({
                "title": title,
                "url": norm_url,
                "source": f"Web / {label}",
                "source_type": "scraped_web",
                "published_at": pub_date,
                "category": "Hardware",
                "raw_summary": summary[:2200] if summary else title,
            })
            if len(site_items) >= max_items:
                return site_items

    # Pass 2: DOM anchor scanning
    for a in soup.find_all("a", href=True):
        href = _unwrap_wayback_url(a["href"].strip())
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue

        full_url = _unwrap_wayback_url(urljoin(base_url, href))
        parsed_url = urlsplit(full_url)
        if parsed_url.netloc.lower().lstrip("www.") != base_netloc:
            continue

        if url_pattern:
            if not re.search(url_pattern, full_url):
                continue
        else:
            if ignore_patterns.search(parsed_url.path) or len(parsed_url.path.strip("/")) < 4:
                continue

        norm_url = normalize_url_fn(full_url)
        if norm_url in seen_urls:
            continue

        parent = a.find_parent(["article", "li", "div", "section"])
        title = clean_scraped_title(a.get_text())
        if is_garbage_title(title):
            heading = a.find(["h1", "h2", "h3", "h4", "span"])
            if heading:
                title = clean_scraped_title(heading.get_text())
        if is_garbage_title(title):
            title = clean_scraped_title(a.get("title") or a.get("aria-label") or "")
        if is_garbage_title(title) and parent:
            parent_heading = parent.find(["h1", "h2", "h3", "h4"])
            if parent_heading:
                title = clean_scraped_title(parent_heading.get_text())
        if is_garbage_title(title):
            continue

        seen_urls.add(norm_url)

        pub_date = _utc_now().isoformat()
        date_match = re.search(r"/(20\d{2})[-/](\d{1,2})[-/](\d{1,2})", full_url)
        if date_match:
            y, m, d = date_match.groups()
            pub_date = f"{y}-{int(m):02d}-{int(d):02d}T00:00:00+00:00"

        summary = ""
        category = "Hardware"
        if parent:
            if not date_match:
                time_tag = parent.find("time")
                if time_tag and time_tag.get("datetime"):
                    pub_date = time_tag["datetime"]
            p_tag = parent.find("p")
            if p_tag:
                summary = clean_scraped_title(p_tag.get_text())
            else:
                summary = clean_scraped_title(parent.get_text()).replace(title, "").strip()[:400]
            cat_tag = parent.find("a", href=re.compile(r"/category/"))
            if cat_tag:
                category = clean_scraped_title(cat_tag.get_text())

        site_items.append({
            "title": title,
            "url": norm_url,
            "source": f"Web / {label}",
            "source_type": "scraped_web",
            "published_at": pub_date,
            "category": category,
            "raw_summary": summary[:2200] if summary else title,
        })
        if len(site_items) >= max_items:
            break

    return site_items


def _scrape_wp_json_api(base_url, label, max_items, normalize_url_fn=_normalize_url):
    parsed = urlsplit(base_url)
    wp_api_url = (
        f"{parsed.scheme}://{parsed.netloc}"
        f"/wp-json/wp/v2/posts?per_page={max_items}&_fields=link,title,excerpt,date_gmt"
    )
    try:
        resp = browser_get(wp_api_url, timeout=20, accept_json=True)
        if resp.status_code != 200:
            return []
        posts = resp.json()
        if not isinstance(posts, list):
            return []
        items = []
        seen = set()
        for post in posts[:max_items]:
            link = normalize_url_fn(str(post.get("link") or ""))
            title_obj = post.get("title") or {}
            raw_title = title_obj.get("rendered", "") if isinstance(title_obj, dict) else str(title_obj)
            title = clean_scraped_title(raw_title)
            if not link or is_garbage_title(title) or link in seen:
                continue
            seen.add(link)
            excerpt_obj = post.get("excerpt") or {}
            raw_excerpt = excerpt_obj.get("rendered", "") if isinstance(excerpt_obj, dict) else str(excerpt_obj)
            summary = clean_scraped_title(raw_excerpt) or title
            pub_date = _clean_text(post.get("date_gmt") or _utc_now().isoformat())
            if pub_date and not pub_date.endswith("Z") and "+" not in pub_date:
                pub_date = f"{pub_date}+00:00"
            items.append({
                "title": title,
                "url": link,
                "source": f"Web / {label}",
                "source_type": "scraped_web",
                "published_at": pub_date,
                "category": "Hardware",
                "raw_summary": summary[:2200],
            })
        return items
    except Exception:
        return []


def _scrape_xml_sitemap(sitemap_urls, label, url_pattern, max_items, normalize_url_fn=_normalize_url):
    for sm_url in sitemap_urls:
        try:
            resp = browser_get(sm_url, timeout=20)
            if resp.status_code != 200:
                continue
            xml_text = decode_response_text(resp)
            soup = BeautifulSoup(xml_text, "html.parser")
            items = []
            seen = set()
            for url_tag in soup.find_all("url"):
                loc_tag = url_tag.find("loc")
                if not loc_tag:
                    continue
                loc = normalize_url_fn(loc_tag.get_text().strip())
                if not loc or loc in seen:
                    continue
                if url_pattern and not re.search(url_pattern, loc):
                    continue
                seen.add(loc)
                title_tag = url_tag.find("news:title") or url_tag.find("title")
                if title_tag and title_tag.get_text().strip():
                    title = clean_scraped_title(title_tag.get_text())
                else:
                    slug = urlsplit(loc).path.rstrip("/").rsplit("/", 1)[-1]
                    title = _clean_text(slug.replace("-", " ").replace("_", " ")).capitalize()
                if is_garbage_title(title):
                    continue
                pub_tag = url_tag.find("news:publication_date") or url_tag.find("lastmod")
                pub_date = _clean_text(pub_tag.get_text()) if pub_tag else _utc_now().isoformat()
                items.append({
                    "title": title,
                    "url": loc,
                    "source": f"Web / {label}",
                    "source_type": "scraped_web",
                    "published_at": pub_date or _utc_now().isoformat(),
                    "category": "Hardware",
                    "raw_summary": title,
                })
                if len(items) >= max_items:
                    return items
            if items:
                return items
        except Exception:
            continue
    return []


def _scrape_wayback_html_fallback(base_url, label, url_pattern, ignore_patterns, max_items, normalize_url_fn=_normalize_url):
    try:
        wb_api_url = f"https://archive.org/wayback/available?url={base_url}"
        wb_resp = requests.get(wb_api_url, timeout=15)
        if wb_resp.status_code != 200:
            return []
        snapshots = (wb_resp.json() or {}).get("archived_snapshots") or {}
        closest = snapshots.get("closest") or {}
        if not closest.get("available") or not closest.get("url"):
            return []
        ts_str = str(closest.get("timestamp") or "")
        if len(ts_str) >= 8:
            try:
                snap_dt = dt.datetime.strptime(ts_str[:8], "%Y%m%d").replace(tzinfo=dt.timezone.utc)
                if (_utc_now() - snap_dt).days > 7:
                    return []
            except Exception:
                pass
        snapshot_url = re.sub(
            r"/web/(\d+)/",
            r"/web/\1id_/",
            closest["url"].replace("http://", "https://"),
        )
        snap_resp = requests.get(snapshot_url, headers=HTML_BROWSER_HEADERS, timeout=25)
        if snap_resp.status_code != 200:
            return []
        return _parse_html_articles(
            decode_response_text(snap_resp), base_url, label, url_pattern, ignore_patterns, max_items, normalize_url_fn
        )
    except Exception:
        return []


def scrape_trendforce_news(base_url, label, max_items, normalize_url_fn=_normalize_url):
    """Dedicated scraper for TrendForce News (clean UTF-8 HTML -> WP feed -> Wayback)."""
    news_url = base_url or "https://www.trendforce.com/news/"
    tf_pattern = r"/(?:news/\d{4}/\d{2}/\d{2}/|presscenter/news/\d+)"
    ignore_patterns = re.compile(r"/(about|contact|privacy|terms|login|search)/?", re.IGNORECASE)

    try:
        resp = browser_get(news_url, timeout=20)
        if resp.status_code == 200:
            items = _parse_html_articles(
                decode_response_text(resp), news_url, label, tf_pattern, ignore_patterns, max_items, normalize_url_fn
            )
            if items:
                log.info(f"scraper[TrendForce]: extracted {len(items)} items from HTML")
                return items
    except Exception as exc:
        log.info(f"scraper[TrendForce]: HTML fetch issue: {repr(exc)}")

    try:
        resp = browser_get("https://www.trendforce.com/news/feed/", timeout=20)
        if resp.status_code == 200 and resp.content:
            items = _parse_rss_bytes_to_scraped_items(resp.content, label, max_items, normalize_url_fn)
            if items:
                log.info(f"scraper[TrendForce]: extracted {len(items)} items from /news/feed/")
                return items
    except Exception as exc:
        log.info(f"scraper[TrendForce]: feed fetch issue: {repr(exc)}")

    items = _scrape_wayback_html_fallback(news_url, label, tf_pattern, ignore_patterns, max_items, normalize_url_fn)
    if items:
        log.info(f"scraper[TrendForce]: recovered {len(items)} items via Wayback snapshot")
    return items


def scrape_datacenterdynamics(base_url, label, max_items, normalize_url_fn=_normalize_url):
    """Dedicated scraper for Data Center Dynamics (Chrome TLS impersonation; zero Google News)."""
    news_url = base_url or "https://www.datacenterdynamics.com/en/news/"
    dcd_pattern = r"/en/(?:news|analysis|opinions|product-news)/[a-z0-9-]+"
    ignore_patterns = re.compile(r"/(about|contact|privacy|terms|login|register|search)/?", re.IGNORECASE)

    for rss_url in (
        "https://www.datacenterdynamics.com/en/rss/",
        "https://www.datacenterdynamics.com/rss/",
    ):
        try:
            resp = browser_get(rss_url, timeout=20)
            if resp.status_code == 200 and resp.content:
                items = _parse_rss_bytes_to_scraped_items(resp.content, label, max_items, normalize_url_fn)
                if items:
                    log.info(f"scraper[DCD]: extracted {len(items)} items via TLS-impersonated RSS")
                    return items
        except Exception as exc:
            log.info(f"scraper[DCD]: RSS issue ({rss_url}): {repr(exc)}")

    try:
        resp = browser_get(news_url, timeout=20)
        if resp.status_code == 200:
            items = _parse_html_articles(
                decode_response_text(resp), news_url, label, dcd_pattern, ignore_patterns, max_items, normalize_url_fn
            )
            if items:
                log.info(f"scraper[DCD]: extracted {len(items)} items from HTML")
                return items
        else:
            log.info(f"scraper[DCD]: HTML returned HTTP {resp.status_code}")
    except Exception as exc:
        log.info(f"scraper[DCD]: HTML fetch issue: {repr(exc)}")

    items = _scrape_xml_sitemap(
        [
            "https://www.datacenterdynamics.com/news-sitemap.xml",
            "https://www.datacenterdynamics.com/en/sitemap.xml",
            "https://www.datacenterdynamics.com/sitemap.xml",
        ],
        label,
        dcd_pattern,
        max_items,
        normalize_url_fn,
    )
    if items:
        log.info(f"scraper[DCD]: extracted {len(items)} items from XML sitemap")
        return items

    items = _scrape_wayback_html_fallback(news_url, label, dcd_pattern, ignore_patterns, max_items, normalize_url_fn)
    if items:
        log.info(f"scraper[DCD]: recovered {len(items)} items via Wayback snapshot")
    return items


def scrape_hpcwire(base_url, label, max_items, normalize_url_fn=_normalize_url):
    """Dedicated scraper for HPCwire (WP REST API -> TLS-impersonated RSS -> HTML -> Wayback; zero Google News)."""
    site_url = base_url or "https://www.hpcwire.com/"
    hpc_pattern = r"/(?:20\d{2}/\d{2}/\d{2}|off-the-wire)/[a-z0-9-]+"
    ignore_patterns = re.compile(r"/(about|contact|privacy|terms|login|register|search|tag|author)/?", re.IGNORECASE)

    items = _scrape_wp_json_api(site_url, label, max_items, normalize_url_fn)
    if items:
        log.info(f"scraper[HPCwire]: extracted {len(items)} items via WP REST API")
        return items

    try:
        resp = browser_get("https://www.hpcwire.com/feed/", timeout=20)
        if resp.status_code == 200 and resp.content:
            items = _parse_rss_bytes_to_scraped_items(resp.content, label, max_items, normalize_url_fn)
            if items:
                log.info(f"scraper[HPCwire]: extracted {len(items)} items via TLS-impersonated RSS")
                return items
    except Exception as exc:
        log.info(f"scraper[HPCwire]: RSS fetch issue: {repr(exc)}")

    try:
        resp = browser_get(site_url, timeout=20)
        if resp.status_code == 200:
            items = _parse_html_articles(
                decode_response_text(resp), site_url, label, hpc_pattern, ignore_patterns, max_items, normalize_url_fn
            )
            if items:
                log.info(f"scraper[HPCwire]: extracted {len(items)} items from HTML")
                return items
        else:
            log.info(f"scraper[HPCwire]: HTML returned HTTP {resp.status_code}")
    except Exception as exc:
        log.info(f"scraper[HPCwire]: HTML fetch issue: {repr(exc)}")

    items = _scrape_wayback_html_fallback(site_url, label, hpc_pattern, ignore_patterns, max_items, normalize_url_fn)
    if items:
        log.info(f"scraper[HPCwire]: recovered {len(items)} items via Wayback snapshot")
    return items


DEDICATED_SCRAPERS = {
    "trendforce.com": scrape_trendforce_news,
    "datacenterdynamics.com": scrape_datacenterdynamics,
    "hpcwire.com": scrape_hpcwire,
}


def scrape_single_html_target(label, target, max_items=12, normalize_url_fn=_normalize_url):
    """Dispatch to a dedicated per-source scraper when available, or fall back to
    the universal Chrome-impersonated HTML scraper (zero Google News fallback)."""
    ignore_patterns = re.compile(
        r"/(about|contact|privacy|terms|login|signup|register|tag|category|author|cart|search|feed)/?",
        re.IGNORECASE,
    )
    if ";;" in target:
        base_url, url_pattern = target.split(";;", 1)
        base_url, url_pattern = base_url.strip(), url_pattern.strip()
    else:
        base_url, url_pattern = target.strip(), None

    parsed_base = urlsplit(base_url)
    if parsed_base.scheme not in ("http", "https") or not parsed_base.netloc:
        log.warning(f"html_scraper: skipping invalid target URL for '{label}': {base_url!r}")
        return []

    domain = parsed_base.netloc.lower().lstrip("www.")
    dedicated_fn = DEDICATED_SCRAPERS.get(domain)
    if dedicated_fn is not None:
        site_items = dedicated_fn(base_url, label, max_items, normalize_url_fn)
        site_items.sort(key=lambda x: x["published_at"], reverse=True)
        log.info(f"html_scraper: scraped {len(site_items)} items from '{label}'")
        return site_items

    site_items = []
    try:
        resp = browser_get(base_url, timeout=20)
        if resp.status_code == 200:
            site_items = _parse_html_articles(
                decode_response_text(resp), base_url, label, url_pattern, ignore_patterns, max_items, normalize_url_fn
            )
        else:
            log.info(
                f"html_scraper: direct fetch for '{label}' returned HTTP {resp.status_code}; trying fallbacks..."
            )
    except Exception as exc:
        log.info(f"html_scraper: direct fetch exception for '{label}': {repr(exc)}; trying fallbacks...")

    if not site_items:
        site_items = _scrape_wp_json_api(base_url, label, max_items, normalize_url_fn)
        if site_items:
            log.info(f"html_scraper: recovered {len(site_items)} items for '{label}' via WP REST API")

    if not site_items:
        site_items = _scrape_wayback_html_fallback(
            base_url, label, url_pattern, ignore_patterns, max_items, normalize_url_fn
        )
        if site_items:
            log.info(f"html_scraper: recovered {len(site_items)} items for '{label}' via Wayback HTML snapshot")

    site_items.sort(key=lambda x: x["published_at"], reverse=True)
    log.info(f"html_scraper: scraped {len(site_items)} items from '{label}'")
    return site_items
