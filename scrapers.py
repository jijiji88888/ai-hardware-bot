import datetime as dt
import html
import json
import logging
import os
import re
from threading import Lock
from urllib.parse import urljoin, urlsplit

import feedparser
import requests
from bs4 import BeautifulSoup

try:
    from curl_cffi import requests as curl_requests
except ImportError:
    curl_requests = None

log = logging.getLogger("ai_hardware_bot")

# Fallback headers used ONLY when curl_cffi is unavailable (plain requests).
# When curl_cffi is active, never override User-Agent or Sec-Ch-Ua* headers:
# curl_cffi sends the exact header set matching its TLS/HTTP2 fingerprint.
HTML_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

SCRAPER_PROXY_URL = os.getenv("SCRAPER_PROXY_URL", "").strip()
JINA_API_KEY = os.getenv("JINA_API_KEY", "").strip()

# Domains known to return HTTP 403 or Cloudflare bot challenges to plain Python requests
# TLS fingerprints; skip straight to browser TLS impersonation (curl_cffi) on these domains.
_TLS_IMPERSONATE_DOMAINS = {
    "datacenterdynamics.com",
    "hpcwire.com",
    "opencompute.org",
}
_TLS_IMPERSONATE_LOCK = Lock()

_IMPERSONATE_PROFILES = ("chrome", "safari", "firefox")


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


def _get_proxies():
    if not SCRAPER_PROXY_URL:
        return None
    return {"http": SCRAPER_PROXY_URL, "https": SCRAPER_PROXY_URL}


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


def _filter_safe_override_headers(headers):
    """Strip fingerprint-sensitive headers (User-Agent, Sec-Ch-Ua*, Sec-Fetch-*)
    so curl_cffi's native TLS/HTTP2/header profile stays 100% consistent."""
    if not headers:
        return {}
    blocked_prefixes = ("user-agent", "sec-ch-ua", "sec-fetch-")
    return {
        k: v
        for k, v in headers.items()
        if not k.lower().startswith(blocked_prefixes)
    }


def browser_get(url, headers=None, timeout=25, accept_json=False):
    """Fetch a URL with curl_cffi browser impersonation (preserving its native
    TLS + HTTP/2 + header fingerprint without conflicting User-Agent overrides),
    rotating browser profiles if the first profile receives HTTP 403."""
    proxies = _get_proxies()

    if curl_requests is not None:
        safe_headers = _filter_safe_override_headers(headers)
        if accept_json:
            safe_headers.setdefault("Accept", "application/json,text/plain,*/*")
        safe_headers.setdefault("Accept-Language", "en-US,en;q=0.9")

        last_resp = None
        for profile in _IMPERSONATE_PROFILES:
            try:
                kwargs = {
                    "impersonate": profile,
                    "timeout": timeout,
                    "allow_redirects": True,
                }
                if safe_headers:
                    kwargs["headers"] = safe_headers
                if proxies:
                    kwargs["proxies"] = proxies
                resp = curl_requests.get(url, **kwargs)
                last_resp = resp
                if resp.status_code == 200:
                    return resp
                if resp.status_code not in (403, 429):
                    return resp
            except Exception:
                continue
        if last_resp is not None:
            return last_resp

    # Plain requests fallback only when curl_cffi is not installed
    req_headers = dict(HTML_BROWSER_HEADERS)
    if accept_json:
        req_headers["Accept"] = "application/json,text/plain,*/*"
    if headers:
        req_headers.update(headers)
    return requests.get(
        url,
        headers=req_headers,
        timeout=timeout,
        allow_redirects=True,
        proxies=proxies,
    )


def fetch_rss_with_tls_fallback(feed_url, request_headers, timeout=35):
    """Fetch an RSS/Atom URL, skipping plain requests on known-403 domains and
    auto-escalating HTTP 403 responses to browser TLS impersonation."""
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
# Dedicated Per-Source Scrapers & Universal Fallback
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
        "continue reading", "just a moment...", "attention required! | cloudflare",
    }:
        return True
    if title.count("\ufffd") >= 2:
        return True
    return False


def _parse_human_date_to_iso(text):
    """Parse common human-readable blog dates (e.g. 'November 5, 2026', 'Nov 5, 2026',
    '2026-11-05', '11/5/2026') into ISO-8601 UTC format, or return empty string."""
    if not text:
        return ""
    cleaned = _clean_text(text)

    # Match 'November 5, 2026' or 'Nov 5, 2026' (optionally prefixed with 'Date:')
    m_long = re.search(
        r"\b("
        r"January|February|March|April|May|June|July|August|September|October|November|December|"
        r"Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec"
        r")\.?\s+(\d{1,2}),?\s+(20\d{2})\b",
        cleaned,
        re.IGNORECASE,
    )
    if m_long:
        month_str, day_str, year_str = m_long.groups()
        month_norm = month_str[:3].capitalize()
        try:
            parsed = dt.datetime.strptime(f"{month_norm} {int(day_str)} {year_str}", "%b %d %Y")
            return parsed.replace(tzinfo=dt.timezone.utc).isoformat()
        except ValueError:
            pass

    # Match ISO 'YYYY-MM-DD'
    m_iso = re.search(r"\b(20\d{2})-(\d{1,2})-(\d{1,2})\b", cleaned)
    if m_iso:
        y, m, d = m_iso.groups()
        try:
            parsed = dt.datetime(int(y), int(m), int(d), tzinfo=dt.timezone.utc)
            return parsed.isoformat()
        except ValueError:
            pass

    # Match US numeric 'M/D/YYYY'
    m_us = re.search(r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b", cleaned)
    if m_us:
        m, d, y = m_us.groups()
        try:
            parsed = dt.datetime(int(y), int(m), int(d), tzinfo=dt.timezone.utc)
            return parsed.isoformat()
        except ValueError:
            pass

    return ""


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
            if url_pattern and not re.search(url_pattern, full_url, re.IGNORECASE):
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
            if not re.search(url_pattern, full_url, re.IGNORECASE):
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
                else:
                    parsed_human_date = _parse_human_date_to_iso(parent.get_text(" ", strip=True))
                    if parsed_human_date:
                        pub_date = parsed_human_date
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


def _parse_markdown_links(md_text, base_url, label, url_pattern, ignore_patterns, max_items, normalize_url_fn=_normalize_url):
    """Extract article items from rendered Markdown (e.g., Jina Reader output)."""
    items = []
    seen = set()
    base_netloc = urlsplit(base_url).netloc.lower().lstrip("www.")

    # Match [Title](https://...) while ignoring markdown image syntax ![alt](img)
    for match in re.finditer(r"(?<!!)\[([^\]]{15,240})\]\((https?://[^\s\)]+)\)", md_text):
        raw_title, raw_url = match.group(1), match.group(2)
        # Clean nested markdown formatting (**bold**, ### headings, image refs)
        cleaned_title = re.sub(r"!\[[^\]]*\]\([^\)]*\)", "", raw_title)
        cleaned_title = re.sub(r"[#*_`]+", "", cleaned_title)
        title = clean_scraped_title(cleaned_title)
        if is_garbage_title(title):
            continue

        parsed = urlsplit(raw_url)
        if parsed.netloc.lower().lstrip("www.") != base_netloc:
            continue
        if url_pattern and not re.search(url_pattern, raw_url, re.IGNORECASE):
            continue
        if not url_pattern and (ignore_patterns.search(parsed.path) or len(parsed.path.strip("/")) < 4):
            continue

        norm_url = normalize_url_fn(raw_url)
        if not norm_url or norm_url in seen:
            continue
        seen.add(norm_url)

        pub_date = _utc_now().isoformat()
        date_match = re.search(r"/(20\d{2})[-/](\d{1,2})[-/](\d{1,2})", norm_url)
        if date_match:
            y, m, d = date_match.groups()
            pub_date = f"{y}-{int(m):02d}-{int(d):02d}T00:00:00+00:00"
        else:
            # Look ahead briefly in rendered Markdown for a 'Date: Month DD, YYYY' line
            trailing = md_text[match.end():match.end() + 240]
            parsed_human_date = _parse_human_date_to_iso(trailing)
            if parsed_human_date:
                pub_date = parsed_human_date

        items.append({
            "title": title,
            "url": norm_url,
            "source": f"Web / {label}",
            "source_type": "scraped_web",
            "published_at": pub_date,
            "category": "Hardware",
            "raw_summary": title,
        })
        if len(items) >= max_items:
            break
    return items


def _scrape_via_jina_reader(base_url, label, url_pattern, ignore_patterns, max_items, normalize_url_fn=_normalize_url):
    """Fetch a rendered page via Jina Reader (r.jina.ai) when direct datacenter IP
    requests hit a Cloudflare JS/WAF challenge."""
    reader_url = f"https://r.jina.ai/{base_url}"
    headers = {
        "Accept": "text/plain,text/markdown,*/*",
        "X-Return-Format": "markdown",
    }
    if JINA_API_KEY:
        headers["Authorization"] = f"Bearer {JINA_API_KEY}"
    try:
        resp = requests.get(reader_url, headers=headers, timeout=30)
        if resp.status_code != 200:
            return []
        md_text = decode_response_text(resp)
        if not md_text or "Just a moment..." in md_text[:500]:
            return []
        return _parse_markdown_links(
            md_text, base_url, label, url_pattern, ignore_patterns, max_items, normalize_url_fn
        )
    except Exception:
        return []


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
                if url_pattern and not re.search(url_pattern, loc, re.IGNORECASE):
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
    """Fetch the most recent HTTP 200 Wayback Machine snapshot within 7 days via CDX API."""
    try:
        from_date = (_utc_now() - dt.timedelta(days=7)).strftime("%Y%m%d")
        cdx_url = (
            f"https://web.archive.org/cdx/search/cdx?url={base_url}"
            f"&output=json&limit=-1&filter=statuscode:200&from={from_date}&fl=timestamp,original"
        )
        cdx_resp = requests.get(cdx_url, timeout=15)
        if cdx_resp.status_code != 200:
            return []
        rows = cdx_resp.json() or []
        if len(rows) < 2:
            return []
        timestamp, original = rows[-1][0], rows[-1][1]
        snapshot_url = f"https://web.archive.org/web/{timestamp}id_/{original}"
        snap_resp = requests.get(snapshot_url, headers=HTML_BROWSER_HEADERS, timeout=25)
        if snap_resp.status_code != 200:
            return []
        return _parse_html_articles(
            decode_response_text(snap_resp), base_url, label, url_pattern, ignore_patterns, max_items, normalize_url_fn
        )
    except Exception:
        return []


def scrape_trendforce_news(base_url, label, max_items, normalize_url_fn=_normalize_url):
    """Dedicated scraper for TrendForce News (clean UTF-8 HTML -> WP feed -> Jina Reader -> Wayback)."""
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

    items = _scrape_via_jina_reader(news_url, label, tf_pattern, ignore_patterns, max_items, normalize_url_fn)
    if items:
        log.info(f"scraper[TrendForce]: extracted {len(items)} items via Jina Reader")
        return items

    items = _scrape_wayback_html_fallback(news_url, label, tf_pattern, ignore_patterns, max_items, normalize_url_fn)
    if items:
        log.info(f"scraper[TrendForce]: recovered {len(items)} items via Wayback snapshot")
    return items


def scrape_datacenterdynamics(base_url, label, max_items, normalize_url_fn=_normalize_url):
    """Dedicated scraper for Data Center Dynamics (clean TLS impersonation -> Jina Reader -> XML sitemap -> Wayback)."""
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
            log.info(f"scraper[DCD]: HTML returned HTTP {resp.status_code}; trying Jina Reader & sitemap...")
    except Exception as exc:
        log.info(f"scraper[DCD]: HTML fetch issue: {repr(exc)}")

    items = _scrape_via_jina_reader(news_url, label, dcd_pattern, ignore_patterns, max_items, normalize_url_fn)
    if items:
        log.info(f"scraper[DCD]: extracted {len(items)} items via Jina Reader")
        return items

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
    """Dedicated scraper for HPCwire (WP REST API -> TLS-impersonated RSS -> HTML -> Jina Reader -> Wayback)."""
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
            log.info(f"scraper[HPCwire]: HTML returned HTTP {resp.status_code}; trying Jina Reader & Wayback...")
    except Exception as exc:
        log.info(f"scraper[HPCwire]: HTML fetch issue: {repr(exc)}")

    items = _scrape_via_jina_reader(site_url, label, hpc_pattern, ignore_patterns, max_items, normalize_url_fn)
    if items:
        log.info(f"scraper[HPCwire]: extracted {len(items)} items via Jina Reader")
        return items

    items = _scrape_wayback_html_fallback(site_url, label, hpc_pattern, ignore_patterns, max_items, normalize_url_fn)
    if items:
        log.info(f"scraper[HPCwire]: recovered {len(items)} items via Wayback snapshot")
    return items


def scrape_ocp_blog(base_url, label, max_items, normalize_url_fn=_normalize_url):
    """Dedicated scraper for Open Compute Project Blog (clean TLS-impersonated HTML -> Jina Reader -> XML sitemap -> Wayback)."""
    parsed_in = urlsplit(base_url or "")
    # Normalize root/legacy RSS URLs to the canonical OCP news/blog listing page
    if parsed_in.path.rstrip("/") in ("", "/blog/rss", "/blog/feed"):
        blog_url = "https://www.opencompute.org/blog"
    else:
        blog_url = base_url or "https://www.opencompute.org/blog"

    ocp_pattern = r"/blog/(?!(?:author|tag|category|rss|feed|page|search)(?:/|$))[a-z0-9][a-z0-9-]{3,}/?$"
    ignore_patterns = re.compile(
        r"/(about|contact|privacy|terms|login|register|search|author|tag|category|rss|feed)/?",
        re.IGNORECASE,
    )

    try:
        resp = browser_get(blog_url, timeout=20)
        if resp.status_code == 200:
            items = _parse_html_articles(
                decode_response_text(resp), blog_url, label, ocp_pattern, ignore_patterns, max_items, normalize_url_fn
            )
            if items:
                log.info(f"scraper[OCP]: extracted {len(items)} items from HTML")
                return items
        else:
            log.info(f"scraper[OCP]: HTML returned HTTP {resp.status_code}; trying Jina Reader & sitemap...")
    except Exception as exc:
        log.info(f"scraper[OCP]: HTML fetch issue: {repr(exc)}")

    items = _scrape_via_jina_reader(blog_url, label, ocp_pattern, ignore_patterns, max_items, normalize_url_fn)
    if items:
        log.info(f"scraper[OCP]: extracted {len(items)} items via Jina Reader")
        return items

    items = _scrape_xml_sitemap(
        [
            "https://www.opencompute.org/sitemap.xml",
            "https://www.opencompute.org/blog/sitemap.xml",
        ],
        label,
        ocp_pattern,
        max_items,
        normalize_url_fn,
    )
    if items:
        log.info(f"scraper[OCP]: extracted {len(items)} items from XML sitemap")
        return items

    items = _scrape_wayback_html_fallback(blog_url, label, ocp_pattern, ignore_patterns, max_items, normalize_url_fn)
    if items:
        log.info(f"scraper[OCP]: recovered {len(items)} items via Wayback snapshot")
    return items


DEDICATED_SCRAPERS = {
    "trendforce.com": scrape_trendforce_news,
    "datacenterdynamics.com": scrape_datacenterdynamics,
    "hpcwire.com": scrape_hpcwire,
    "opencompute.org": scrape_ocp_blog,
}


def scrape_single_html_target(label, target, max_items=12, normalize_url_fn=_normalize_url):
    """Dispatch to a dedicated per-source scraper when available, or fall back to
    the universal browser-impersonated HTML scraper."""
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
        site_items = _scrape_via_jina_reader(
            base_url, label, url_pattern, ignore_patterns, max_items, normalize_url_fn
        )
        if site_items:
            log.info(f"html_scraper: recovered {len(site_items)} items for '{label}' via Jina Reader")

    if not site_items:
        site_items = _scrape_wayback_html_fallback(
            base_url, label, url_pattern, ignore_patterns, max_items, normalize_url_fn
        )
        if site_items:
            log.info(f"html_scraper: recovered {len(site_items)} items for '{label}' via Wayback HTML snapshot")

    site_items.sort(key=lambda x: x["published_at"], reverse=True)
    log.info(f"html_scraper: scraped {len(site_items)} items from '{label}'")
    return site_items
