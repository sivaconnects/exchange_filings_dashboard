"""
NSE Exchange Filings Scraper  ─  Web Scraping Edition
=======================================================
Uses NSE's internal JSON endpoints with proper session/cookie handling.
Falls back to HTML scraping via BeautifulSoup if JSON endpoints fail.

KEY REQUIREMENT: NSE's API only works after setting up a real browser session
(visiting home page to get cookies). Without the session, all API calls fail.
"""

import requests
import json
import time
import logging
import re
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

try:
    from xbrl_parser import bulk_enrich
except ImportError:
    def bulk_enrich(filings, *a, **kw): return filings

try:
    from state_tracker import make_filing_id
except ImportError:
    def make_filing_id(f):
        return f"{f.get('exchange','')}-{f.get('symbol','')}-{f.get('filing_date','')}-{f.get('subject','')[:30]}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────
BASE     = "https://www.nseindia.com"
ARCHIVES = "https://nsearchives.nseindia.com"

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
}

JSON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
    "Connection": "keep-alive",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "X-Requested-With": "XMLHttpRequest",
}

SESSION = requests.Session()
_SESSION_READY = False


# ─── Session ──────────────────────────────────────────────────────────

def init_session():
    """Visit NSE pages to acquire session cookies before making API calls."""
    global _SESSION_READY
    if _SESSION_READY:
        return
    try:
        logger.info("Initialising NSE session...")
        # Step 1: Home page — sets nseappid cookie
        r = SESSION.get(BASE, headers=BROWSER_HEADERS, timeout=20)
        logger.info(f"NSE home: {r.status_code}")
        time.sleep(2)
        # Step 2: Corporate filings page — NSE checks Referer
        r2 = SESSION.get(
            f"{BASE}/companies-listing/corporate-filings-announcements",
            headers=BROWSER_HEADERS, timeout=20
        )
        time.sleep(2)
        # Step 3: Market data page (builds realistic cookie profile)
        SESSION.get(f"{BASE}/market-data/live-equity-market", headers=BROWSER_HEADERS, timeout=15)
        time.sleep(1)
        _SESSION_READY = True
        logger.info(f"NSE session ready. Cookies: {list(SESSION.cookies.keys())}")
    except Exception as e:
        logger.error(f"NSE session init failed: {e}")


def _safe_get_json(url: str, retries: int = 3):
    global _SESSION_READY
    for attempt in range(retries):
        try:
            r = SESSION.get(url, headers=JSON_HEADERS, timeout=25)
            if r.status_code == 401:
                logger.warning("NSE 401 — refreshing session")
                _SESSION_READY = False
                init_session()
                time.sleep(2)
                continue
            r.raise_for_status()
            return r.json()
        except json.JSONDecodeError:
            logger.warning(f"NSE response not JSON at {url}: {r.text[:100]}")
            return None
        except Exception as e:
            wait = 2 ** attempt
            logger.warning(f"NSE GET attempt {attempt+1} — {url}: {e}. Retry in {wait}s")
            time.sleep(wait)
    return None


def _today() -> str:
    return datetime.now().strftime("%d-%m-%Y")


def _parse_items(raw) -> list:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("data", "Data", "result", "Result", "announcements"):
            val = raw.get(key)
            if isinstance(val, list):
                return val
        for val in raw.values():
            if isinstance(val, list) and val:
                return val
    return []


def _att_url(raw: str) -> str:
    if not raw:
        return ""
    raw = raw.strip()
    if raw.startswith("http"):
        return raw
    return f"{ARCHIVES}/{raw.lstrip('/')}"


def _base_filing(item: dict, segment: str, category: str) -> dict:
    att_raw = (
        item.get("attchmnt") or item.get("sm_filingtm") or
        item.get("attachment") or item.get("attcmt") or ""
    ).strip()
    att_full = _att_url(att_raw)
    is_xbrl = att_full.lower().endswith((".xml", ".xbrl"))

    filing = {
        "exchange":       "NSE",
        "segment":        segment,
        "category":       category,
        "filing_id":      str(item.get("an_id") or item.get("seqno") or item.get("bm_id") or ""),
        "symbol":         item.get("symbol") or item.get("sm_symbol") or "",
        "company":        item.get("company") or item.get("sm_name") or item.get("companyName") or "",
        "subject":        item.get("subject") or item.get("anct_subject") or item.get("desc") or item.get("purpose") or "",
        "description":    item.get("body") or item.get("desc") or item.get("bm_desc") or "",
        "filing_date":    item.get("exchdisstime") or item.get("an_dt") or item.get("filingDate") or "",
        "xbrl_url":       att_full if is_xbrl else "",
        "attachment_url": att_full if not is_xbrl else "",
        "detail_url":     f"{BASE}/companies-listing/corporate-filings-announcements",
        "scraped_at":     datetime.now().isoformat(),
    }
    filing["filing_id"] = filing["filing_id"] or make_filing_id(filing)
    return filing


# ─── Category Fetchers ────────────────────────────────────────────────

def fetch_announcements(segment: str = "equities") -> list:
    seg_label = "EQUITY" if segment == "equities" else "SME"
    td = _today()
    url = f"{BASE}/api/corporate-announcements?index={segment}&from_date={td}&to_date={td}"
    raw = _safe_get_json(url)
    items = _parse_items(raw)
    logger.info(f"NSE announcements raw: {len(items)} [{segment}]")

    out = [_base_filing(item, seg_label, "Corporate Announcement") for item in items]
    out = bulk_enrich(out, SESSION, JSON_HEADERS)
    logger.info(f"NSE {segment} announcements: {len(out)}")
    return out


def fetch_board_meetings(segment: str = "equities") -> list:
    seg_label = "EQUITY" if segment == "equities" else "SME"
    td = _today()
    future = (datetime.now() + timedelta(days=30)).strftime("%d-%m-%Y")

    # Try current-day window first, then future window (board meetings are often scheduled ahead)
    for date_range in [(td, td), (td, future)]:
        url = f"{BASE}/api/corporate-board-meetings?index={segment}&from_date={date_range[0]}&to_date={date_range[1]}"
        raw = _safe_get_json(url)
        items = _parse_items(raw)
        if items:
            break
        time.sleep(1)

    logger.info(f"NSE board meetings raw: {len(items)} [{segment}]")
    out = []
    for item in items:
        f = _base_filing(item, seg_label, "Board Meeting")
        f["subject"]      = item.get("purpose") or item.get("bm_desc") or item.get("anct_subject") or item.get("subject") or ""
        f["description"]  = item.get("bm_desc") or item.get("desc") or ""
        f["meeting_date"] = item.get("bm_date") or item.get("meetingDate") or item.get("bm_dt") or ""
        f["filing_date"]  = item.get("an_dt") or item.get("exchdisstime") or f["meeting_date"]
        f["detail_url"]   = f"{BASE}/companies-listing/corporate-filings-board-meetings"
        out.append(f)

    out = bulk_enrich(out, SESSION, JSON_HEADERS)
    logger.info(f"NSE {segment} board meetings: {len(out)}")
    return out


def fetch_financial_results(segment: str = "equities") -> list:
    seg_label = "EQUITY" if segment == "equities" else "SME"
    td = _today()

    items = []
    for period in ["Quarterly", ""]:
        period_param = f"&period={period}" if period else ""
        url = f"{BASE}/api/corporates-financial-results?index={segment}&from_date={td}&to_date={td}{period_param}"
        raw = _safe_get_json(url)
        found = _parse_items(raw)
        if found:
            items = found
            break
        time.sleep(1)

    logger.info(f"NSE financial results raw: {len(items)} [{segment}]")
    out = []
    for item in items:
        f = _base_filing(item, seg_label, "Financial Result")
        att_raw = item.get("attachment") or item.get("attchmnt") or ""
        f["attachment_url"] = _att_url(att_raw)
        f["xbrl_url"]    = ""
        f["period"]      = item.get("period") or item.get("quarterEnded") or ""
        f["result_type"] = item.get("resultType") or "Quarterly"
        f["filing_date"] = item.get("filingDate") or item.get("an_dt") or item.get("exchdisstime") or ""
        f["subject"]     = item.get("desc") or item.get("subject") or f"Results {f['period']}"
        f["detail_url"]  = f"{BASE}/companies-listing/corporate-filings-financial-results"
        out.append(f)

    logger.info(f"NSE {segment} results: {len(out)}")
    return out


def fetch_corporate_actions(segment: str = "equities") -> list:
    seg_label = "EQUITY" if segment == "equities" else "SME"
    td = _today()
    future = (datetime.now() + timedelta(days=7)).strftime("%d-%m-%Y")
    url = f"{BASE}/api/corporates-corporate-actions?index={segment}&from_date={td}&to_date={future}"
    raw = _safe_get_json(url)
    items = _parse_items(raw)
    logger.info(f"NSE corporate actions raw: {len(items)} [{segment}]")

    out = []
    for item in items:
        f = _base_filing(item, seg_label, "Corporate Action")
        f["company"]     = item.get("company") or item.get("comp") or item.get("companyName") or ""
        f["subject"]     = item.get("subject") or item.get("action") or ""
        f["action"]      = f["subject"]
        f["ex_date"]     = item.get("exDate") or item.get("ex_date") or ""
        f["record_date"] = item.get("recDate") or item.get("record_date") or ""
        f["remarks"]     = item.get("remarks") or ""
        f["filing_date"] = item.get("an_dt") or item.get("exchdisstime") or f["ex_date"]
        f["detail_url"]  = f"{BASE}/companies-listing/corporate-filings-corporate-actions"
        out.append(f)

    logger.info(f"NSE {segment} actions: {len(out)}")
    return out


def fetch_shareholding(segment: str = "equities") -> list:
    seg_label = "EQUITY" if segment == "equities" else "SME"
    td = _today()
    url = f"{BASE}/api/corporate-share-holdings-master?index={segment}&from_date={td}&to_date={td}"
    raw = _safe_get_json(url)
    items = _parse_items(raw)
    logger.info(f"NSE shareholding raw: {len(items)} [{segment}]")

    out = []
    for item in items:
        f = _base_filing(item, seg_label, "Shareholding Pattern")
        att_raw = item.get("attachment") or item.get("attchmnt") or ""
        f["attachment_url"]   = _att_url(att_raw)
        f["xbrl_url"]         = ""
        f["period"]           = item.get("period") or item.get("quarter") or ""
        f["filing_date"]      = item.get("filingDate") or item.get("an_dt") or ""
        f["promoter_holding"] = str(item.get("promoterHolding") or item.get("promoter") or "")
        f["public_holding"]   = str(item.get("publicHolding") or item.get("public") or "")
        f["fii_holding"]      = str(item.get("fiiHolding") or "")
        f["dii_holding"]      = str(item.get("diiHolding") or "")
        f["detail_url"]       = f"{BASE}/companies-listing/corporate-filings-shareholding-pattern"
        out.append(f)

    logger.info(f"NSE {segment} shareholding: {len(out)}")
    return out


# ─── HTML Fallback ────────────────────────────────────────────────────

def _scrape_html_fallback(segment: str = "equities") -> list:
    """Last resort: parse announcements table from NSE HTML."""
    seg_label = "EQUITY" if segment == "equities" else "SME"
    try:
        url = f"{BASE}/companies-listing/corporate-filings-announcements"
        r = SESSION.get(url, headers=BROWSER_HEADERS, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")
        out = []

        table = None
        for t in soup.find_all("table"):
            ths = [th.text.strip().lower() for th in t.find_all("th")]
            if any("symbol" in th for th in ths):
                table = t
                break

        if table:
            for row in table.find_all("tr")[1:100]:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) < 3:
                    continue
                a_tag = row.find("a", href=re.compile(r"\.pdf|attachment|xbrl", re.I))
                att = ""
                if a_tag:
                    href = a_tag.get("href", "")
                    att = href if href.startswith("http") else f"{BASE}{href}"
                f = {
                    "exchange": "NSE", "segment": seg_label,
                    "category": "Corporate Announcement",
                    "symbol":   cells[0] if len(cells) > 0 else "",
                    "company":  cells[1] if len(cells) > 1 else "",
                    "subject":  cells[2] if len(cells) > 2 else "",
                    "filing_date": cells[-1] if cells else "",
                    "attachment_url": att, "xbrl_url": "",
                    "scraped_at": datetime.now().isoformat(),
                }
                f["filing_id"] = make_filing_id(f)
                out.append(f)

        logger.info(f"NSE HTML fallback: {len(out)} [{segment}]")
        return out
    except Exception as e:
        logger.error(f"NSE HTML fallback failed: {e}")
        return []


# ─── Main Entry ───────────────────────────────────────────────────────

def scrape_all() -> dict:
    init_session()

    result = {"equity": {}, "sme": {}}
    segment_map = {"equity": "equities", "sme": "sme"}

    for key, seg in segment_map.items():
        logger.info(f"--- NSE scraping: {key} ---")

        anns = fetch_announcements(seg)
        if not anns:
            logger.info(f"NSE JSON announcements empty for {seg}, trying HTML fallback")
            anns = _scrape_html_fallback(seg)
        result[key]["corporate_announcements"] = anns
        time.sleep(2)

        result[key]["board_meetings"]        = fetch_board_meetings(seg);    time.sleep(2)
        result[key]["financial_results"]     = fetch_financial_results(seg); time.sleep(2)
        result[key]["corporate_actions"]     = fetch_corporate_actions(seg); time.sleep(2)
        result[key]["shareholding_patterns"] = fetch_shareholding(seg);      time.sleep(2)

    return result


if __name__ == "__main__":
    d = scrape_all()
    for seg in ("equity", "sme"):
        for cat, items in d[seg].items():
            print(f"  NSE {seg} {cat}: {len(items)}")
    print(json.dumps(d, indent=2, default=str)[:3000])
