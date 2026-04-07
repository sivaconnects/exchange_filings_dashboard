"""
BSE Exchange Filings Scraper  ─  Web Scraping Edition
=======================================================
Uses BSE's public JSON API endpoints with proper session/cookie/header handling.
Falls back to BeautifulSoup HTML scraping if JSON fails.

BSE API base: https://api.bseindia.com/BseIndiaAPI/api
BSE PDF base: https://www.bseindia.com/xml-data/corpfiling/AttachLive/

Categories covered (BSE-specific):
  AGM/EGM, Board Meetings, Company Updates, Corporate Actions,
  Insider Trading/SAST, New Listings, Results, Integrated Filings, Others

IMPORTANT: BSE API requires Referer and Origin headers from bseindia.com.
Without correct session + CORS headers, the API returns 403 or empty.
"""

import requests
import json
import time
import logging
import re
from datetime import datetime
from bs4 import BeautifulSoup

try:
    from state_tracker import make_filing_id
except ImportError:
    def make_filing_id(f):
        return f"{f.get('exchange','')}-{f.get('symbol','')}-{f.get('filing_date','')}-{f.get('subject','')[:30]}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────
BSE_HOME = "https://www.bseindia.com"
BSE_API  = "https://api.bseindia.com/BseIndiaAPI/api"

# Headers for HTML page loads
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
}

# Headers for API/JSON calls — MUST include Origin + Referer from bseindia.com
API_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Origin":  "https://www.bseindia.com",
    "Referer": "https://www.bseindia.com/corporates/ann.html",
    "Connection": "keep-alive",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
}

SESSION = requests.Session()

# BSE category code → label
BSE_CAT_MAP = {
    "AGM":              "AGM / EGM",
    "BM":               "Board Meeting",
    "Company Update":   "Company Update",
    "Corp. Action":     "Corporate Action",
    "insider":          "Insider Trading / SAST",
    "New Listing":      "New Listing",
    "Result":           "Financial Result",
    "Integrated":       "Integrated Filings",
    "Others":           "Others",
    "-1":               "All Announcements",
}

# Segment code for BSE API
_SEG_CODE = {"equity": "E", "sme": "ES"}


# ─── Session ──────────────────────────────────────────────────────────

def init_session():
    """Visit BSE home and announcements page to get session cookies."""
    try:
        logger.info("Initialising BSE session...")
        # Step 1: Home page
        SESSION.get(BSE_HOME, headers=BROWSER_HEADERS, timeout=20)
        time.sleep(2)
        # Step 2: Corporate announcements page
        SESSION.get(f"{BSE_HOME}/corporates/ann.html", headers=BROWSER_HEADERS, timeout=20)
        time.sleep(2)
        # Step 3: Another page to build cookie profile
        SESSION.get(f"{BSE_HOME}/corporates/boardmeetings.html", headers=BROWSER_HEADERS, timeout=15)
        time.sleep(1)
        logger.info(f"BSE session ready. Cookies: {list(SESSION.cookies.keys())}")
    except Exception as e:
        logger.error(f"BSE session init failed: {e}")


# ─── Helpers ──────────────────────────────────────────────────────────

def _today() -> str:
    """BSE API expects dd/MM/YYYY."""
    return datetime.now().strftime("%d/%m/%Y")


def _att_url(filename: str) -> str:
    if not filename:
        return ""
    filename = filename.strip()
    if filename.startswith("http"):
        return filename
    # BSE attachment pattern
    return f"{BSE_HOME}/xml-data/corpfiling/AttachLive/{filename}"


def _safe_get(url: str, params: dict = None, retries: int = 3):
    """GET with BSE API headers + retry."""
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, headers=API_HEADERS, timeout=25)
            if r.status_code in (401, 403):
                logger.warning(f"BSE {r.status_code} — re-initialising session")
                init_session()
                continue
            r.raise_for_status()
            ct = r.headers.get("Content-Type", "")
            if "json" in ct or url.endswith("/w"):
                try:
                    return r.json()
                except Exception:
                    logger.debug(f"BSE response not JSON: {r.text[:200]}")
                    return None
            return None
        except Exception as e:
            wait = 2 ** attempt
            logger.warning(f"BSE GET attempt {attempt+1} — {url}: {e}. Retry in {wait}s")
            time.sleep(wait)
    return None


def _extract_items(raw) -> list:
    """Extract list of items from various BSE API response shapes."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("Table", "Table1", "data", "Data", "Result", "result", "announcements"):
            val = raw.get(key)
            if isinstance(val, list) and val:
                return val
        # Last resort: any list value
        for val in raw.values():
            if isinstance(val, list) and val:
                return val
    return []


def _build_filing(item: dict, segment: str, category: str) -> dict:
    att_file = (
        item.get("ATTACHMENTNAME") or item.get("Attachmentname") or
        item.get("AttachmentName") or item.get("attachmentname") or ""
    )
    return {
        "exchange":       "BSE",
        "segment":        segment.upper(),
        "category":       category,
        "bse_category":   category,
        "filing_id":      str(item.get("NEWSID") or item.get("NewsId") or item.get("newsid") or ""),
        "symbol":         str(item.get("SCRIP_CD") or item.get("ScripCode") or item.get("scripcd") or ""),
        "company":        item.get("SLONGNAME") or item.get("LongName") or item.get("longname") or "",
        "subject":        item.get("HEADLINE") or item.get("Headline") or item.get("headline") or "",
        "description":    item.get("NEWSSUB") or item.get("NewsSub") or item.get("newssub") or "",
        "filing_date":    item.get("NEWS_DT") or item.get("NewsDate") or item.get("newsdate") or "",
        "subcategory":    item.get("SUBCATEGORYNAME") or item.get("SubCatName") or "",
        "attachment_url": _att_url(att_file),
        "detail_url":     f"{BSE_HOME}/corporates/ann.html",
        "scraped_at":     datetime.now().isoformat(),
    }


# ─── Primary API: AnnGetData ──────────────────────────────────────────
# BSE's main announcements endpoint used by their website

def fetch_by_category(cat_code: str, segment: str = "equity") -> list:
    """
    Fetch BSE announcements for a specific category.
    Uses the AnnGetData endpoint which powers BSE's own website.
    """
    today = _today()
    seg_code = _SEG_CODE.get(segment, "E")
    cat_label = BSE_CAT_MAP.get(cat_code, cat_code)

    # BSE API: multiple endpoint variants (the website uses these)
    endpoints = [
        f"{BSE_API}/AnnGetData/w",
        f"{BSE_API}/AnnouncementCat/w",
        f"{BSE_API}/Announcements/w",
    ]

    params_variants = [
        {
            "strCat":      cat_code,
            "strType":     "C",
            "strScrip":    "",
            "strSearch":   "P",
            "strToDate":   today,
            "strFromDate": today,
            "mykey":       "announcements",
            "segment":     seg_code,
        },
        {
            "Category":    cat_code,
            "Type":        "C",
            "ScripCode":   "",
            "Search":      "P",
            "ToDate":      today,
            "FromDate":    today,
            "Segment":     seg_code,
        },
    ]

    for endpoint in endpoints:
        for params in params_variants:
            raw = _safe_get(endpoint, params)
            items = _extract_items(raw)
            if items:
                logger.info(f"BSE {segment} [{cat_label}] via {endpoint.split('/')[-1]}: {len(items)}")
                out = []
                for item in items:
                    f = _build_filing(item, segment, cat_label)
                    f["filing_id"] = f["filing_id"] or make_filing_id(f)
                    out.append(f)
                return out
            time.sleep(0.5)

    logger.info(f"BSE {segment} [{cat_label}]: 0 (all endpoints tried)")
    return []


# ─── Board Meetings: dedicated endpoint ───────────────────────────────

def fetch_board_meetings(segment: str = "equity") -> list:
    today = _today()
    seg_code = _SEG_CODE.get(segment, "E")

    # Try dedicated board meetings endpoint first
    endpoints = [
        (f"{BSE_API}/BoardMeeting/w", {"strFromDate": today, "strToDate": today, "strScrip": "", "segment": seg_code}),
        (f"{BSE_API}/BoardMeetings/w", {"strFromDate": today, "strToDate": today, "strScrip": "", "segment": seg_code}),
        (f"{BSE_API}/Corpaction/w",    {"strFromDate": today, "strToDate": today, "strAction": "BM", "segment": seg_code}),
    ]

    for url, params in endpoints:
        raw = _safe_get(url, params)
        items = _extract_items(raw)
        if items:
            out = []
            for item in items:
                f = {
                    "exchange":     "BSE",
                    "segment":      segment.upper(),
                    "category":     "Board Meeting",
                    "bse_category": "Board Meeting",
                    "filing_id":    str(item.get("NEWSID") or item.get("NewsId") or ""),
                    "symbol":       str(item.get("SCRIP_CD") or item.get("ScripCode") or ""),
                    "company":      item.get("SLONGNAME") or item.get("LongName") or item.get("COMPANY_NAME") or "",
                    "subject":      item.get("PURPOSE") or item.get("Purpose") or item.get("HEADLINE") or "",
                    "description":  item.get("REMARKS") or item.get("Remarks") or item.get("NEWSSUB") or "",
                    "meeting_date": item.get("MEETING_DATE") or item.get("MeetingDate") or item.get("DT_TM") or "",
                    "filing_date":  item.get("NEWS_DT") or item.get("NewsDate") or item.get("MEETING_DATE") or "",
                    "attachment_url": "",
                    "detail_url":   f"{BSE_HOME}/corporates/boardmeetings.html",
                    "scraped_at":   datetime.now().isoformat(),
                }
                f["filing_id"] = f["filing_id"] or make_filing_id(f)
                out.append(f)
            logger.info(f"BSE {segment} board meetings (dedicated): {len(out)}")
            return out
        time.sleep(0.5)

    # Fallback to generic announcements category
    logger.info(f"BSE dedicated board meeting endpoint empty, using generic category")
    return fetch_by_category("BM", segment)


# ─── Results ──────────────────────────────────────────────────────────

def fetch_results(segment: str = "equity") -> list:
    today = _today()
    seg_code = _SEG_CODE.get(segment, "E")

    endpoints = [
        (f"{BSE_API}/FinancialResult/w", {"strFromDate": today, "strToDate": today, "strScrip": "", "segment": seg_code}),
        (f"{BSE_API}/FinancialResults/w", {"strFromDate": today, "strToDate": today, "segment": seg_code}),
    ]
    for url, params in endpoints:
        raw = _safe_get(url, params)
        items = _extract_items(raw)
        if items:
            out = []
            for item in items:
                att = item.get("ATTACHMENTNAME") or item.get("Attachmentname") or ""
                f = {
                    "exchange":       "BSE",
                    "segment":        segment.upper(),
                    "category":       "Financial Result",
                    "bse_category":   "Financial Result",
                    "filing_id":      str(item.get("NEWSID") or ""),
                    "symbol":         str(item.get("SCRIP_CD") or item.get("ScripCode") or ""),
                    "company":        item.get("SLONGNAME") or item.get("LongName") or "",
                    "subject":        item.get("HEADLINE") or item.get("Type_of_Meeting") or "Results",
                    "period":         item.get("PERIOD") or item.get("Period") or "",
                    "result_type":    item.get("RESULT_TYPE") or item.get("Type_of_Meeting") or "Quarterly",
                    "filing_date":    item.get("NEWS_DT") or item.get("NewsDate") or "",
                    "attachment_url": _att_url(att),
                    "detail_url":     f"{BSE_HOME}/corporates/results.aspx",
                    "scraped_at":     datetime.now().isoformat(),
                }
                f["filing_id"] = f["filing_id"] or make_filing_id(f)
                out.append(f)
            logger.info(f"BSE {segment} results (dedicated): {len(out)}")
            return out
        time.sleep(0.5)

    return fetch_by_category("Result", segment)


# ─── Shareholding ─────────────────────────────────────────────────────

def fetch_shareholding(segment: str = "equity") -> list:
    today = _today()
    seg_code = _SEG_CODE.get(segment, "E")

    endpoints = [
        (f"{BSE_API}/ShareHoldingPatterns/w", {"strFromDate": today, "strToDate": today, "strScrip": "", "segment": seg_code}),
        (f"{BSE_API}/ShareholdingPatterns/w",  {"strFromDate": today, "strToDate": today, "segment": seg_code}),
    ]
    for url, params in endpoints:
        raw = _safe_get(url, params)
        items = _extract_items(raw)
        if items:
            out = []
            for item in items:
                att = item.get("ATTACHMENTNAME") or ""
                f = {
                    "exchange":          "BSE",
                    "segment":           segment.upper(),
                    "category":          "Shareholding Pattern",
                    "bse_category":      "Shareholding Pattern",
                    "filing_id":         str(item.get("NEWSID") or ""),
                    "symbol":            str(item.get("SCRIP_CD") or ""),
                    "company":           item.get("SLONGNAME") or "",
                    "period":            item.get("QUARTER") or item.get("Period") or "",
                    "filing_date":       item.get("NEWS_DT") or "",
                    "promoter_holding":  str(item.get("PROMOTER_HOLDING") or ""),
                    "public_holding":    str(item.get("PUBLIC_HOLDING") or ""),
                    "fii_holding":       str(item.get("FII_HOLDING") or ""),
                    "dii_holding":       str(item.get("DII_HOLDING") or ""),
                    "attachment_url":    _att_url(att),
                    "detail_url":        f"{BSE_HOME}/corporates/shareholding-pattern.html",
                    "scraped_at":        datetime.now().isoformat(),
                }
                f["filing_id"] = f["filing_id"] or make_filing_id(f)
                out.append(f)
            logger.info(f"BSE {segment} shareholding: {len(out)}")
            return out
        time.sleep(0.5)

    logger.info(f"BSE {segment} shareholding: 0")
    return []


# ─── HTML Scraping Fallback ───────────────────────────────────────────
# If ALL JSON API endpoints fail, scrape the BSE website HTML directly

def _scrape_ann_html(segment: str = "equity") -> list:
    """
    Fallback: scrape BSE announcements table from HTML.
    BSE renders a paginated table on /corporates/ann.html.
    """
    try:
        url = f"{BSE_HOME}/corporates/ann.html"
        r = SESSION.get(url, headers=BROWSER_HEADERS, timeout=25)
        soup = BeautifulSoup(r.text, "html.parser")
        out = []

        # Look for the announcements table
        table = None
        for t in soup.find_all("table"):
            headers_txt = " ".join(th.text.lower() for th in t.find_all("th"))
            if "scrip" in headers_txt or "company" in headers_txt or "subject" in headers_txt:
                table = t
                break

        if table:
            for row in table.find_all("tr")[1:50]:  # max 50 rows
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if len(cells) < 3:
                    continue
                # Find attachment link
                a_tag = row.find("a", href=re.compile(r"(AttachLive|\.pdf)", re.I))
                att = ""
                if a_tag:
                    href = a_tag.get("href", "")
                    att = href if href.startswith("http") else f"{BSE_HOME}{href}"

                f = {
                    "exchange":     "BSE",
                    "segment":      segment.upper(),
                    "category":     "Corporate Announcement",
                    "bse_category": "All Announcements",
                    "symbol":       cells[0] if len(cells) > 0 else "",
                    "company":      cells[1] if len(cells) > 1 else "",
                    "subject":      cells[2] if len(cells) > 2 else "",
                    "filing_date":  cells[-1] if cells else "",
                    "attachment_url": att,
                    "detail_url":   url,
                    "scraped_at":   datetime.now().isoformat(),
                }
                f["filing_id"] = make_filing_id(f)
                out.append(f)

        logger.info(f"BSE HTML fallback: {len(out)} announcements [{segment}]")
        return out
    except Exception as e:
        logger.error(f"BSE HTML fallback failed: {e}")
        return []


# ─── Main Entry ───────────────────────────────────────────────────────

def scrape_all() -> dict:
    init_session()

    result = {"equity": {}, "sme": {}}

    for seg in ["equity", "sme"]:
        logger.info(f"--- BSE scraping: {seg} ---")

        result[seg]["agm_egm"]            = fetch_by_category("AGM",           seg); time.sleep(1.5)
        result[seg]["board_meetings"]     = fetch_board_meetings(seg);               time.sleep(1.5)
        result[seg]["company_updates"]    = fetch_by_category("Company Update", seg); time.sleep(1.5)
        result[seg]["corporate_actions"]  = fetch_by_category("Corp. Action",   seg); time.sleep(1.5)
        result[seg]["insider_trading"]    = fetch_by_category("insider",        seg); time.sleep(1.5)
        result[seg]["new_listings"]       = fetch_by_category("New Listing",    seg); time.sleep(1.5)
        result[seg]["results"]            = fetch_results(seg);                      time.sleep(1.5)
        result[seg]["integrated_filings"] = fetch_by_category("Integrated",     seg); time.sleep(1.5)
        result[seg]["others"]             = fetch_by_category("Others",         seg); time.sleep(1.5)
        result[seg]["shareholding_patterns"] = fetch_shareholding(seg);              time.sleep(1.5)

        # Build combined "corporate_announcements" from all categories for the dashboard
        all_ann = []
        for items in result[seg].values():
            if isinstance(items, list):
                all_ann.extend(items)
        result[seg]["corporate_announcements"] = all_ann

        # If everything is zero, try HTML fallback
        total = sum(len(v) for v in result[seg].values() if isinstance(v, list))
        if total == 0:
            logger.warning(f"BSE {seg}: all JSON endpoints empty, trying HTML fallback")
            html_data = _scrape_ann_html(seg)
            if html_data:
                result[seg]["corporate_announcements"] = html_data

    return result


if __name__ == "__main__":
    d = scrape_all()
    for seg in ("equity", "sme"):
        for cat, items in d[seg].items():
            if cat != "corporate_announcements":  # skip merged
                print(f"  BSE {seg} {cat}: {len(items)}")
    print(json.dumps(d, indent=2, default=str)[:3000])
