"""
BSE Exchange Filings Scraper  ─  Production Grade
===================================================
Covers BSE's own categories (separate from NSE):
  • AGM / EGM
  • Board Meetings
  • Company Updates
  • Corporate Actions
  • Insider Trading / SAST
  • New Listings
  • Results  (Quarterly / Half-Yearly / Annual)
  • Integrated Filings
  • Others / All

Both Equity (Main Board) and SME (BSE SME / Emerge) segments.

BSE attachment URL pattern:
  https://www.bseindia.com/xml-data/corpfiling/AttachLive/{ATTACHMENTNAME}
"""

import requests
import json
import time
import logging
from datetime import datetime

from state_tracker import make_filing_id

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────
BSE_HOME = "https://www.bseindia.com"
BSE_API  = "https://api.bseindia.com/BseIndiaAPI/api"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Origin":  "https://www.bseindia.com",
    "Referer": "https://www.bseindia.com/corporates/ann.html",
    "Connection": "keep-alive",
    "DNT": "1",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# BSE category code → human label
BSE_CATEGORIES = {
    "-1":            "All Announcements",
    "AGM":           "AGM / EGM",
    "BM":            "Board Meeting",
    "Company Update": "Company Update",
    "Corp. Action":  "Corporate Action",
    "insider":       "Insider Trading / SAST",
    "New Listing":   "New Listing",
    "Result":        "Financial Result",
    "Integrated":    "Integrated Filings",
    "Others":        "Others",
}

# Segment code
_SEG_CODE = {"equity": "E", "sme": "ES"}


# ─── Session ──────────────────────────────────────────────────────────

def init_session():
    try:
        SESSION.get(BSE_HOME, timeout=15)
        time.sleep(2)
        SESSION.get(f"{BSE_HOME}/corporates/ann.html", timeout=15)
        time.sleep(1)
        logger.info("BSE session initialised")
    except Exception as e:
        logger.error(f"BSE session init failed: {e}")


# ─── Helpers ──────────────────────────────────────────────────────────

def _today() -> str:
    """BSE expects dd/MM/YYYY"""
    return datetime.now().strftime("%d/%m/%Y")


def _att_url(filename: str) -> str:
    if not filename:
        return ""
    filename = filename.strip()
    if filename.startswith("http"):
        return filename
    return f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{filename}"


def _safe_get(url: str, params: dict = None, retries: int = 3):
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=20)
            r.raise_for_status()
            ct = r.headers.get("Content-Type", "")
            if "json" in ct:
                return r.json()
            return None
        except Exception as e:
            wait = 2 ** attempt
            logger.warning(f"BSE GET attempt {attempt+1} failed → {url} : {e}. Retry in {wait}s")
            time.sleep(wait)
    return None


def _extract_items(raw) -> list:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("Table", "Table1", "data", "Data", "Result"):
            val = raw.get(key)
            if isinstance(val, list) and val:
                return val
    return []


# ─── Announcements (all BSE categories via single endpoint) ───────────

def fetch_by_category(cat_code: str, segment: str = "equity") -> list:
    """
    Generic fetch for any BSE announcement category.
    Uses BSE's AnnGetData endpoint.
    """
    today = _today()
    seg_code = _SEG_CODE.get(segment, "E")
    cat_label = BSE_CATEGORIES.get(cat_code, cat_code)

    params = {
        "strCat":      cat_code,
        "strType":     "C",
        "strScrip":    "",
        "strSearch":   "P",
        "strToDate":   today,
        "strFromDate": today,
        "mykey":       "announcements",
        "segment":     seg_code,
    }
    raw = _safe_get(f"{BSE_API}/AnnGetData/w", params)
    items = _extract_items(raw)

    out = []
    for item in items:
        att_file = item.get("ATTACHMENTNAME", item.get("Attachmentname", ""))
        filing = {
            "exchange":       "BSE",
            "segment":        segment.upper(),
            "category":       cat_label,
            "bse_category":   cat_code,
            "filing_id":      str(item.get("NEWSID", item.get("NewsId", ""))),
            "symbol":         str(item.get("SCRIP_CD", item.get("ScripCode", ""))),
            "company":        item.get("SLONGNAME",    item.get("LongName", "")),
            "subject":        item.get("HEADLINE",     item.get("Headline", "")),
            "description":    item.get("NEWSSUB",      item.get("NewsSub", "")),
            "filing_date":    item.get("NEWS_DT",      item.get("NewsDate", "")),
            "subcategory":    item.get("SUBCATEGORYNAME", item.get("SubCatName", "")),
            "attachment_url": _att_url(att_file),
            "detail_url":     f"{BSE_HOME}/corporates/ann.html",
            "scraped_at":     datetime.now().isoformat(),
        }
        filing["filing_id"] = filing["filing_id"] or make_filing_id(filing)
        out.append(filing)

    logger.info(f"BSE {segment} [{cat_label}]: {len(out)}")
    return out


# ─── Dedicated Board Meetings endpoint ────────────────────────────────

def fetch_board_meetings(segment: str = "equity") -> list:
    today = _today()
    seg_code = _SEG_CODE.get(segment, "E")

    params = {
        "strFromDate": today,
        "strToDate":   today,
        "strScrip":    "",
        "segment":     seg_code,
    }
    raw = _safe_get(f"{BSE_API}/BoardMeetings/w", params)
    items = _extract_items(raw)

    if not items:
        # Fallback to generic category fetch
        return fetch_by_category("BM", segment)

    out = []
    for item in items:
        filing = {
            "exchange":     "BSE",
            "segment":      segment.upper(),
            "category":     "Board Meeting",
            "bse_category": "BM",
            "filing_id":    str(item.get("NEWSID", item.get("NewsId", ""))),
            "symbol":       str(item.get("SCRIP_CD", item.get("ScripCode", ""))),
            "company":      item.get("SLONGNAME", item.get("LongName", "")),
            "subject":      item.get("PURPOSE",    item.get("Purpose", "")),
            "description":  item.get("REMARKS",    item.get("Remarks", "")),
            "meeting_date": item.get("MEETING_DATE", item.get("MeetingDate", "")),
            "filing_date":  item.get("NEWS_DT",    item.get("NewsDate", "")),
            "attachment_url": "",
            "detail_url":   f"{BSE_HOME}/corporates/boardmeetings.html",
            "scraped_at":   datetime.now().isoformat(),
        }
        filing["filing_id"] = filing["filing_id"] or make_filing_id(filing)
        out.append(filing)

    logger.info(f"BSE {segment} board meetings (dedicated): {len(out)}")
    return out


# ─── Shareholding Pattern ─────────────────────────────────────────────

def fetch_shareholding(segment: str = "equity") -> list:
    today = _today()
    seg_code = _SEG_CODE.get(segment, "E")

    params = {
        "strFromDate": today,
        "strToDate":   today,
        "strScrip":    "",
        "segment":     seg_code,
    }
    raw = _safe_get(f"{BSE_API}/ShareholdingPatterns/w", params)
    items = _extract_items(raw)

    out = []
    for item in items:
        att_file = item.get("ATTACHMENTNAME", "")
        filing = {
            "exchange":          "BSE",
            "segment":           segment.upper(),
            "category":          "Shareholding Pattern",
            "bse_category":      "Shareholding",
            "filing_id":         str(item.get("NEWSID", "")),
            "symbol":            str(item.get("SCRIP_CD", "")),
            "company":           item.get("SLONGNAME", ""),
            "period":            item.get("QUARTER",   item.get("Period", "")),
            "filing_date":       item.get("NEWS_DT",   ""),
            "promoter_holding":  str(item.get("PROMOTER_HOLDING", "")),
            "public_holding":    str(item.get("PUBLIC_HOLDING",   "")),
            "fii_holding":       str(item.get("FII_HOLDING",      "")),
            "dii_holding":       str(item.get("DII_HOLDING",      "")),
            "attachment_url":    _att_url(att_file),
            "detail_url":        f"{BSE_HOME}/corporates/shareholding-pattern.html",
            "scraped_at":        datetime.now().isoformat(),
        }
        filing["filing_id"] = filing["filing_id"] or make_filing_id(filing)
        out.append(filing)

    logger.info(f"BSE {segment} shareholding: {len(out)}")
    return out


# ─── Main Entry ───────────────────────────────────────────────────────

def scrape_all() -> dict:
    init_session()

    result = {"equity": {}, "sme": {}}

    for seg in ["equity", "sme"]:
        # All the BSE-specific category buckets
        result[seg]["agm_egm"]            = fetch_by_category("AGM",            seg); time.sleep(1)
        result[seg]["board_meetings"]     = fetch_board_meetings(seg);                time.sleep(1)
        result[seg]["company_updates"]    = fetch_by_category("Company Update",  seg); time.sleep(1)
        result[seg]["corporate_actions"]  = fetch_by_category("Corp. Action",    seg); time.sleep(1)
        result[seg]["insider_trading"]    = fetch_by_category("insider",         seg); time.sleep(1)
        result[seg]["new_listings"]       = fetch_by_category("New Listing",     seg); time.sleep(1)
        result[seg]["results"]            = fetch_by_category("Result",          seg); time.sleep(1)
        result[seg]["integrated_filings"] = fetch_by_category("Integrated",      seg); time.sleep(1)
        result[seg]["others"]             = fetch_by_category("Others",          seg); time.sleep(1)
        result[seg]["shareholding_patterns"] = fetch_shareholding(seg);               time.sleep(1)

    return result


if __name__ == "__main__":
    d = scrape_all()
    print(json.dumps(d, indent=2, default=str)[:4000])
