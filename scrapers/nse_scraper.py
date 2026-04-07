"""
NSE Exchange Filings Scraper  ─  Production Grade
===================================================
Covers:
  • Equity & SME segments
  • Corporate Announcements  (with XBRL parsing for attachment PDF + description)
  • Board Meetings
  • Financial Results
  • Corporate Actions
  • Shareholding Patterns

Key improvements over v1:
  - XBRL files are parsed to extract real PDF attachment URLs and full descriptions
  - Board meetings: all fields correctly mapped
  - Filing IDs tracked for incremental scraping
  - Proper retry logic
"""

import requests
import json
import time
import logging
from datetime import datetime

from xbrl_parser import bulk_enrich
from state_tracker import make_filing_id

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────
BASE = "https://www.nseindia.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
    "DNT": "1",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

_SESSION_READY = False


# ─── Session ──────────────────────────────────────────────────────────

def init_session():
    global _SESSION_READY
    try:
        SESSION.get(BASE, timeout=15)
        time.sleep(2)
        SESSION.get(f"{BASE}/companies-listing/corporate-filings-announcements", timeout=15)
        time.sleep(1)
        _SESSION_READY = True
        logger.info("NSE session initialised")
    except Exception as e:
        logger.error(f"NSE session init failed: {e}")


def _safe_get(url: str, retries: int = 3) -> list | dict:
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=20)
            r.raise_for_status()
            data = r.json()
            return data
        except Exception as e:
            wait = 2 ** attempt
            logger.warning(f"NSE GET attempt {attempt+1} failed — {url} → {e}. Retry in {wait}s")
            time.sleep(wait)
    return []


def _today() -> str:
    return datetime.now().strftime("%d-%m-%Y")


# ─── Helpers ──────────────────────────────────────────────────────────

def _build_attachment_url(item: dict) -> str:
    """
    Resolve the best attachment URL from an NSE API item.
    NSE may return:
      - 'attchmnt'  : XBRL URL  → parse to get real PDF URL
      - 'sm_filingtm': sometimes has direct PDF path
    We store the XBRL URL in 'xbrl_url' and the PDF in 'attachment_url'.
    The XBRL enrichment step will fill attachment_url later.
    """
    raw = (item.get("attchmnt") or item.get("sm_filingtm") or "").strip()
    if not raw:
        return ""
    if raw.startswith("http"):
        return raw
    # Relative path
    return f"https://nsearchives.nseindia.com/{raw.lstrip('/')}"


# ─── Scraper Functions ────────────────────────────────────────────────

def fetch_announcements(segment: str = "equities") -> list:
    td = _today()
    url = f"{BASE}/api/corporate-announcements?index={segment}&from_date={td}&to_date={td}"
    raw = _safe_get(url)
    items = raw if isinstance(raw, list) else raw.get("data", []) if isinstance(raw, dict) else []

    out = []
    for item in items:
        att_url = _build_attachment_url(item)
        filing = {
            "exchange":       "NSE",
            "segment":        segment.upper().replace("EQUITIES", "EQUITY"),
            "category":       "Corporate Announcement",
            "filing_id":      item.get("an_id", item.get("seqno", "")),
            "symbol":         item.get("symbol", item.get("sm_isin", "")),
            "company":        item.get("company", item.get("sm_name", "")),
            "subject":        item.get("subject", item.get("anct_subject", item.get("desc", ""))),
            "description":    item.get("body", item.get("desc", "")),
            "filing_date":    item.get("exchdisstime", item.get("an_dt", "")),
            "xbrl_url":       att_url if att_url.lower().endswith((".xml", ".xbrl")) else "",
            "attachment_url": att_url if not att_url.lower().endswith((".xml", ".xbrl")) else "",
            "detail_url":     f"{BASE}/companies-listing/corporate-filings-announcements",
            "scraped_at":     datetime.now().isoformat(),
        }
        filing["filing_id"] = filing["filing_id"] or make_filing_id(filing)
        out.append(filing)

    # Enrich via XBRL (fills description, real attachment_url, etc.)
    out = bulk_enrich(out, SESSION, HEADERS)
    logger.info(f"NSE {segment} announcements: {len(out)}")
    return out


def fetch_board_meetings(segment: str = "equities") -> list:
    td = _today()
    url = f"{BASE}/api/corporate-board-meetings?index={segment}&from_date={td}&to_date={td}"
    raw = _safe_get(url)
    items = raw if isinstance(raw, list) else raw.get("data", []) if isinstance(raw, dict) else []

    out = []
    for item in items:
        att_url = _build_attachment_url(item)
        filing = {
            "exchange":       "NSE",
            "segment":        segment.upper().replace("EQUITIES", "EQUITY"),
            "category":       "Board Meeting",
            "filing_id":      item.get("an_id", item.get("bm_id", "")),
            "symbol":         item.get("symbol", ""),
            "company":        item.get("company", item.get("sm_name", "")),
            # NSE board meeting subject/purpose lives in multiple fields
            "subject":        (item.get("purpose") or item.get("bm_desc") or
                               item.get("anct_subject") or item.get("subject") or ""),
            "description":    item.get("bm_desc", item.get("desc", "")),
            "meeting_date":   item.get("bm_date", item.get("meetingDate", "")),
            "filing_date":    item.get("an_dt", item.get("exchdisstime", "")),
            "xbrl_url":       att_url if att_url.lower().endswith((".xml", ".xbrl")) else "",
            "attachment_url": att_url if not att_url.lower().endswith((".xml", ".xbrl")) else "",
            "detail_url":     f"{BASE}/companies-listing/corporate-filings-board-meetings",
            "scraped_at":     datetime.now().isoformat(),
        }
        filing["filing_id"] = filing["filing_id"] or make_filing_id(filing)
        out.append(filing)

    out = bulk_enrich(out, SESSION, HEADERS)
    logger.info(f"NSE {segment} board meetings: {len(out)}")
    return out


def fetch_financial_results(segment: str = "equities") -> list:
    td = _today()
    url = (f"{BASE}/api/corporates-financial-results"
           f"?index={segment}&period=Quarterly&from_date={td}&to_date={td}")
    raw = _safe_get(url)
    items = raw if isinstance(raw, list) else raw.get("data", []) if isinstance(raw, dict) else []

    out = []
    for item in items:
        att_raw = item.get("attachment", item.get("attchmnt", ""))
        if att_raw and not att_raw.startswith("http"):
            att_raw = f"https://nsearchives.nseindia.com/{att_raw.lstrip('/')}"
        filing = {
            "exchange":       "NSE",
            "segment":        segment.upper().replace("EQUITIES", "EQUITY"),
            "category":       "Financial Result",
            "filing_id":      item.get("an_id", item.get("seqno", "")),
            "symbol":         item.get("symbol", ""),
            "company":        item.get("company", item.get("companyName", "")),
            "subject":        item.get("desc", item.get("subject", "Quarterly Results")),
            "period":         item.get("period", item.get("quarterEnded", "")),
            "result_type":    item.get("resultType", "Quarterly"),
            "filing_date":    item.get("filingDate", item.get("an_dt", item.get("exchdisstime", ""))),
            "attachment_url": att_raw,
            "detail_url":     f"{BASE}/companies-listing/corporate-filings-financial-results",
            "scraped_at":     datetime.now().isoformat(),
        }
        filing["filing_id"] = filing["filing_id"] or make_filing_id(filing)
        out.append(filing)

    logger.info(f"NSE {segment} results: {len(out)}")
    return out


def fetch_corporate_actions(segment: str = "equities") -> list:
    td = _today()
    url = f"{BASE}/api/corporates-corporate-actions?index={segment}&from_date={td}&to_date={td}"
    raw = _safe_get(url)
    items = raw if isinstance(raw, list) else raw.get("data", []) if isinstance(raw, dict) else []

    out = []
    for item in items:
        filing = {
            "exchange":     "NSE",
            "segment":      segment.upper().replace("EQUITIES", "EQUITY"),
            "category":     "Corporate Action",
            "filing_id":    item.get("an_id", item.get("seqno", "")),
            "symbol":       item.get("symbol", ""),
            "company":      item.get("company", item.get("comp", item.get("companyName", ""))),
            "subject":      item.get("subject", item.get("action", "")),
            "action":       item.get("subject", item.get("action", "")),
            "ex_date":      item.get("exDate", item.get("ex_date", "")),
            "record_date":  item.get("recDate", item.get("record_date", "")),
            "remarks":      item.get("remarks", ""),
            "filing_date":  item.get("an_dt", item.get("exchdisstime", "")),
            "attachment_url": "",
            "detail_url":   f"{BASE}/companies-listing/corporate-filings-corporate-actions",
            "scraped_at":   datetime.now().isoformat(),
        }
        filing["filing_id"] = filing["filing_id"] or make_filing_id(filing)
        out.append(filing)

    logger.info(f"NSE {segment} actions: {len(out)}")
    return out


def fetch_shareholding(segment: str = "equities") -> list:
    td = _today()
    url = f"{BASE}/api/corporate-share-holdings-master?index={segment}&from_date={td}&to_date={td}"
    raw = _safe_get(url)
    items = raw if isinstance(raw, list) else raw.get("data", []) if isinstance(raw, dict) else []

    out = []
    for item in items:
        att_raw = item.get("attachment", item.get("attchmnt", ""))
        if att_raw and not att_raw.startswith("http"):
            att_raw = f"https://nsearchives.nseindia.com/{att_raw.lstrip('/')}"
        filing = {
            "exchange":          "NSE",
            "segment":           segment.upper().replace("EQUITIES", "EQUITY"),
            "category":          "Shareholding Pattern",
            "filing_id":         item.get("an_id", item.get("seqno", "")),
            "symbol":            item.get("symbol", ""),
            "company":           item.get("companyName", item.get("company", "")),
            "period":            item.get("period", item.get("quarter", "")),
            "filing_date":       item.get("filingDate", item.get("an_dt", "")),
            "promoter_holding":  str(item.get("promoterHolding", item.get("promoter", ""))),
            "public_holding":    str(item.get("publicHolding", item.get("public", ""))),
            "fii_holding":       str(item.get("fiiHolding", "")),
            "dii_holding":       str(item.get("diiHolding", "")),
            "attachment_url":    att_raw,
            "detail_url":        f"{BASE}/companies-listing/corporate-filings-shareholding-pattern",
            "scraped_at":        datetime.now().isoformat(),
        }
        filing["filing_id"] = filing["filing_id"] or make_filing_id(filing)
        out.append(filing)

    logger.info(f"NSE {segment} shareholding: {len(out)}")
    return out


# ─── Main Entry ───────────────────────────────────────────────────────

def scrape_all() -> dict:
    if not _SESSION_READY:
        init_session()

    result = {"equity": {}, "sme": {}}
    segment_map = {"equity": "equities", "sme": "sme"}

    for key, seg in segment_map.items():
        result[key]["corporate_announcements"] = fetch_announcements(seg);  time.sleep(1.5)
        result[key]["board_meetings"]           = fetch_board_meetings(seg); time.sleep(1.5)
        result[key]["financial_results"]        = fetch_financial_results(seg); time.sleep(1.5)
        result[key]["corporate_actions"]        = fetch_corporate_actions(seg); time.sleep(1.5)
        result[key]["shareholding_patterns"]    = fetch_shareholding(seg);   time.sleep(1.5)

    return result


if __name__ == "__main__":
    d = scrape_all()
    print(json.dumps(d, indent=2, default=str)[:4000])
