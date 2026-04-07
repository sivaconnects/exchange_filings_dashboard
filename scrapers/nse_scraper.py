"""
NSE Exchange Filings Scraper — TRUE BROWSER SCRAPING
======================================================
Uses Playwright (headless Chromium) to PHYSICALLY VISIT the NSE website
pages exactly like a human user. The browser runs JavaScript, handles cookies,
and the site loads its data. We intercept the network responses the site itself
receives, giving us the same data a user sees in their browser.

NO direct API calls. We open the actual website URL, let it load,
capture what loads in the browser, parse it.

Pages visited:
  - https://www.nseindia.com/ (home — sets session cookies)
  - https://www.nseindia.com/companies-listing/corporate-filings-announcements
  - https://www.nseindia.com/companies-listing/corporate-filings-board-meetings
  - https://www.nseindia.com/companies-listing/corporate-filings-financial-results
  - https://www.nseindia.com/companies-listing/corporate-filings-corporate-actions
  - https://www.nseindia.com/companies-listing/corporate-filings-shareholding-pattern
"""

import json
import logging
import time
import re
from datetime import datetime, timedelta
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

try:
    from xbrl_parser import fetch_xbrl_attachment
except ImportError:
    def fetch_xbrl_attachment(url): return url

try:
    from state_tracker import make_filing_id
except ImportError:
    def make_filing_id(f):
        return "{}-{}-{}-{}".format(
            f.get("exchange",""), f.get("symbol",""),
            f.get("filing_date","")[:10], f.get("subject","")[:20]
        )

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nse")

NSE_HOME = "https://www.nseindia.com"
ARCHIVES = "https://nsearchives.nseindia.com"

# Pages we visit — the browser handles JS, cookies, everything
NSE_PAGES = {
    "corporate_announcements": f"{NSE_HOME}/companies-listing/corporate-filings-announcements",
    "board_meetings":          f"{NSE_HOME}/companies-listing/corporate-filings-board-meetings",
    "financial_results":       f"{NSE_HOME}/companies-listing/corporate-filings-financial-results",
    "corporate_actions":       f"{NSE_HOME}/companies-listing/corporate-filings-corporate-actions",
    "shareholding_patterns":   f"{NSE_HOME}/companies-listing/corporate-filings-shareholding-pattern",
}

# URL patterns in NSE's internal XHR calls we intercept
NSE_API_PATTERNS = {
    "corporate_announcements": "corporate-announcements",
    "board_meetings":          "corporate-board-meetings",
    "financial_results":       "corporates-financial-results",
    "corporate_actions":       "corporates-corporate-actions",
    "shareholding_patterns":   "corporate-share-holdings",
}


def _today() -> str:
    return datetime.now().strftime("%d-%m-%Y")


def _att_url(raw: str) -> str:
    if not raw:
        return ""
    raw = str(raw).strip()
    if raw.startswith("http"):
        return raw
    return f"{ARCHIVES}/{raw.lstrip('/')}"


def _norm_segment(seg: str) -> str:
    return "EQUITY" if seg in ("equities", "equity") else "SME"


def _parse_announcements(items: list, segment: str) -> list:
    out = []
    for item in items:
        att_raw = (item.get("attchmnt") or item.get("sm_filingtm") or
                   item.get("attachment") or "")
        att_full = _att_url(att_raw)
        is_xbrl = att_full.lower().endswith((".xml", ".xbrl"))
        f = {
            "exchange":       "NSE",
            "segment":        _norm_segment(segment),
            "category":       "Corporate Announcement",
            "filing_id":      str(item.get("an_id") or item.get("seqno") or ""),
            "symbol":         item.get("symbol") or item.get("sm_symbol") or "",
            "company":        item.get("company") or item.get("sm_name") or "",
            "subject":        item.get("subject") or item.get("anct_subject") or item.get("desc") or "",
            "description":    item.get("body") or item.get("desc") or "",
            "filing_date":    item.get("exchdisstime") or item.get("an_dt") or "",
            "xbrl_url":       att_full if is_xbrl else "",
            "attachment_url": att_full if not is_xbrl else "",
            "detail_url":     NSE_PAGES["corporate_announcements"],
            "scraped_at":     datetime.now().isoformat(),
        }
        if not f["filing_id"]:
            f["filing_id"] = make_filing_id(f)
        out.append(f)
    return out


def _parse_board_meetings(items: list, segment: str) -> list:
    out = []
    for item in items:
        att_raw = item.get("attchmnt") or item.get("attachment") or ""
        att_full = _att_url(att_raw)
        is_xbrl = att_full.lower().endswith((".xml", ".xbrl"))
        f = {
            "exchange":       "NSE",
            "segment":        _norm_segment(segment),
            "category":       "Board Meeting",
            "filing_id":      str(item.get("an_id") or item.get("bm_id") or ""),
            "symbol":         item.get("symbol") or "",
            "company":        item.get("company") or item.get("sm_name") or "",
            "subject":        (item.get("purpose") or item.get("bm_desc") or
                               item.get("anct_subject") or item.get("subject") or ""),
            "description":    item.get("bm_desc") or item.get("desc") or "",
            "meeting_date":   item.get("bm_date") or item.get("meetingDate") or item.get("bm_dt") or "",
            "filing_date":    item.get("an_dt") or item.get("exchdisstime") or "",
            "xbrl_url":       att_full if is_xbrl else "",
            "attachment_url": att_full if not is_xbrl else "",
            "detail_url":     NSE_PAGES["board_meetings"],
            "scraped_at":     datetime.now().isoformat(),
        }
        if not f["filing_id"]:
            f["filing_id"] = make_filing_id(f)
        out.append(f)
    return out


def _parse_financial_results(items: list, segment: str) -> list:
    out = []
    for item in items:
        att_raw = item.get("attachment") or item.get("attchmnt") or ""
        f = {
            "exchange":       "NSE",
            "segment":        _norm_segment(segment),
            "category":       "Financial Result",
            "filing_id":      str(item.get("an_id") or item.get("seqno") or ""),
            "symbol":         item.get("symbol") or "",
            "company":        item.get("company") or item.get("companyName") or "",
            "subject":        item.get("desc") or item.get("subject") or "Results",
            "period":         item.get("period") or item.get("quarterEnded") or "",
            "result_type":    item.get("resultType") or "Quarterly",
            "filing_date":    item.get("filingDate") or item.get("an_dt") or item.get("exchdisstime") or "",
            "attachment_url": _att_url(att_raw),
            "xbrl_url":       "",
            "detail_url":     NSE_PAGES["financial_results"],
            "scraped_at":     datetime.now().isoformat(),
        }
        if not f["filing_id"]:
            f["filing_id"] = make_filing_id(f)
        out.append(f)
    return out


def _parse_corporate_actions(items: list, segment: str) -> list:
    out = []
    for item in items:
        action = item.get("subject") or item.get("action") or ""
        f = {
            "exchange":       "NSE",
            "segment":        _norm_segment(segment),
            "category":       "Corporate Action",
            "filing_id":      str(item.get("an_id") or item.get("seqno") or ""),
            "symbol":         item.get("symbol") or "",
            "company":        item.get("company") or item.get("companyName") or "",
            "subject":        action,
            "action":         action,
            "ex_date":        item.get("exDate") or item.get("ex_date") or "",
            "record_date":    item.get("recDate") or item.get("record_date") or "",
            "remarks":        item.get("remarks") or "",
            "filing_date":    item.get("an_dt") or item.get("exchdisstime") or "",
            "attachment_url": "",
            "xbrl_url":       "",
            "detail_url":     NSE_PAGES["corporate_actions"],
            "scraped_at":     datetime.now().isoformat(),
        }
        if not f["filing_id"]:
            f["filing_id"] = make_filing_id(f)
        out.append(f)
    return out


def _parse_shareholding(items: list, segment: str) -> list:
    out = []
    for item in items:
        att_raw = item.get("attachment") or item.get("attchmnt") or ""
        f = {
            "exchange":          "NSE",
            "segment":           _norm_segment(segment),
            "category":          "Shareholding Pattern",
            "filing_id":         str(item.get("an_id") or item.get("seqno") or ""),
            "symbol":            item.get("symbol") or "",
            "company":           item.get("companyName") or item.get("company") or "",
            "period":            item.get("period") or item.get("quarter") or "",
            "filing_date":       item.get("filingDate") or item.get("an_dt") or "",
            "promoter_holding":  str(item.get("promoterHolding") or item.get("promoter") or ""),
            "public_holding":    str(item.get("publicHolding") or item.get("public") or ""),
            "fii_holding":       str(item.get("fiiHolding") or ""),
            "dii_holding":       str(item.get("diiHolding") or ""),
            "attachment_url":    _att_url(att_raw),
            "xbrl_url":          "",
            "detail_url":        NSE_PAGES["shareholding_patterns"],
            "scraped_at":        datetime.now().isoformat(),
        }
        if not f["filing_id"]:
            f["filing_id"] = make_filing_id(f)
        out.append(f)
    return out


PARSERS = {
    "corporate_announcements": _parse_announcements,
    "board_meetings":          _parse_board_meetings,
    "financial_results":       _parse_financial_results,
    "corporate_actions":       _parse_corporate_actions,
    "shareholding_patterns":   _parse_shareholding,
}


def _extract_list(raw_json) -> list:
    """Pull list of items from various NSE response shapes."""
    if isinstance(raw_json, list):
        return raw_json
    if isinstance(raw_json, dict):
        for key in ("data", "Data", "result", "Result", "announcements"):
            v = raw_json.get(key)
            if isinstance(v, list):
                return v
        # any list value
        for v in raw_json.values():
            if isinstance(v, list) and v:
                return v
    return []


# ─────────────────────────────────────────────────────────────────────────
# Core browser scraping function
# ─────────────────────────────────────────────────────────────────────────

def _scrape_segment(segment_param: str) -> dict:
    """
    Open a Playwright browser, visit NSE website, collect all filing categories.
    segment_param = 'equities' or 'sme'
    """
    results = {cat: [] for cat in NSE_PAGES.keys()}
    td = _today()

    log.info(f"Opening browser for NSE [{segment_param}]...")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-accelerated-2d-canvas",
                "--no-first-run",
                "--no-zygote",
                "--disable-gpu",
                "--window-size=1920,1080",
            ]
        )

        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            java_script_enabled=True,
            ignore_https_errors=True,
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
            }
        )

        page = context.new_page()

        # ── Step 1: Visit home page to acquire cookies ──────────────
        log.info("Visiting NSE home to acquire session...")
        try:
            page.goto(NSE_HOME, wait_until="domcontentloaded", timeout=30000)
            time.sleep(3)
        except PWTimeout:
            log.warning("NSE home load timeout — proceeding anyway")

        # ── Step 2: Visit each category page ───────────────────────
        for cat_key, page_url in NSE_PAGES.items():
            captured_responses = []
            api_pattern = NSE_API_PATTERNS[cat_key]

            def handle_response(resp, _pattern=api_pattern, _cat=cat_key):
                """Intercept XHR/fetch calls the NSE page makes."""
                if _pattern in resp.url and "api" in resp.url:
                    # Filter for today's date in the URL
                    if segment_param in resp.url or "index=" in resp.url:
                        try:
                            j = resp.json()
                            items = _extract_list(j)
                            if items:
                                log.info(f"  NSE captured {len(items)} items for {_cat} [{segment_param}]")
                                captured_responses.extend(items)
                        except Exception as e:
                            log.debug(f"  Response parse error: {e}")

            page.on("response", handle_response)

            log.info(f"Visiting: {page_url}")
            try:
                page.goto(page_url, wait_until="networkidle", timeout=35000)
                time.sleep(2)
            except PWTimeout:
                log.warning(f"  Timeout on {page_url} — trying to extract anyway")

            page.remove_listener("response", handle_response)

            # Parse captured JSON
            if captured_responses:
                parser = PARSERS[cat_key]
                results[cat_key] = parser(captured_responses, segment_param)
                log.info(f"  NSE {segment_param} {cat_key}: {len(results[cat_key])}")
            else:
                # Fallback: try to read table directly from DOM
                log.info(f"  No XHR intercepted for {cat_key}, trying DOM table parse...")
                results[cat_key] = _parse_dom_table(page, cat_key, segment_param)

            time.sleep(2)

        browser.close()

    return results


def _parse_dom_table(page, cat_key: str, segment: str) -> list:
    """
    Fallback: directly query rendered table rows from the page DOM.
    Works when XHR interception doesn't capture the response.
    """
    try:
        # Wait a bit more for lazy-loaded content
        page.wait_for_timeout(3000)

        # Try common table selectors NSE uses
        row_selectors = [
            "table tbody tr",
            "[class*='table'] tbody tr",
            ".dataTable tbody tr",
            "[id*='table'] tbody tr",
        ]
        rows = []
        for sel in row_selectors:
            rows = page.query_selector_all(sel)
            if rows and len(rows) > 1:
                break

        if not rows:
            return []

        out = []
        for row in rows:
            cells = row.query_selector_all("td")
            if len(cells) < 2:
                continue
            cell_texts = [c.inner_text().strip() for c in cells]

            # Find any PDF/attachment links
            att = ""
            for a in row.query_selector_all("a"):
                href = a.get_attribute("href") or ""
                if href and (".pdf" in href.lower() or ".xbrl" in href.lower() or ".xml" in href.lower() or "attachment" in href.lower()):
                    att = href if href.startswith("http") else f"{NSE_HOME}{href}"
                    break

            f = {
                "exchange":       "NSE",
                "segment":        _norm_segment(segment),
                "category":       cat_key.replace("_", " ").title(),
                "symbol":         cell_texts[0] if len(cell_texts) > 0 else "",
                "company":        cell_texts[1] if len(cell_texts) > 1 else "",
                "subject":        cell_texts[2] if len(cell_texts) > 2 else "",
                "filing_date":    cell_texts[-1] if cell_texts else "",
                "attachment_url": att,
                "xbrl_url":       "",
                "detail_url":     NSE_PAGES.get(cat_key, NSE_HOME),
                "scraped_at":     datetime.now().isoformat(),
            }
            f["filing_id"] = make_filing_id(f)
            out.append(f)

        log.info(f"  DOM table fallback: {len(out)} rows for {cat_key} [{segment}]")
        return out
    except Exception as e:
        log.error(f"  DOM table parse failed: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────
# XBRL enrichment — fetch the XBRL/XML attachment files
# These are direct file downloads, not API calls
# ─────────────────────────────────────────────────────────────────────────

def _enrich_xbrl(filings: list) -> list:
    """
    For filings that have an XBRL URL, fetch the file and extract:
    - Real PDF attachment URL
    - Full description text
    - Subject / company name
    """
    import requests
    from xml.etree import ElementTree as ET

    NAMESPACES = [
        "http://www.sebi.gov.in/xbrl/2025-05-28/in-capmkt",
        "http://www.sebi.gov.in/xbrl/2024-05-28/in-capmkt",
        "http://www.sebi.gov.in/xbrl/2023-05-28/in-capmkt",
    ]
    FIELD_MAP = {
        "SubjectOfAnnouncement":     "subject",
        "DescriptionOfAnnouncement": "description",
        "AttachmentURL":             "attachment_url",
        "NameOfTheCompany":          "company",
        "DateAndTimeOfSubmission":   "filing_date",
    }

    def fetch_xbrl(url: str) -> dict:
        try:
            r = requests.get(url, timeout=12,
                             headers={"User-Agent": "Mozilla/5.0 NSE-XBRL-Fetcher"})
            r.raise_for_status()
            root = ET.fromstring(r.text)
            result = {}
            for tag, field in FIELD_MAP.items():
                # Try each namespace
                val = None
                for ns in NAMESPACES:
                    el = root.find(f"{{{ns}}}{tag}")
                    if el is not None and el.text:
                        val = el.text.strip()
                        break
                # Namespace-agnostic fallback
                if not val:
                    for el in root.iter():
                        if el.tag.split("}")[-1] == tag and el.text:
                            val = el.text.strip()
                            break
                if val:
                    result[field] = val
            return result
        except Exception as e:
            log.debug(f"XBRL fetch failed {url}: {e}")
            return {}

    enriched = []
    for f in filings:
        xbrl_url = f.get("xbrl_url", "")
        if xbrl_url and xbrl_url.lower().endswith((".xml", ".xbrl")):
            data = fetch_xbrl(xbrl_url)
            if data:
                for field, val in data.items():
                    if field == "attachment_url":
                        f["attachment_url"] = val  # always override with real PDF
                    elif val and not f.get(field):
                        f[field] = val
        enriched.append(f)
    return enriched


# ─────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────

def scrape_all() -> dict:
    result = {"equity": {}, "sme": {}}

    log.info("=== NSE Scraper: Equity ===")
    eq_data = _scrape_segment("equities")
    for cat, items in eq_data.items():
        items = _enrich_xbrl(items)
        result["equity"][cat] = items
        log.info(f"NSE equity {cat}: {len(items)} filings")

    time.sleep(3)

    log.info("=== NSE Scraper: SME ===")
    sme_data = _scrape_segment("sme")
    for cat, items in sme_data.items():
        items = _enrich_xbrl(items)
        result["sme"][cat] = items
        log.info(f"NSE sme {cat}: {len(items)} filings")

    return result


if __name__ == "__main__":
    data = scrape_all()
    for seg in ("equity", "sme"):
        for cat, items in data[seg].items():
            print(f"NSE {seg} {cat}: {len(items)}")
    print(json.dumps(data, indent=2, default=str)[:3000])
