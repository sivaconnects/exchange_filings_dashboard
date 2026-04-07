"""
BSE Exchange Filings Scraper — TRUE BROWSER SCRAPING
======================================================
Uses Playwright (headless Chromium) to PHYSICALLY VISIT BSE website pages.
The browser runs BSE's JavaScript, handles cookies/sessions automatically,
and we intercept the network responses exactly as the browser receives them.

NO direct API calls. We open the actual BSE website, let it load,
capture what loads in the browser, extract every table row.

Pages visited:
  - https://www.bseindia.com/                   (home — cookies)
  - https://www.bseindia.com/corporates/ann.html  (announcements)
  - https://www.bseindia.com/corporates/boardmeetings.html
  - https://www.bseindia.com/corporates/results.aspx
  - https://www.bseindia.com/corporates/corporate_act.html

BSE Categories:
  AGM/EGM, Board Meetings, Company Updates, Corporate Actions,
  Insider Trading/SAST, New Listings, Results, Integrated Filings, Others
"""

import json
import logging
import time
import re
from datetime import datetime

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

try:
    from state_tracker import make_filing_id
except ImportError:
    def make_filing_id(f):
        return "{}-{}-{}-{}".format(
            f.get("exchange",""), f.get("symbol",""),
            f.get("filing_date","")[:10], f.get("subject","")[:20]
        )

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bse")

BSE_HOME    = "https://www.bseindia.com"
BSE_PDF_BASE = f"{BSE_HOME}/xml-data/corpfiling/AttachLive"

# The actual pages we visit on BSE
BSE_PAGES = {
    "announcements":    f"{BSE_HOME}/corporates/ann.html",
    "board_meetings":   f"{BSE_HOME}/corporates/boardmeetings.html",
    "results":          f"{BSE_HOME}/corporates/results.aspx",
    "corporate_actions":f"{BSE_HOME}/corporates/corporate_act.html",
    "insider":          f"{BSE_HOME}/corporates/Insider-Trading-Init.html",
}

# URL patterns in BSE's internal AJAX calls
BSE_API_PATTERNS = [
    "AnnGetData", "AnnouncementCat", "Announcements",
    "BoardMeeting", "FinancialResult", "Corpaction",
    "InsiderTrading", "NewListing",
]

# BSE category codes on their announcements page
BSE_CATS = {
    "-1":            "All Announcements",
    "AGM":           "AGM / EGM",
    "BM":            "Board Meeting",
    "Company Update":"Company Update",
    "Corp. Action":  "Corporate Action",
    "insider":       "Insider Trading / SAST",
    "New Listing":   "New Listing",
    "Result":        "Financial Result",
    "Integrated":    "Integrated Filings",
    "Others":        "Others",
}


def _today_bse() -> str:
    """BSE uses dd/MM/YYYY."""
    return datetime.now().strftime("%d/%m/%Y")


def _att_url(filename: str) -> str:
    if not filename:
        return ""
    filename = str(filename).strip()
    if filename.startswith("http"):
        return filename
    return f"{BSE_PDF_BASE}/{filename}"


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
        "company":        (item.get("SLONGNAME") or item.get("LongName") or
                           item.get("longname") or item.get("COMPANY_NAME") or ""),
        "subject":        (item.get("HEADLINE") or item.get("Headline") or
                           item.get("headline") or item.get("PURPOSE") or ""),
        "description":    (item.get("NEWSSUB") or item.get("NewsSub") or
                           item.get("newssub") or item.get("REMARKS") or ""),
        "filing_date":    (item.get("NEWS_DT") or item.get("NewsDate") or
                           item.get("newsdate") or item.get("MEETING_DATE") or ""),
        "meeting_date":   item.get("MEETING_DATE") or item.get("MeetingDate") or "",
        "subcategory":    item.get("SUBCATEGORYNAME") or item.get("SubCatName") or "",
        "attachment_url": _att_url(att_file),
        "detail_url":     BSE_PAGES["announcements"],
        "scraped_at":     datetime.now().isoformat(),
    }


def _extract_list(raw) -> list:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("Table", "Table1", "data", "Data", "Result", "result",
                    "announcements", "Announcements"):
            v = raw.get(key)
            if isinstance(v, list) and v:
                return v
        for v in raw.values():
            if isinstance(v, list) and v:
                return v
    return []


# ─────────────────────────────────────────────────────────────────────────
# Browser scraping: visit BSE announcements page, cycle through categories
# ─────────────────────────────────────────────────────────────────────────

def _scrape_bse_announcements(segment: str = "equity") -> dict:
    """
    Visit bseindia.com/corporates/ann.html with a real browser.
    BSE's page has a dropdown to filter by category — the browser
    makes AJAX calls which we intercept to get structured data.
    """
    seg_code = "E" if segment == "equity" else "ES"
    results = {
        "agm_egm": [], "board_meetings": [], "company_updates": [],
        "corporate_actions": [], "insider_trading": [], "new_listings": [],
        "results": [], "integrated_filings": [], "others": [],
        "corporate_announcements": [],
    }

    log.info(f"Opening browser for BSE [{segment}]...")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--no-first-run",
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
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Origin": "https://www.bseindia.com",
            }
        )

        page = context.new_page()

        # ── Step 1: Visit BSE home for cookies ──────────────────────
        log.info("Visiting BSE home to acquire session cookies...")
        try:
            page.goto(BSE_HOME, wait_until="domcontentloaded", timeout=30000)
            time.sleep(3)
        except PWTimeout:
            log.warning("BSE home timeout — proceeding anyway")

        # ── Step 2: Visit announcements page ─────────────────────────
        all_captured = []

        def handle_response(resp):
            if any(pat in resp.url for pat in BSE_API_PATTERNS):
                try:
                    j = resp.json()
                    items = _extract_list(j)
                    if items:
                        all_captured.extend(items)
                        log.info(f"  BSE intercepted {len(items)} items from {resp.url.split('/')[-1]}")
                except Exception as e:
                    log.debug(f"  BSE response parse: {e}")

        page.on("response", handle_response)

        log.info(f"Visiting BSE announcements: {BSE_PAGES['announcements']}")
        try:
            page.goto(BSE_PAGES["announcements"], wait_until="networkidle", timeout=35000)
            time.sleep(3)
        except PWTimeout:
            log.warning("BSE announcements page timeout")

        # ── Step 3: Try cycling through category dropdowns ───────────
        # BSE has a category select that triggers AJAX reloads
        cat_selectors = ["#ddlPeriod", "#ddlcat", "#select_category", "select[name*='cat']",
                         "select[id*='cat']", "select[id*='Cat']"]

        for sel in cat_selectors:
            try:
                if page.query_selector(sel):
                    options = page.query_selector_all(f"{sel} option")
                    for opt in options:
                        val = opt.get_attribute("value") or ""
                        if val and val != "-1":
                            page.select_option(sel, val)
                            time.sleep(2)
                    break
            except Exception:
                pass

        # ── Step 4: Visit board meetings page ────────────────────────
        log.info(f"Visiting BSE board meetings: {BSE_PAGES['board_meetings']}")
        try:
            page.goto(BSE_PAGES["board_meetings"], wait_until="networkidle", timeout=30000)
            time.sleep(3)
        except PWTimeout:
            log.warning("BSE board meetings timeout")

        # ── Step 5: Visit results page ───────────────────────────────
        log.info(f"Visiting BSE results: {BSE_PAGES['results']}")
        try:
            page.goto(BSE_PAGES["results"], wait_until="networkidle", timeout=30000)
            time.sleep(3)
        except PWTimeout:
            log.warning("BSE results timeout")

        # ── Step 6: Visit insider trading page ───────────────────────
        log.info(f"Visiting BSE insider: {BSE_PAGES['insider']}")
        try:
            page.goto(BSE_PAGES["insider"], wait_until="networkidle", timeout=30000)
            time.sleep(3)
        except PWTimeout:
            log.warning("BSE insider timeout")

        page.remove_listener("response", handle_response)

        # ── Step 7: If nothing intercepted, parse DOM tables ─────────
        if not all_captured:
            log.info("  No XHR intercepted — trying DOM table parse on BSE...")
            # Go back to announcements page and parse HTML table
            try:
                page.goto(BSE_PAGES["announcements"], wait_until="networkidle", timeout=30000)
                time.sleep(3)
                dom_items = _parse_bse_dom(page, segment)
                results["corporate_announcements"] = dom_items
                log.info(f"  BSE DOM parse: {len(dom_items)} rows")
            except Exception as e:
                log.error(f"  BSE DOM parse failed: {e}")

        browser.close()

    # ── Categorize captured items ────────────────────────────────────
    if all_captured:
        log.info(f"BSE total captured: {len(all_captured)} items — categorising...")
        for item in all_captured:
            f = _build_filing(item, segment, _get_category(item))
            f["filing_id"] = f["filing_id"] or make_filing_id(f)

            cat = _get_category(item)
            # Map to result bucket
            bucket = _cat_to_bucket(cat)
            results[bucket].append(f)
            # Also add to combined
            results["corporate_announcements"].append(f)

    return results


def _get_category(item: dict) -> str:
    """Determine category from BSE item fields."""
    # Check subcategory, category name fields
    for field in ("SUBCATEGORYNAME", "SubCatName", "CATEGORYNAME", "CategoryName",
                  "TYPE_OF_ANNOUNCEMENT", "TypeOfAnnouncement"):
        val = item.get(field, "")
        if val:
            val_lower = val.lower()
            if "agm" in val_lower or "egm" in val_lower or "general meeting" in val_lower:
                return "AGM / EGM"
            if "board" in val_lower:
                return "Board Meeting"
            if "insider" in val_lower or "sast" in val_lower:
                return "Insider Trading / SAST"
            if "result" in val_lower:
                return "Financial Result"
            if "action" in val_lower or "dividend" in val_lower or "bonus" in val_lower:
                return "Corporate Action"
            if "listing" in val_lower:
                return "New Listing"
            if "integrated" in val_lower:
                return "Integrated Filings"

    # Check headline
    headline = (item.get("HEADLINE") or item.get("Headline") or "").lower()
    if "board meeting" in headline:
        return "Board Meeting"
    if "agm" in headline or "egm" in headline:
        return "AGM / EGM"
    if "result" in headline or "quarterly" in headline:
        return "Financial Result"
    if "insider" in headline:
        return "Insider Trading / SAST"
    if "dividend" in headline or "bonus" in headline or "split" in headline:
        return "Corporate Action"

    return "Company Update"


def _cat_to_bucket(cat: str) -> str:
    mapping = {
        "AGM / EGM":             "agm_egm",
        "Board Meeting":         "board_meetings",
        "Company Update":        "company_updates",
        "Corporate Action":      "corporate_actions",
        "Insider Trading / SAST":"insider_trading",
        "New Listing":           "new_listings",
        "Financial Result":      "results",
        "Integrated Filings":    "integrated_filings",
    }
    return mapping.get(cat, "others")


def _parse_bse_dom(page, segment: str) -> list:
    """Parse BSE announcements table directly from rendered DOM."""
    out = []
    try:
        # BSE table selectors
        for sel in ["table#tblAnnouncements tbody tr",
                    "table.table tbody tr",
                    "table tbody tr"]:
            rows = page.query_selector_all(sel)
            if rows and len(rows) > 1:
                for row in rows:
                    cells = row.query_selector_all("td")
                    if len(cells) < 3:
                        continue
                    cell_texts = [c.inner_text().strip() for c in cells]

                    att = ""
                    for a in row.query_selector_all("a"):
                        href = a.get_attribute("href") or ""
                        if ".pdf" in href.lower() or "AttachLive" in href:
                            att = href if href.startswith("http") else f"{BSE_HOME}{href}"
                            break

                    f = {
                        "exchange":       "BSE",
                        "segment":        segment.upper(),
                        "category":       "Corporate Announcement",
                        "bse_category":   "All Announcements",
                        "symbol":         cell_texts[0] if len(cell_texts) > 0 else "",
                        "company":        cell_texts[1] if len(cell_texts) > 1 else "",
                        "subject":        cell_texts[2] if len(cell_texts) > 2 else "",
                        "filing_date":    cell_texts[-1] if cell_texts else "",
                        "attachment_url": att,
                        "detail_url":     BSE_PAGES["announcements"],
                        "scraped_at":     datetime.now().isoformat(),
                    }
                    f["filing_id"] = make_filing_id(f)
                    out.append(f)
                break
    except Exception as e:
        log.error(f"BSE DOM parse error: {e}")
    return out


# ─────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────

def scrape_all() -> dict:
    result = {"equity": {}, "sme": {}}

    log.info("=== BSE Scraper: Equity ===")
    result["equity"] = _scrape_bse_announcements("equity")
    for cat, items in result["equity"].items():
        if cat != "corporate_announcements":
            log.info(f"BSE equity {cat}: {len(items)}")

    time.sleep(5)

    log.info("=== BSE Scraper: SME ===")
    result["sme"] = _scrape_bse_announcements("sme")
    for cat, items in result["sme"].items():
        if cat != "corporate_announcements":
            log.info(f"BSE sme {cat}: {len(items)}")

    return result


if __name__ == "__main__":
    data = scrape_all()
    for seg in ("equity", "sme"):
        for cat, items in data[seg].items():
            if cat != "corporate_announcements":
                print(f"BSE {seg} {cat}: {len(items)}")
    print(json.dumps(data, indent=2, default=str)[:3000])
