"""
BSE Exchange Filings Scraper
Pure web scraping - no paid API
Scrapes BSE India for Equity and SME filings
"""

import requests
import json
import time
import logging
from datetime import datetime
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.bseindia.com/",
}

BASE = "https://www.bseindia.com"
SESSION = requests.Session()

def init_session():
    try:
        SESSION.get(BASE, headers=HEADERS, timeout=15)
        time.sleep(2)
        logger.info("BSE session ready")
    except Exception as e:
        logger.error(f"BSE session init: {e}")

def today_bse():
    """BSE uses dd/MM/YYYY format"""
    return datetime.now().strftime("%d/%m/%Y")

def safe_get(url, params=None):
    try:
        r = SESSION.get(url, headers=HEADERS, params=params, timeout=20)
        r.raise_for_status()
        ct = r.headers.get("Content-Type", "")
        if "json" in ct:
            return r.json()
        return r.text
    except Exception as e:
        logger.error(f"BSE GET {url} → {e}")
        return None

# ─── BSE segment codes ──────────────────────────────────────────────
# Equity: Category = 0 (All), Segment = E (Equity Main Board)
# SME:    Segment = ES (SME/Emerge)

def fetch_announcements(segment="equity"):
    """
    BSE Corporate Announcements from the public listing page JSON endpoint
    """
    today = today_bse()
    seg_code = "E" if segment == "equity" else "ES"
    url = f"{BASE}/corporates/ann.html"
    
    # Try the data endpoint BSE uses internally
    data_url = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
    params = {
        "strCat": "-1",
        "strType": "C",
        "strScrip": "",
        "strSearch": "P",
        "strToDate": today,
        "strFromDate": today,
        "mykey": "announcements",
        "segment": seg_code
    }
    
    raw = safe_get(data_url, params)
    out = []
    
    if isinstance(raw, dict):
        items = raw.get("Table", raw.get("data", []))
    elif isinstance(raw, list):
        items = raw
    else:
        items = []
    
    for item in items:
        out.append({
            "exchange": "BSE", "segment": segment.upper(), "category": "Corporate Announcement",
            "symbol": str(item.get("SCRIP_CD", item.get("scrip_cd", ""))),
            "company": item.get("SLONGNAME", item.get("company_name", "")),
            "subject": item.get("HEADLINE", item.get("headline", "")),
            "filing_date": item.get("NEWS_DT", item.get("dt_tm", "")),
            "attachment": item.get("ATTACHMENTNAME", ""),
            "detail_url": f"https://www.bseindia.com/corporates/ann.html",
            "scraped_at": datetime.now().isoformat()
        })
    
    logger.info(f"BSE {segment} announcements: {len(out)}")
    return out

def fetch_board_meetings(segment="equity"):
    today = today_bse()
    seg_code = "E" if segment == "equity" else "ES"
    
    url = "https://api.bseindia.com/BseIndiaAPI/api/BoardMeetings/w"
    params = {
        "strFromDate": today,
        "strToDate": today,
        "strScrip": "",
        "segment": seg_code
    }
    
    raw = safe_get(url, params)
    out = []
    
    items = []
    if isinstance(raw, dict):
        items = raw.get("Table", raw.get("data", []))
    elif isinstance(raw, list):
        items = raw
    
    for item in items:
        out.append({
            "exchange": "BSE", "segment": segment.upper(), "category": "Board Meeting",
            "symbol": str(item.get("SCRIP_CD", "")),
            "company": item.get("SLONGNAME", item.get("company_name", "")),
            "subject": item.get("PURPOSE", item.get("purpose", "")),
            "meeting_date": item.get("MEETING_DATE", ""),
            "filing_date": item.get("NEWS_DT", ""),
            "detail_url": "https://www.bseindia.com/corporates/boardmeetings.html",
            "scraped_at": datetime.now().isoformat()
        })
    
    logger.info(f"BSE {segment} board meetings: {len(out)}")
    return out

def fetch_financial_results(segment="equity"):
    today = today_bse()
    seg_code = "E" if segment == "equity" else "ES"
    
    url = "https://api.bseindia.com/BseIndiaAPI/api/FinancialResults/w"
    params = {
        "strFromDate": today,
        "strToDate": today,
        "strScrip": "",
        "strType": "Quarterly",
        "segment": seg_code
    }
    
    raw = safe_get(url, params)
    out = []
    items = []
    
    if isinstance(raw, dict):
        items = raw.get("Table", raw.get("data", []))
    elif isinstance(raw, list):
        items = raw
    
    for item in items:
        out.append({
            "exchange": "BSE", "segment": segment.upper(), "category": "Financial Result",
            "symbol": str(item.get("SCRIP_CD", "")),
            "company": item.get("SLONGNAME", ""),
            "period": item.get("PERIOD", item.get("quarter", "")),
            "result_type": item.get("RESULT_TYPE", "Quarterly"),
            "filing_date": item.get("NEWS_DT", ""),
            "attachment": item.get("ATTACHMENT", ""),
            "detail_url": "https://www.bseindia.com/corporates/financial-results.html",
            "scraped_at": datetime.now().isoformat()
        })
    
    logger.info(f"BSE {segment} results: {len(out)}")
    return out

def fetch_corporate_actions(segment="equity"):
    today = today_bse()
    seg_code = "E" if segment == "equity" else "ES"
    
    url = "https://api.bseindia.com/BseIndiaAPI/api/CorporateAction/w"
    params = {
        "strFromDate": today,
        "strToDate": today,
        "strScrip": "",
        "segment": seg_code
    }
    
    raw = safe_get(url, params)
    out = []
    items = []
    
    if isinstance(raw, dict):
        items = raw.get("Table", raw.get("data", []))
    elif isinstance(raw, list):
        items = raw
    
    for item in items:
        out.append({
            "exchange": "BSE", "segment": segment.upper(), "category": "Corporate Action",
            "symbol": str(item.get("SCRIP_CD", "")),
            "company": item.get("SLONGNAME", ""),
            "action": item.get("PURPOSE", item.get("action", "")),
            "ex_date": item.get("EX_DATE", ""),
            "record_date": item.get("RD_DATE", ""),
            "remarks": item.get("REMARKS", ""),
            "detail_url": "https://www.bseindia.com/corporates/corporate-actions.html",
            "scraped_at": datetime.now().isoformat()
        })
    
    logger.info(f"BSE {segment} actions: {len(out)}")
    return out

def fetch_shareholding(segment="equity"):
    today = today_bse()
    seg_code = "E" if segment == "equity" else "ES"
    
    url = "https://api.bseindia.com/BseIndiaAPI/api/ShareholdingPatterns/w"
    params = {
        "strFromDate": today,
        "strToDate": today,
        "strScrip": "",
        "segment": seg_code
    }
    
    raw = safe_get(url, params)
    out = []
    items = []
    
    if isinstance(raw, dict):
        items = raw.get("Table", raw.get("data", []))
    elif isinstance(raw, list):
        items = raw
    
    for item in items:
        out.append({
            "exchange": "BSE", "segment": segment.upper(), "category": "Shareholding Pattern",
            "symbol": str(item.get("SCRIP_CD", "")),
            "company": item.get("SLONGNAME", ""),
            "period": item.get("QUARTER", ""),
            "filing_date": item.get("NEWS_DT", ""),
            "promoter_holding": item.get("PROMOTER_HOLDING", ""),
            "public_holding": item.get("PUBLIC_HOLDING", ""),
            "detail_url": "https://www.bseindia.com/corporates/shareholding-pattern.html",
            "scraped_at": datetime.now().isoformat()
        })
    
    logger.info(f"BSE {segment} shareholding: {len(out)}")
    return out

def scrape_all():
    init_session()
    result = {"equity": {}, "sme": {}}
    
    for seg in ["equity", "sme"]:
        result[seg]["corporate_announcements"] = fetch_announcements(seg);  time.sleep(1.5)
        result[seg]["board_meetings"]           = fetch_board_meetings(seg); time.sleep(1.5)
        result[seg]["financial_results"]        = fetch_financial_results(seg); time.sleep(1.5)
        result[seg]["corporate_actions"]        = fetch_corporate_actions(seg); time.sleep(1.5)
        result[seg]["shareholding_patterns"]    = fetch_shareholding(seg);   time.sleep(1.5)
    
    return result

if __name__ == "__main__":
    d = scrape_all()
    print(json.dumps(d, indent=2, default=str)[:3000])
