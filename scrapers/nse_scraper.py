"""
NSE Exchange Filings Scraper
Pure web scraping - no paid API
Covers Equity and SME segments
Categories: Corporate Announcements, Board Meetings, Financial Results,
            Corporate Actions, Shareholding Patterns
"""

import requests
import json
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

BASE = "https://www.nseindia.com"
SESSION = requests.Session()

def init_session():
    """Hit NSE homepage first to set cookies — mandatory for NSE scraping"""
    try:
        SESSION.get(BASE, headers=HEADERS, timeout=15)
        time.sleep(2)
        SESSION.get(f"{BASE}/companies-listing/corporate-filings-announcements", headers=HEADERS, timeout=15)
        time.sleep(1)
        logger.info("NSE session ready")
    except Exception as e:
        logger.error(f"Session init: {e}")

def today_range():
    today = datetime.now().strftime("%d-%m-%Y")
    return today, today

def safe_get(url):
    try:
        r = SESSION.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.error(f"GET {url} → {e}")
        return []

def fetch_announcements(segment="equities"):
    fd, td = today_range()
    data = safe_get(f"{BASE}/api/corporate-announcements?index={segment}&from_date={fd}&to_date={td}")
    out = []
    for item in (data if isinstance(data, list) else []):
        out.append({
            "exchange": "NSE", "segment": segment.upper(), "category": "Corporate Announcement",
            "symbol": item.get("symbol", ""), "company": item.get("company", item.get("sm_name", "")),
            "subject": item.get("subject", item.get("anct_subject", "")),
            "filing_date": item.get("exchdisstime", item.get("an_dt", "")),
            "attachment": item.get("attchmnt", ""),
            "detail_url": f"https://www.nseindia.com/companies-listing/corporate-filings-announcements",
            "scraped_at": datetime.now().isoformat()
        })
    logger.info(f"NSE {segment} announcements: {len(out)}")
    return out

def fetch_board_meetings(segment="equities"):
    fd, td = today_range()
    data = safe_get(f"{BASE}/api/corporate-board-meetings?index={segment}&from_date={fd}&to_date={td}")
    out = []
    for item in (data if isinstance(data, list) else []):
        out.append({
            "exchange": "NSE", "segment": segment.upper(), "category": "Board Meeting",
            "symbol": item.get("symbol", ""), "company": item.get("company", ""),
            "subject": item.get("purpose", ""), "meeting_date": item.get("bm_date", ""),
            "filing_date": item.get("an_dt", ""), "description": item.get("bm_desc", ""),
            "detail_url": "https://www.nseindia.com/companies-listing/corporate-filings-board-meetings",
            "scraped_at": datetime.now().isoformat()
        })
    logger.info(f"NSE {segment} board meetings: {len(out)}")
    return out

def fetch_financial_results(segment="equities"):
    fd, td = today_range()
    data = safe_get(f"{BASE}/api/corporates-financial-results?index={segment}&period=Quarterly&from_date={fd}&to_date={td}")
    out = []
    for item in (data if isinstance(data, list) else []):
        out.append({
            "exchange": "NSE", "segment": segment.upper(), "category": "Financial Result",
            "symbol": item.get("symbol", ""), "company": item.get("company", ""),
            "period": item.get("period", ""), "result_type": item.get("resultType", ""),
            "filing_date": item.get("filingDate", item.get("an_dt", "")),
            "attachment": item.get("attachment", ""),
            "detail_url": "https://www.nseindia.com/companies-listing/corporate-filings-financial-results",
            "scraped_at": datetime.now().isoformat()
        })
    logger.info(f"NSE {segment} results: {len(out)}")
    return out

def fetch_corporate_actions(segment="equities"):
    fd, td = today_range()
    data = safe_get(f"{BASE}/api/corporates-corporate-actions?index={segment}&from_date={fd}&to_date={td}")
    out = []
    for item in (data if isinstance(data, list) else []):
        out.append({
            "exchange": "NSE", "segment": segment.upper(), "category": "Corporate Action",
            "symbol": item.get("symbol", ""), "company": item.get("company", item.get("comp", "")),
            "action": item.get("subject", item.get("action", "")),
            "ex_date": item.get("exDate", ""), "record_date": item.get("recDate", ""),
            "remarks": item.get("remarks", ""),
            "detail_url": "https://www.nseindia.com/companies-listing/corporate-filings-corporate-actions",
            "scraped_at": datetime.now().isoformat()
        })
    logger.info(f"NSE {segment} actions: {len(out)}")
    return out

def fetch_shareholding(segment="equities"):
    fd, td = today_range()
    data = safe_get(f"{BASE}/api/corporate-share-holdings-master?index={segment}&from_date={fd}&to_date={td}")
    out = []
    for item in (data if isinstance(data, list) else []):
        out.append({
            "exchange": "NSE", "segment": segment.upper(), "category": "Shareholding Pattern",
            "symbol": item.get("symbol", ""), "company": item.get("companyName", item.get("company", "")),
            "period": item.get("period", ""), "filing_date": item.get("filingDate", item.get("an_dt", "")),
            "promoter_holding": item.get("promoterHolding", ""),
            "public_holding": item.get("publicHolding", ""),
            "detail_url": "https://www.nseindia.com/companies-listing/corporate-filings-shareholding-pattern",
            "scraped_at": datetime.now().isoformat()
        })
    logger.info(f"NSE {segment} shareholding: {len(out)}")
    return out

def scrape_all():
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
    print(json.dumps(d, indent=2, default=str)[:3000])
