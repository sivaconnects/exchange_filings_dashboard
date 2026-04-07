"""
Main Runner — Exchange Filings Dashboard
=========================================
Orchestrates:
1. NSE scraping
2. BSE scraping
3. Opportunity analysis
4. Save to data/json/ and data/csv/
5. Generate dashboard data.json for GitHub Pages

Run daily via GitHub Actions at 11:30 PM IST (18:00 UTC)
"""

import json
import csv
import os
import logging
from datetime import datetime
from pathlib import Path

from nse_scraper import scrape_all as nse_scrape_all
from bse_scraper import scrape_all as bse_scrape_all
from opportunity_analyzer import analyze_filings, generate_opportunity_summary

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
DATA_JSON = ROOT / "data" / "json"
DATA_CSV  = ROOT / "data" / "csv"
DOCS      = ROOT / "docs"

today_str = datetime.now().strftime("%Y-%m-%d")


def merge_nse_bse(nse_data: dict, bse_data: dict) -> dict:
    """Merge NSE and BSE data into unified structure"""
    merged = {"equity": {}, "sme": {}}
    
    categories = [
        "corporate_announcements", "board_meetings",
        "financial_results", "corporate_actions", "shareholding_patterns"
    ]
    
    for seg in ["equity", "sme"]:
        for cat in categories:
            nse_items = nse_data.get(seg, {}).get(cat, [])
            bse_items = bse_data.get(seg, {}).get(cat, [])
            merged[seg][cat] = nse_items + bse_items
    
    return merged


def save_json(data: dict, filename: str):
    path = DATA_JSON / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str, ensure_ascii=False)
    logger.info(f"Saved JSON: {path}")


def save_csv(rows: list, filename: str, fieldnames: list = None):
    if not rows:
        logger.info(f"No data for CSV: {filename}")
        return
    
    path = DATA_CSV / filename
    keys = fieldnames or list(rows[0].keys())
    
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    
    logger.info(f"Saved CSV: {path} ({len(rows)} rows)")


def flatten_all(merged: dict) -> list:
    """Flatten merged data into a single list for CSV export"""
    flat = []
    for seg in ["equity", "sme"]:
        for cat, items in merged.get(seg, {}).items():
            flat.extend(items)
    return flat


def generate_important_filings(merged: dict) -> list:
    """
    Important filings = high-value categories across both exchanges
    Board meetings + Financial results + Corporate actions (bonus/buyback/split)
    """
    important = []
    high_value_categories = ["board_meetings", "financial_results", "corporate_actions"]
    
    for seg in ["equity", "sme"]:
        for cat in high_value_categories:
            important.extend(merged.get(seg, {}).get(cat, []))
    
    # Sort by filing date, newest first
    important.sort(key=lambda x: x.get("filing_date", ""), reverse=True)
    return important


def build_dashboard_data(merged: dict, opportunities: dict) -> dict:
    """
    Build the single data.json that powers the GitHub Pages dashboard
    """
    return {
        "last_updated": datetime.now().isoformat(),
        "date": today_str,
        "equity": merged.get("equity", {}),
        "sme": merged.get("sme", {}),
        "important_filings": generate_important_filings(merged),
        "opportunities": opportunities,
        "stats": {
            "total_filings": sum(
                len(items)
                for seg in merged.values()
                for items in seg.values()
            ),
            "equity_filings": sum(len(v) for v in merged.get("equity", {}).values()),
            "sme_filings": sum(len(v) for v in merged.get("sme", {}).values()),
            "opportunities_found": opportunities.get("total_opportunities", 0),
            "high_opportunity_count": len(opportunities.get("high_opportunity", [])),
        }
    }


def main():
    logger.info(f"=== Exchange Filings Run: {today_str} ===")
    
    # 1. Scrape
    logger.info("Scraping NSE...")
    try:
        nse_data = nse_scrape_all()
    except Exception as e:
        logger.error(f"NSE scrape failed: {e}")
        nse_data = {"equity": {}, "sme": {}}
    
    logger.info("Scraping BSE...")
    try:
        bse_data = bse_scrape_all()
    except Exception as e:
        logger.error(f"BSE scrape failed: {e}")
        bse_data = {"equity": {}, "sme": {}}
    
    # 2. Merge
    merged = merge_nse_bse(nse_data, bse_data)
    
    # 3. Analyze opportunities
    logger.info("Analyzing opportunities...")
    opps = analyze_filings(merged)
    opp_summary = generate_opportunity_summary(opps)
    
    # 4. Save JSON files
    save_json(nse_data,     f"nse_{today_str}.json")
    save_json(bse_data,     f"bse_{today_str}.json")
    save_json(merged,       f"combined_{today_str}.json")
    save_json(opp_summary,  f"opportunities_{today_str}.json")
    
    # 5. Save CSVs
    flat = flatten_all(merged)
    save_csv(flat, f"all_filings_{today_str}.csv")
    if opps:
        save_csv(opps, f"opportunities_{today_str}.csv")
    
    # 6. Build dashboard data.json (used by GitHub Pages)
    dashboard_data = build_dashboard_data(merged, opp_summary)
    dashboard_path = DOCS / "data.json"
    with open(dashboard_path, "w", encoding="utf-8") as f:
        json.dump(dashboard_data, f, indent=2, default=str, ensure_ascii=False)
    logger.info(f"Dashboard data saved to {dashboard_path}")
    
    # 7. Summary
    stats = dashboard_data["stats"]
    logger.info(f"=== DONE ===")
    logger.info(f"Total filings: {stats['total_filings']}")
    logger.info(f"  Equity: {stats['equity_filings']}")
    logger.info(f"  SME: {stats['sme_filings']}")
    logger.info(f"Opportunities: {stats['opportunities_found']}")
    logger.info(f"High opportunity: {stats['high_opportunity_count']}")


if __name__ == "__main__":
    main()
