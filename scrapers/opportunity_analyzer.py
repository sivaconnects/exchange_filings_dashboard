"""
Opportunity Analyzer — Multibagger Signal Detection Engine
==========================================================
Goal: Surface high-potential stocks BEFORE institutional discovery.

Signals tracked:
1. Promoter buying / promoter stake increase
2. Institutional (FII/DII) entry signals from shareholding patterns
3. Turnaround signals (consecutive quarterly improvement)
4. Bulk/Block deal activity anomalies
5. Unusual volume of board meeting calls (capex, expansion keywords)
6. Undervalued SME stocks filing strong results
7. Consistent dividend + buyback signals (cash-rich companies)
8. Insider activity: ESOPs, preferential allotments
"""

import json
import os
import re
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ── Keywords that signal growth / expansion intent ──────────────────
BULLISH_KEYWORDS = [
    "expansion", "capex", "new plant", "capacity addition", "new order",
    "acquisition", "merger", "jv", "joint venture", "new facility",
    "preferential allotment", "qip", "fundraise", "rights issue",
    "dividend", "buyback", "bonus", "stock split",
    "record profit", "highest ever", "all time high", "strong growth",
    "new product", "export order", "government contract", "tender",
    "demerger", "restructuring", "debt free", "zero debt",
    "fii", "dii", "institutional", "mutual fund", "stake acquisition"
]

TURNAROUND_KEYWORDS = [
    "turnaround", "profit after loss", "return to profit", "swing to profit",
    "improved margins", "cost reduction", "restructuring complete",
    "debt reduced", "npa resolved", "resolution plan"
]

CAUTION_KEYWORDS = [
    "fraud", "scam", "sebi notice", "penalty", "default", "insolvency",
    "nclt", "winding up", "strike off", "suspension", "delisting"
]

def score_filing(filing: dict) -> dict:
    """
    Score a single filing for opportunity signals.
    Returns the filing enriched with opportunity_score and signals list.
    """
    text = " ".join([
        filing.get("subject", ""),
        filing.get("description", ""),
        filing.get("action", ""),
        filing.get("remarks", ""),
        filing.get("purpose", ""),
    ]).lower()
    
    score = 0
    signals = []
    
    # Positive signals
    for kw in BULLISH_KEYWORDS:
        if kw in text:
            score += 2
            signals.append(f"🟢 {kw.title()}")
    
    for kw in TURNAROUND_KEYWORDS:
        if kw in text:
            score += 3
            signals.append(f"🔵 Turnaround: {kw.title()}")
    
    # Caution signals
    for kw in CAUTION_KEYWORDS:
        if kw in text:
            score -= 5
            signals.append(f"🔴 Caution: {kw.title()}")
    
    # Category bonuses
    category = filing.get("category", "")
    if category == "Shareholding Pattern":
        promoter = filing.get("promoter_holding", "")
        if promoter:
            try:
                p = float(str(promoter).replace("%", "").strip())
                if p > 60:
                    score += 2
                    signals.append(f"🟢 High Promoter Holding: {p}%")
                if p > 70:
                    score += 2
                    signals.append("🟢 Very High Promoter Conviction")
            except:
                pass
    
    if category == "Corporate Action":
        action_text = filing.get("action", "").lower()
        if "bonus" in action_text or "split" in action_text:
            score += 3
            signals.append("🟢 Bonus/Split — Retail Friendly Signal")
        if "dividend" in action_text:
            score += 2
            signals.append("🟢 Dividend — Cash Generating Company")
        if "buyback" in action_text:
            score += 3
            signals.append("🟢 Buyback — Management Confidence")
        if "rights" in action_text:
            score += 1
            signals.append("🟡 Rights Issue — Expansion Funding")
    
    if category == "Board Meeting":
        purpose = filing.get("subject", "").lower()
        if any(k in purpose for k in ["fund", "raise", "capex", "acquisition", "expansion"]):
            score += 3
            signals.append("🟢 Board Meeting: Growth Agenda")
    
    # SME bonus — less tracked, more discovery potential
    if filing.get("segment", "") == "SME":
        score += 1
        signals.append("🔍 SME Segment — Under-Researched")
    
    level = "⚪ Neutral"
    if score >= 8:
        level = "🔥 High Opportunity"
    elif score >= 5:
        level = "✅ Moderate Opportunity"
    elif score >= 3:
        level = "👀 Watch"
    elif score < 0:
        level = "⚠️ Caution"
    
    return {
        **filing,
        "opportunity_score": score,
        "opportunity_level": level,
        "opportunity_signals": signals
    }


def analyze_filings(all_data: dict) -> list:
    """
    Flatten all filings, score each, return top opportunities
    """
    flat = []
    
    for segment in ["equity", "sme"]:
        for category, filings in all_data.get(segment, {}).items():
            for f in filings:
                flat.append(f)
    
    scored = [score_filing(f) for f in flat]
    
    # Filter meaningful signals only (score >= 3)
    opportunities = [f for f in scored if f["opportunity_score"] >= 3]
    
    # Sort by score descending
    opportunities.sort(key=lambda x: x["opportunity_score"], reverse=True)
    
    logger.info(f"Total filings analyzed: {len(flat)}")
    logger.info(f"Opportunities identified: {len(opportunities)}")
    
    return opportunities


def generate_opportunity_summary(opportunities: list) -> dict:
    """
    Build a structured summary for the dashboard opportunities section
    """
    high = [o for o in opportunities if o["opportunity_score"] >= 8]
    moderate = [o for o in opportunities if 5 <= o["opportunity_score"] < 8]
    watch = [o for o in opportunities if 3 <= o["opportunity_score"] < 5]
    
    return {
        "generated_at": datetime.now().isoformat(),
        "total_opportunities": len(opportunities),
        "high_opportunity": high,
        "moderate_opportunity": moderate,
        "watch_list": watch,
        "top_10": opportunities[:10]
    }


if __name__ == "__main__":
    # Test with sample data
    sample = {
        "equity": {
            "corporate_actions": [{
                "exchange": "NSE", "segment": "EQUITY", "category": "Corporate Action",
                "symbol": "TESTCO", "company": "Test Company Ltd",
                "action": "Bonus Issue 1:1", "ex_date": "2024-01-15",
                "scraped_at": datetime.now().isoformat()
            }],
            "board_meetings": [{
                "exchange": "NSE", "segment": "EQUITY", "category": "Board Meeting",
                "symbol": "GROWCO", "company": "Growth Company Ltd",
                "subject": "Fundraise via QIP for capacity expansion",
                "scraped_at": datetime.now().isoformat()
            }]
        },
        "sme": {}
    }
    
    opps = analyze_filings(sample)
    summary = generate_opportunity_summary(opps)
    print(json.dumps(summary, indent=2, default=str))
