"""
Opportunity Analyzer — Multibagger Signal Detection
=====================================================
Rule-based scoring (no LLM required) that runs even without an API key.
LLM insights are layered on top via llm_analyzer.py.

Signals scored:
  Bullish  +2 each  : expansion, capex, order, JV, preferential, dividend, etc.
  Turnaround +3 each: profit after loss, debt reduced, etc.
  Caution  -5 each  : fraud, SEBI notice, insolvency, default, etc.

Bonus multipliers:
  Promoter holding > 70%  : +2
  Buyback                 : +3
  Bonus/split             : +3
  SME segment             : +1 (under-researched)
  Board meeting (growth)  : +3
"""

import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

BULLISH = [
    "expansion", "capex", "new plant", "capacity addition", "new order",
    "acquisition", "merger", "jv", "joint venture", "new facility",
    "preferential allotment", "qip", "fundraise", "rights issue",
    "dividend", "buyback", "bonus", "stock split",
    "record profit", "highest ever", "all time high", "strong growth",
    "new product", "export order", "government contract", "tender",
    "demerger", "restructuring", "debt free", "zero debt",
    "fii", "dii", "institutional", "mutual fund", "stake acquisition",
    "large order", "order book", "asset acquisition", "patent",
    "new drug", "approval", "nod", "partnership", "mou",
]

TURNAROUND = [
    "turnaround", "profit after loss", "return to profit", "swing to profit",
    "improved margins", "cost reduction", "restructuring complete",
    "debt reduced", "npa resolved", "resolution plan", "back to black",
    "positive ebitda", "fy profit",
]

CAUTION = [
    "fraud", "scam", "sebi notice", "penalty", "default", "insolvency",
    "nclt", "winding up", "strike off", "suspension", "delisting",
    "promoter pledge", "invocation", "npa", "bad loans", "audit qualification",
    "going concern", "impairment", "write-off", "forensic audit",
]


def score_filing(filing: dict) -> dict:
    text = " ".join(filter(None, [
        filing.get("subject", ""),
        filing.get("description", ""),
        filing.get("action", ""),
        filing.get("remarks", ""),
        filing.get("board_purpose", ""),
    ])).lower()

    score   = 0
    signals = []

    for kw in BULLISH:
        if kw in text:
            score += 2
            signals.append(f"🟢 {kw.title()}")

    for kw in TURNAROUND:
        if kw in text:
            score += 3
            signals.append(f"🔵 Turnaround: {kw.title()}")

    for kw in CAUTION:
        if kw in text:
            score -= 5
            signals.append(f"🔴 Caution: {kw.title()}")

    # Category bonuses
    cat = filing.get("category", "")

    if cat == "Shareholding Pattern":
        ph = filing.get("promoter_holding", "")
        try:
            p = float(str(ph).replace("%", "").strip())
            if p > 70:
                score += 4; signals.append(f"🟢 Promoter Holding {p}% (Very High)")
            elif p > 60:
                score += 2; signals.append(f"🟢 Promoter Holding {p}%")
        except Exception:
            pass

    if cat == "Corporate Action":
        act = (filing.get("action") or filing.get("subject") or "").lower()
        if "bonus" in act or "split" in act:
            score += 3; signals.append("🟢 Bonus/Split — Retail Signal")
        if "dividend" in act:
            score += 2; signals.append("🟢 Dividend")
        if "buyback" in act:
            score += 3; signals.append("🟢 Buyback — Mgmt Confidence")
        if "rights" in act:
            score += 1; signals.append("🟡 Rights Issue")

    if cat == "Board Meeting":
        purpose = (filing.get("subject") or filing.get("description") or "").lower()
        growth_words = ["fund", "raise", "capex", "acquisition", "expansion", "qip", "ncd"]
        if any(k in purpose for k in growth_words):
            score += 3; signals.append("🟢 Board — Growth Agenda")

    if filing.get("segment", "") == "SME":
        score += 1; signals.append("🔍 SME — Under-Researched")

    # FII/DII entry
    if "fii" in text or "dii" in text or "institutional" in text:
        score += 2; signals.append("🟢 FII/DII Interest")

    level = "⚪ Neutral"
    if score >= 8:
        level = "🔥 High Opportunity"
    elif score >= 5:
        level = "✅ Moderate Opportunity"
    elif score >= 3:
        level = "👀 Watch"
    elif score < 0:
        level = "⚠️ Red Flag"

    return {
        **filing,
        "opportunity_score":   score,
        "opportunity_level":   level,
        "opportunity_signals": signals[:8],  # cap displayed signals
    }


def analyze_filings(all_data: dict) -> list:
    flat = []
    for exchange_key in ("nse", "bse"):
        for segment in ("equity", "sme"):
            for cat_items in all_data.get(exchange_key, {}).get(segment, {}).values():
                if isinstance(cat_items, list):
                    flat.extend(cat_items)

    # Fallback for old merged structure
    if not flat:
        for segment in ("equity", "sme"):
            for cat_items in all_data.get(segment, {}).values():
                if isinstance(cat_items, list):
                    flat.extend(cat_items)

    scored = [score_filing(f) for f in flat]
    opportunities = [f for f in scored if f["opportunity_score"] >= 3]
    opportunities.sort(key=lambda x: x["opportunity_score"], reverse=True)

    logger.info(f"Scored {len(flat)} filings → {len(opportunities)} opportunities")
    return opportunities


def generate_opportunity_summary(opportunities: list) -> dict:
    high     = [o for o in opportunities if o["opportunity_score"] >= 8]
    moderate = [o for o in opportunities if 5 <= o["opportunity_score"] < 8]
    watch    = [o for o in opportunities if 3 <= o["opportunity_score"] < 5]
    red_flags = [o for o in opportunities if o["opportunity_score"] < 0]

    return {
        "generated_at":       datetime.now().isoformat(),
        "total_opportunities": len(opportunities),
        "high_opportunity":   high,
        "moderate_opportunity": moderate,
        "watch_list":         watch,
        "red_flags":          red_flags,
        "top_10":             opportunities[:10],
    }
