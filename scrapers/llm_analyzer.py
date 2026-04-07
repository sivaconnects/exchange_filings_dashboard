"""
LLM Analyzer — Investment Insight Engine
==========================================
Uses Anthropic Claude (claude-sonnet-4-20250514) to analyze today's filings
and generate structured insights across 20+ investment categories.

Requires ANTHROPIC_API_KEY as an environment variable (GitHub Secret).
If the key is absent the module returns empty insights gracefully.

Investment categories generated:
  great_results_q1/q2/q3, growth_triggers, revenue_guidance,
  order_book, preferential_issues, large_orders, capacity_expansions,
  new_products, jv_partnerships, promoter_buying, bulk_deals,
  analyst_meets, asset_expansion, red_flags, cyclical, credit_ratings,
  stocks_correction_10pct, open_offers, star_investors, fii_dii,
  buffett_picks, lynch_picks, bogle_picks
"""

import os
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

try:
    import anthropic
    _HAS_ANTHROPIC = True
except ImportError:
    _HAS_ANTHROPIC = False
    logger.warning("anthropic package not installed — LLM insights disabled")

_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL    = "claude-sonnet-4-20250514"


# ─── Category definitions ─────────────────────────────────────────────

INSIGHT_CATEGORIES = {
    "great_results_latest":  "Companies that announced excellent quarterly results TODAY (latest quarter)",
    "great_results_prev":    "Companies with great results from the immediately previous quarter mentioned in today's filings",
    "great_results_prev2":   "Companies with great results from the quarter before previous mentioned in today's filings",
    "growth_triggers":       "Companies with strong growth triggers: new orders, new products, capacity expansions, govt contracts",
    "revenue_guidance":      "Companies that gave revenue or earnings guidance — vs actual numbers if available",
    "order_book":            "Companies announcing large order wins or updating their order book",
    "preferential_issues":   "Companies doing preferential allotments or QIP fundraises",
    "large_orders":          "Large order receipts — identify company, order size, customer type",
    "capacity_expansions":   "Capex announcements, new plant, new facility, capacity additions",
    "new_products":          "New product launches, new drug filings, new technology announcements",
    "jv_partnerships":       "Joint ventures, partnerships, MoUs, acquisitions announced today",
    "promoter_buying":       "Promoter stake increases, promoter buying from bulk/block deals or shareholding changes",
    "bulk_deals":            "Curated bulk and block deal highlights — who is buying/selling large blocks",
    "analyst_meets":         "Companies hosting analyst meets, investor days, or earnings calls",
    "asset_expansion":       "Companies expanding asset base — land acquisition, new subsidiaries, vertical integration",
    "red_flags":             "SEBI notices, fraud allegations, poor results, defaults, insolvency, heavy penalties, pledging concerns",
    "cyclical":              "Cyclical sector companies (metals, cement, chemicals, auto) with notable announcements",
    "credit_ratings":        "Credit rating upgrades or downgrades announced",
    "stocks_correction":     "Companies whose filings suggest significant price correction risk (>10%)",
    "open_offers":           "Open offer announcements — acquirer, price, shares",
    "star_investors":        "Filings mentioning well-known investors (Radhakishan Damani, Rakesh Jhunjhunwala estate, Dolly Khanna, etc.)",
    "fii_dii":               "FII/DII shareholding changes — notable entries or exits",
    "buffett_picks":         "Stocks matching Warren Buffett criteria: competitive moat, consistent earnings, low debt, undervalued",
    "lynch_picks":           "Stocks matching Peter Lynch criteria: growth at reasonable price (GARP), under-followed, strong earnings momentum",
    "bogle_picks":           "Broad market / index-like exposure themes from today's filings (sector leaders, low-cost, diversified)",
}


# ─── Prompt builders ─────────────────────────────────────────────────

def _build_system_prompt() -> str:
    return (
        "You are an expert Indian stock market analyst specializing in exchange filings.\n"
        "Today's date is " + datetime.now().strftime("%d %b %Y") + ".\n"
        "You will receive today's NSE+BSE corporate filings in JSON form.\n"
        "Your job is to extract structured insights in EXACTLY the JSON format requested.\n"
        "Be concise. Focus on actionable data. Do NOT hallucinate numbers — only use what is in the filings.\n"
        "Return ONLY valid JSON with no markdown fences.\n"
    )


def _build_user_prompt(category_key: str, category_desc: str, filings_text: str) -> str:
    return f"""
Analyze the following Indian exchange filings for today.

TASK: {category_desc}

Return a JSON array of objects. Each object must have:
  "symbol"      : NSE/BSE ticker (string)
  "company"     : Company name (string)
  "exchange"    : NSE or BSE (string)
  "headline"    : One-sentence insight (string, max 120 chars)
  "detail"      : 2-3 sentences of detail (string)
  "signal"      : Positive / Negative / Neutral / Watch (string)
  "tags"        : List of relevant tags like ["results","growth","SME"] (array)
  "filing_date" : Date from the filing if available (string)
  "attachment_url": PDF URL if available in the filing (string)

Return an empty array [] if no relevant filings found for this category.

FILINGS:
{filings_text}
"""


# ─── Core LLM call ───────────────────────────────────────────────────

def _call_llm(system: str, user: str) -> list:
    if not _HAS_ANTHROPIC or not _API_KEY:
        return []
    try:
        client = anthropic.Anthropic(api_key=_API_KEY)
        msg = client.messages.create(
            model=MODEL,
            max_tokens=1500,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        text = msg.content[0].text.strip()
        # Strip any accidental markdown fences
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text)
    except Exception as e:
        logger.error(f"LLM call failed: {e}")
        return []


# ─── Filing context builder ───────────────────────────────────────────

def _flatten_for_llm(all_data: dict, max_items: int = 200) -> str:
    """
    Convert nested filing data into a compact text block for the LLM.
    Keeps only fields useful for analysis. Truncates to max_items total.
    """
    flat = []

    def _add_from(source_dict: dict, exchange_label: str):
        for segment, categories in source_dict.items():
            if not isinstance(categories, dict):
                continue
            for cat_name, filings in categories.items():
                if not isinstance(filings, list):
                    continue
                for f in filings:
                    flat.append({
                        "exchange":      exchange_label,
                        "segment":       segment,
                        "category":      f.get("category", cat_name),
                        "symbol":        f.get("symbol", ""),
                        "company":       f.get("company", ""),
                        "subject":       f.get("subject", f.get("action", "")),
                        "description":   f.get("description", "")[:300],
                        "filing_date":   f.get("filing_date", ""),
                        "attachment_url": f.get("attachment_url", ""),
                        "promoter_holding": f.get("promoter_holding", ""),
                        "period":        f.get("period", ""),
                        "meeting_date":  f.get("meeting_date", ""),
                    })

    _add_from(all_data.get("nse", {}), "NSE")
    _add_from(all_data.get("bse", {}), "BSE")

    # Truncate
    flat = flat[:max_items]
    return json.dumps(flat, ensure_ascii=False, default=str)


# ─── Main entry ───────────────────────────────────────────────────────

def generate_insights(all_data: dict) -> dict:
    """
    Run LLM analysis on today's filings across all insight categories.
    Returns dict keyed by category_key → list of insight objects.
    """
    if not _HAS_ANTHROPIC or not _API_KEY:
        logger.info("Skipping LLM insights — ANTHROPIC_API_KEY not set")
        return _empty_insights()

    logger.info("Starting LLM insight generation...")
    filings_text = _flatten_for_llm(all_data)
    system = _build_system_prompt()

    insights = {}
    for cat_key, cat_desc in INSIGHT_CATEGORIES.items():
        logger.info(f"  Analyzing: {cat_key}")
        user = _build_user_prompt(cat_key, cat_desc, filings_text)
        result = _call_llm(system, user)
        insights[cat_key] = result
        # Small delay to avoid rate limits
        import time; time.sleep(0.5)

    insights["generated_at"] = datetime.now().isoformat()
    insights["model_used"]   = MODEL
    insights["total_insights"] = sum(
        len(v) for v in insights.values() if isinstance(v, list)
    )
    logger.info(f"LLM insights complete — {insights['total_insights']} total items")
    return insights


def _empty_insights() -> dict:
    empty = {k: [] for k in INSIGHT_CATEGORIES}
    empty["generated_at"]    = datetime.now().isoformat()
    empty["model_used"]      = "none"
    empty["total_insights"]  = 0
    return empty


if __name__ == "__main__":
    # Quick smoke test with empty data
    result = generate_insights({})
    print(json.dumps(result, indent=2, default=str)[:1000])
