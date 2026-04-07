"""
Main Runner — Exchange Filings Dashboard
=========================================
Orchestrates:
  1. NSE scraping  (equity + SME, all categories)
  2. BSE scraping  (equity + SME, all BSE categories)
  3. State-based deduplication  (only process new filings per 5-min run)
  4. Rule-based opportunity scoring
  5. LLM insight generation  (if ANTHROPIC_API_KEY is set)
  6. Save raw JSON + CSV files
  7. Build docs/data.json for GitHub Pages dashboard
  8. Persist state for next run

Run via GitHub Actions every 5 minutes during market hours.
"""

import json
import csv
import os
import sys
import logging
from datetime import datetime
from pathlib import Path

# Support running from scrapers/ or from root
sys.path.insert(0, str(Path(__file__).parent))

from nse_scraper  import scrape_all as nse_scrape
from bse_scraper  import scrape_all as bse_scrape
from opportunity_analyzer import analyze_filings, generate_opportunity_summary
from llm_analyzer import generate_insights
import state_tracker as st

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

ROOT      = Path(__file__).parent.parent
DATA_JSON = ROOT / "data" / "json"
DATA_CSV  = ROOT / "data" / "csv"
DOCS      = ROOT / "docs"
today_str = datetime.now().strftime("%Y-%m-%d")


# ─── Helpers ──────────────────────────────────────────────────────────

def _mkdir():
    for d in (DATA_JSON, DATA_CSV, DOCS):
        d.mkdir(parents=True, exist_ok=True)


def save_json(data, filename: str):
    path = DATA_JSON / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str, ensure_ascii=False)
    logger.info(f"Saved JSON: {path}")


def save_csv(rows: list, filename: str):
    if not rows:
        return
    path = DATA_CSV / filename
    keys = list({k for r in rows for k in r.keys()})
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Saved CSV: {path} ({len(rows)} rows)")


def flatten_exchange(data: dict) -> list:
    """Flatten {equity:{cat:[filings]}, sme:{...}} into a list."""
    flat = []
    for segment in ("equity", "sme"):
        for items in data.get(segment, {}).values():
            if isinstance(items, list):
                flat.extend(items)
    return flat


# ─── Deduplication ────────────────────────────────────────────────────

def deduplicate(new_data: dict, state: dict, exchange: str) -> dict:
    """
    For each segment+category in new_data, filter out already-seen filing IDs.
    Mutates state with newly-seen IDs. Returns filtered data.
    """
    out = {}
    for segment, categories in new_data.items():
        out[segment] = {}
        for cat_name, filings in categories.items():
            if not isinstance(filings, list):
                out[segment][cat_name] = filings
                continue
            # Attach filing_id if missing
            for f in filings:
                if not f.get("filing_id"):
                    f["filing_id"] = st.make_filing_id(f)

            seen = st.get_seen_ids(state, exchange, segment, cat_name)
            new  = st.filter_new(filings, seen, "filing_id")
            if new:
                new_ids = [f["filing_id"] for f in new if f.get("filing_id")]
                st.mark_seen(state, exchange, segment, cat_name, new_ids)
            out[segment][cat_name] = new
    return out


# ─── Merge NSE + BSE for combined views ───────────────────────────────

def build_combined(nse: dict, bse: dict) -> dict:
    """
    Build a combined NSE+BSE flat view, normalising BSE categories
    to match NSE category names where possible.
    """
    BSE_TO_NSE_CAT = {
        "Board Meeting":        "board_meetings",
        "Financial Result":     "financial_results",
        "Corporate Action":     "corporate_actions",
        "Shareholding Pattern": "shareholding_patterns",
        "AGM / EGM":            "corporate_announcements",
        "Company Update":       "corporate_announcements",
        "New Listing":          "corporate_announcements",
        "Integrated Filings":   "corporate_announcements",
        "Others":               "corporate_announcements",
        "Insider Trading / SAST": "insider_trading",
    }

    combined = {"equity": {}, "sme": {}}
    std_cats = [
        "corporate_announcements", "board_meetings", "financial_results",
        "corporate_actions", "shareholding_patterns", "insider_trading",
    ]

    for seg in ("equity", "sme"):
        for cat in std_cats:
            combined[seg][cat] = list(nse.get(seg, {}).get(cat, []))

        # Merge BSE by mapping its categories
        for bse_cat_name, bse_filings in bse.get(seg, {}).items():
            if not isinstance(bse_filings, list):
                continue
            # Determine target combined bucket
            # Try category label on first item
            target = None
            if bse_filings:
                cat_label = bse_filings[0].get("category", "")
                target = BSE_TO_NSE_CAT.get(cat_label)
            if not target:
                target = BSE_TO_NSE_CAT.get(bse_cat_name, "corporate_announcements")
            combined[seg].setdefault(target, []).extend(bse_filings)

    return combined


def important_filings(combined: dict) -> list:
    priority_cats = ["board_meetings", "financial_results", "corporate_actions", "insider_trading"]
    items = []
    for seg in ("equity", "sme"):
        for cat in priority_cats:
            items.extend(combined.get(seg, {}).get(cat, []))
    items.sort(key=lambda x: x.get("filing_date", ""), reverse=True)
    return items


def _count_filings(data: dict) -> int:
    total = 0
    for seg_data in data.values():
        if isinstance(seg_data, dict):
            for items in seg_data.values():
                if isinstance(items, list):
                    total += len(items)
    return total


# ─── Load existing data.json (for merging today's runs) ───────────────

def _load_existing_dashboard() -> dict:
    path = DOCS / "data.json"
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _merge_into_existing(existing: dict, key_path: list, new_items: list) -> None:
    """
    Merge new_items into existing[key_path[0]][key_path[1]][...].
    Deduplicates by filing_id.
    """
    node = existing
    for key in key_path[:-1]:
        node = node.setdefault(key, {})
    last_key = key_path[-1]
    current = node.get(last_key, [])

    seen_ids = {f.get("filing_id", "") for f in current if f.get("filing_id")}
    for item in new_items:
        fid = item.get("filing_id", "")
        if not fid or fid not in seen_ids:
            current.append(item)
            if fid:
                seen_ids.add(fid)
    node[last_key] = current


def _merge_nse_into_dashboard(existing: dict, nse_new: dict):
    for seg, cats in nse_new.items():
        for cat, items in cats.items():
            if isinstance(items, list) and items:
                _merge_into_existing(existing.setdefault("nse", {}), [seg, cat], items)


def _merge_bse_into_dashboard(existing: dict, bse_new: dict):
    for seg, cats in bse_new.items():
        for cat, items in cats.items():
            if isinstance(items, list) and items:
                _merge_into_existing(existing.setdefault("bse", {}), [seg, cat], items)


# ─── Build full dashboard JSON ────────────────────────────────────────

def build_dashboard_json(existing: dict, opp_summary: dict, insights: dict) -> dict:
    nse_data = existing.get("nse", {"equity": {}, "sme": {}})
    bse_data = existing.get("bse", {"equity": {}, "sme": {}})
    combined = build_combined(nse_data, bse_data)
    imp      = important_filings(combined)

    # Recompute stats
    nse_total = _count_filings(nse_data)
    bse_total = _count_filings(bse_data)

    return {
        "last_updated":       datetime.now().isoformat(),
        "date":               today_str,
        "nse":                nse_data,
        "bse":                bse_data,
        "combined":           combined,
        "important_filings":  imp,
        "opportunities":      opp_summary,
        "insights":           insights,
        "stats": {
            "total_filings":          nse_total + bse_total,
            "nse_filings":            nse_total,
            "bse_filings":            bse_total,
            "opportunities_found":    opp_summary.get("total_opportunities", 0),
            "high_opportunity_count": len(opp_summary.get("high_opportunity", [])),
            "insights_generated":     insights.get("total_insights", 0),
        },
    }


# ─── Main ─────────────────────────────────────────────────────────────

def main():
    _mkdir()
    logger.info(f"=== Exchange Filings Run: {today_str} ===")

    # Load persisted state (for dedup)
    # FORCE_FULL=true in GitHub Actions manual trigger resets state
    force_full = os.environ.get("FORCE_FULL", "false").lower() == "true"
    if force_full:
        logger.info("FORCE_FULL=true — resetting dedup state")
        state = {}
    else:
        state = st.load_state()

    # ── NSE ──
    logger.info("Scraping NSE...")
    try:
        nse_raw = nse_scrape()
    except Exception as e:
        logger.error(f"NSE scrape failed: {e}")
        nse_raw = {"equity": {}, "sme": {}}

    nse_new = deduplicate(nse_raw, state, "NSE")
    nse_new_count = _count_filings(nse_new)
    logger.info(f"NSE new filings this run: {nse_new_count}")

    # ── BSE ──
    logger.info("Scraping BSE...")
    try:
        bse_raw = bse_scrape()
    except Exception as e:
        logger.error(f"BSE scrape failed: {e}")
        bse_raw = {"equity": {}, "sme": {}}

    bse_new = deduplicate(bse_raw, state, "BSE")
    bse_new_count = _count_filings(bse_new)
    logger.info(f"BSE new filings this run: {bse_new_count}")

    # Save state early
    st.save_state(state)

    # ── Load & merge into existing dashboard data ──
    # NOTE: We ALWAYS write docs/data.json — even with 0 new filings —
    # so the dashboard shows a fresh timestamp and the file always exists.
    existing = _load_existing_dashboard()
    _merge_nse_into_dashboard(existing, nse_new)
    _merge_bse_into_dashboard(existing, bse_new)

    # ── Opportunities (rule-based) ──
    combined = build_combined(
        existing.get("nse", {}),
        existing.get("bse", {})
    )
    all_for_analysis = {"nse": existing.get("nse", {}), "bse": existing.get("bse", {})}
    opps         = analyze_filings(all_for_analysis)
    opp_summary  = generate_opportunity_summary(opps)

    # ── LLM Insights (only if new data) ──
    insights = existing.get("insights", {})
    if os.environ.get("ANTHROPIC_API_KEY"):
        try:
            insights = generate_insights(all_for_analysis)
        except Exception as e:
            logger.error(f"LLM insights failed: {e}")
    else:
        logger.info("ANTHROPIC_API_KEY not set — skipping LLM insights")

    # ── Save raw daily JSON (only when there are new items) ──
    if nse_new_count:
        save_json(nse_new, f"nse_{today_str}.json")
    if bse_new_count:
        save_json(bse_new, f"bse_{today_str}.json")

    # ── Save CSVs ──
    all_new = flatten_exchange(nse_new) + flatten_exchange(bse_new)
    if all_new:
        save_csv(all_new, f"all_filings_{today_str}.csv")
    if opps:
        save_csv(opps[:500], f"opportunities_{today_str}.csv")

    # ── Build & save dashboard data.json ──
    dashboard = build_dashboard_json(existing, opp_summary, insights)
    dash_path = DOCS / "data.json"
    with open(dash_path, "w", encoding="utf-8") as f:
        json.dump(dashboard, f, indent=2, default=str, ensure_ascii=False)
    logger.info(f"Dashboard data.json updated → {dash_path}")

    # ── Summary ──
    stats = dashboard["stats"]
    logger.info("=== RUN COMPLETE ===")
    logger.info(f"  New this run  — NSE: {nse_new_count}  BSE: {bse_new_count}  (0 = APIs may be rate-limiting, data.json still updated)")
    logger.info(f"  Total today   — NSE: {stats['nse_filings']}  BSE: {stats['bse_filings']}")
    logger.info(f"  Opportunities : {stats['opportunities_found']}  (High: {stats['high_opportunity_count']})")
    logger.info(f"  LLM insights  : {stats['insights_generated']}")


if __name__ == "__main__":
    main()
