"""
State Tracker — Incremental Scraping
======================================
Stores the last-seen filing IDs per (exchange, segment, category) so that
each 5-minute run only processes genuinely NEW filings.

State file: data/state.json
Format:
{
  "NSE:equity:corporate_announcements": {
    "last_run": "2026-04-07T14:30:00",
    "seen_ids": ["id1", "id2", ...]
  },
  ...
}
"""

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

ROOT      = Path(__file__).parent.parent
STATE_FILE = ROOT / "data" / "state.json"  # committed alongside data/

# Maximum IDs to keep per bucket (prevents unbounded growth)
_MAX_IDS = 2000


def load_state() -> dict:
    try:
        if STATE_FILE.exists():
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"Could not load state: {e}")
    return {}


def save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, default=str)


def get_seen_ids(state: dict, exchange: str, segment: str, category: str) -> set:
    key = f"{exchange}:{segment}:{category}"
    return set(state.get(key, {}).get("seen_ids", []))


def mark_seen(state: dict, exchange: str, segment: str, category: str, ids: list):
    key = f"{exchange}:{segment}:{category}"
    existing = set(state.get(key, {}).get("seen_ids", []))
    existing.update(ids)
    # Trim to prevent unbounded growth — keep most recent
    trimmed = list(existing)[-_MAX_IDS:]
    state[key] = {
        "last_run": datetime.now().isoformat(),
        "seen_ids": trimmed
    }


def filter_new(filings: list, seen_ids: set, id_field: str = "filing_id") -> list:
    """Return only filings whose id_field is NOT in seen_ids."""
    new_filings = []
    for f in filings:
        fid = f.get(id_field, "")
        if fid and fid not in seen_ids:
            new_filings.append(f)
        elif not fid:
            # No ID available — include it (dedup by content hash later if needed)
            new_filings.append(f)
    return new_filings


def make_filing_id(filing: dict) -> str:
    """
    Construct a stable filing ID from available fields.
    Priority: use exchange-specific IDs, fall back to composite key.
    """
    # NSE uses 'an_id' or the symbol+date combo
    for field in ["an_id", "newsid", "NEWSID", "bm_id", "seqno"]:
        val = filing.get(field, "")
        if val:
            return f"{filing.get('exchange','')}-{val}"

    # Composite fallback
    parts = [
        filing.get("exchange", ""),
        filing.get("segment", ""),
        filing.get("symbol", ""),
        filing.get("filing_date", ""),
        (filing.get("subject", "") or filing.get("action", ""))[:40],
    ]
    return "|".join(p for p in parts if p)
