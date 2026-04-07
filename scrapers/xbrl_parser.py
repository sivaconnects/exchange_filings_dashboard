"""
XBRL Parser — NSE Exchange Filings
====================================
NSE wraps each announcement in an XBRL/XML file.
This module fetches those files and extracts:
  - AttachmentURL  → actual PDF hyperlink
  - Description    → full announcement text
  - Subject        → short subject
  - Category       → NSE category
  - Company        → company name
  - Filing date / meeting date etc.

Usage:
  from xbrl_parser import enrich_with_xbrl
  enriched = enrich_with_xbrl(filing_item, session, headers)
"""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from xml.etree import ElementTree as ET

logger = logging.getLogger(__name__)

# All known SEBI XBRL namespace URIs (schema versions change year to year)
_KNOWN_NS = [
    "http://www.sebi.gov.in/xbrl/2025-05-28/in-capmkt",
    "http://www.sebi.gov.in/xbrl/2024-05-28/in-capmkt",
    "http://www.sebi.gov.in/xbrl/2023-05-28/in-capmkt",
    "http://www.sebi.gov.in/xbrl/2022-05-28/in-capmkt",
]

# Tags we want to extract from XBRL → output field name
_FIELD_MAP = {
    "NSESymbol":                "xbrl_symbol",
    "NameOfTheCompany":         "company",
    "SubjectOfAnnouncement":    "subject",
    "DescriptionOfAnnouncement":"description",
    "AttachmentURL":            "attachment_url",
    "DateAndTimeOfSubmission":  "filing_date",
    "CategoryOfAnnouncement":   "xbrl_category",
    "TypeOfMeeting":            "meeting_type",
    "DateOfMeeting":            "meeting_date",
    "PurposeOfBoardMeeting":    "board_purpose",
}


# ─────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────

def enrich_with_xbrl(filing: dict, session, headers: dict) -> dict:
    """
    Given a filing dict with an 'attachment' or 'xbrl_url' field,
    fetch the XBRL file and overlay extracted fields onto the dict.
    Returns the enriched dict (original is NOT mutated).
    """
    url = filing.get("xbrl_url") or filing.get("attachment", "")
    if not _is_xbrl(url):
        return filing

    parsed = _fetch_and_parse(url, session, headers)
    if not parsed:
        return filing

    out = dict(filing)
    for key, val in parsed.items():
        # Don't overwrite non-empty values that came from the API
        if val and not out.get(key):
            out[key] = val
        # Always overwrite attachment_url with the real PDF link
        if key == "attachment_url" and val:
            out["attachment_url"] = val
    return out


def bulk_enrich(filings: list, session, headers: dict, max_workers: int = 8) -> list:
    """
    Enrich a list of filings concurrently.
    Only processes filings whose attachment is an XBRL/XML file.
    """
    to_enrich = [(i, f) for i, f in enumerate(filings) if _is_xbrl(f.get("xbrl_url") or f.get("attachment", ""))]
    if not to_enrich:
        return filings

    results = list(filings)  # copy
    lock = threading.Lock()

    def _worker(idx_filing):
        idx, filing = idx_filing
        enriched = enrich_with_xbrl(filing, session, headers)
        with lock:
            results[idx] = enriched

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_worker, item) for item in to_enrich]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                logger.debug(f"XBRL worker error: {e}")

    return results


# ─────────────────────────────────────────────────────────────────────
# Internals
# ─────────────────────────────────────────────────────────────────────

def _is_xbrl(url: str) -> bool:
    if not url:
        return False
    low = url.lower().strip()
    return low.endswith(".xml") or low.endswith(".xbrl") or "xbrl" in low


def _fetch_and_parse(url: str, session, headers: dict) -> dict:
    try:
        r = session.get(url, headers=headers, timeout=12)
        r.raise_for_status()
        return _parse_xml(r.text)
    except Exception as e:
        logger.debug(f"XBRL fetch failed {url}: {e}")
        return {}


def _parse_xml(xml_text: str) -> dict:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.debug(f"XML parse error: {e}")
        return {}

    result = {}
    for tag_name, field in _FIELD_MAP.items():
        val = _find_tag(root, tag_name)
        if val:
            result[field] = val.strip()
    return result


def _find_tag(root, tag_name: str):
    """Try all known namespaces then fall back to tag-name-only scan."""
    for ns in _KNOWN_NS:
        el = root.find(f"{{{ns}}}{tag_name}")
        if el is not None and el.text:
            return el.text

    # Namespace-agnostic fallback: iterate all elements
    for el in root.iter():
        if el.tag.split("}")[-1] == tag_name and el.text:
            return el.text
    return None
