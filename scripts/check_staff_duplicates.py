"""Find duplicate Agency Staff rows in Notion by email or normalized name.

Read-only — never writes. Lists each cluster with row IDs so the user can
reconcile manually.

Run (from the repo root):
    uv run python scripts/check_staff_duplicates.py
"""

from __future__ import annotations

import os
import re
import sys
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv

# This script lives in scripts/; add the repo root to sys.path so the
# `src...` imports resolve when run as `uv run python scripts/<name>.py`.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.gong_to_notion.agency_and_staff_fill import (
    AGENCY_STAFF_DATA_SOURCE_ID,
    _read_email_property,
    _read_relation_ids,
    _read_title_property,
)
from src.gong_to_notion.notion_client import NotionClient


def _normalize_name(raw: str) -> str:
    s = (raw or "").strip().lower()
    # Drop punctuation, collapse whitespace.
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def main() -> int:
    load_dotenv()
    token = os.environ.get("NOTION_TOKEN")
    if not token:
        print("NOTION_TOKEN not set", file=sys.stderr)
        return 1

    with NotionClient(token) as notion:
        rows = notion.query_data_source(AGENCY_STAFF_DATA_SOURCE_ID)

    print(f"Loaded {len(rows)} Agency Staff rows.\n")

    by_email: dict[str, list[dict]] = defaultdict(list)
    by_name: dict[str, list[dict]] = defaultdict(list)

    for row in rows:
        row_id = row.get("id") or ""
        props = row.get("properties") or {}
        url = row.get("url") or ""
        name = _read_title_property(props.get("Name"))
        email = _read_email_property(props.get("Email"))
        agency_ids = _read_relation_ids(props.get("Agency"))
        info = {
            "id": row_id,
            "url": url,
            "name": name,
            "email": email or "",
            "agency_ids": agency_ids,
        }
        if email:
            by_email[email.strip().lower()].append(info)
        norm = _normalize_name(name)
        if norm:
            by_name[norm].append(info)

    email_dupes = {k: v for k, v in by_email.items() if len(v) > 1}
    name_dupes = {k: v for k, v in by_name.items() if len(v) > 1}

    print(f"=== Duplicate emails: {len(email_dupes)} cluster(s) ===")
    for email, group in sorted(email_dupes.items()):
        print(f"\n  {email}  ({len(group)} rows)")
        for r in group:
            ag = ",".join(r["agency_ids"]) or "—"
            print(f"    - {r['name']!r}  agency={ag}")
            print(f"      {r['url']}")

    # Suppress name clusters that are entirely covered by an email cluster
    # so we don't double-report the obvious ones.
    email_dupe_ids: set[str] = {r["id"] for grp in email_dupes.values() for r in grp}

    name_only = {}
    for k, group in name_dupes.items():
        ids = {r["id"] for r in group}
        if ids.issubset(email_dupe_ids):
            continue
        name_only[k] = group

    print(f"\n=== Duplicate names (not already covered by email): {len(name_only)} cluster(s) ===")
    for norm, group in sorted(name_only.items()):
        print(f"\n  {norm!r}  ({len(group)} rows)")
        for r in group:
            ag = ",".join(r["agency_ids"]) or "—"
            print(f"    - email={r['email'] or '—'}  agency={ag}")
            print(f"      {r['url']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
