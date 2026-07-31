"""One-off (idempotent): add the `Other Emails` property to Agency Staff.

The importer keys Staff rows on the `Email` property. People routinely appear in
Gong under a second address (agency domain migrations, personal vs work, typo
domains Gong picked up from a calendar invite), which produced a second Staff
row every time. `Other Emails` holds every *additional* address a person is
known by, and `load_fill_caches` indexes it alongside `Email`, so a call using
any known address resolves to the existing row.

Free text rather than multi_select on purpose: each address belongs to exactly
one person, so a multi_select would accumulate hundreds of single-use options
and clutter every filter menu in the database.

Run (from the repo root):
    uv run python scripts/add_staff_other_emails_property.py [--dry-run]
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.gong_to_notion.agency_and_staff_fill import (  # noqa: E402
    AGENCY_STAFF_DATA_SOURCE_ID,
    OTHER_EMAILS_PROP,
)
from src.gong_to_notion.notion_client import NotionClient  # noqa: E402

DESCRIPTION = (
    "Additional addresses this person is known by, comma-separated. Indexed by "
    "the Gong importer alongside Email so a call from any of them resolves to "
    "this row instead of creating a duplicate."
)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="report state, write nothing")
    args = ap.parse_args()

    load_dotenv()
    token = os.getenv("NOTION_TOKEN")
    if not token:
        print("Error: NOTION_TOKEN must be set in .env", file=sys.stderr)
        return 2

    with NotionClient(token) as notion:
        ds = notion._request("GET", f"/data_sources/{AGENCY_STAFF_DATA_SOURCE_ID}")
        props = ds.get("properties") or {}
        existing = props.get(OTHER_EMAILS_PROP)
        if existing:
            print(
                f"[other-emails] {OTHER_EMAILS_PROP!r} already exists "
                f"(type={existing.get('type')}). Nothing to do.",
                file=sys.stderr,
            )
            return 0

        print(f"[other-emails] {OTHER_EMAILS_PROP!r} is missing", file=sys.stderr)
        if args.dry_run:
            print("[other-emails] dry run — would add it as rich_text", file=sys.stderr)
            return 0

        notion._request(
            "PATCH",
            f"/data_sources/{AGENCY_STAFF_DATA_SOURCE_ID}",
            json={
                "properties": {
                    OTHER_EMAILS_PROP: {
                        "type": "rich_text",
                        "rich_text": {},
                        "description": DESCRIPTION,
                    }
                }
            },
        )
        after = notion._request("GET", f"/data_sources/{AGENCY_STAFF_DATA_SOURCE_ID}")
        added = (after.get("properties") or {}).get(OTHER_EMAILS_PROP)
        if not added:
            print("[other-emails] ERROR: property still absent after PATCH", file=sys.stderr)
            return 1
        print(f"[other-emails] added as {added.get('type')}", file=sys.stderr)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
