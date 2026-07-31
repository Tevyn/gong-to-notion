"""One-off: repair Agency Staff rows whose Name is just an email address.

Those rows all came from the April 23-24 seeding batch, before the importer's
current behavior (which writes the literal "Unknown" when Gong gives no name).
This script brings them in line with that behavior:

  - Name is email-shaped and matches the Email property  -> Name = "Unknown"
  - Name is email-shaped and Email is blank              -> Email = the address,
                                                            Name = "Unknown"
  - Name is email-shaped but Email holds a *different*
    address                                             -> left alone, reported
    (the title would be the only copy of that address)

Nothing else is touched: rows with a human-readable name, and rows already
titled "Unknown", are skipped. No page is archived and no relation changes.

Run (from the repo root):
    uv run python scripts/fix_email_titled_staff.py --dry-run
    uv run python scripts/fix_email_titled_staff.py
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.gong_to_notion.agency_and_staff_fill import (  # noqa: E402
    AGENCY_STAFF_DATA_SOURCE_ID,
    _read_email_property,
    _read_title_property,
)
from src.gong_to_notion.notion_client import NotionClient, NotionError  # noqa: E402

PLACEHOLDER_NAME = "Unknown"

# Deliberately loose: we only need to recognize "this title is an address",
# and every value we act on is cross-checked against the Email property.
EMAIL_SHAPED = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# Notion tolerates ~3 writes/sec; the client retries 429s but pacing is cheaper.
WRITE_PAUSE_SECS = 0.34


def _title_payload(name: str) -> dict:
    return {"title": [{"type": "text", "text": {"content": name}}]}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="print the plan, write nothing")
    ap.add_argument("--out", default="email_titled_staff_changes.tsv",
                    help="TSV log of every row acted on")
    args = ap.parse_args()

    load_dotenv()
    token = os.getenv("NOTION_TOKEN")
    if not token:
        print("Error: NOTION_TOKEN must be set in .env", file=sys.stderr)
        return 2

    with NotionClient(token) as notion:
        rows = notion.query_data_source(AGENCY_STAFF_DATA_SOURCE_ID)
        print(f"[fix-titles] {len(rows)} Agency Staff rows", file=sys.stderr)

        rename_only: list[tuple[str, str, str]] = []       # (page_id, addr, url)
        rename_and_fill: list[tuple[str, str, str]] = []    # (page_id, addr, url)
        conflicts: list[tuple[str, str, str]] = []          # (title, email, url)

        for row in rows:
            page_id = row.get("id") or ""
            props = row.get("properties") or {}
            title = _read_title_property(props.get("Name"))
            if not EMAIL_SHAPED.match(title.strip()):
                continue
            addr = title.strip()
            email = (_read_email_property(props.get("Email")) or "").strip()
            url = row.get("url", "")
            if not email:
                rename_and_fill.append((page_id, addr, url))
            elif email.lower() == addr.lower():
                rename_only.append((page_id, addr, url))
            else:
                conflicts.append((addr, email, url))

        total = len(rename_only) + len(rename_and_fill)
        print(
            f"[fix-titles] email-titled rows: {total + len(conflicts)}\n"
            f"  Email already matches, retitle only: {len(rename_only)}\n"
            f"  Email blank, copy address up then retitle: {len(rename_and_fill)}\n"
            f"  Email holds a different address, SKIPPED: {len(conflicts)}",
            file=sys.stderr,
        )
        for addr, email, url in conflicts:
            print(f"  ! title {addr!r} vs Email {email!r} — review: {url}", file=sys.stderr)

        if args.dry_run:
            for _, addr, url in rename_only[:10]:
                print(f"  would retitle {addr} -> {PLACEHOLDER_NAME}", file=sys.stderr)
            for _, addr, url in rename_and_fill[:10]:
                print(f"  would set Email={addr}, retitle -> {PLACEHOLDER_NAME}", file=sys.stderr)
            print(f"[fix-titles] dry run — nothing written ({total} rows would change)",
                  file=sys.stderr)
            return 0

        log = Path(args.out)
        failures = 0
        with log.open("w") as fh:
            fh.write("action\taddress\tnew_name\tpage_url\n")
            for kind, batch in (("retitle", rename_only), ("fill_email_and_retitle", rename_and_fill)):
                for page_id, addr, url in batch:
                    payload: dict = {"Name": _title_payload(PLACEHOLDER_NAME)}
                    if kind == "fill_email_and_retitle":
                        payload["Email"] = {"email": addr}
                    try:
                        notion.update_page(page_id, payload)
                        fh.write(f"{kind}\t{addr}\t{PLACEHOLDER_NAME}\t{url}\n")
                    except NotionError as e:
                        failures += 1
                        print(f"  ! {addr}: {e}", file=sys.stderr)
                    time.sleep(WRITE_PAUSE_SECS)

        print(f"[fix-titles] done: {total - failures} updated, {failures} failed, "
              f"{len(conflicts)} skipped. Log: {log}", file=sys.stderr)
        return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
