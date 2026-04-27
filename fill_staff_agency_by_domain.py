"""One-off: backfill `Agency` on Agency Staff rows by email-domain match.

For every Agency Staff row whose `Agency` relation is blank, look up the
domain of its `Email` in the Agencies → Email Domains map and write the
matching agency (if any). Blanks-only — never overwrites an existing
Agency. Free-mail (gmail/yahoo/...) and internal (goswift.ly) domains
are skipped, since neither uniquely identifies an Agency.

Run:
    uv run python fill_staff_agency_by_domain.py --dry-run
    uv run python fill_staff_agency_by_domain.py
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from src.gong_to_notion.agency_and_staff_fill import (
    AGENCY_STAFF_DATA_SOURCE_ID,
    FREE_MAIL_DOMAINS,
    INTERNAL_DOMAIN,
    extract_domain,
    load_fill_caches,
)
from src.gong_to_notion.notion_client import NotionClient

_ROOT = Path(__file__).resolve().parent


def _read_title(prop: dict | None) -> str:
    if not prop:
        return ""
    parts = prop.get("title") or []
    return "".join((p.get("plain_text") or "") for p in parts).strip()


def _read_email(prop: dict | None) -> str:
    if not prop:
        return ""
    if prop.get("type") == "email":
        return (prop.get("email") or "").strip()
    parts = prop.get("rich_text") or []
    return "".join((p.get("plain_text") or "") for p in parts).strip()


def _read_relation_ids(prop: dict | None) -> list[str]:
    if not prop:
        return []
    rels = prop.get("relation") or []
    return [r.get("id", "") for r in rels if r.get("id")]


def main() -> int:
    load_dotenv(_ROOT / ".env")
    token = os.getenv("NOTION_TOKEN")
    if not token:
        raise SystemExit("NOTION_TOKEN must be set in .env")

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Report the planned writes without modifying Notion.",
    )
    args = ap.parse_args()

    with NotionClient(token) as notion:
        print("[load] Agencies + Agency Staff caches...", file=sys.stderr)
        caches = load_fill_caches(notion)
        print(
            f"  {len(caches.domain_to_agency)} agency domains, "
            f"{len(caches.email_to_staff)} staff emails",
            file=sys.stderr,
        )

        print("[load] querying Agency Staff rows...", file=sys.stderr)
        staff_rows = notion.query_data_source(AGENCY_STAFF_DATA_SOURCE_ID)
        print(f"  {len(staff_rows)} staff rows", file=sys.stderr)

        filled: list[tuple[str, str, str]] = []        # (name, email, agency_id)
        already_set: int = 0
        no_email: list[str] = []                       # name
        free_mail: list[tuple[str, str]] = []          # (name, email)
        unmatched: list[tuple[str, str]] = []          # (name, email)

        for row in staff_rows:
            page_id = row.get("id", "")
            if not page_id:
                continue
            props = row.get("properties") or {}
            name = _read_title(props.get("Name"))
            email = _read_email(props.get("Email"))
            existing_agency = _read_relation_ids(props.get("Agency"))

            if existing_agency:
                already_set += 1
                continue
            if not email:
                no_email.append(name or "(unnamed)")
                continue

            domain = extract_domain(email)
            if not domain:
                unmatched.append((name, email))
                continue

            agency_id = caches.domain_to_agency.get(domain)
            if not agency_id:
                if domain in FREE_MAIL_DOMAINS or domain == INTERNAL_DOMAIN:
                    free_mail.append((name, email))
                else:
                    unmatched.append((name, email))
                continue

            if not args.dry_run:
                notion.update_page(
                    page_id, {"Agency": {"relation": [{"id": agency_id}]}}
                )
            filled.append((name, email, agency_id))
            print(f"  {'would fill' if args.dry_run else 'filled'} {email} → {agency_id}", file=sys.stderr)

    print("")
    print("=" * 72)
    print(f"{'DRY-RUN — ' if args.dry_run else ''}Agency Staff Agency backfill")
    print("=" * 72)
    print(f"  {'Would fill' if args.dry_run else 'Filled'}:    {len(filled)}")
    print(f"  Already set:    {already_set}")
    print(f"  No email:       {len(no_email)}")
    print(f"  Free-mail/internal (no Agency to match): {len(free_mail)}")
    print(f"  Unmatched:      {len(unmatched)}  (corporate domain, no Agency)")

    if unmatched:
        # Group by domain so the user can see which Agencies are missing
        # an Email Domains entry vs. truly unknown contacts.
        by_domain: dict[str, list[tuple[str, str]]] = {}
        for name, email in unmatched:
            d = extract_domain(email) or "(?)"
            by_domain.setdefault(d, []).append((name, email))
        print("\nUnmatched corporate domains (count — sample):")
        for d in sorted(by_domain, key=lambda k: -len(by_domain[k])):
            rows = by_domain[d]
            sample = rows[0]
            extra = f" + {len(rows) - 1} more" if len(rows) > 1 else ""
            print(f"  {d}  ({len(rows)})  e.g. {sample[0]!r} <{sample[1]}>{extra}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
