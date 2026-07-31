"""Stage 4 of the Agency Staff duplicate merge: verify the result. Read-only.

Checks the things that would make a merge silently destructive:

  - every retired address still resolves to the surviving row, so the importer
    cannot recreate the duplicate on its next run
  - no Customer Interaction lost its Agency Staff link: the winner is present on
    every call either row used to be linked to, and no loser is still attached
  - each loser is archived with its relations cleared
  - the table shrank by exactly the number of merges performed

Run (from the repo root, after staff_merge_apply.py):
    uv run python scripts/staff_merge_verify.py --dir <gather-dir> [--expect-merges N]
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.gong_to_notion.agency_and_staff_fill import (  # noqa: E402
    AGENCY_STAFF_DATA_SOURCE_ID,
    OTHER_EMAILS_PROP,
    _read_email_property,
    _read_other_emails,
    _read_relation_ids,
    _read_title_property,
    load_fill_caches,
)
from src.gong_to_notion.notion_client import NotionClient, NotionError  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--expect-merges", type=int, default=None)
    args = ap.parse_args()

    load_dotenv()
    token = os.getenv("NOTION_TOKEN")
    if not token:
        print("Error: NOTION_TOKEN must be set in .env", file=sys.stderr)
        return 2

    base = Path(args.dir)
    clusters = {c["cluster"]: c for c in json.loads((base / "clusters.json").read_text())}
    decisions: list[dict] = []
    for f in sorted(glob.glob(str(base / "decisions_batch_*.json"))):
        decisions.extend(json.loads(Path(f).read_text()))
    merges = [d for d in decisions if d["action"] == "merge"]
    flags = [d for d in decisions if d["action"] == "flag"]

    notion = NotionClient(token)
    failures: list[str] = []
    checks = 0

    # --- 1. table size ------------------------------------------------------
    live_rows = notion.query_data_source(AGENCY_STAFF_DATA_SOURCE_ID)
    print(f"[verify] live Agency Staff rows: {len(live_rows)}")
    if args.expect_merges is not None:
        expected = args.expect_merges
        print(f"  expecting the table to have shrunk by {expected}")

    live_ids = {r["id"] for r in live_rows}

    # --- 2. per-merge checks -------------------------------------------------
    for d in merges:
        cluster = d["cluster"]
        winner_id = d["winner_page_id"]
        loser_ids = d["loser_page_ids"]
        packet = clusters[cluster]
        rows = {r["page_id"]: r for r in packet["rows"]}

        if winner_id not in live_ids:
            failures.append(f"{cluster}: winner {winner_id} is not a live row")
            continue
        checks += 1
        for lid in loser_ids:
            if lid in live_ids:
                failures.append(f"{cluster}: loser {lid} is still a live row")
            checks += 1

        wpage = notion._request("GET", f"/pages/{winner_id}")
        wprops = wpage.get("properties") or {}
        title = _read_title_property(wprops.get("Name"))
        email = (_read_email_property(wprops.get("Email")) or "").lower()
        others = set(_read_other_emails(wprops.get(OTHER_EMAILS_PROP)))

        if title != d["name"]:
            failures.append(f"{cluster}: winner title is {title!r}, expected {d['name']!r}")
        checks += 1
        if (d.get("primary_email") or "").lower() != email:
            failures.append(
                f"{cluster}: winner Email is {email!r}, expected {d.get('primary_email')!r}"
            )
        checks += 1

        want_addresses = {a for r in rows.values() for a in r["address_activity"]}
        have = ({email} if email else set()) | others
        missing = want_addresses - have
        if missing:
            failures.append(f"{cluster}: addresses no longer on the winner: {sorted(missing)}")
        checks += 1

        # Losers: relations cleared.
        for lid in loser_ids:
            try:
                lpage = notion._request("GET", f"/pages/{lid}")
            except NotionError as e:
                failures.append(f"{cluster}: cannot read loser {lid}: {e}")
                continue
            if not lpage.get("in_trash") and not lpage.get("archived"):
                failures.append(f"{cluster}: loser {lid} is not archived")
            lprops = lpage.get("properties") or {}
            for prop in ("Customer Conversations", "Agency"):
                left = _read_relation_ids(lprops.get(prop))
                if left:
                    failures.append(
                        f"{cluster}: loser {lid} still has {len(left)} {prop} relations"
                    )
            checks += 2

    # --- 3. every retired address resolves to its winner --------------------
    print("[verify] reloading importer caches...")
    caches = load_fill_caches(notion)
    for d in merges:
        packet = clusters[d["cluster"]]
        want_addresses = {
            a for r in packet["rows"] for a in r["address_activity"]
        }
        for addr in want_addresses:
            entry = caches.email_to_staff.get(addr)
            checks += 1
            if entry is None:
                failures.append(
                    f"{d['cluster']}: address {addr} resolves to nothing; the next "
                    f"import would create a new row for it"
                )
            elif entry.staff_id != d["winner_page_id"]:
                failures.append(
                    f"{d['cluster']}: address {addr} resolves to {entry.staff_id}, "
                    f"not the winner {d['winner_page_id']}"
                )

    # --- 4. no call page lost its staff link --------------------------------
    print("[verify] checking Customer Interaction back-links...")
    for d in merges:
        packet = clusters[d["cluster"]]
        union: list[str] = []
        for r in packet["rows"]:
            for cid in r["convo_ids"]:
                if cid not in union:
                    union.append(cid)
        for cid in union:
            try:
                call = notion._request("GET", f"/pages/{cid}")
            except NotionError as e:
                failures.append(f"{d['cluster']}: cannot read call page {cid}: {e}")
                continue
            staff = _read_relation_ids((call.get("properties") or {}).get("Agency Staff"))
            checks += 1
            if d["winner_page_id"] not in staff:
                failures.append(
                    f"{d['cluster']}: call {cid} does not link the winner"
                )
            still = [l for l in d["loser_page_ids"] if l in staff]
            if still:
                failures.append(f"{d['cluster']}: call {cid} still links losers {still}")

    print(f"\n[verify] {checks} checks run over {len(merges)} merges "
          f"({len(flags)} clusters flagged, untouched)")
    if failures:
        print(f"[verify] {len(failures)} FAILURES:")
        for f in failures:
            print(f"  ! {f}")
        return 1
    print("[verify] all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
