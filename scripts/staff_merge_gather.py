"""Stage 1 of the Agency Staff duplicate merge: gather facts. Read-only.

Re-derives the duplicate clusters from scratch, then collects everything a
reviewer needs to decide each one, including the pieces that are easy to get
wrong by hand:

  - relation lists read through the property-item endpoint, so the 25-entry
    truncation on page objects cannot silently drop links
  - Gong call dates per address, which is the only sound recency signal
    (Notion's `last edited` mostly records this importer's own writes)
  - page body block types, since inline child databases cannot be migrated

Also computes a *suggested* decision per the merge rules so a reviewer confirms
or overrides rather than starting cold. Writes one JSON file per batch plus a
combined file.

Run (from the repo root):
    uv run python scripts/staff_merge_gather.py --out-dir <dir> [--batches 12]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.gong_to_notion.agency_and_staff_fill import (  # noqa: E402
    AGENCIES_DATA_SOURCE_ID,
    AGENCY_STAFF_DATA_SOURCE_ID,
    LAST_CONTACTED_PROP,
    OTHER_EMAILS_PROP,
    _read_email_property,
    _read_formula_date,
    _read_other_emails,
    _read_relation_ids,
    _read_rich_text_property,
    _read_title_property,
)
from src.gong_to_notion.gong_client import fetch_calls_extensive  # noqa: E402
from src.gong_to_notion.notion_client import NotionClient  # noqa: E402

GONG_FROM = "2026-01-01T00:00:00Z"

# Titles that identify nobody. Two rows sharing one of these is not evidence
# that they are the same person.
PLACEHOLDER_TITLES = {"unknown", "untitled", ""}

EMAIL_SHAPED = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# Single-token titles ("Andy", "Nick") are too weak to cluster on.
MIN_NAME_TOKENS = 2

FREE_MAIL_HINT = {
    "gmail.com", "googlemail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "live.com", "icloud.com", "me.com", "aol.com", "protonmail.com", "proton.me",
}


def norm_name(raw: str) -> str:
    return re.sub(r"[^a-z]", "", (raw or "").lower())


def name_tokens(raw: str) -> int:
    return len([t for t in re.split(r"[\s,]+", (raw or "").strip()) if t])


def domain_of(email: str) -> str:
    return email.split("@", 1)[1].lower() if "@" in email else ""


def read_full_relation(notion: NotionClient, page_id: str, prop: dict) -> list[str]:
    """Page through a relation property. Page objects cap at 25 entries."""
    ids = [r.get("id", "") for r in (prop.get("relation") or []) if r.get("id")]
    if not prop.get("has_more"):
        return ids
    out: list[str] = []
    cursor = None
    prop_id = prop.get("id")
    while True:
        params: dict = {"page_size": 100}
        if cursor:
            params["start_cursor"] = cursor
        page = notion._request(
            "GET", f"/pages/{page_id}/properties/{prop_id}", params=params
        )
        for item in page.get("results", []):
            rel = item.get("relation") or {}
            if rel.get("id"):
                out.append(rel["id"])
        if not page.get("has_more"):
            break
        cursor = page.get("next_cursor")
        if not cursor:
            break
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--batches", type=int, default=12)
    args = ap.parse_args()

    load_dotenv()
    token = os.getenv("NOTION_TOKEN")
    if not token:
        print("Error: NOTION_TOKEN must be set in .env", file=sys.stderr)
        return 2
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    notion = NotionClient(token)

    print("[gather] loading Agencies + Agency Staff...", file=sys.stderr)
    agency_names = {
        a["id"]: _read_title_property((a.get("properties") or {}).get("Name"))
        for a in notion.query_data_source(AGENCIES_DATA_SOURCE_ID)
        if a.get("id")
    }
    staff_rows = notion.query_data_source(AGENCY_STAFF_DATA_SOURCE_ID)
    print(f"  {len(agency_names)} agencies, {len(staff_rows)} staff rows", file=sys.stderr)

    print(f"[gather] fetching Gong calls from {GONG_FROM}...", file=sys.stderr)
    calls = fetch_calls_extensive(GONG_FROM, None)
    gong_names: dict[str, set[str]] = defaultdict(set)
    gong_dates: dict[str, list[str]] = defaultdict(list)
    for call in calls.values():
        started = (call.get("started") or "")[:10]
        for p in call.get("participants", []):
            email = (p.get("email") or "").strip().lower()
            if not email:
                continue
            if started:
                gong_dates[email].append(started)
            name = (p.get("name") or "").strip()
            if name and name.lower() != "unknown" and not EMAIL_SHAPED.match(name):
                gong_names[email].add(name)
    for e in gong_dates:
        gong_dates[e].sort()
    print(f"  {len(calls)} calls, {len(gong_dates)} addresses seen", file=sys.stderr)

    # --- derive clusters -----------------------------------------------------
    # Identity is the best human name we can find for the row: its own title
    # when that is a real name, else a name Gong used for the same address.
    rows: list[dict] = []
    for row in staff_rows:
        rid = row.get("id")
        if not rid:
            continue
        props = row.get("properties") or {}
        title = _read_title_property(props.get("Name")).strip()
        email = (_read_email_property(props.get("Email")) or "").strip().lower()
        others = _read_other_emails(props.get(OTHER_EMAILS_PROP))
        addresses = [a for a in [email, *others] if a]

        identity_source = "title"
        identity = title
        if title.lower() in PLACEHOLDER_TITLES or EMAIL_SHAPED.match(title):
            identity = ""
            for addr in addresses:
                if gong_names.get(addr):
                    identity = sorted(gong_names[addr], key=len, reverse=True)[0]
                    identity_source = "gong"
                    break
        rows.append({
            "id": rid,
            "url": row.get("url", ""),
            "title": title,
            "identity": identity,
            "identity_source": identity_source,
            "email": email,
            "other_emails": others,
            "addresses": addresses,
            "props": props,
            "created": row.get("created_time", ""),
            "last_edited": row.get("last_edited_time", ""),
        })

    groups: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        key = norm_name(r["identity"])
        if not key or r["identity"].lower() in PLACEHOLDER_TITLES:
            continue
        if name_tokens(r["identity"]) < MIN_NAME_TOKENS:
            continue  # a bare first name is not identity
        groups[key].append(r)
    clusters = {k: v for k, v in groups.items() if len(v) > 1}
    print(
        f"[gather] {len(clusters)} clusters covering "
        f"{sum(len(v) for v in clusters.values())} rows",
        file=sys.stderr,
    )

    # --- per-row detail ------------------------------------------------------
    detail: dict[str, dict] = {}
    total = sum(len(v) for v in clusters.values())
    done = 0
    for key, members in clusters.items():
        for r in members:
            props = r["props"]
            convo_prop = props.get("Customer Conversations") or {}
            agency_prop = props.get("Agency") or {}
            convos = read_full_relation(notion, r["id"], convo_prop)
            agencies = read_full_relation(notion, r["id"], agency_prop)
            blocks = notion._request(
                "GET", f"/blocks/{r['id']}/children", params={"page_size": 100}
            ).get("results", [])
            block_types: dict[str, int] = defaultdict(int)
            body_text: list[str] = []
            for b in blocks:
                btype = b.get("type", "?")
                block_types[btype] += 1
                payload = b.get(btype) or {}
                text = "".join(
                    (t.get("plain_text") or "") for t in (payload.get("rich_text") or [])
                ).strip()
                if text:
                    body_text.append(f"{btype}: {text[:300]}")
                elif btype == "child_database":
                    body_text.append(f"child_database: {payload.get('title', '(untitled)')}")

            # Relations Notion omits from the data-source schema but still
            # returns on pages. Empty everywhere today; carried anyway.
            hidden = {
                name: _read_relation_ids(props.get(name))
                for name in ("Feedback", "Changes", "Research besties",
                             "Research Insights", "Teams interacted with")
            }

            addr_activity = {}
            for addr in r["addresses"]:
                dates = gong_dates.get(addr, [])
                addr_activity[addr] = {
                    "calls": len(dates),
                    "first": dates[0] if dates else None,
                    "last": dates[-1] if dates else None,
                    "free_mail": domain_of(addr) in FREE_MAIL_HINT,
                }

            detail[r["id"]] = {
                "cluster": key,
                "page_id": r["id"],
                "url": r["url"],
                "title": r["title"],
                "identity": r["identity"],
                "identity_source": r["identity_source"],
                "email": r["email"],
                "other_emails": r["other_emails"],
                "address_activity": addr_activity,
                "created": r["created"],
                "last_edited": r["last_edited"],
                "last_contacted": _read_formula_date(props.get(LAST_CONTACTED_PROP)),
                "role": _read_rich_text_property(props.get("Role")),
                "notes": _read_rich_text_property(props.get("Notes")),
                "department": ((props.get("Department") or {}).get("select") or {}).get("name"),
                "decision_maker": (
                    (props.get("Decision maker for Swiftly purchase?") or {}).get("select") or {}
                ).get("name"),
                "quotes": (
                    props.get("Quotes about openess to future conversations") or {}
                ).get("phone_number"),
                "agency_ids": agencies,
                "agency_names": [agency_names.get(a, a) for a in agencies],
                "convo_ids": convos,
                "convo_count": len(convos),
                "convo_truncated_in_page_object": bool(convo_prop.get("has_more")),
                "block_count": len(blocks),
                "block_types": dict(block_types),
                "has_child_database": block_types.get("child_database", 0) > 0,
                "body_text": body_text,
                "hidden_relations": {k: v for k, v in hidden.items() if v},
            }
            done += 1
            if done % 20 == 0:
                print(f"  {done}/{total} rows", file=sys.stderr)

    # --- suggested decision per cluster -------------------------------------
    packets = []
    for key, members in clusters.items():
        rs = [detail[m["id"]] for m in members]
        bodies = [r for r in rs if r["block_count"]]
        by_convos = sorted(rs, key=lambda r: -r["convo_count"])

        if len(bodies) == 1:
            winner, why = bodies[0], "only this row has page body content, which cannot be moved"
        elif len(bodies) > 1:
            winner, why = by_convos[0], "multiple rows have bodies; needs review"
        else:
            winner = by_convos[0]
            why = f"most call links ({winner['convo_count']})"
            if by_convos[0]["convo_count"] == by_convos[1]["convo_count"]:
                tie = [r for r in rs if r["email"]] or rs
                winner = tie[0]
                why = "tie on call links; first row with an email"

        addr_last = {}
        for r in rs:
            for addr, act in r["address_activity"].items():
                if act["last"] and (addr not in addr_last or act["last"] > addr_last[addr]):
                    addr_last[addr] = act["last"]
        ranked = sorted(addr_last.items(), key=lambda kv: kv[1], reverse=True)
        suggested_primary = ranked[0][0] if ranked else (winner["email"] or None)
        all_addresses = sorted({a for r in rs for a in r["address_activity"]})

        distinct_domains = {domain_of(a) for a in all_addresses if a}
        concurrent = [
            a for a, last in addr_last.items() if last >= "2026-05-01"
        ]
        agencies_present = [tuple(r["agency_ids"]) for r in rs if r["agency_ids"]]
        conflicting_agencies = len({a for a in agencies_present}) > 1

        packets.append({
            "cluster": key,
            "identity": rs[0]["identity"],
            "rows": rs,
            "suggested": {
                "winner_page_id": winner["page_id"],
                "winner_reason": why,
                "primary_email": suggested_primary,
                "other_emails": [a for a in all_addresses if a != suggested_primary],
                "role": next((r["role"] for r in by_convos if r["role"]), ""),
            },
            "signals": {
                "all_addresses": all_addresses,
                "address_last_call": addr_last,
                "addresses_active_since_may": concurrent,
                "concurrent": len(concurrent) > 1,
                "distinct_domains": sorted(d for d in distinct_domains if d),
                "rows_with_body": len(bodies),
                "any_child_database": any(r["has_child_database"] for r in rs),
                "conflicting_agencies": conflicting_agencies,
                "min_convo_count": min(r["convo_count"] for r in rs),
                "identity_from_gong": any(r["identity_source"] == "gong" for r in rs),
            },
        })

    packets.sort(key=lambda p: (p["signals"]["min_convo_count"], p["cluster"]))
    combined = out_dir / "clusters.json"
    combined.write_text(json.dumps(packets, indent=2, default=str))
    print(f"[gather] wrote {combined} ({len(packets)} clusters)", file=sys.stderr)

    n = max(1, args.batches)
    size = (len(packets) + n - 1) // n
    for i in range(0, len(packets), size):
        batch = packets[i : i + size]
        path = out_dir / f"batch_{i // size + 1:02d}.json"
        path.write_text(json.dumps(batch, indent=2, default=str))
        print(f"  {path.name}: {len(batch)} clusters "
              f"({', '.join(b['cluster'] for b in batch)})", file=sys.stderr)

    truncated = [r["url"] for r in detail.values() if r["convo_truncated_in_page_object"]]
    print(f"\n[gather] rows whose relations were truncated in the page object "
          f"(read in full here): {len(truncated)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
