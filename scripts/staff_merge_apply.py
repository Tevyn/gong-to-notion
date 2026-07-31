"""Stage 3 of the Agency Staff duplicate merge: validate decisions, then apply.

Reads the cluster facts from staff_merge_gather.py plus the reviewed decision
files, validates every decision against the merge rules, and only then writes.
Validation failures abort the whole run before anything is written: a partially
applied merge is worse than none.

The mechanical parts are deliberately here rather than in the reviewer's hands:

  - relation unions are computed from lists read through the property-item
    endpoint, so the 25-entry truncation on page objects cannot drop links
  - `Agency` and `Customer Conversations` are dual_property relations, so writing
    the winner updates every Customer Interaction and Agency page automatically;
    there is no per-call-page loop to get wrong
  - the winner is written and re-read *before* the loser is touched, so no call
    page is ever left with zero Agency Staff
  - losers are archived to Notion's trash, never hard-deleted

Run (from the repo root):
    uv run python scripts/staff_merge_apply.py --dir <gather-dir> --dry-run
    uv run python scripts/staff_merge_apply.py --dir <gather-dir> --only ericmeier
    uv run python scripts/staff_merge_apply.py --dir <gather-dir>
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.gong_to_notion.agency_and_staff_fill import (  # noqa: E402
    OTHER_EMAILS_PROP,
    _other_emails_payload,
    _read_email_property,
    _read_other_emails,
    _read_relation_ids,
    _read_rich_text_property,
    _read_title_property,
    _role_payload,
    _title_payload,
)
from src.gong_to_notion.notion_client import NotionClient, NotionError  # noqa: E402

HIDDEN_RELATIONS = (
    "Feedback", "Changes", "Research besties", "Research Insights",
    "Teams interacted with",
)
WRITE_PAUSE_SECS = 0.34

# Notion caps a rich_text property value at 2000 chars, and a phone_number at 100.
NOTES_MAX_CHARS = 2000
QUOTES_MAX_CHARS = 100


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
        page = notion._request("GET", f"/pages/{page_id}/properties/{prop_id}", params=params)
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


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate(clusters: dict[str, dict], decisions: list[dict]) -> list[str]:
    """Return a list of problems. Empty means safe to apply."""
    problems: list[str] = []
    seen: set[str] = set()

    for d in decisions:
        key = d.get("cluster")
        where = f"cluster {key!r}"
        if key not in clusters:
            problems.append(f"{where}: not present in the gathered clusters")
            continue
        if key in seen:
            problems.append(f"{where}: decided more than once")
            continue
        seen.add(key)

        packet = clusters[key]
        rows = {r["page_id"]: r for r in packet["rows"]}
        action = d.get("action")
        if action not in ("merge", "flag"):
            problems.append(f"{where}: action must be 'merge' or 'flag', got {action!r}")
            continue
        if action == "flag":
            if not d.get("flag_reason"):
                problems.append(f"{where}: flagged with no flag_reason")
            continue

        winner = d.get("winner_page_id")
        losers = d.get("loser_page_ids") or []
        if winner not in rows:
            problems.append(f"{where}: winner_page_id {winner!r} is not a row in this cluster")
            continue
        unknown = [l for l in losers if l not in rows]
        if unknown:
            problems.append(f"{where}: loser page ids not in this cluster: {unknown}")
            continue
        if winner in losers:
            problems.append(f"{where}: winner also listed as a loser")
            continue
        if set([winner, *losers]) != set(rows):
            missing = set(rows) - set([winner, *losers])
            problems.append(f"{where}: rows unaccounted for: {sorted(missing)}")
            continue

        # The invariant that matters: no address may be dropped, or the importer
        # recreates the duplicate on its next run.
        cluster_addresses = {a for r in rows.values() for a in r["address_activity"]}
        primary = (d.get("primary_email") or "").strip().lower()
        others = {(a or "").strip().lower() for a in (d.get("other_emails") or [])}
        others.discard("")
        kept = ({primary} if primary else set()) | others
        lost = cluster_addresses - kept
        if lost:
            problems.append(f"{where}: these addresses would be lost: {sorted(lost)}")
        invented = kept - cluster_addresses
        if invented:
            problems.append(f"{where}: addresses not present on either row: {sorted(invented)}")
        if primary and primary in others:
            problems.append(f"{where}: primary_email is also listed in other_emails")
        if cluster_addresses and not primary:
            problems.append(f"{where}: cluster has addresses but no primary_email chosen")

        # Body content cannot be moved, so a lone body holder has to win.
        bodies = [pid for pid, r in rows.items() if r["block_count"]]
        if len(bodies) == 1 and bodies[0] != winner:
            problems.append(
                f"{where}: {bodies[0]} holds the only page body but is not the winner"
            )
        if len(bodies) > 1:
            problems.append(
                f"{where}: more than one row has page body content; needs a human"
            )

        role = d.get("role")
        if role is None:
            problems.append(f"{where}: role missing (use \"\" to leave the winner's own)")
        elif role != "":
            existing = {r["role"] for r in rows.values() if r["role"]}
            if role not in existing:
                problems.append(
                    f"{where}: role {role!r} is not one of the existing values {sorted(existing)}"
                )
        if not (d.get("name") or "").strip():
            problems.append(f"{where}: name is empty")

    undecided = set(clusters) - seen
    if undecided:
        problems.append(f"no decision for {len(undecided)} clusters: {sorted(undecided)}")
    return problems


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------


def merge_one(
    notion: NotionClient,
    decision: dict,
    packet: dict,
    dry_run: bool,
) -> dict:
    """Apply one merge. Returns a log record."""
    winner_id = decision["winner_page_id"]
    loser_ids = decision["loser_page_ids"]

    winner_page = notion._request("GET", f"/pages/{winner_id}")
    wprops = winner_page.get("properties") or {}
    w_convos = read_full_relation(notion, winner_id, wprops.get("Customer Conversations") or {})
    w_agencies = read_full_relation(notion, winner_id, wprops.get("Agency") or {})
    w_notes = _read_rich_text_property(wprops.get("Notes"))
    w_role = _read_rich_text_property(wprops.get("Role"))
    w_dept = ((wprops.get("Department") or {}).get("select") or {}).get("name")
    w_dm = ((wprops.get("Decision maker for Swiftly purchase?") or {}).get("select") or {}).get("name")
    w_quotes = (wprops.get("Quotes about openess to future conversations") or {}).get("phone_number")
    w_hidden = {n: _read_relation_ids(wprops.get(n)) for n in HIDDEN_RELATIONS}

    convos = list(w_convos)
    agencies = list(w_agencies)
    notes_parts = [w_notes] if w_notes else []
    dept, dm, quotes = w_dept, w_dm, w_quotes
    hidden = {n: list(v) for n, v in w_hidden.items()}
    loser_records = []
    roles_seen = {w_role} if w_role else set()

    for lid in loser_ids:
        lpage = notion._request("GET", f"/pages/{lid}")
        lprops = lpage.get("properties") or {}
        l_convos = read_full_relation(notion, lid, lprops.get("Customer Conversations") or {})
        l_agencies = read_full_relation(notion, lid, lprops.get("Agency") or {})
        l_notes = _read_rich_text_property(lprops.get("Notes"))
        l_role = _read_rich_text_property(lprops.get("Role"))
        l_email = (_read_email_property(lprops.get("Email")) or "").strip().lower()
        if l_role:
            roles_seen.add(l_role)

        for cid in l_convos:
            if cid not in convos:
                convos.append(cid)
        for aid in l_agencies:
            if aid not in agencies:
                agencies.append(aid)
        if l_notes and l_notes not in notes_parts:
            notes_parts.append(f"From merged duplicate ({l_email or 'no email'}): {l_notes}")
        if dept is None:
            dept = ((lprops.get("Department") or {}).get("select") or {}).get("name")
        if dm is None:
            dm = ((lprops.get("Decision maker for Swiftly purchase?") or {}).get("select") or {}).get("name")
        if quotes is None:
            quotes = (lprops.get("Quotes about openess to future conversations") or {}).get("phone_number")
        for n in HIDDEN_RELATIONS:
            for rid in _read_relation_ids(lprops.get(n)):
                if rid not in hidden[n]:
                    hidden[n].append(rid)

        loser_records.append({
            "id": lid,
            "url": lpage.get("url", ""),
            "email": l_email,
            "convos": l_convos,
            "agencies": l_agencies,
        })

    final_role = decision["role"] or w_role
    superseded = sorted(r for r in roles_seen if r and r != final_role)
    for old in superseded:
        line = f"Previously: {old}"
        if line not in notes_parts:
            notes_parts.append(line)

    # `Quotes about openess to future conversations` is typed phone_number in
    # Notion, which rejects values over 100 chars. Some rows already hold far
    # more than that, so only write it when we are actually copying a value up,
    # and park an over-long one in Notes rather than failing the merge.
    quotes_to_write = quotes if quotes != w_quotes else None
    if quotes_to_write is not None and len(quotes_to_write) > QUOTES_MAX_CHARS:
        notes_parts.append(
            f"Quotes about openness to future conversations (from merged duplicate): "
            f"{quotes_to_write}"
        )
        quotes_to_write = None

    breadcrumb = "Merged duplicate " + "; ".join(
        f"{r['email'] or '(no email)'} ({r['url']})" for r in loser_records
    ) + "."
    notes_parts.append(breadcrumb)
    notes = "\n".join(p for p in notes_parts if p)
    if len(notes) > NOTES_MAX_CHARS:
        print(
            f"  ! {decision['cluster']}: composed Notes is {len(notes)} chars, "
            f"truncating to {NOTES_MAX_CHARS}",
            file=sys.stderr,
        )

    props: dict = {
        "Name": _title_payload(decision["name"]),
        "Customer Conversations": {"relation": [{"id": c} for c in convos]},
        "Agency": {"relation": [{"id": a} for a in agencies]},
        "Notes": {"rich_text": [{"type": "text", "text": {"content": notes[:NOTES_MAX_CHARS]}}]},
    }
    if decision.get("primary_email"):
        props["Email"] = {"email": decision["primary_email"]}
    if decision.get("other_emails"):
        props[OTHER_EMAILS_PROP] = _other_emails_payload(sorted(decision["other_emails"]))
    if final_role:
        props["Role"] = _role_payload(final_role)
    # Selects: only write when copying a value up from a loser, never rewrite the
    # winner's own value back.
    if dept is not None and dept != w_dept:
        props["Department"] = {"select": {"name": dept}}
    if dm is not None and dm != w_dm:
        props["Decision maker for Swiftly purchase?"] = {"select": {"name": dm}}
    if quotes_to_write is not None:
        props["Quotes about openess to future conversations"] = {"phone_number": quotes_to_write}
    for n in HIDDEN_RELATIONS:
        if hidden[n]:
            props[n] = {"relation": [{"id": r} for r in hidden[n]]}

    record = {
        "cluster": decision["cluster"],
        "winner_url": winner_page.get("url", ""),
        "winner_id": winner_id,
        "primary_email": decision.get("primary_email") or "",
        "other_emails": ",".join(sorted(decision.get("other_emails") or [])),
        "convos_before": len(w_convos),
        "convos_after": len(convos),
        "agencies_after": len(agencies),
        "losers": ",".join(r["url"] for r in loser_records),
        "role": final_role,
        "superseded_roles": ";".join(superseded),
    }

    if dry_run:
        record["applied"] = "dry-run"
        return record

    # Winner first, so no call page is ever left with zero Agency Staff.
    notion.update_page(winner_id, props)
    time.sleep(WRITE_PAUSE_SECS)

    check = notion._request("GET", f"/pages/{winner_id}")
    cprops = check.get("properties") or {}
    got_convos = read_full_relation(notion, winner_id, cprops.get("Customer Conversations") or {})
    if set(got_convos) != set(convos):
        raise NotionError(
            f"{decision['cluster']}: winner call links after write are "
            f"{len(got_convos)}, expected {len(convos)}. Loser left untouched."
        )
    got_others = set(_read_other_emails(cprops.get(OTHER_EMAILS_PROP)))
    want_others = {a.lower() for a in (decision.get("other_emails") or [])}
    if got_others != want_others:
        raise NotionError(
            f"{decision['cluster']}: Other Emails after write is {sorted(got_others)}, "
            f"expected {sorted(want_others)}. Loser left untouched."
        )

    for r in loser_records:
        clear: dict = {
            "Customer Conversations": {"relation": []},
            "Agency": {"relation": []},
        }
        notion.update_page(r["id"], clear)
        time.sleep(WRITE_PAUSE_SECS)
        notion._request("PATCH", f"/pages/{r['id']}", json={"in_trash": True})
        time.sleep(WRITE_PAUSE_SECS)

    record["applied"] = "yes"
    return record


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dir", required=True, help="directory written by staff_merge_gather.py")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", action="append", default=None,
                    help="apply just these clusters (repeatable)")
    ap.add_argument("--skip", action="append", default=None,
                    help="skip these clusters, e.g. ones already applied (repeatable)")
    ap.add_argument("--out", default="staff_merge_log.tsv")
    args = ap.parse_args()

    load_dotenv()
    token = os.getenv("NOTION_TOKEN")
    if not token:
        print("Error: NOTION_TOKEN must be set in .env", file=sys.stderr)
        return 2

    base = Path(args.dir)
    clusters = {c["cluster"]: c for c in json.loads((base / "clusters.json").read_text())}
    decisions: list[dict] = []
    files = sorted(glob.glob(str(base / "decisions_batch_*.json")))
    for f in files:
        decisions.extend(json.loads(Path(f).read_text()))
    print(f"[apply] {len(clusters)} clusters, {len(decisions)} decisions "
          f"from {len(files)} files", file=sys.stderr)

    problems = validate(clusters, decisions)
    if problems:
        print(f"\n[apply] VALIDATION FAILED ({len(problems)} problems). Nothing written.",
              file=sys.stderr)
        for p in problems:
            print(f"  ! {p}", file=sys.stderr)
        return 1
    print("[apply] validation passed", file=sys.stderr)

    merges = [d for d in decisions if d["action"] == "merge"]
    flags = [d for d in decisions if d["action"] == "flag"]
    if args.only:
        merges = [d for d in merges if d["cluster"] in set(args.only)]
        print(f"[apply] limited to {len(merges)} cluster(s): "
              f"{', '.join(d['cluster'] for d in merges)}", file=sys.stderr)
    if args.skip:
        skip = set(args.skip)
        before = len(merges)
        merges = [d for d in merges if d["cluster"] not in skip]
        print(f"[apply] skipping {before - len(merges)} already-applied cluster(s): "
              f"{', '.join(sorted(skip))}", file=sys.stderr)
    print(f"[apply] {len(merges)} to merge, {len(flags)} flagged for review",
          file=sys.stderr)

    records: list[dict] = []
    failures = 0
    for d in merges:
        try:
            rec = merge_one(notion := NotionClient(token), d, clusters[d["cluster"]], args.dry_run)
            notion.close()
            records.append(rec)
            print(f"  {'would merge' if args.dry_run else 'merged'} {d['cluster']}: "
                  f"{rec['convos_before']} -> {rec['convos_after']} call links, "
                  f"primary {rec['primary_email'] or '(none)'}", file=sys.stderr)
        except Exception as e:
            failures += 1
            print(f"  ! {d['cluster']}: {type(e).__name__}: {e}", file=sys.stderr)

    if not args.dry_run and records:
        out = Path(args.out)
        cols = list(records[0].keys())
        with out.open("a" if out.exists() else "w") as fh:
            if not out.exists() or out.stat().st_size == 0:
                fh.write("\t".join(cols) + "\n")
            for r in records:
                fh.write("\t".join(str(r.get(c, "")) for c in cols) + "\n")
        print(f"[apply] log: {out}", file=sys.stderr)

    if flags:
        print("\n[apply] flagged, NOT merged:", file=sys.stderr)
        for d in flags:
            print(f"  - {d['cluster']}: {d['flag_reason']}", file=sys.stderr)

    print(f"\n[apply] {len(records)} merged, {failures} failed, {len(flags)} flagged",
          file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
