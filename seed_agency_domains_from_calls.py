"""One-off: seed Agency `Email Domains` from observed call attendance.

For every Customer Conversation that has exactly one Agency, every external
Staff attendee implicitly belongs to that Agency. We aggregate
(staff_email_domain → agency) votes across all single-Agency calls, and
when one Agency wins a domain by an overwhelming majority, propose adding
that domain to its `Email Domains`.

Skipped:
  - Domains already claimed by any Agency (we never re-seed or move).
  - Free-mail and internal (goswift.ly) — non-identifying.
  - Domains with too few votes or no clear winner (left for human review).

Output is a proposal list. `--apply` writes the additions; without it, the
script is read-only.

Run:
    uv run python seed_agency_domains_from_calls.py --dry-run
    uv run python seed_agency_domains_from_calls.py --apply
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv

from src.gong_to_notion.agency_and_staff_fill import (
    AGENCIES_DATA_SOURCE_ID,
    AGENCY_STAFF_DATA_SOURCE_ID,
    FREE_MAIL_DOMAINS,
    INTERNAL_DOMAIN,
    extract_domain,
)
from src.gong_to_notion.notion_client import NotionClient

_ROOT = Path(__file__).resolve().parent

CONVERSATIONS_DATA_SOURCE_ID = "c9db2d38-cf18-4758-985a-99aadc826665"

# Tunables for the proposal threshold. A domain only seeds onto an Agency if
# at least MIN_VOTES distinct Staff with that domain attended single-Agency
# calls AND ≥ MIN_DOMINANCE of those votes went to one Agency.
MIN_VOTES = 2
MIN_DOMINANCE = 0.75

# Human-reviewed allowlist. The vote-aggregation heuristic produces tight
# proposals (≥75% dominance, ≥2 voters) but it can't tell apart "Agency's
# real second domain" from "consulting firm that happens to work with this
# Agency a lot." Only domains in this set are written; everything else is
# reported but skipped, including future re-runs that surface new domains.
APPROVED_DOMAINS: frozenset[str] = frozenset(
    {
        "mdot.maryland.gov",
        "torranceca.gov",
        "nctd.org",
        "dcta.net",
        "nashville.gov",
        "banningca.gov",
        "arlingtonva.us",
        "ct.gov",
        "co.ocean.nj.us",
        "suffolkcountyny.gov",
        "umd.edu",
        "everettwa.gov",
        "lavta.org",
        "bcdcog.com",
        "greensboro-nc.gov",
        "ssfca.gov",
        "townoftruckee.gov",
        "ucdavis.edu",
        "ridevrt.org",
        "catransit.org",
        "detroitmi.gov",
        "durhamnc.gov",
        "co.pg.md.us",
        "ellensburgwa.gov",
        "kerncounty.com",
        "lawrenceks.gov",
        "metrotrains.com.au",
        "pbc.gov",
        "ci.longview.wa.us",
        "seatransit.org",
        "tcatmail.com",
        "phoenix.gov",
    }
)


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
    return [r.get("id", "") for r in (prop.get("relation") or []) if r.get("id")]


def _read_multi_select(prop: dict | None) -> list[str]:
    if not prop:
        return []
    return [
        (o.get("name") or "").strip()
        for o in (prop.get("multi_select") or [])
        if (o.get("name") or "").strip()
    ]


def main() -> int:
    load_dotenv(_ROOT / ".env")
    token = os.getenv("NOTION_TOKEN")
    if not token:
        raise SystemExit("NOTION_TOKEN must be set in .env")

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="Write the proposed additions.")
    ap.add_argument("--dry-run", action="store_true", help="Default behavior; explicit for clarity.")
    ap.add_argument(
        "--min-votes",
        type=int,
        default=MIN_VOTES,
        help=f"Min distinct Staff per domain (default {MIN_VOTES}).",
    )
    ap.add_argument(
        "--min-dominance",
        type=float,
        default=MIN_DOMINANCE,
        help=f"Min share of votes one Agency must hold (default {MIN_DOMINANCE}).",
    )
    args = ap.parse_args()

    with NotionClient(token) as notion:
        print("[load] Agencies...", file=sys.stderr)
        agencies = notion.query_data_source(AGENCIES_DATA_SOURCE_ID)
        agency_name: dict[str, str] = {}
        agency_existing_domains: dict[str, set[str]] = {}
        all_claimed_domains: set[str] = set()
        for a in agencies:
            aid = a.get("id", "")
            if not aid:
                continue
            props = a.get("properties") or {}
            agency_name[aid] = _read_title(props.get("Name"))
            domains = {d.lower() for d in _read_multi_select(props.get("Email Domains")) if d}
            agency_existing_domains[aid] = domains
            all_claimed_domains |= domains

        print(f"  {len(agencies)} agencies, {len(all_claimed_domains)} claimed domains", file=sys.stderr)

        print("[load] Agency Staff...", file=sys.stderr)
        staff_rows = notion.query_data_source(AGENCY_STAFF_DATA_SOURCE_ID)
        staff_email: dict[str, str] = {}
        for row in staff_rows:
            sid = row.get("id", "")
            if not sid:
                continue
            email = _read_email((row.get("properties") or {}).get("Email"))
            if email:
                staff_email[sid] = email
        print(f"  {len(staff_rows)} staff rows, {len(staff_email)} with email", file=sys.stderr)

        print("[load] Customer Conversations...", file=sys.stderr)
        calls = notion.query_data_source(CONVERSATIONS_DATA_SOURCE_ID)
        print(f"  {len(calls)} calls", file=sys.stderr)

        # (domain, agency_id) → set(staff_id) — vote per distinct staff to
        # avoid one chatty person dominating their own domain's tally.
        votes: dict[tuple[str, str], set[str]] = defaultdict(set)
        single_agency_calls = 0
        for call in calls:
            props = call.get("properties") or {}
            agencies_on_call = _read_relation_ids(props.get("Agencies"))
            if len(agencies_on_call) != 1:
                continue
            single_agency_calls += 1
            agency_id = agencies_on_call[0]
            for sid in _read_relation_ids(props.get("Agency Staff")):
                email = staff_email.get(sid)
                if not email:
                    continue
                domain = extract_domain(email)
                if not domain or domain in FREE_MAIL_DOMAINS or domain == INTERNAL_DOMAIN:
                    continue
                if domain in all_claimed_domains:
                    continue
                votes[(domain, agency_id)].add(sid)

        print(f"  {single_agency_calls} single-Agency calls contributed votes", file=sys.stderr)

        # Aggregate per-domain.
        per_domain: dict[str, dict[str, int]] = defaultdict(dict)
        for (domain, aid), staff_set in votes.items():
            per_domain[domain][aid] = len(staff_set)

        proposals: list[tuple[str, str, str, int, int]] = []
        # (domain, agency_id, agency_name, winning_votes, total_votes)
        ambiguous: list[tuple[str, list[tuple[str, int]]]] = []
        thin: list[tuple[str, int]] = []  # (domain, total_votes < min_votes)
        for domain, by_agency in per_domain.items():
            total = sum(by_agency.values())
            if total < args.min_votes:
                thin.append((domain, total))
                continue
            winner_aid, winner_votes = max(by_agency.items(), key=lambda kv: kv[1])
            if winner_votes / total < args.min_dominance:
                ambiguous.append(
                    (
                        domain,
                        sorted(
                            ((agency_name[a], v) for a, v in by_agency.items()),
                            key=lambda x: -x[1],
                        ),
                    )
                )
                continue
            proposals.append(
                (domain, winner_aid, agency_name[winner_aid], winner_votes, total)
            )

        proposals.sort(key=lambda p: (-p[3], p[2], p[0]))
        approved = [p for p in proposals if p[0] in APPROVED_DOMAINS]
        not_approved = [p for p in proposals if p[0] not in APPROVED_DOMAINS]

        print("")
        print("=" * 72)
        print(f"Seed Email Domains from call attendance — {'APPLY' if args.apply else 'DRY RUN'}")
        print("=" * 72)
        print(f"  Min votes:            {args.min_votes}")
        print(f"  Min dominance:        {args.min_dominance:.0%}")
        print(f"  Domains proposed:     {len(proposals)}")
        print(f"  In allowlist:         {len(approved)}")
        print(f"  Not in allowlist:     {len(not_approved)}")
        print(f"  Ambiguous (>1 Agency): {len(ambiguous)}")
        print(f"  Thin (<min votes):    {len(thin)}")

        if approved:
            print("\nApproved (domain → Agency, winning votes / total votes):")
            for domain, _, name, win, total in approved:
                share = f"{win}/{total}"
                print(f"  + {domain:32s} → {name}  ({share})")

        if not_approved:
            print("\nProposed but not in allowlist (skipped):")
            for domain, _, name, win, total in not_approved:
                share = f"{win}/{total}"
                print(f"  - {domain:32s} → {name}  ({share})")

        if ambiguous:
            print("\nAmbiguous — no Agency reached threshold:")
            for domain, breakdown in sorted(ambiguous):
                pretty = ", ".join(f"{n}={v}" for n, v in breakdown)
                print(f"  ? {domain:32s}  {pretty}")

        if not args.apply:
            return 0

        if not approved:
            return 0

        print("\nWriting...", file=sys.stderr)
        # Group approved proposals by Agency so we issue one update per Agency.
        per_agency_new: dict[str, set[str]] = defaultdict(set)
        for domain, aid, _, _, _ in approved:
            per_agency_new[aid].add(domain)

        errors = 0
        for aid, new_domains in per_agency_new.items():
            combined = sorted(agency_existing_domains.get(aid, set()) | new_domains)
            try:
                notion.update_page(
                    aid,
                    {"Email Domains": {"multi_select": [{"name": d} for d in combined]}},
                )
                print(
                    f"  {agency_name[aid]}: +{', '.join(sorted(new_domains))}",
                    file=sys.stderr,
                )
            except Exception as e:
                errors += 1
                print(f"  ! {agency_name[aid]}: {type(e).__name__}: {e}", file=sys.stderr)
        print(f"Done. {len(per_agency_new) - errors} agencies updated, {errors} errored.")
        return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
