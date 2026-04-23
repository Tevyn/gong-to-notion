"""Entrypoint: `uv run python -m gong_to_notion ...`.

Resolves the time window, fetches Gong calls + transcripts, filters to
external-customer calls, and creates one Notion page per call in the
Customer Interactions DB.

Dedup is a single batched query per run (see notion_client).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

from .agency_and_staff_fill import (
    AGENCIES_DATA_SOURCE_ID,
    AGENCY_STAFF_DATA_SOURCE_ID,
    FREE_MAIL_DOMAINS,
    INTERNAL_DOMAIN,
    apply_to_page,
    extract_domain,
    extract_participants_emails,
    gong_external_people,
    load_fill_caches,
    resolve_call_links,
)
from .gong_client import fetch_calls_extensive, fetch_transcripts
from .mapping import (
    build_participant_blocks,
    build_participants_toggle,
    build_properties,
    build_transcript_paragraph_blocks,
    build_transcript_toggle,
    resolve_facilitator_email,
)
from .notion_client import NotionClient, NotionError
from .report import CreatedRow, FailedRow, RunReport, SkippedRow

_ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# Time window
# ---------------------------------------------------------------------------

_SINCE_RE = re.compile(r"^(\d+)([dh])$", re.IGNORECASE)


def _normalize_iso(value: str) -> str:
    """Bare `YYYY-MM-DD` → `YYYY-MM-DDT00:00:00Z`. Full ISO passes through."""
    if "T" in value:
        return value
    return f"{value}T00:00:00Z"


def _parse_since(since: str) -> timedelta:
    m = _SINCE_RE.match(since.strip())
    if not m:
        raise ValueError(f"--since must look like '7d' or '12h', got {since!r}")
    n = int(m.group(1))
    unit = m.group(2).lower()
    return timedelta(days=n) if unit == "d" else timedelta(hours=n)


def resolve_window(args: argparse.Namespace) -> tuple[str, str]:
    """Return (from_iso, to_iso) in UTC, suitable for Gong's fromDateTime/toDateTime."""
    now = datetime.now(timezone.utc).replace(microsecond=0)
    if args.since:
        if args.start or args.end:
            raise SystemExit("--since is mutually exclusive with --start/--end")
        delta = _parse_since(args.since)
        return (now - delta).isoformat().replace("+00:00", "Z"), now.isoformat().replace("+00:00", "Z")
    if args.start:
        start = _normalize_iso(args.start)
        end = _normalize_iso(args.end) if args.end else now.isoformat().replace("+00:00", "Z")
        return start, end
    raise SystemExit("Provide either --since or --start (see PRD.md CLI section).")


# ---------------------------------------------------------------------------
# Call record assembly
# ---------------------------------------------------------------------------

def fetch_call_metadata(from_iso: str, to_iso: str) -> list[dict]:
    """Fetch extensive metadata only. Each call retains `_speaker_map` for later
    use by `attach_transcripts`."""
    calls = fetch_calls_extensive(from_iso, to_iso)
    out = list(calls.values())
    out.sort(key=lambda c: c.get("started", ""))
    return out


def attach_transcripts(calls: list[dict]) -> None:
    """Fetch transcripts for the given calls and merge them in place. Consumes
    each call's `_speaker_map` to resolve speaker names."""
    if not calls:
        return
    transcripts = fetch_transcripts(call_ids=[c["call_id"] for c in calls])
    for call in calls:
        speaker_map = call.pop("_speaker_map", {})
        resolved_turns: list[dict] = []
        for mono in transcripts.get(call["call_id"], []):
            speaker_name = speaker_map.get(
                mono["speakerId"], f"Speaker {mono['speakerId']}"
            )
            resolved_turns.append(
                {
                    "speaker": speaker_name,
                    "sentences": mono["sentences"],
                }
            )
        call["transcript"] = resolved_turns


def filter_external(calls: list[dict]) -> list[dict]:
    """Keep calls with ≥1 party whose Gong affiliation == 'External'."""
    kept = []
    for c in calls:
        if any(p.get("affiliation") == "External" for p in c.get("participants", [])):
            kept.append(c)
    return kept


def drop_private(calls: list[dict]) -> tuple[list[dict], int]:
    """Drop calls flagged as private in Gong. Returns (kept, dropped_count)."""
    kept = [c for c in calls if not c.get("is_private")]
    return kept, len(calls) - len(kept)


# ---------------------------------------------------------------------------
# Per-call processing
# ---------------------------------------------------------------------------

def process_call(
    call: dict,
    notion: NotionClient,
    data_source_id: str,
    email_to_user_id: dict[str, str],
    report: RunReport,
    dry_run: bool,
    dump_sink: list | None = None,
    fill_caches=None,
) -> None:
    title = call.get("title") or call.get("call_id", "<unknown>")
    gong_url = call.get("url", "")

    facilitator_email = resolve_facilitator_email(call)
    properties = build_properties(call, email_to_user_id, facilitator_email)
    toggle_block = build_transcript_toggle()
    turn_blocks = build_transcript_paragraph_blocks(call.get("transcript", []))
    participants_toggle = build_participants_toggle()
    participant_blocks = build_participant_blocks(call.get("participants", []))

    if dump_sink is not None:
        dump_sink.append(
            {
                "gong_call_id": call.get("call_id"),
                "gong_url": gong_url,
                "title": title,
                "facilitator_email": facilitator_email,
                "notion_request": {
                    "create_page": {
                        "parent": {
                            "type": "data_source_id",
                            "data_source_id": data_source_id,
                        },
                        "properties": properties,
                    },
                    "append_participants_toggle": {"children": [participants_toggle]},
                    "append_participant_items": {
                        "target": "participants toggle block created above",
                        "block_count": len(participant_blocks),
                        "children": participant_blocks,
                    },
                    "append_transcript_toggle": {"children": [toggle_block]},
                    "append_transcript_turns": {
                        "target": "transcript toggle block created above",
                        "block_count": len(turn_blocks),
                        "children": turn_blocks,
                    },
                },
            }
        )

    if dry_run:
        # We still count it as "would-be created" so dry-run counts match a real run.
        report.created.append(
            CreatedRow(title=title, notion_url="(dry-run)", gong_url=gong_url)
        )
        return

    try:
        # 1. Create the page with no body — POST /pages' response does not
        #    include child-block IDs, so we can't fish the toggle's id out of it.
        # 2. Append the toggle as a child of the page; this PATCH response
        #    returns the created block object (with its id).
        # 3. Stream transcript turns into that toggle in ≤100-block batches.
        page = notion.create_page(
            data_source_id=data_source_id,
            properties=properties,
        )
        created_participants = notion.append_block_children(
            page["id"], [participants_toggle]
        )
        if not created_participants:
            raise NotionError("Appending participants toggle returned no block id.")
        participants_toggle_id = created_participants[0]["id"]
        notion.append_block_children(participants_toggle_id, participant_blocks)

        created_toggles = notion.append_block_children(page["id"], [toggle_block])
        if not created_toggles:
            raise NotionError("Appending transcript toggle returned no block id.")
        toggle_id = created_toggles[0]["id"]
        notion.append_block_children(toggle_id, turn_blocks)
        report.created.append(
            CreatedRow(
                title=title,
                notion_url=page.get("url", ""),
                gong_url=gong_url,
            )
        )

        if fill_caches is not None:
            try:
                external = gong_external_people(call.get("participants", []))
                resolution = resolve_call_links(
                    notion, external, fill_caches, dry_run=False
                )
                apply_to_page(
                    notion,
                    page_id=page["id"],
                    agency_ids=resolution.agency_ids,
                    staff_ids=resolution.staff_ids,
                    existing_agency_ids=[],  # freshly created page — fields are blank
                    existing_staff_ids=[],
                    dry_run=False,
                )
            except Exception as fill_err:
                print(
                    f"[agency-fill] WARN: {title} — {type(fill_err).__name__}: {fill_err}",
                    file=sys.stderr,
                )
    except NotionError as e:
        report.failed.append(
            FailedRow(title=title, error_class="Notion API error", message=str(e))
        )
    except Exception as e:
        report.failed.append(
            FailedRow(title=title, error_class=type(e).__name__, message=str(e))
        )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise SystemExit(
            f"Error: {name} must be set in .env (see PRD.md Config section)."
        )
    return val


def _add_run_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--since", help="Lookback like '7d' or '12h'.")
    p.add_argument("--start", help="Window start (ISO date or datetime, UTC).")
    p.add_argument("--end", help="Window end (ISO date or datetime, UTC).")
    p.add_argument(
        "--dry-run", action="store_true", help="Do not write to Notion."
    )
    p.add_argument(
        "--dump",
        metavar="PATH",
        help="Write the exact Notion request payloads (properties + body blocks) "
        "for every would-be-created call to PATH as JSON. Works with or without "
        "--dry-run.",
    )
    p.add_argument(
        "--skip-fill",
        action="store_true",
        help="Do not run the deterministic Agency/Staff fill on newly created pages.",
    )


def _add_backfill_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--since", help="Window lookback like '30d'.")
    p.add_argument("--start", help="Window start (ISO date/datetime, UTC).")
    p.add_argument("--end", help="Window end (ISO date/datetime, UTC).")
    p.add_argument("--dry-run", action="store_true", help="No writes; print plan only.")
    p.add_argument(
        "--limit",
        type=int,
        help="Optional cap on candidate pages processed (for smoke tests).",
    )


def cmd_run(args: argparse.Namespace) -> int:
    from_iso, to_iso = resolve_window(args)

    notion_token = _require_env("NOTION_TOKEN")
    data_source_id = _require_env("NOTION_DATABASE_ID")
    _require_env("GONG_ACCESS_KEY")
    _require_env("GONG_ACCESS_KEY_SECRET")
    _require_env("GONG_BASE_URL")

    report = RunReport(window_start=from_iso, window_end=to_iso, dry_run=args.dry_run)

    print(f"Fetching Gong call metadata {from_iso} → {to_iso} ...", file=sys.stderr)
    all_calls = fetch_call_metadata(from_iso, to_iso)
    report.candidates_from_gong = len(all_calls)
    external = filter_external(all_calls)
    report.external_customer = len(external)
    external, dropped_private = drop_private(external)
    report.private_excluded = dropped_private
    print(
        f"  {len(all_calls)} candidates, {len(external) + dropped_private} external-customer "
        f"({dropped_private} private excluded)",
        file=sys.stderr,
    )

    with NotionClient(notion_token) as notion:
        print("Resolving Notion users...", file=sys.stderr)
        email_to_user_id = notion.build_email_to_user_id()
        print(f"  {len(email_to_user_id)} person users with email", file=sys.stderr)

        print("Fetching existing Link-to-source values for dedup...", file=sys.stderr)
        existing_urls = notion.fetch_existing_source_urls(data_source_id)
        print(f"  {len(existing_urls)} existing pages with a source URL", file=sys.stderr)

        new_calls: list[dict] = []
        for call in external:
            gong_url = call.get("url", "")
            if gong_url and gong_url in existing_urls:
                report.skipped.append(
                    SkippedRow(
                        title=call.get("title") or call.get("call_id", "<unknown>"),
                        notion_page_id=existing_urls[gong_url],
                    )
                )
            else:
                new_calls.append(call)
        print(
            f"  {len(report.skipped)} already imported, {len(new_calls)} to process",
            file=sys.stderr,
        )

        if new_calls:
            print(f"Fetching transcripts for {len(new_calls)} calls...", file=sys.stderr)
            attach_transcripts(new_calls)

        dump_sink: list | None = [] if args.dump else None

        fill_caches = None
        if new_calls and not args.skip_fill and not args.dry_run:
            print(
                "Loading Agency + Agency Staff caches for deterministic fill...",
                file=sys.stderr,
            )
            fill_caches = load_fill_caches(notion)
            print(
                f"  {len(fill_caches.domain_to_agency)} agency domains, "
                f"{len(fill_caches.email_to_staff)} staff emails",
                file=sys.stderr,
            )

        for call in new_calls:
            process_call(
                call,
                notion,
                data_source_id,
                email_to_user_id,
                report,
                args.dry_run,
                dump_sink,
                fill_caches=fill_caches,
            )

    if dump_sink is not None:
        dump_path = Path(args.dump)
        dump_path.write_text(json.dumps(dump_sink, indent=2, ensure_ascii=False))
        print(
            f"Wrote {len(dump_sink)} Notion request payloads to {dump_path}",
            file=sys.stderr,
        )

    print(report.format())
    return 1 if report.failed else 0


# ---------------------------------------------------------------------------
# backfill-agency-and-staff
# ---------------------------------------------------------------------------


def _resolve_backfill_window(args: argparse.Namespace) -> tuple[str, str]:
    """Same shape as resolve_window but both ends optional; defaults to all-time."""
    now = datetime.now(timezone.utc).replace(microsecond=0)
    if args.since:
        if args.start or args.end:
            raise SystemExit("--since is mutually exclusive with --start/--end")
        delta = _parse_since(args.since)
        return (
            (now - delta).isoformat().replace("+00:00", "Z"),
            now.isoformat().replace("+00:00", "Z"),
        )
    start = _normalize_iso(args.start) if args.start else "1970-01-01T00:00:00Z"
    end = _normalize_iso(args.end) if args.end else now.isoformat().replace("+00:00", "Z")
    return start, end


def cmd_backfill_agency_and_staff(args: argparse.Namespace) -> int:
    notion_token = _require_env("NOTION_TOKEN")
    data_source_id = _require_env("NOTION_DATABASE_ID")

    from_iso, to_iso = _resolve_backfill_window(args)
    print(
        f"[backfill] window (Created): {from_iso} → {to_iso}"
        f"{' — DRY RUN' if args.dry_run else ''}",
        file=sys.stderr,
    )

    with NotionClient(notion_token) as notion:
        print("[backfill] loading Agency + Agency Staff caches...", file=sys.stderr)
        caches = load_fill_caches(notion)
        print(
            f"  {len(caches.domain_to_agency)} agency domains, "
            f"{len(caches.email_to_staff)} staff emails",
            file=sys.stderr,
        )

        filter_payload = {
            "and": [
                {"timestamp": "created_time", "created_time": {"on_or_after": from_iso}},
                {"timestamp": "created_time", "created_time": {"before": to_iso}},
                {"property": "Format", "select": {"equals": "Gong Recording"}},
                {
                    "or": [
                        {"property": "Agencies", "relation": {"is_empty": True}},
                        {"property": "Agency Staff", "relation": {"is_empty": True}},
                    ]
                },
            ]
        }
        print("[backfill] querying candidate pages...", file=sys.stderr)
        pages = notion.query_data_source(
            data_source_id,
            filter=filter_payload,
            sorts=[{"timestamp": "created_time", "direction": "descending"}],
        )
        print(f"  {len(pages)} candidates", file=sys.stderr)
        if args.limit:
            pages = pages[: args.limit]
            print(f"  limited to first {len(pages)}", file=sys.stderr)

        totals = {
            "pages_seen": 0,
            "pages_updated": 0,
            "agencies_added": 0,
            "staff_added": 0,
            "new_staff": 0,
            "pages_no_emails": 0,
            "pages_no_toggle": 0,
            "pages_errored": 0,
            "pages_noop": 0,
        }
        no_toggle_pages: list[tuple[str, str]] = []

        for page in pages:
            totals["pages_seen"] += 1
            page_id = page.get("id", "")
            page_url = page.get("url", "")
            props = page.get("properties") or {}
            title = _read_title(props.get("Conversation Title") or props.get("Contact title"))
            existing_agency_ids = _relation_ids(props.get("Agencies"))
            existing_staff_ids = _relation_ids(props.get("Agency Staff"))
            try:
                toggle_present, participants = extract_participants_emails(notion, page_id)
                if not toggle_present:
                    totals["pages_no_toggle"] += 1
                    no_toggle_pages.append((title, page_url))
                    print(f"  - {title} — no Participants toggle", file=sys.stderr)
                    continue
                external = [
                    p
                    for p in participants
                    if extract_domain(p.get("email")) != INTERNAL_DOMAIN
                ]
                if not external:
                    totals["pages_no_emails"] += 1
                    print(f"  - {title} — Participants toggle empty / internal-only", file=sys.stderr)
                    continue
                resolution = resolve_call_links(
                    notion, external, caches, dry_run=args.dry_run
                )
                result = apply_to_page(
                    notion,
                    page_id=page_id,
                    agency_ids=resolution.agency_ids,
                    staff_ids=resolution.staff_ids,
                    existing_agency_ids=existing_agency_ids,
                    existing_staff_ids=existing_staff_ids,
                    dry_run=args.dry_run,
                )
                if result["agencies_added"] or result["staff_added"]:
                    totals["pages_updated"] += 1
                    totals["agencies_added"] += result["agencies_added"]
                    totals["staff_added"] += result["staff_added"]
                    totals["new_staff"] += len(resolution.new_staff)
                    print(
                        f"  - {title} — +{result['agencies_added']} agencies, "
                        f"+{result['staff_added']} staff "
                        f"({len(resolution.new_staff)} new)",
                        file=sys.stderr,
                    )
                else:
                    totals["pages_noop"] += 1
            except Exception as e:
                totals["pages_errored"] += 1
                print(
                    f"  - {title} — ERROR {type(e).__name__}: {e}",
                    file=sys.stderr,
                )

    print("")
    print("Backfill Agency + Staff fill report")
    print(f"  Window (Created): {from_iso} → {to_iso}")
    print(f"  Dry run:          {'yes' if args.dry_run else 'no'}")
    print(f"  Candidates seen:  {totals['pages_seen']}")
    print(f"  Pages updated:    {totals['pages_updated']}")
    print(f"  No-op pages:      {totals['pages_noop']}")
    print(f"  Pages w/o emails: {totals['pages_no_emails']}")
    print(f"  Pages w/o toggle: {totals['pages_no_toggle']}")
    print(f"  Pages errored:    {totals['pages_errored']}")
    print(f"  Agency links +:   {totals['agencies_added']}")
    print(f"  Staff links +:    {totals['staff_added']}")
    print(f"  New Staff rows:   {totals['new_staff']}")
    if no_toggle_pages:
        print("")
        print(f"Pages missing Participants toggle ({len(no_toggle_pages)}):")
        for title, url in no_toggle_pages:
            print(f"  {url}  {title}")
    return 1 if totals["pages_errored"] else 0


# ---------------------------------------------------------------------------
# seed-agency-domains
# ---------------------------------------------------------------------------


def cmd_seed_agency_domains(args: argparse.Namespace) -> int:
    """Derive Email Domains for each Agency from its existing Staff emails."""
    notion_token = _require_env("NOTION_TOKEN")

    with NotionClient(notion_token) as notion:
        print("[seed] loading Agencies and Agency Staff...", file=sys.stderr)
        agencies = notion.query_data_source(AGENCIES_DATA_SOURCE_ID)
        staff = notion.query_data_source(AGENCY_STAFF_DATA_SOURCE_ID)

        agency_names: dict[str, str] = {}
        existing_domains: dict[str, set[str]] = {}
        for a in agencies:
            aid = a.get("id", "")
            props = a.get("properties") or {}
            agency_names[aid] = _read_title(props.get("Name"))
            existing_domains[aid] = set(
                d.lower()
                for d in _multi_select_values(props.get("Email Domains"))
                if d
            )

        derived: dict[str, set[str]] = {aid: set() for aid in agency_names}
        staff_without_agency = 0
        staff_without_email = 0
        for row in staff:
            props = row.get("properties") or {}
            rel = _relation_ids(props.get("Agency"))
            email_prop = props.get("Email") or {}
            email = None
            if email_prop.get("type") == "email":
                email = (email_prop.get("email") or "").strip() or None
            elif email_prop.get("type") == "rich_text":
                parts = email_prop.get("rich_text") or []
                email = (
                    "".join((p.get("plain_text") or "") for p in parts).strip() or None
                )
            if not email:
                staff_without_email += 1
                continue
            if not rel:
                staff_without_agency += 1
                continue
            domain = extract_domain(email)
            if not domain:
                continue
            if domain in FREE_MAIL_DOMAINS or domain == INTERNAL_DOMAIN:
                continue
            for aid in rel:
                derived.setdefault(aid, set()).add(domain)

        print(
            f"  {len(agencies)} agencies, {len(staff)} staff "
            f"({staff_without_email} no email, {staff_without_agency} no agency)",
            file=sys.stderr,
        )

        planned: list[tuple[str, str, list[str]]] = []  # (id, name, new_domains_to_add)
        for aid, domains in derived.items():
            if not domains:
                continue
            to_add = sorted(domains - existing_domains.get(aid, set()))
            if not to_add:
                continue
            planned.append((aid, agency_names.get(aid, "(unknown)"), to_add))

        print("")
        print(f"Seed Email Domains — {'DRY RUN' if args.dry_run else 'APPLY'}")
        print(f"  Agencies to update: {len(planned)}")
        for aid, name, to_add in planned:
            print(f"  - {name} [{aid}] += {to_add}")

        if args.dry_run or not planned:
            return 0

        print("")
        print("Writing...", file=sys.stderr)
        errors = 0
        for aid, name, to_add in planned:
            combined = sorted(existing_domains.get(aid, set()) | set(to_add))
            try:
                notion.update_page(
                    aid,
                    {
                        "Email Domains": {
                            "multi_select": [{"name": d} for d in combined]
                        }
                    },
                )
            except Exception as e:
                errors += 1
                print(f"  ! {name}: {type(e).__name__}: {e}", file=sys.stderr)
        print(f"Done. {len(planned) - errors} updated, {errors} errored.")
        return 1 if errors else 0


# ---------------------------------------------------------------------------
# Local tiny readers (avoid importing the lot from agency_and_staff_fill)
# ---------------------------------------------------------------------------


def _read_title(prop: dict | None) -> str:
    if not prop:
        return "(untitled)"
    parts = prop.get("title") or []
    text = "".join((p.get("plain_text") or "") for p in parts).strip()
    return text or "(untitled)"


def _relation_ids(prop: dict | None) -> list[str]:
    if not prop:
        return []
    return [r.get("id", "") for r in (prop.get("relation") or []) if r.get("id")]


def _multi_select_values(prop: dict | None) -> list[str]:
    if not prop:
        return []
    return [
        (o.get("name") or "").strip()
        for o in (prop.get("multi_select") or [])
        if (o.get("name") or "").strip()
    ]


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


_KNOWN_SUBCOMMANDS = {"run", "backfill-agency-and-staff", "seed-agency-domains"}


def main() -> int:
    load_dotenv(_ROOT / ".env")

    parser = argparse.ArgumentParser(
        prog="gong_to_notion",
        description="Import external Gong calls into the Customer Interactions Notion DB.",
    )
    sub = parser.add_subparsers(dest="command")

    run_p = sub.add_parser("run", help="Import Gong calls into Notion (default).")
    _add_run_args(run_p)

    bf_p = sub.add_parser(
        "backfill-agency-and-staff",
        help="Deterministic Agency/Staff fill over existing Customer Interactions pages.",
    )
    _add_backfill_args(bf_p)

    seed_p = sub.add_parser(
        "seed-agency-domains",
        help="Derive Email Domains on each Agency from its existing Staff emails.",
    )
    seed_p.add_argument(
        "--dry-run", action="store_true", help="Print plan without writing."
    )

    # Back-compat: if no subcommand given, treat as 'run' (unless user is
    # asking for top-level help).
    argv = sys.argv[1:]
    if argv and argv[0] in ("-h", "--help"):
        parser.parse_args(argv)  # prints help and exits
    if not argv or argv[0] not in _KNOWN_SUBCOMMANDS:
        argv = ["run"] + argv
    args = parser.parse_args(argv)

    if args.command == "run":
        return cmd_run(args)
    if args.command == "backfill-agency-and-staff":
        return cmd_backfill_agency_and_staff(args)
    if args.command == "seed-agency-domains":
        return cmd_seed_agency_domains(args)
    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
