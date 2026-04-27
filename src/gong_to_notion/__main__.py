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
    domain_from_url,
    extract_domain,
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
                    notion,
                    external,
                    fill_caches,
                    sf_account_ids=call.get("salesforce_account_ids", []),
                    dry_run=False,
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


_GONG_CALL_ID_RE = re.compile(r"[?&]id=(\d+)")

_GONG_BATCH_SIZE = 100  # calls/extensive cap per request.


def _gong_call_id_from_url(url: str | None) -> str | None:
    if not url:
        return None
    m = _GONG_CALL_ID_RE.search(url)
    return m.group(1) if m else None


def cmd_backfill_agency_and_staff(args: argparse.Namespace) -> int:
    """Re-fetch Gong for every Gong Recording page in the window and fill
    Purpose / Agencies / Agency Staff / Role from the authoritative SF +
    parties data. Purpose and Agencies overwrite when Gong has a value;
    Agency Staff is additive; Role is blanks-only on existing rows."""
    notion_token = _require_env("NOTION_TOKEN")
    data_source_id = _require_env("NOTION_DATABASE_ID")
    _require_env("GONG_ACCESS_KEY")
    _require_env("GONG_ACCESS_KEY_SECRET")
    _require_env("GONG_BASE_URL")

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
            f"{len(caches.sf_account_to_agency)} SF accounts, "
            f"{len(caches.email_to_staff)} staff emails",
            file=sys.stderr,
        )

        filter_payload = {
            "and": [
                {"timestamp": "created_time", "created_time": {"on_or_after": from_iso}},
                {"timestamp": "created_time", "created_time": {"before": to_iso}},
                {"property": "Format", "select": {"equals": "Gong Recording"}},
            ]
        }
        print("[backfill] querying candidate pages...", file=sys.stderr)
        pages = notion.query_data_source(
            data_source_id,
            filter=filter_payload,
            sorts=[{"timestamp": "created_time", "direction": "descending"}],
        )
        print(f"  {len(pages)} Gong Recording pages", file=sys.stderr)
        if args.limit:
            pages = pages[: args.limit]
            print(f"  limited to first {len(pages)}", file=sys.stderr)

        # Pass 1: parse Gong call IDs. Pages with no Gong URL get reported but
        # contribute nothing to the batch fetch.
        page_call_ids: dict[str, str] = {}      # page_id → gong_call_id
        pages_without_gong_id: list[tuple[str, str]] = []
        for page in pages:
            page_id = page.get("id", "")
            props = page.get("properties") or {}
            title = _read_title(props.get("Conversation Title"))
            link = (props.get("Link to source") or {}).get("url") or ""
            cid = _gong_call_id_from_url(link)
            if not cid:
                pages_without_gong_id.append((title, page.get("url", "")))
                continue
            page_call_ids[page_id] = cid

        call_ids = sorted(set(page_call_ids.values()))
        print(
            f"[backfill] fetching Gong calls/extensive for {len(call_ids)} "
            f"call IDs (in batches of {_GONG_BATCH_SIZE})...",
            file=sys.stderr,
        )
        gong_by_id: dict[str, dict] = {}
        for i in range(0, len(call_ids), _GONG_BATCH_SIZE):
            chunk = call_ids[i : i + _GONG_BATCH_SIZE]
            gong_by_id.update(fetch_calls_extensive(call_ids=chunk))
        print(f"  got {len(gong_by_id)} Gong calls back", file=sys.stderr)

        totals = {
            "pages_seen": 0,
            "pages_updated": 0,
            "agencies_changed": 0,
            "purpose_changed": 0,
            "staff_added": 0,
            "new_staff": 0,
            "roles_added": 0,
            "pages_no_gong_id": len(pages_without_gong_id),
            "pages_no_gong_call": 0,
            "pages_errored": 0,
            "pages_noop": 0,
        }

        for page in pages:
            totals["pages_seen"] += 1
            page_id = page.get("id", "")
            props = page.get("properties") or {}
            title = _read_title(props.get("Conversation Title"))
            existing_agency_ids = _relation_ids(props.get("Agencies"))
            existing_staff_ids = _relation_ids(props.get("Agency Staff"))
            existing_purpose = _multi_select_values(props.get("Purpose"))

            cid = page_call_ids.get(page_id)
            if not cid:
                print(f"  - {title} — no Gong ID in Link to source", file=sys.stderr)
                continue

            call = gong_by_id.get(cid)
            if not call:
                totals["pages_no_gong_call"] += 1
                print(f"  - {title} — Gong call {cid} not returned", file=sys.stderr)
                continue

            try:
                external = gong_external_people(call.get("participants", []))
                resolution = resolve_call_links(
                    notion,
                    external,
                    caches,
                    sf_account_ids=call.get("salesforce_account_ids", []),
                    dry_run=args.dry_run,
                )
                result = apply_to_page(
                    notion,
                    page_id=page_id,
                    agency_ids=resolution.agency_ids,
                    staff_ids=resolution.staff_ids,
                    existing_agency_ids=existing_agency_ids,
                    existing_staff_ids=existing_staff_ids,
                    purpose=call.get("purpose"),
                    existing_purpose=existing_purpose,
                    overwrite_agencies=True,
                    overwrite_purpose=True,
                    dry_run=args.dry_run,
                )
                changed = (
                    result["agencies_changed"]
                    or result["staff_added"]
                    or result["purpose_changed"]
                    or resolution.roles_updated
                )
                if changed:
                    totals["pages_updated"] += 1
                    totals["agencies_changed"] += result["agencies_changed"]
                    totals["purpose_changed"] += result["purpose_changed"]
                    totals["staff_added"] += result["staff_added"]
                    totals["new_staff"] += len(resolution.new_staff)
                    totals["roles_added"] += resolution.roles_updated
                    print(
                        f"  - {title} — "
                        f"agencies {'overwrite' if result['agencies_changed'] else 'keep'}, "
                        f"purpose {'overwrite' if result['purpose_changed'] else 'keep'}, "
                        f"+{result['staff_added']} staff ({len(resolution.new_staff)} new), "
                        f"+{resolution.roles_updated} roles",
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
    print("Backfill Agency + Staff + Purpose + Role report")
    print(f"  Window (Created):   {from_iso} → {to_iso}")
    print(f"  Dry run:            {'yes' if args.dry_run else 'no'}")
    print(f"  Candidates seen:    {totals['pages_seen']}")
    print(f"  Pages updated:      {totals['pages_updated']}")
    print(f"  No-op pages:        {totals['pages_noop']}")
    print(f"  Pages w/o Gong ID:  {totals['pages_no_gong_id']}")
    print(f"  Pages no Gong call: {totals['pages_no_gong_call']}")
    print(f"  Pages errored:      {totals['pages_errored']}")
    print(f"  Agencies changed:   {totals['agencies_changed']}")
    print(f"  Purpose changed:    {totals['purpose_changed']}")
    print(f"  Staff links +:      {totals['staff_added']}")
    print(f"  New Staff rows:     {totals['new_staff']}")
    print(f"  Roles filled:       {totals['roles_added']}")
    if pages_without_gong_id:
        print("")
        print(f"Pages without a Gong call ID in Link to source ({len(pages_without_gong_id)}):")
        for title, url in pages_without_gong_id:
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
# seed-agency-domains-from-website
# ---------------------------------------------------------------------------


def cmd_seed_agency_domains_from_website(args: argparse.Namespace) -> int:
    """Derive Email Domains for each Agency from its Website URL.

    The Website field is read-only here — we only derive a normalized host
    and, if safe, append it to the Email Domains multi_select. Collisions
    (multiple Agencies whose Websites normalize to the same host) and
    cross-claims (derived host already listed as a domain on a different
    Agency) are reported and left for human review, not auto-written.
    """
    notion_token = _require_env("NOTION_TOKEN")

    with NotionClient(notion_token) as notion:
        print("[seed-website] loading Agencies...", file=sys.stderr)
        agencies = notion.query_data_source(AGENCIES_DATA_SOURCE_ID)

        agency_names: dict[str, str] = {}
        agency_websites: dict[str, str] = {}
        existing_domains: dict[str, set[str]] = {}
        for a in agencies:
            aid = a.get("id", "")
            if not aid:
                continue
            props = a.get("properties") or {}
            agency_names[aid] = _read_title(props.get("Name"))
            website_prop = props.get("Website") or {}
            url = (website_prop.get("url") or "").strip()
            if url:
                agency_websites[aid] = url
            existing_domains[aid] = {
                d.lower()
                for d in _multi_select_values(props.get("Email Domains"))
                if d
            }

        print(
            f"  {len(agencies)} agencies, {len(agency_websites)} with a Website",
            file=sys.stderr,
        )

        derived: dict[str, str] = {}
        unparseable: list[tuple[str, str]] = []  # (name, url)
        domain_claims: dict[str, list[str]] = {}  # host -> [agency_ids]
        for aid, url in agency_websites.items():
            host = domain_from_url(url)
            if not host or host in FREE_MAIL_DOMAINS or host == INTERNAL_DOMAIN:
                unparseable.append((agency_names[aid], url))
                continue
            derived[aid] = host
            domain_claims.setdefault(host, []).append(aid)

        other_claims: dict[str, set[str]] = {}
        for aid, domains in existing_domains.items():
            for d in domains:
                other_claims.setdefault(d, set()).add(aid)

        planned: list[tuple[str, str, str]] = []        # (aid, name, host)
        already_set: list[tuple[str, str]] = []         # (name, host)
        cross_claimed: list[tuple[str, str, list[str]]] = []  # (name, host, other_names)
        for aid, host in derived.items():
            name = agency_names[aid]
            if len(domain_claims[host]) > 1:
                continue  # collision handled below
            if host in existing_domains.get(aid, set()):
                already_set.append((name, host))
                continue
            others = other_claims.get(host, set()) - {aid}
            if others:
                cross_claimed.append(
                    (name, host, sorted(agency_names[x] for x in others))
                )
                continue
            planned.append((aid, name, host))

        collisions: list[tuple[str, list[str]]] = []
        for host, aids in domain_claims.items():
            if len(aids) > 1:
                collisions.append(
                    (host, sorted(agency_names[x] for x in aids))
                )

        print("")
        print(
            f"Seed Email Domains from Website — {'DRY RUN' if args.dry_run else 'APPLY'}"
        )
        print(f"  Agencies with a Website:     {len(agency_websites)}")
        print(f"  Unparseable / free-mail:     {len(unparseable)}")
        print(f"  Collisions (>1 Agency):      {len(collisions)}")
        print(f"  Cross-claimed elsewhere:     {len(cross_claimed)}")
        print(f"  Already set on this Agency:  {len(already_set)}")
        print(f"  To write:                    {len(planned)}")

        if planned:
            print("")
            print("Planned additions:")
            for _, name, host in planned:
                print(f"  + {name}: {host}")
        if collisions:
            print("")
            print("Collisions — Website normalized to a shared host, needs review:")
            for host, names in collisions:
                print(f"  {host}: {', '.join(names)}")
        if cross_claimed:
            print("")
            print("Cross-claimed — host already on a different Agency, needs review:")
            for name, host, others in cross_claimed:
                print(f"  {name} → {host}  (also on: {', '.join(others)})")
        if unparseable:
            print("")
            print("Websites that didn't yield a usable host:")
            for name, url in unparseable:
                print(f"  {name}: {url!r}")

        if args.dry_run or not planned:
            return 0

        print("")
        print("Writing...", file=sys.stderr)
        errors = 0
        for aid, name, host in planned:
            combined = sorted(existing_domains.get(aid, set()) | {host})
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


_KNOWN_SUBCOMMANDS = {
    "run",
    "backfill-agency-and-staff",
    "seed-agency-domains",
    "seed-agency-domains-from-website",
}


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

    seed_web_p = sub.add_parser(
        "seed-agency-domains-from-website",
        help="Derive Email Domains on each Agency from its Website URL.",
    )
    seed_web_p.add_argument(
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
    if args.command == "seed-agency-domains-from-website":
        return cmd_seed_agency_domains_from_website(args)
    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
