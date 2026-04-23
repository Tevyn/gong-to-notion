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

def build_call_records(from_iso: str, to_iso: str) -> list[dict]:
    """Fetch extensive metadata + transcripts, merge into the shape mapping.py expects."""
    calls = fetch_calls_extensive(from_iso, to_iso)
    if not calls:
        return []
    transcripts = fetch_transcripts(call_ids=list(calls.keys()))
    out: list[dict] = []
    for call_id, call in calls.items():
        speaker_map = call.pop("_speaker_map", {})
        resolved_turns: list[dict] = []
        for mono in transcripts.get(call_id, []):
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
        out.append(call)
    out.sort(key=lambda c: c.get("started", ""))
    return out


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
    existing_urls: dict[str, str],
    report: RunReport,
    dry_run: bool,
    dump_sink: list | None = None,
) -> None:
    title = call.get("title") or call.get("call_id", "<unknown>")
    gong_url = call.get("url", "")

    if gong_url and gong_url in existing_urls:
        report.skipped.append(
            SkippedRow(title=title, notion_page_id=existing_urls[gong_url])
        )
        return

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


def main() -> int:
    load_dotenv(_ROOT / ".env")

    parser = argparse.ArgumentParser(
        prog="gong_to_notion",
        description="Import external Gong calls into the Customer Interactions Notion DB.",
    )
    parser.add_argument("--since", help="Lookback like '7d' or '12h'.")
    parser.add_argument("--start", help="Window start (ISO date or datetime, UTC).")
    parser.add_argument("--end", help="Window end (ISO date or datetime, UTC).")
    parser.add_argument(
        "--dry-run", action="store_true", help="Do not write to Notion."
    )
    parser.add_argument(
        "--dump",
        metavar="PATH",
        help="Write the exact Notion request payloads (properties + body blocks) "
        "for every would-be-created call to PATH as JSON. Works with or without "
        "--dry-run.",
    )
    args = parser.parse_args()

    from_iso, to_iso = resolve_window(args)

    notion_token = _require_env("NOTION_TOKEN")
    data_source_id = _require_env("NOTION_DATABASE_ID")
    _require_env("GONG_ACCESS_KEY")
    _require_env("GONG_ACCESS_KEY_SECRET")
    _require_env("GONG_BASE_URL")

    report = RunReport(window_start=from_iso, window_end=to_iso, dry_run=args.dry_run)

    print(f"Fetching Gong calls {from_iso} → {to_iso} ...", file=sys.stderr)
    all_calls = build_call_records(from_iso, to_iso)
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

        dump_sink: list | None = [] if args.dump else None

        for call in external:
            process_call(
                call,
                notion,
                data_source_id,
                email_to_user_id,
                existing_urls,
                report,
                args.dry_run,
                dump_sink,
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


if __name__ == "__main__":
    sys.exit(main())
