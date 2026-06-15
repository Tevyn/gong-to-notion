"""Upsert Notion Agencies from an SFDC Accounts Report xlsx.

Matches xlsx rows to existing Notion Agencies by normalized Account Name
(reusing alias-aware normalization), and:
  - On matched pages, fills any of the supported properties that are blank
    in Notion. Never overwrites a non-empty value.
  - On unmatched xlsx rows, optionally CREATES a new Agency page with
    Name, Account ID, Account Stage, Classification, and any other
    supported columns present in the row.

Default mode is dry-run; pass --apply to actually write. Pass --no-create
to skip creating new pages (updates only). Conflicts (existing value differs
from xlsx) are logged, never clobbered.

Run (from the repo root; needs the `scripts` extra for openpyxl):
    uv run --extra scripts python scripts/fill_agency_account_ids.py PATH.xlsx
    uv run --extra scripts python scripts/fill_agency_account_ids.py PATH.xlsx --apply
    uv run --extra scripts python scripts/fill_agency_account_ids.py PATH.xlsx --apply --no-create
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

# This script lives in scripts/; add the repo root to sys.path so the
# `src...` imports resolve when run as `uv run python scripts/<name>.py`.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.gong_to_notion.agency_and_staff_fill import (
    AGENCIES_DATA_SOURCE_ID,
    normalize_sf_account_id,
)
from src.gong_to_notion.notion_client import NotionClient

_ROOT = Path(__file__).resolve().parents[1]

HEADER_SENTINEL = "Account Name"


# ---------------------------------------------------------------------------
# Name normalization — matches xlsx Account Name to Notion Agency title
# ---------------------------------------------------------------------------

_PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)
_WS_RE = re.compile(r"\s+")
_TRAILING_PAREN_RE = re.compile(r"\s*\(([^()]*)\)\s*$")


def normalize_name(raw: str) -> str:
    s = (raw or "").strip().lower()
    s = _PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def name_candidates(raw: str) -> set[str]:
    keys: set[str] = set()
    s = (raw or "").strip()
    if not s:
        return keys
    full = normalize_name(s)
    if full:
        keys.add(full)
    m = _TRAILING_PAREN_RE.search(s)
    if m:
        outer = _TRAILING_PAREN_RE.sub("", s).strip()
        inner = m.group(1).strip()
        if outer:
            k = normalize_name(outer)
            if k:
                keys.add(k)
        if inner:
            k = normalize_name(inner)
            if k:
                keys.add(k)
    return keys


# ---------------------------------------------------------------------------
# Column mapping config
# ---------------------------------------------------------------------------


# `xlsx_col` is the header text in the SFDC report. `notion_prop` is the
# Notion property name. `kind` controls payload shape and validation.
# Some xlsx columns appear with curly-quote variants — we normalize before
# header lookup, so the spec uses ASCII quotes.
@dataclass(frozen=True)
class ColMap:
    xlsx_col: str
    notion_prop: str
    kind: str  # title | text | select | multi_select | number | date


COLUMN_MAPPINGS: list[ColMap] = [
    ColMap("Account ID", "Account ID", "text"),
    ColMap("Account Stage", "Account Stage", "select"),
    ColMap("Market Segment", "CS Tier", "select"),
    ColMap("Customer ARR", "Customer ARR", "number"),
    ColMap("TAM", "TAM", "number"),
    ColMap("Contracted Fleet", "Contracted Fleet", "number"),
    ColMap("Product Summary", "Products", "multi_select"),
    ColMap("Customer Since", "Customer Since", "date"),
    ColMap(
        "Current CAD/AVL Contract Expiration",
        "Current CAD/AVL Contract Expiration",
        "date",
    ),
    ColMap("Total User Accounts", "User Accounts", "number"),
    ColMap("Renewal Date", "Renewal Date", "date"),
    ColMap("Churn Likelihood", "Churn Likelihood", "select"),
    ColMap("Renewal Risk Reason", "Renewal Risk Reason", "text"),
    ColMap("Who provides alerts for the agency?", "Who Provides Alerts", "select"),
    ColMap(
        "Passenger-Facing Tools Integrated",
        "Passenger Facing Tools",
        "multi_select",
    ),
    ColMap("Vehicle Health System Vendor", "Vehicle Health System", "select"),
    ColMap("Hardware Maintained by Swiftly", "Hardware Maintained by Swiftly", "select"),
    ColMap("Tablets", "Tablets", "select"),
    ColMap("APC Processor", "APC Processor", "select"),
    ColMap(
        "Partners integrated with Swiftly's APIs",
        "Partners Integrated with Swiftly API",
        "select",
    ),
]


# ---------------------------------------------------------------------------
# Value transforms
# ---------------------------------------------------------------------------


_MARKET_SEGMENT_PREFIX_RE = re.compile(r"^\s*\d+\s*[\.\)\-]?\s*")


def transform_market_segment(raw: str) -> str:
    """SFDC values like '2.Enterprise' → 'Enterprise'; map 'Mid-Market' →
    'Mid-market' to match the existing Notion option."""
    s = _MARKET_SEGMENT_PREFIX_RE.sub("", raw or "").strip()
    # Normalize Mid-Market casing variants
    if s.lower() == "mid-market":
        return "Mid-market"
    return s


# Per-column value transform (raw cell str -> str). Default: identity.
COLUMN_TRANSFORMS: dict[str, callable] = {
    "Market Segment": transform_market_segment,
}


def bucket_classification(stage: str) -> str | None:
    """Map raw SF Account Stage to the Notion `Classification` bucket."""
    if not stage:
        return None
    s = stage.strip().lower()
    if s == "won":
        return "Customer"
    if s in ("lost/nurture", "lost", "closed lost"):
        return "Closed Lost"
    return "Prospect"


# ---------------------------------------------------------------------------
# Date parsing (SF report uses M/D/YYYY)
# ---------------------------------------------------------------------------


def parse_date(raw: Any) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date().isoformat()
    s = str(raw).strip()
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def parse_number(raw: Any) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw).strip().replace(",", "").replace("$", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def split_multi(raw: str) -> list[str]:
    """Split a SF multi-value cell on commas or semicolons (SF mixes both),
    dropping empties and trailing punctuation."""
    if not raw:
        return []
    parts = re.split(r"[;,]", raw)
    return [p.strip().rstrip(".;,") for p in parts if p.strip().strip(",.;")]


# ---------------------------------------------------------------------------
# xlsx parsing
# ---------------------------------------------------------------------------


def _normalize_header_cell(c: Any) -> str:
    """Normalize curly quotes / extra whitespace so header lookup is forgiving."""
    s = "" if c is None else str(c)
    return (
        s.replace("’", "'")
        .replace("‘", "'")
        .replace("“", '"')
        .replace("”", '"')
        .strip()
    )


def parse_accounts_xlsx_full(path: Path) -> tuple[list[dict[str, Any]], dict[str, int], list[str]]:
    """Return (rows, col_to_idx, owner_names_seen).

    Each row is a dict keyed by xlsx column name (only for columns we care
    about), with raw cell values (str/number/datetime). The xlsx leading
    title/filter rows are skipped via the 'Account Name' header sentinel.
    """
    import openpyxl  # type: ignore[import-not-found]

    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb.active

    header_row_idx: int | None = None
    header: list[str] = []
    for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
        cells = [_normalize_header_cell(c) for c in row]
        if HEADER_SENTINEL in cells:
            header_row_idx = i
            header = cells
            break
    if header_row_idx is None:
        raise SystemExit(
            f"Could not find header row containing {HEADER_SENTINEL!r} in {path}"
        )

    # Index columns we recognize
    col_to_idx: dict[str, int] = {}
    for col in [HEADER_SENTINEL, "Account Owner", *(m.xlsx_col for m in COLUMN_MAPPINGS)]:
        if col in header:
            col_to_idx[col] = header.index(col)

    # Account Stage is required for bucketing — fail loudly if missing.
    if "Account Stage" not in col_to_idx:
        raise SystemExit(
            "xlsx is missing the 'Account Stage' column. Re-export the SFDC "
            "Accounts Report with that column included."
        )
    if "Account ID" not in col_to_idx:
        raise SystemExit("xlsx is missing the 'Account ID' column.")

    rows: list[dict[str, Any]] = []
    owner_names: list[str] = []
    seen_names: set[str] = set()

    for row in ws.iter_rows(min_row=header_row_idx + 1, values_only=True):
        name_cell = row[col_to_idx[HEADER_SENTINEL]] if col_to_idx[HEADER_SENTINEL] < len(row) else None
        name = ("" if name_cell is None else str(name_cell)).strip()
        raw_id = row[col_to_idx["Account ID"]] if col_to_idx["Account ID"] < len(row) else None
        sf_id = normalize_sf_account_id(("" if raw_id is None else str(raw_id)).strip())
        if not name or not sf_id:
            continue
        if name in seen_names:
            print(
                f"[xlsx] WARN: duplicate Account Name {name!r} — keeping first",
                file=sys.stderr,
            )
            continue
        seen_names.add(name)

        record: dict[str, Any] = {"Account Name": name, "Account ID": sf_id}
        for col, idx in col_to_idx.items():
            if col in (HEADER_SENTINEL, "Account ID"):
                continue
            v = row[idx] if idx < len(row) else None
            record[col] = v

        if record.get("Account Owner"):
            owner_names.append(str(record["Account Owner"]).strip())
        rows.append(record)

    return rows, col_to_idx, owner_names


# ---------------------------------------------------------------------------
# Notion read helpers
# ---------------------------------------------------------------------------


def _read_title(prop: dict | None) -> str:
    if not prop:
        return ""
    parts = prop.get("title") or []
    return "".join((p.get("plain_text") or "") for p in parts).strip()


def _read_rich_text(prop: dict | None) -> str:
    if not prop:
        return ""
    parts = prop.get("rich_text") or []
    return "".join((p.get("plain_text") or "") for p in parts).strip()


def _read_select(prop: dict | None) -> str:
    if not prop:
        return ""
    sel = prop.get("select")
    return (sel or {}).get("name") or ""


def _read_multi_select(prop: dict | None) -> list[str]:
    if not prop:
        return []
    return [o.get("name") or "" for o in (prop.get("multi_select") or []) if o.get("name")]


def _read_number(prop: dict | None) -> float | None:
    if not prop:
        return None
    return prop.get("number")


def _read_date(prop: dict | None) -> str:
    if not prop:
        return ""
    d = prop.get("date") or {}
    return d.get("start") or ""


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, list):
        return len(value) == 0
    return False


# ---------------------------------------------------------------------------
# Notion write payloads
# ---------------------------------------------------------------------------


def _title_payload(value: str) -> dict:
    return {"title": [{"type": "text", "text": {"content": value}}]}


def _rich_text_payload(value: str) -> dict:
    return {"rich_text": [{"type": "text", "text": {"content": value}}]}


def _select_payload(value: str) -> dict:
    return {"select": {"name": value}}


def _multi_select_payload(values: list[str]) -> dict:
    return {"multi_select": [{"name": v} for v in values]}


def _number_payload(value: float) -> dict:
    return {"number": value}


def _date_payload(iso: str) -> dict:
    return {"date": {"start": iso}}


# ---------------------------------------------------------------------------
# Schema lookup (option validation for select/multi_select)
# ---------------------------------------------------------------------------


@dataclass
class SchemaInfo:
    select_options: dict[str, set[str]] = field(default_factory=dict)
    multi_select_options: dict[str, set[str]] = field(default_factory=dict)
    prop_types: dict[str, str] = field(default_factory=dict)


def infer_schema_from_pages(pages: list[dict]) -> SchemaInfo:
    """Read every page's properties to learn the existing select/multi-select
    options. We avoid a separate schema endpoint by reading what's actually
    present on pages — sufficient for validation since we only validate
    against options Notion has seen before."""
    info = SchemaInfo()
    for p in pages:
        props = p.get("properties") or {}
        for name, prop in props.items():
            t = prop.get("type")
            if not t:
                continue
            info.prop_types.setdefault(name, t)
            if t == "select":
                opt = (prop.get("select") or {}).get("name")
                if opt:
                    info.select_options.setdefault(name, set()).add(opt)
            elif t == "multi_select":
                for o in prop.get("multi_select") or []:
                    if o.get("name"):
                        info.multi_select_options.setdefault(name, set()).add(o["name"])
    return info


# ---------------------------------------------------------------------------
# Per-row property build
# ---------------------------------------------------------------------------


@dataclass
class RowDelta:
    """Holds the desired property values for a single xlsx row, plus a
    breakdown of which made it through select-option validation."""
    name: str
    sf_id: str
    desired: dict[str, dict] = field(default_factory=dict)
    classification: str | None = None
    skipped_select: list[tuple[str, str]] = field(default_factory=list)  # (prop, value)
    skipped_multi_values: list[tuple[str, str]] = field(default_factory=list)


def build_row_delta(record: dict[str, Any], schema: SchemaInfo) -> RowDelta:
    name = str(record["Account Name"])
    sf_id = str(record["Account ID"])
    delta = RowDelta(name=name, sf_id=sf_id)

    for cm in COLUMN_MAPPINGS:
        if cm.xlsx_col not in record:
            continue
        raw = record.get(cm.xlsx_col)
        # Apply transform
        if cm.kind in ("select", "text") and isinstance(raw, str):
            tx = COLUMN_TRANSFORMS.get(cm.xlsx_col)
            if tx:
                raw = tx(raw)

        if cm.kind == "text":
            if raw is None:
                continue
            s = str(raw).strip()
            if not s:
                continue
            delta.desired[cm.notion_prop] = _rich_text_payload(s)

        elif cm.kind == "select":
            if raw is None:
                continue
            s = str(raw).strip()
            if not s:
                continue
            # Validate against known options for this property if we've seen any.
            known = schema.select_options.get(cm.notion_prop)
            if known is not None and s not in known:
                # Special case: Account Stage is pre-seeded; if Notion has a
                # different option set, log and skip.
                delta.skipped_select.append((cm.notion_prop, s))
                continue
            delta.desired[cm.notion_prop] = _select_payload(s)

        elif cm.kind == "multi_select":
            if raw is None:
                continue
            parts = split_multi(str(raw))
            known = schema.multi_select_options.get(cm.notion_prop)
            kept: list[str] = []
            for p in parts:
                if known is not None and p not in known:
                    delta.skipped_multi_values.append((cm.notion_prop, p))
                    continue
                kept.append(p)
            if kept:
                delta.desired[cm.notion_prop] = _multi_select_payload(kept)

        elif cm.kind == "number":
            n = parse_number(raw)
            if n is None:
                continue
            delta.desired[cm.notion_prop] = _number_payload(n)

        elif cm.kind == "date":
            iso = parse_date(raw)
            if not iso:
                continue
            delta.desired[cm.notion_prop] = _date_payload(iso)

    # Classification bucket from raw Account Stage
    stage_raw = record.get("Account Stage")
    if isinstance(stage_raw, str):
        bucket = bucket_classification(stage_raw)
        if bucket:
            delta.classification = bucket
            delta.desired["Classification"] = _select_payload(bucket)

    return delta


# ---------------------------------------------------------------------------
# Diffing — only write properties that are blank on the existing page
# ---------------------------------------------------------------------------


def filter_to_blanks(
    desired: dict[str, dict], page_props: dict
) -> tuple[dict[str, dict], list[tuple[str, str, str]]]:
    """Return (updates_to_write, conflicts). Conflicts are
    (prop_name, xlsx_value, notion_value) for non-blank existing values that
    differ from the xlsx — never overwritten."""
    updates: dict[str, dict] = {}
    conflicts: list[tuple[str, str, str]] = []

    for prop_name, payload in desired.items():
        existing = page_props.get(prop_name)
        prop_type = (existing or {}).get("type")

        if "rich_text" in payload:
            existing_val = _read_rich_text(existing)
            new_val = "".join(p["text"]["content"] for p in payload["rich_text"])
            if _is_blank(existing_val):
                updates[prop_name] = payload
            elif existing_val != new_val:
                conflicts.append((prop_name, new_val, existing_val))
        elif "select" in payload:
            existing_val = _read_select(existing)
            new_val = (payload["select"] or {}).get("name") or ""
            if _is_blank(existing_val):
                updates[prop_name] = payload
            elif existing_val != new_val:
                conflicts.append((prop_name, new_val, existing_val))
        elif "multi_select" in payload:
            existing_vals = _read_multi_select(existing)
            new_vals = [o["name"] for o in payload["multi_select"]]
            if not existing_vals:
                updates[prop_name] = payload
            elif set(existing_vals) != set(new_vals):
                conflicts.append(
                    (prop_name, ", ".join(new_vals), ", ".join(existing_vals))
                )
        elif "number" in payload:
            existing_val = _read_number(existing)
            new_val = payload["number"]
            if existing_val is None:
                updates[prop_name] = payload
            elif existing_val != new_val:
                conflicts.append((prop_name, str(new_val), str(existing_val)))
        elif "date" in payload:
            existing_val = _read_date(existing)
            new_val = (payload["date"] or {}).get("start") or ""
            if _is_blank(existing_val):
                updates[prop_name] = payload
            elif existing_val != new_val:
                conflicts.append((prop_name, new_val, existing_val))

        # title is only used at create-time; skip on update path

    return updates, conflicts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    load_dotenv(_ROOT / ".env")
    token = os.getenv("NOTION_TOKEN")
    if not token:
        raise SystemExit("NOTION_TOKEN must be set in .env")

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("xlsx", type=Path, help="Path to the SFDC Accounts Report xlsx.")
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Actually write to Notion. Default is dry-run.",
    )
    ap.add_argument(
        "--no-create",
        action="store_true",
        help="When --apply is set, only update existing Agencies; do not create new pages.",
    )
    args = ap.parse_args()

    if not args.xlsx.exists():
        raise SystemExit(f"File not found: {args.xlsx}")

    dry_run = not args.apply

    print(f"[xlsx] reading {args.xlsx} ...", file=sys.stderr)
    xlsx_rows, col_index, owner_names = parse_accounts_xlsx_full(args.xlsx)
    present_cols = sorted(col_index.keys())
    print(
        f"  {len(xlsx_rows)} rows; columns: {', '.join(present_cols)}",
        file=sys.stderr,
    )

    with NotionClient(token) as notion:
        print("[notion] loading Agencies ...", file=sys.stderr)
        agencies = notion.query_data_source(AGENCIES_DATA_SOURCE_ID)
        print(f"  {len(agencies)} agency pages", file=sys.stderr)

        schema = infer_schema_from_pages(agencies)

        # Index Notion pages by every candidate key derivable from Name and Full name,
        # plus by normalized SF Account ID (preferred when present — avoids name
        # collisions when SF has multiple accounts that share a display name).
        key_to_pages: dict[str, set[str]] = {}
        sf_id_to_page: dict[str, str] = {}
        # page_info[pid] = (display_name, props_dict)
        page_info: dict[str, tuple[str, dict]] = {}
        for a in agencies:
            pid = a.get("id")
            if not pid:
                continue
            props = a.get("properties") or {}
            name = _read_title(props.get("Name"))
            full = _read_rich_text(props.get("Full name"))
            page_info[pid] = (name, props)
            for k in name_candidates(name) | name_candidates(full):
                key_to_pages.setdefault(k, set()).add(pid)
            existing_sf = normalize_sf_account_id(_read_rich_text(props.get("Account ID")))
            if existing_sf:
                sf_id_to_page[existing_sf] = pid

        # Stats
        update_writes: list[tuple[str, str, dict]] = []  # (page_id, name, props_payload)
        already_set_count = 0
        prop_fill_counts: Counter = Counter()
        all_conflicts: list[tuple[str, str, str, str]] = []  # (name, prop, xlsx_val, notion_val)
        ambiguous: list[tuple[str, list[str]]] = []
        unmatched_xlsx: list[dict[str, Any]] = []
        matched_pages: set[str] = set()
        skipped_select_total: Counter = Counter()
        skipped_multi_total: Counter = Counter()
        bucket_counts: Counter = Counter()
        stage_counts: Counter = Counter()

        for record in xlsx_rows:
            stage_counts[str(record.get("Account Stage") or "").strip()] += 1
            delta = build_row_delta(record, schema)
            for p, v in delta.skipped_select:
                skipped_select_total[(p, v)] += 1
            for p, v in delta.skipped_multi_values:
                skipped_multi_total[(p, v)] += 1
            if delta.classification:
                bucket_counts[delta.classification] += 1

            # Prefer SF Account ID match — avoids false hits when SF has
            # multiple accounts sharing a display name.
            pid: str | None = sf_id_to_page.get(record["Account ID"])
            if pid is None:
                candidates = name_candidates(record["Account Name"])
                hits: set[str] = set()
                for k in candidates:
                    hits |= key_to_pages.get(k, set())
                # Drop any name hits that already have a *different* SF Account ID —
                # those are clearly different accounts that happen to share a name.
                hits = {
                    h for h in hits
                    if not (
                        normalize_sf_account_id(
                            _read_rich_text(page_info[h][1].get("Account ID"))
                        )
                        and normalize_sf_account_id(
                            _read_rich_text(page_info[h][1].get("Account ID"))
                        ) != record["Account ID"]
                    )
                }
                if not hits:
                    unmatched_xlsx.append(record)
                    continue
                if len(hits) > 1:
                    ambiguous.append(
                        (record["Account Name"], sorted(page_info[h][0] for h in hits))
                    )
                    continue
                (pid,) = hits
            matched_pages.add(pid)
            notion_name, notion_props = page_info[pid]
            updates, conflicts = filter_to_blanks(delta.desired, notion_props)
            for prop, xv, nv in conflicts:
                all_conflicts.append((notion_name, prop, xv, nv))
            if updates:
                update_writes.append((pid, notion_name, updates))
                for prop in updates.keys():
                    prop_fill_counts[prop] += 1
            else:
                already_set_count += 1

        unmatched_agencies = [
            (info[0], _read_rich_text(info[1].get("Account ID")))
            for pid, info in page_info.items()
            if pid not in matched_pages
        ]

        # Apply updates
        if not dry_run:
            for pid, notion_name, payload in update_writes:
                notion.update_page(pid, payload)
                print(f"  updated {notion_name!r}: {sorted(payload.keys())}", file=sys.stderr)

        # Apply creates
        creates_done: list[str] = []
        creates_planned: list[tuple[str, dict]] = []  # (name, properties)
        if not args.no_create:
            for record in unmatched_xlsx:
                delta = build_row_delta(record, schema)
                props_payload: dict[str, dict] = {
                    "Name": _title_payload(record["Account Name"]),
                    **delta.desired,
                }
                creates_planned.append((record["Account Name"], props_payload))
                if not dry_run:
                    notion.create_page(AGENCIES_DATA_SOURCE_ID, props_payload)
                    creates_done.append(record["Account Name"])
                    print(f"  created {record['Account Name']!r}", file=sys.stderr)

    # -------- Report --------
    print("")
    print("=" * 78)
    print(f"{'DRY-RUN — ' if dry_run else ''}Agency upsert from xlsx")
    print("=" * 78)
    print(f"  xlsx rows:           {len(xlsx_rows)}")
    print(f"  matched (existing):  {len(matched_pages)}")
    print(f"    fully up-to-date:  {already_set_count}")
    print(
        f"    {'would update' if dry_run else 'updated'}:      "
        f"{len(update_writes)}"
    )
    print(f"  unmatched xlsx:      {len(unmatched_xlsx)}  (no Agency with that name)")
    if args.no_create:
        print(f"    {'would create' if dry_run else 'created'}:      0  (--no-create)")
    else:
        print(
            f"    {'would create' if dry_run else 'created'}:      "
            f"{len(creates_planned) if dry_run else len(creates_done)}"
        )
    print(f"  ambiguous matches:   {len(ambiguous)}")
    print(f"  conflicts:           {len(all_conflicts)}  (existing value differs — NOT overwritten)")
    print(
        f"  unmatched Notion:    {len(unmatched_agencies)}  "
        f"(Agencies not present in xlsx)"
    )

    print("\nAccount Stage distribution (xlsx):")
    for stage, n in stage_counts.most_common():
        print(f"  {n:5}  {stage!r}")

    print("\nClassification bucket counts (derived):")
    for b, n in bucket_counts.most_common():
        print(f"  {n:5}  {b}")

    if prop_fill_counts:
        print(f"\nProperty fills on existing pages ({'planned' if dry_run else 'applied'}):")
        for prop, n in prop_fill_counts.most_common():
            print(f"  {n:5}  {prop}")

    if skipped_select_total:
        print("\nSelect values dropped (option not present on Agencies; add it in Notion if you want them):")
        for (prop, val), n in skipped_select_total.most_common():
            print(f"  {n:5}  {prop} = {val!r}")

    if skipped_multi_total:
        print("\nMulti-select values dropped (option not present):")
        for (prop, val), n in skipped_multi_total.most_common():
            print(f"  {n:5}  {prop} contains {val!r}")

    if all_conflicts:
        print("\nConflicts (existing Notion value differs from xlsx — NOT overwritten):")
        for name, prop, xv, nv in all_conflicts[:50]:
            print(f"  - {name!r}  {prop}: xlsx={xv!r}  notion={nv!r}")
        if len(all_conflicts) > 50:
            print(f"  ... and {len(all_conflicts) - 50} more")

    if ambiguous:
        print("\nAmbiguous matches (xlsx name matches multiple Agencies — resolve by hand):")
        for xlsx_name, notion_names in ambiguous:
            print(f"  - {xlsx_name!r} ↔ {notion_names}")

    if owner_names:
        owner_counts = Counter(owner_names)
        print(f"\nDistinct Account Owners in xlsx ({len(owner_counts)} unique — populate Account Manager by hand):")
        for o, n in owner_counts.most_common():
            print(f"  {n:5}  {o}")

    if unmatched_agencies:
        print(
            f"\nNotion Agencies not in xlsx ({len(unmatched_agencies)} — left untouched, "
            "expected because xlsx is filtered to recent activity):"
        )
        for name, current in sorted(unmatched_agencies)[:20]:
            suffix = f"  [Account ID: {current}]" if current else ""
            print(f"  - {name}{suffix}")
        if len(unmatched_agencies) > 20:
            print(f"  ... and {len(unmatched_agencies) - 20} more")

    if dry_run:
        print("\n[DRY-RUN] No writes performed. Re-run with --apply to write.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
