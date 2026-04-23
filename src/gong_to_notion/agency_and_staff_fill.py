"""Deterministic Agency + Agency Staff linker for Customer Interactions pages.

Flow:
  - Load Agencies → {email_domain: agency_page_id} once per run.
  - Load Agency Staff → {lowercased_email: staff_page_id} once per run.
  - For each call's external participants:
      match domain → Agency (may miss);
      find or create Staff row by email (with Agency relation if matched);
      link the Staff row to the call; link the Agency too if matched.
  - Only fills blank fields — never clobbers existing values.

Purpose is not in scope here — the LLM skill still owns it.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

from .notion_client import NotionClient, NotionError

AGENCIES_DATA_SOURCE_ID = "b9945686-eb26-42bb-9020-1e8075466f42"
AGENCY_STAFF_DATA_SOURCE_ID = "664ccf5e-8cdf-43a4-863c-cfe8ccdef26b"

INTERNAL_DOMAIN = "goswift.ly"

# Excluded when deriving Agency domains from existing Staff emails. Not used
# to filter Staff creation: a @gmail.com contact is still a valid Staff row,
# it just won't deterministically resolve to an Agency via domain.
FREE_MAIL_DOMAINS: frozenset[str] = frozenset(
    {
        "gmail.com",
        "googlemail.com",
        "yahoo.com",
        "yahoo.co.uk",
        "hotmail.com",
        "outlook.com",
        "live.com",
        "msn.com",
        "icloud.com",
        "me.com",
        "mac.com",
        "aol.com",
        "protonmail.com",
        "proton.me",
        "pm.me",
        "fastmail.com",
        "ymail.com",
    }
)


# ---------------------------------------------------------------------------
# Caches passed between calls in a run
# ---------------------------------------------------------------------------


@dataclass
class StaffPropertyTypes:
    """Which Notion property types the Agency Staff DB uses. Detected once,
    re-used when creating new rows so we write back the shape the DB expects."""

    email: str = "email"  # "email" or "rich_text"


@dataclass
class FillCaches:
    domain_to_agency: dict[str, str]            # domain → agency page id
    email_to_staff: dict[str, str]              # lowercased email → staff page id
    staff_prop_types: StaffPropertyTypes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def extract_domain(email: str | None) -> str | None:
    if not email:
        return None
    email = email.strip().lower()
    if "@" not in email:
        return None
    domain = email.rsplit("@", 1)[1].strip()
    return domain or None


def _read_email_property(prop: dict | None) -> str | None:
    """Accept either {type:'email', email: '...'} or rich_text with the email."""
    if not prop:
        return None
    ptype = prop.get("type")
    if ptype == "email":
        return (prop.get("email") or "").strip() or None
    if ptype == "rich_text":
        parts = prop.get("rich_text") or []
        if not parts:
            return None
        text = "".join((p.get("plain_text") or "") for p in parts).strip()
        return text or None
    # Some DBs may nest it differently — fall back.
    if isinstance(prop.get("email"), str):
        return prop["email"].strip() or None
    return None


def _read_title_property(prop: dict | None) -> str:
    if not prop:
        return ""
    parts = prop.get("title") or []
    return "".join((p.get("plain_text") or "") for p in parts).strip()


def _read_multi_select_values(prop: dict | None) -> list[str]:
    if not prop:
        return []
    opts = prop.get("multi_select") or []
    return [(o.get("name") or "").strip() for o in opts if (o.get("name") or "").strip()]


def _read_relation_ids(prop: dict | None) -> list[str]:
    if not prop:
        return []
    rels = prop.get("relation") or []
    return [r.get("id", "") for r in rels if r.get("id")]


def _read_people_ids(prop: dict | None) -> list[str]:
    if not prop:
        return []
    people = prop.get("people") or []
    return [p.get("id", "") for p in people if p.get("id")]


def _title_payload(name: str) -> dict:
    return {"title": [{"type": "text", "text": {"content": name}}]}


def _email_payload(email: str, prop_type: str) -> dict:
    if prop_type == "rich_text":
        return {"rich_text": [{"type": "text", "text": {"content": email}}]}
    return {"email": email}


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


def load_fill_caches(notion: NotionClient) -> FillCaches:
    """Load Agencies → domain map and Agency Staff → email map, once per run."""
    agency_rows = notion.query_data_source(AGENCIES_DATA_SOURCE_ID)
    staff_rows = notion.query_data_source(AGENCY_STAFF_DATA_SOURCE_ID)

    domain_to_agency: dict[str, str] = {}
    for row in agency_rows:
        row_id = row.get("id")
        if not row_id:
            continue
        props = row.get("properties") or {}
        domains = _read_multi_select_values(props.get("Email Domains"))
        name = _read_title_property(props.get("Name"))
        for raw in domains:
            domain = raw.strip().lower().lstrip("@")
            if not domain:
                continue
            if domain in FREE_MAIL_DOMAINS or domain == INTERNAL_DOMAIN:
                continue
            prior = domain_to_agency.get(domain)
            if prior and prior != row_id:
                print(
                    f"[agency-fill] WARN: domain {domain!r} claimed by multiple agencies "
                    f"({prior} and {row_id} — {name!r}). Keeping first.",
                    file=sys.stderr,
                )
                continue
            domain_to_agency[domain] = row_id

    # Detect Email prop type from the first staff row that carries an Email.
    staff_prop_types = StaffPropertyTypes()
    for row in staff_rows:
        props = row.get("properties") or {}
        email_prop = props.get("Email")
        if email_prop and email_prop.get("type") in ("email", "rich_text"):
            staff_prop_types.email = email_prop["type"]
            break

    email_to_staff: dict[str, str] = {}
    for row in staff_rows:
        row_id = row.get("id")
        if not row_id:
            continue
        props = row.get("properties") or {}
        email = _read_email_property(props.get("Email"))
        if not email:
            continue
        key = email.strip().lower()
        if not key:
            continue
        email_to_staff.setdefault(key, row_id)

    return FillCaches(
        domain_to_agency=domain_to_agency,
        email_to_staff=email_to_staff,
        staff_prop_types=staff_prop_types,
    )


# ---------------------------------------------------------------------------
# Staff resolution / creation
# ---------------------------------------------------------------------------


@dataclass
class StaffResolution:
    staff_id: str
    created: bool
    agency_id: str | None  # agency set on the staff row, if any


def find_or_create_staff(
    notion: NotionClient,
    email: str,
    name: str | None,
    agency_id: str | None,
    caches: FillCaches,
    dry_run: bool = False,
) -> StaffResolution | None:
    """Return the Agency Staff page id for `email`, creating if absent.

    In dry-run, existing matches still resolve (read-only); a missing row
    returns a resolution with a placeholder id that callers treat as
    'would-create'.
    """
    email = (email or "").strip().lower()
    if not email:
        return None

    hit = caches.email_to_staff.get(email)
    if hit:
        return StaffResolution(staff_id=hit, created=False, agency_id=None)

    properties: dict = {
        "Name": _title_payload((name or email).strip() or email),
        "Email": _email_payload(email, caches.staff_prop_types.email),
    }
    if agency_id:
        properties["Agency"] = {"relation": [{"id": agency_id}]}

    if dry_run:
        placeholder = f"dry-run:new-staff:{email}"
        caches.email_to_staff[email] = placeholder
        return StaffResolution(staff_id=placeholder, created=True, agency_id=agency_id)

    page = notion.create_page(
        data_source_id=AGENCY_STAFF_DATA_SOURCE_ID,
        properties=properties,
    )
    new_id = page.get("id", "")
    caches.email_to_staff[email] = new_id
    return StaffResolution(staff_id=new_id, created=True, agency_id=agency_id)


# ---------------------------------------------------------------------------
# Per-call resolution
# ---------------------------------------------------------------------------


@dataclass
class CallResolution:
    agency_ids: list[str]
    staff_ids: list[str]
    new_staff: list[tuple[str, str]]  # (email, staff_id) — for reporting


def resolve_call_links(
    notion: NotionClient,
    external_people: list[dict],
    caches: FillCaches,
    dry_run: bool = False,
) -> CallResolution:
    """Given a list of {email, name} dicts for external participants, return
    the Agency + Agency Staff relation targets for this call.

    Skips internal-domain emails and entries with no email. Unmatched domains
    still yield a Staff row (blank Agency on the row, nothing linked on the
    call for that agency)."""
    agency_ids: list[str] = []
    staff_ids: list[str] = []
    new_staff: list[tuple[str, str]] = []
    seen_agencies: set[str] = set()
    seen_staff: set[str] = set()

    for p in external_people:
        email = (p.get("email") or "").strip().lower()
        if not email:
            continue
        domain = extract_domain(email)
        if not domain or domain == INTERNAL_DOMAIN:
            continue

        agency_id = caches.domain_to_agency.get(domain)
        resolution = find_or_create_staff(
            notion,
            email=email,
            name=p.get("name"),
            agency_id=agency_id,
            caches=caches,
            dry_run=dry_run,
        )
        if resolution is None:
            continue

        if resolution.staff_id and resolution.staff_id not in seen_staff:
            seen_staff.add(resolution.staff_id)
            staff_ids.append(resolution.staff_id)
        if resolution.created:
            new_staff.append((email, resolution.staff_id))
        if agency_id and agency_id not in seen_agencies:
            seen_agencies.add(agency_id)
            agency_ids.append(agency_id)

    return CallResolution(
        agency_ids=agency_ids, staff_ids=staff_ids, new_staff=new_staff
    )


# ---------------------------------------------------------------------------
# Writing back to the call page
# ---------------------------------------------------------------------------


def apply_to_page(
    notion: NotionClient,
    page_id: str,
    agency_ids: list[str],
    staff_ids: list[str],
    existing_agency_ids: list[str],
    existing_staff_ids: list[str],
    dry_run: bool = False,
) -> dict[str, int]:
    """Update the call page with only the fields that are currently blank.

    Returns {'agencies_added': N, 'staff_added': N} reflecting what was
    (or would be) written. If a field is non-empty, it's omitted entirely —
    no clobbering.
    """
    props: dict = {}
    added_agencies = 0
    added_staff = 0

    if not existing_agency_ids and agency_ids:
        props["Agencies"] = {"relation": [{"id": aid} for aid in agency_ids]}
        added_agencies = len(agency_ids)
    if not existing_staff_ids and staff_ids:
        props["Agency Staff"] = {"relation": [{"id": sid} for sid in staff_ids]}
        added_staff = len(staff_ids)

    if not props:
        return {"agencies_added": 0, "staff_added": 0}

    if not dry_run:
        # Placeholder IDs (from dry-run staff creation) must never reach the API.
        for rel in props.values():
            for item in rel["relation"]:
                if item["id"].startswith("dry-run:"):
                    raise NotionError(
                        "Refusing to write dry-run placeholder id to Notion."
                    )
        notion.update_page(page_id, props)

    return {"agencies_added": added_agencies, "staff_added": added_staff}


# ---------------------------------------------------------------------------
# Participants toggle parsing (used by backfill)
# ---------------------------------------------------------------------------


_PARTICIPANTS_HEADER = "Participants"


def _rich_text_to_plain(block: dict, block_type: str) -> str:
    parts = (block.get(block_type) or {}).get("rich_text") or []
    return "".join((p.get("plain_text") or "") for p in parts)


def extract_participants_emails(
    notion: NotionClient, page_id: str
) -> tuple[bool, list[dict]]:
    """Walk the page's children to find the Participants toggle.

    Returns (toggle_present, people) where people is a list of
    {'name': str|None, 'email': str} for each bullet that carries an email.
    toggle_present is False when the page has no Participants toggle at all
    (vs. toggle is there but empty / email-less), so callers can surface that
    case separately for manual handling.
    """
    children = notion.get_block_children(page_id)
    toggle_id: str | None = None
    for block in children:
        if block.get("type") != "heading_2":
            continue
        h2 = block.get("heading_2") or {}
        if not h2.get("is_toggleable"):
            continue
        text = _rich_text_to_plain(block, "heading_2").strip()
        if text == _PARTICIPANTS_HEADER:
            toggle_id = block.get("id")
            break
    if not toggle_id:
        return False, []

    bullets = notion.get_block_children(toggle_id)
    out: list[dict] = []
    for b in bullets:
        if b.get("type") != "bulleted_list_item":
            continue
        text = _rich_text_to_plain(b, "bulleted_list_item").strip()
        if not text:
            continue
        parsed = _parse_participant_line(text)
        if parsed and parsed.get("email"):
            out.append(parsed)
    return True, out


def _parse_participant_line(text: str) -> dict | None:
    """Accept 'Name <email>', bare 'email', or 'name' (no-email → None)."""
    text = text.strip()
    if not text:
        return None
    if "<" in text and ">" in text:
        name, _, rest = text.partition("<")
        email = rest.rstrip(">").strip()
        return {"name": name.strip() or None, "email": email or None}
    if "@" in text and " " not in text:
        return {"name": None, "email": text}
    return None


# ---------------------------------------------------------------------------
# Convenience: turn a Gong participant list into the external-people shape
# ---------------------------------------------------------------------------


def gong_external_people(participants: list[dict]) -> list[dict]:
    """Filter Gong participants to those flagged External and return
    [{'name', 'email'}] — used by the import path where affiliation is known."""
    out: list[dict] = []
    for p in participants:
        if p.get("affiliation") != "External":
            continue
        email = (p.get("email") or "").strip()
        if not email:
            continue
        out.append({"name": p.get("name"), "email": email})
    return out
