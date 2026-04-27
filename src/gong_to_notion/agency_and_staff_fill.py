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

import re
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
class StaffCacheEntry:
    """Everything we need about an existing Agency Staff row to decide whether
    to top up its Role on backfill."""

    staff_id: str
    role: str  # empty string when blank


@dataclass
class FillCaches:
    domain_to_agency: dict[str, str]            # domain → agency page id
    sf_account_to_agency: dict[str, str]        # 15-char SF Account id → agency page id
    email_to_staff: dict[str, StaffCacheEntry]  # lowercased email → staff row metadata
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


# Salesforce Account IDs start with 001 and are 15 or 18 chars of base-62.
# We normalize to the 15-char prefix so the 15- and 18-char forms match.
_SF_ACCOUNT_ID_RE = re.compile(r"\b(001[0-9A-Za-z]{12}(?:[0-9A-Za-z]{3})?)\b")


def extract_sf_account_id(url: str | None) -> str | None:
    """Pull an SF Account ID (normalized to 15 chars) out of a Salesforce URL.

    Matches Lightning (`/lightning/r/Account/001.../view`) and classic
    (`my.salesforce.com/001...`) link shapes. Returns None when no 001-prefixed
    ID is found.
    """
    if not url:
        return None
    m = _SF_ACCOUNT_ID_RE.search(url)
    if not m:
        return None
    return m.group(1)[:15]


def normalize_sf_account_id(raw: str | None) -> str | None:
    """Trim SF IDs to the 15-char canonical form so 15/18 variants collapse."""
    if not raw:
        return None
    s = raw.strip()
    if not s.startswith("001"):
        return None
    return s[:15] if len(s) >= 15 else None


def domain_from_url(raw: str | None) -> str | None:
    """Normalize an Agency Website URL to a bare host for domain matching.

    Handles bare hosts, www. prefixes, mixed case, schemes, ports, userinfo,
    and paths (the path is discarded — we only want the host for Email
    Domains matching). The original Website value on the row is never
    rewritten by callers.
    """
    from urllib.parse import urlparse

    s = (raw or "").strip()
    if not s:
        return None
    if "://" not in s:
        s = "https://" + s
    try:
        netloc = urlparse(s).netloc
    except Exception:
        return None
    netloc = netloc.split("@")[-1]          # drop userinfo
    netloc = netloc.split(":")[0].strip().lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    if not netloc or "/" in netloc or " " in netloc or "." not in netloc:
        return None
    return netloc


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


def _read_rich_text_property(prop: dict | None) -> str:
    """Read a plain string out of a rich_text (aka 'text') Notion property."""
    if not prop:
        return ""
    parts = prop.get("rich_text") or []
    return "".join((p.get("plain_text") or "") for p in parts).strip()


def _read_url_property(prop: dict | None) -> str:
    if not prop:
        return ""
    return (prop.get("url") or "").strip()


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


def _role_payload(role: str) -> dict:
    return {"rich_text": [{"type": "text", "text": {"content": role}}]}


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


def load_fill_caches(notion: NotionClient) -> FillCaches:
    """Load Agencies → domain / SF-account maps and Agency Staff → email map,
    once per run."""
    agency_rows = notion.query_data_source(AGENCIES_DATA_SOURCE_ID)
    staff_rows = notion.query_data_source(AGENCY_STAFF_DATA_SOURCE_ID)

    domain_to_agency: dict[str, str] = {}
    sf_account_to_agency: dict[str, str] = {}
    for row in agency_rows:
        row_id = row.get("id")
        if not row_id:
            continue
        props = row.get("properties") or {}
        name = _read_title_property(props.get("Name"))

        domains = _read_multi_select_values(props.get("Email Domains"))
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

        # Primary: the `Account ID` rich_text property (manually populated from
        # the SF export). Fall back to parsing `Salesforce Link` in case some
        # rows only have the URL filled in.
        sf_id = normalize_sf_account_id(
            _read_rich_text_property(props.get("Account ID"))
        ) or extract_sf_account_id(_read_url_property(props.get("Salesforce Link")))
        if sf_id:
            prior = sf_account_to_agency.get(sf_id)
            if prior and prior != row_id:
                print(
                    f"[agency-fill] WARN: SF Account {sf_id} claimed by multiple agencies "
                    f"({prior} and {row_id} — {name!r}). Keeping first.",
                    file=sys.stderr,
                )
            else:
                sf_account_to_agency[sf_id] = row_id

    # Detect Email prop type from the first staff row that carries an Email.
    staff_prop_types = StaffPropertyTypes()
    for row in staff_rows:
        props = row.get("properties") or {}
        email_prop = props.get("Email")
        if email_prop and email_prop.get("type") in ("email", "rich_text"):
            staff_prop_types.email = email_prop["type"]
            break

    email_to_staff: dict[str, StaffCacheEntry] = {}
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
        role = _read_rich_text_property(props.get("Role"))
        email_to_staff.setdefault(
            key, StaffCacheEntry(staff_id=row_id, role=role)
        )

    return FillCaches(
        domain_to_agency=domain_to_agency,
        sf_account_to_agency=sf_account_to_agency,
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
    role_updated: bool = False  # filled a previously-blank Role on an existing row


def find_or_create_staff(
    notion: NotionClient,
    email: str,
    name: str | None,
    agency_id: str | None,
    caches: FillCaches,
    role: str | None = None,
    dry_run: bool = False,
) -> StaffResolution | None:
    """Return the Agency Staff page id for `email`, creating if absent.

    In dry-run, existing matches still resolve (read-only); a missing row
    returns a resolution with a placeholder id that callers treat as
    'would-create'. When `role` is non-empty, fill it on newly created rows
    and on existing rows whose Role is blank (never clobber an existing role).
    """
    email = (email or "").strip().lower()
    if not email:
        return None

    role_clean = (role or "").strip()

    hit = caches.email_to_staff.get(email)
    if hit:
        role_updated = False
        if role_clean and not hit.role:
            if not dry_run:
                notion.update_page(hit.staff_id, {"Role": _role_payload(role_clean)})
            hit.role = role_clean
            role_updated = True
        return StaffResolution(
            staff_id=hit.staff_id,
            created=False,
            agency_id=None,
            role_updated=role_updated,
        )

    properties: dict = {
        "Name": _title_payload((name or email).strip() or email),
        "Email": _email_payload(email, caches.staff_prop_types.email),
    }
    if agency_id:
        properties["Agency"] = {"relation": [{"id": agency_id}]}
    if role_clean:
        properties["Role"] = _role_payload(role_clean)

    if dry_run:
        placeholder = f"dry-run:new-staff:{email}"
        caches.email_to_staff[email] = StaffCacheEntry(
            staff_id=placeholder, role=role_clean
        )
        return StaffResolution(
            staff_id=placeholder, created=True, agency_id=agency_id
        )

    page = notion.create_page(
        data_source_id=AGENCY_STAFF_DATA_SOURCE_ID,
        properties=properties,
    )
    new_id = page.get("id", "")
    caches.email_to_staff[email] = StaffCacheEntry(staff_id=new_id, role=role_clean)
    return StaffResolution(staff_id=new_id, created=True, agency_id=agency_id)


# ---------------------------------------------------------------------------
# Per-call resolution
# ---------------------------------------------------------------------------


@dataclass
class CallResolution:
    agency_ids: list[str]
    staff_ids: list[str]
    new_staff: list[tuple[str, str]]  # (email, staff_id) — for reporting
    roles_updated: int = 0            # existing Staff rows whose blank Role we filled


def resolve_call_links(
    notion: NotionClient,
    external_people: list[dict],
    caches: FillCaches,
    sf_account_ids: list[str] | None = None,
    dry_run: bool = False,
) -> CallResolution:
    """Given a list of {email, name, title?} dicts for external participants plus
    the call's Salesforce Account IDs, return Agency + Agency Staff relation
    targets for this call.

    Agencies are resolved first from Gong's SF Account context (primary); then
    per-participant email domain (fallback). Participant `title` (if present)
    feeds the Agency Staff `Role` field — only on creation or when the existing
    row's Role is blank."""
    agency_ids: list[str] = []
    staff_ids: list[str] = []
    new_staff: list[tuple[str, str]] = []
    seen_agencies: set[str] = set()
    seen_staff: set[str] = set()
    roles_updated = 0

    # SF Account → Agency (primary source of truth).
    for raw_sf in sf_account_ids or []:
        sf_id = normalize_sf_account_id(raw_sf)
        if not sf_id:
            continue
        sf_agency = caches.sf_account_to_agency.get(sf_id)
        if sf_agency and sf_agency not in seen_agencies:
            seen_agencies.add(sf_agency)
            agency_ids.append(sf_agency)

    # A single "default" agency to attach to newly-created Staff rows when
    # the email-domain path doesn't resolve. If there's exactly one SF-derived
    # agency for this call, use it; otherwise leave Staff agency unset and
    # let humans fill it.
    default_agency = agency_ids[0] if len(agency_ids) == 1 else None

    for p in external_people:
        email = (p.get("email") or "").strip().lower()
        if not email:
            continue
        domain = extract_domain(email)
        if not domain or domain == INTERNAL_DOMAIN:
            continue

        domain_agency = caches.domain_to_agency.get(domain)
        if domain_agency and domain_agency not in seen_agencies:
            seen_agencies.add(domain_agency)
            agency_ids.append(domain_agency)

        staff_agency = domain_agency or default_agency
        resolution = find_or_create_staff(
            notion,
            email=email,
            name=p.get("name"),
            agency_id=staff_agency,
            caches=caches,
            role=p.get("title"),
            dry_run=dry_run,
        )
        if resolution is None:
            continue

        if resolution.staff_id and resolution.staff_id not in seen_staff:
            seen_staff.add(resolution.staff_id)
            staff_ids.append(resolution.staff_id)
        if resolution.created:
            new_staff.append((email, resolution.staff_id))
        if resolution.role_updated:
            roles_updated += 1

    return CallResolution(
        agency_ids=agency_ids,
        staff_ids=staff_ids,
        new_staff=new_staff,
        roles_updated=roles_updated,
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
    purpose: str | None = None,
    existing_purpose: list[str] | None = None,
    overwrite_agencies: bool = False,
    overwrite_purpose: bool = False,
    dry_run: bool = False,
) -> dict[str, int]:
    """Update the call page.

    - **Agencies**: when `overwrite_agencies=True`, replace existing with the
      computed set whenever they differ and `agency_ids` is non-empty.
      Otherwise only write if currently blank (importer behavior).
    - **Agency Staff**: always additive. Writes the union of existing + new
      when new staff would be added; never removes existing links.
    - **Purpose**: when `overwrite_purpose=True`, write the given single-option
      multi_select whenever `purpose` is non-empty and differs from
      `existing_purpose`. Otherwise untouched.

    Returns a dict of counters: `agencies_changed` (1/0), `staff_added`
    (count of new relations appended), `purpose_changed` (1/0).
    """
    props: dict = {}
    counters = {"agencies_changed": 0, "staff_added": 0, "purpose_changed": 0}

    existing_agencies_set = set(existing_agency_ids or [])
    computed_agencies_set = set(agency_ids or [])
    if agency_ids and computed_agencies_set != existing_agencies_set:
        if not existing_agency_ids:
            props["Agencies"] = {"relation": [{"id": aid} for aid in agency_ids]}
            counters["agencies_changed"] = 1
        elif overwrite_agencies:
            props["Agencies"] = {"relation": [{"id": aid} for aid in agency_ids]}
            counters["agencies_changed"] = 1

    existing_staff_set = set(existing_staff_ids or [])
    new_staff_only = [sid for sid in (staff_ids or []) if sid not in existing_staff_set]
    if new_staff_only:
        union = list(existing_staff_ids or []) + new_staff_only
        props["Agency Staff"] = {"relation": [{"id": sid} for sid in union]}
        counters["staff_added"] = len(new_staff_only)

    purpose_clean = (purpose or "").strip()
    if overwrite_purpose and purpose_clean:
        existing_purpose_set = set(existing_purpose or [])
        if existing_purpose_set != {purpose_clean}:
            props["Purpose"] = {"multi_select": [{"name": purpose_clean}]}
            counters["purpose_changed"] = 1

    if not props:
        return counters

    if not dry_run:
        # Placeholder IDs (from dry-run staff creation) must never reach the API.
        for key, val in props.items():
            if key in ("Agencies", "Agency Staff"):
                for item in val.get("relation", []):
                    if item.get("id", "").startswith("dry-run:"):
                        raise NotionError(
                            "Refusing to write dry-run placeholder id to Notion."
                        )
        notion.update_page(page_id, props)

    return counters


# ---------------------------------------------------------------------------
# Convenience: turn a Gong participant list into the external-people shape
# ---------------------------------------------------------------------------


def gong_external_people(participants: list[dict]) -> list[dict]:
    """Filter Gong participants to those flagged External and return
    [{'name', 'email', 'title'}] — used by the import path where affiliation
    is known. `title` is Gong's job-title string, empty if absent."""
    out: list[dict] = []
    for p in participants:
        if p.get("affiliation") != "External":
            continue
        email = (p.get("email") or "").strip()
        if not email:
            continue
        out.append(
            {
                "name": p.get("name"),
                "email": email,
                "title": (p.get("title") or "").strip(),
            }
        )
    return out
