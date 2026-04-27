"""Translate a Gong call dict into Notion page properties + body blocks.

Input shape is whatever `fetch_gong_calls.fetch_calls_extensive` +
`fetch_transcripts` produce, merged into one dict per call (see
`__main__.build_call_records`).
"""

from __future__ import annotations

# Notion hard limits. A single rich-text run may not exceed 2000 chars; a
# rich_text *property* value is also capped at 2000 chars (so Summary is
# truncated rather than split).
RICH_TEXT_RUN_CHAR_LIMIT = 2000
SUMMARY_CHAR_LIMIT = 2000


# ---------------------------------------------------------------------------
# Rich text helpers
# ---------------------------------------------------------------------------

def _rich_text_runs(text: str) -> list[dict]:
    """Split `text` into ≤2000-char rich-text runs for a single block."""
    if not text:
        return []
    runs: list[dict] = []
    for i in range(0, len(text), RICH_TEXT_RUN_CHAR_LIMIT):
        chunk = text[i : i + RICH_TEXT_RUN_CHAR_LIMIT]
        runs.append({"type": "text", "text": {"content": chunk}})
    return runs


def _title_value(text: str) -> list[dict]:
    # Titles have the same 2000-char-per-run limit; splitting is safe.
    return _rich_text_runs(text or "Untitled Call")


# ---------------------------------------------------------------------------
# Property builder
# ---------------------------------------------------------------------------

def build_properties(
    call: dict,
    email_to_user_id: dict[str, str],
    facilitator_email: str | None,
) -> dict:
    """Build the Notion `properties` payload for one call.

    Only deterministic fields are filled; judgment fields are left absent.
    """
    participants = call.get("participants", [])

    # Swiftlets Involved: every internal participant that maps to a Notion user.
    # Deduplicate by user_id since Gong can list a person twice.
    swiftlets: list[dict] = []
    seen: set[str] = set()
    for p in participants:
        if p.get("affiliation") != "Internal":
            continue
        email = (p.get("email") or "").lower()
        uid = email_to_user_id.get(email)
        if not uid or uid in seen:
            continue
        seen.add(uid)
        swiftlets.append({"object": "user", "id": uid})

    facilitator: list[dict] = []
    if facilitator_email:
        uid = email_to_user_id.get(facilitator_email.lower())
        if uid:
            facilitator = [{"object": "user", "id": uid}]

    summary = (call.get("brief") or "").strip()
    if len(summary) > SUMMARY_CHAR_LIMIT:
        summary = summary[:SUMMARY_CHAR_LIMIT]

    props: dict = {
        "Conversation Title": {"title": _title_value(call.get("title", ""))},
        "Date": {"date": {"start": call.get("started", "")}},
        "Format": {"select": {"name": "Gong Recording"}},
        "Link to source": {"url": call.get("url", "")},
        "Swiftlets Involved": {"people": swiftlets},
        "Facilitator": {"people": facilitator},
    }
    if summary:
        props["Summary"] = {"rich_text": _rich_text_runs(summary)}

    purpose = (call.get("purpose") or "").strip()
    if purpose:
        props["Purpose"] = {"multi_select": [{"name": purpose}]}

    return props


# ---------------------------------------------------------------------------
# Body blocks
# ---------------------------------------------------------------------------

def _format_timestamp(seconds: float | int | None) -> str:
    if seconds is None:
        return "00:00:00"
    total = int(seconds)
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _turn_text(speaker: str, ts: str, body: str) -> str:
    return f"{speaker} ({ts}): {body}"


def build_transcript_paragraph_blocks(transcript: list[dict]) -> list[dict]:
    """One paragraph block per speaker turn. Empty transcript → single placeholder."""
    if not transcript:
        return [_paragraph("(No transcript available.)")]

    blocks: list[dict] = []
    for turn in transcript:
        speaker = turn.get("speaker") or "Unknown Speaker"
        sentences = turn.get("sentences") or []
        if not sentences:
            continue
        start = sentences[0].get("start", 0)
        # Gong's `start` is in milliseconds.
        ts = _format_timestamp(start / 1000 if start else 0)
        body = " ".join((s.get("text") or "").strip() for s in sentences).strip()
        if not body:
            continue
        blocks.append(_paragraph(_turn_text(speaker, ts, body)))

    if not blocks:
        return [_paragraph("(No transcript available.)")]
    return blocks


def _paragraph(text: str) -> dict:
    return {
        "object": "block",
        "type": "paragraph",
        "paragraph": {"rich_text": _rich_text_runs(text)},
    }


def build_transcript_toggle() -> dict:
    """Toggleable H2 header. Transcript turns are appended as children
    separately because a long call exceeds Notion's 100-child-per-request cap."""
    return {
        "object": "block",
        "type": "heading_2",
        "heading_2": {
            "rich_text": [{"type": "text", "text": {"content": "Transcript"}}],
            "is_toggleable": True,
        },
    }


def build_participants_toggle() -> dict:
    """Toggleable H2 header for the Participants section."""
    return {
        "object": "block",
        "type": "heading_2",
        "heading_2": {
            "rich_text": [{"type": "text", "text": {"content": "Participants"}}],
            "is_toggleable": True,
        },
    }


def build_participant_blocks(participants: list[dict]) -> list[dict]:
    """One bulleted_list_item per unique participant. 'Name <email>' when both
    are known, email-only when name is missing, name-only when email is missing.
    Deduped by email (case-insensitive), falling back to name."""
    if not participants:
        return [_bulleted("(No participants listed.)")]

    seen: set[str] = set()
    blocks: list[dict] = []
    for p in participants:
        name = (p.get("name") or "").strip()
        email = (p.get("email") or "").strip()
        # fetch_gong_calls.py fills the literal string "Unknown" when Gong
        # returns no name — treat that as missing so we don't render it.
        if name.lower() == "unknown":
            name = ""
        key = email.lower() or name.lower()
        if not key or key in seen:
            continue
        seen.add(key)
        if name and email:
            text = f"{name} <{email}>"
        elif email:
            text = email
        else:
            text = name
        blocks.append(_bulleted(text))

    if not blocks:
        return [_bulleted("(No participants listed.)")]
    return blocks


def _bulleted(text: str) -> dict:
    return {
        "object": "block",
        "type": "bulleted_list_item",
        "bulleted_list_item": {"rich_text": _rich_text_runs(text)},
    }


# ---------------------------------------------------------------------------
# Facilitator resolution from raw call record
# ---------------------------------------------------------------------------

def resolve_facilitator_email(call: dict) -> str | None:
    """Match the call's primary_user_id to a party's email. None if unresolved."""
    primary = call.get("primary_user_id")
    if not primary:
        return None
    for p in call.get("participants", []):
        if p.get("user_id") == primary:
            email = p.get("email")
            return email or None
    return None
