"""Gong API client: fetch call metadata and transcripts.

Two public functions, both keyed by `call_id`:
  - fetch_calls_extensive(from_dt, to_dt, call_ids=None) -> {id: call_data}
  - fetch_transcripts(from_dt, to_dt, call_ids=None)     -> {id: [monologues]}

Auth is HTTP Basic from GONG_ACCESS_KEY / GONG_ACCESS_KEY_SECRET. Region host
comes from GONG_BASE_URL (e.g. https://us-XXXXX.api.gong.io).
"""

from __future__ import annotations

import base64
import os
from typing import Iterator

import httpx


def _base_url() -> str:
    url = os.getenv("GONG_BASE_URL", "")
    if not url:
        raise RuntimeError("GONG_BASE_URL must be set in .env")
    return url


def _auth_header() -> dict[str, str]:
    key = os.getenv("GONG_ACCESS_KEY", "")
    secret = os.getenv("GONG_ACCESS_KEY_SECRET", "")
    if not key or not secret:
        raise RuntimeError(
            "GONG_ACCESS_KEY and GONG_ACCESS_KEY_SECRET must be set in .env"
        )
    token = base64.b64encode(f"{key}:{secret}".encode()).decode()
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json"}


def _paginated_post(endpoint: str, body: dict) -> Iterator[dict]:
    """Yield each page of results; follows Gong's `records.cursor` pagination."""
    cursor: str | None = None
    with httpx.Client(base_url=_base_url(), headers=_auth_header(), timeout=60.0) as c:
        while True:
            payload = {**body}
            if cursor:
                payload["cursor"] = cursor
            resp = c.post(f"/v2/{endpoint}", json=payload)
            resp.raise_for_status()
            data = resp.json()
            yield data
            cursor = (data.get("records") or {}).get("cursor") or data.get("cursor")
            if not cursor:
                break


def fetch_calls_extensive(
    from_dt: str | None = None,
    to_dt: str | None = None,
    call_ids: list[str] | None = None,
) -> dict[str, dict]:
    """Fetch detailed call metadata. Returns {call_id: call_data}."""
    filt: dict = {}
    if from_dt:
        filt["fromDateTime"] = from_dt
    if to_dt:
        filt["toDateTime"] = to_dt
    if call_ids:
        filt["callIds"] = call_ids

    body = {
        "filter": filt,
        "contentSelector": {
            "exposedFields": {
                # `parties` is a top-level exposedField, NOT under `content`.
                # Nesting it under content silently returns `parties: []`.
                "parties": True,
                "content": {"brief": True},
                "collaboration": {"publicComments": False},
            }
        },
    }

    calls_by_id: dict[str, dict] = {}
    for page in _paginated_post("calls/extensive", body):
        for call in page.get("calls", []):
            meta = call.get("metaData", {})
            parties = call.get("parties", [])
            content = call.get("content", {}) or {}
            participants = []
            speaker_map: dict[str, str] = {}
            for p in parties:
                name = p.get("name", "Unknown")
                email = p.get("emailAddress", "")
                affiliation = p.get("affiliation", "Unknown")
                speaker_id = p.get("speakerId")
                user_id = p.get("userId", "")
                participants.append(
                    {
                        "name": name,
                        "email": email,
                        "affiliation": affiliation,
                        "user_id": user_id,
                    }
                )
                if speaker_id:
                    speaker_map[speaker_id] = name

            duration_secs = meta.get("duration", 0)
            calls_by_id[meta.get("id", "")] = {
                "call_id": meta.get("id", ""),
                "title": meta.get("title", "Untitled Call"),
                "url": meta.get("url", ""),
                "started": meta.get("started", ""),
                "duration_minutes": round(duration_secs / 60, 1) if duration_secs else 0,
                "direction": meta.get("direction", ""),
                "primary_user_id": meta.get("primaryUserId", ""),
                "is_private": bool(meta.get("isPrivate", False)),
                "brief": content.get("brief", ""),
                "participants": participants,
                "_speaker_map": speaker_map,
            }
    return calls_by_id


def fetch_transcripts(
    from_dt: str | None = None,
    to_dt: str | None = None,
    call_ids: list[str] | None = None,
) -> dict[str, list[dict]]:
    """Fetch transcripts. Returns {call_id: [monologues]}."""
    filt: dict = {}
    if from_dt:
        filt["fromDateTime"] = from_dt
    if to_dt:
        filt["toDateTime"] = to_dt
    if call_ids:
        filt["callIds"] = call_ids

    transcripts_by_id: dict[str, list[dict]] = {}
    for page in _paginated_post("calls/transcript", {"filter": filt}):
        for ct in page.get("callTranscripts", []):
            call_id = ct.get("callId", "")
            monologues = []
            for mono in ct.get("transcript", []):
                monologues.append(
                    {
                        "speakerId": mono.get("speakerId", ""),
                        "topic": mono.get("topic"),
                        "sentences": [
                            {
                                "start": s.get("start", 0),
                                "end": s.get("end", 0),
                                "text": s.get("text", ""),
                            }
                            for s in mono.get("sentences", [])
                        ],
                    }
                )
            transcripts_by_id[call_id] = monologues
    return transcripts_by_id
