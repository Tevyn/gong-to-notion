"""Thin Notion REST client scoped to what this importer needs.

We hit four endpoints:
  - GET  /v1/users                             — build email → user_id map
  - POST /v1/data_sources/{id}/query           — batch dedup lookup
  - POST /v1/pages                             — create a page
  - PATCH /v1/blocks/{id}/children             — append transcript turns

The Notion-Version constant below dictates the parent shape:
`data_source_id` (new) rather than `database_id` (pre-2025-09-03).
"""

from __future__ import annotations

import time
from typing import Iterator

import httpx

NOTION_VERSION = "2026-03-11"
NOTION_API = "https://api.notion.com/v1"

# Notion hard limit on `children` per append-block-children call.
BLOCK_CHILDREN_PER_REQUEST = 100


class NotionError(RuntimeError):
    """Wraps a failed Notion HTTP call with status + parsed body."""

    def __init__(self, message: str, status: int | None = None, body: dict | None = None):
        super().__init__(message)
        self.status = status
        self.body = body or {}


class NotionClient:
    def __init__(self, token: str, timeout: float = 30.0):
        if not token:
            raise ValueError("NOTION_TOKEN is required")
        self._client = httpx.Client(
            base_url=NOTION_API,
            headers={
                "Authorization": f"Bearer {token}",
                "Notion-Version": NOTION_VERSION,
                "Content-Type": "application/json",
            },
            timeout=timeout,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ---- HTTP plumbing -----------------------------------------------------

    def _request(self, method: str, path: str, **kwargs) -> dict:
        """Send a request with light 429/5xx retry. Raises NotionError on failure."""
        attempts = 0
        while True:
            attempts += 1
            resp = self._client.request(method, path, **kwargs)
            if resp.status_code == 429 and attempts < 5:
                retry_after = float(resp.headers.get("Retry-After", "1"))
                time.sleep(retry_after)
                continue
            if 500 <= resp.status_code < 600 and attempts < 3:
                time.sleep(1.5 * attempts)
                continue
            if resp.status_code >= 400:
                try:
                    body = resp.json()
                except Exception:
                    body = {"raw": resp.text}
                raise NotionError(
                    f"{method} {path} → {resp.status_code}: {body.get('message', body)}",
                    status=resp.status_code,
                    body=body,
                )
            if not resp.content:
                return {}
            return resp.json()

    # ---- Users -------------------------------------------------------------

    def list_all_users(self) -> list[dict]:
        """Return every user object in the workspace (paginated)."""
        out: list[dict] = []
        cursor: str | None = None
        while True:
            params = {"page_size": 100}
            if cursor:
                params["start_cursor"] = cursor
            page = self._request("GET", "/users", params=params)
            out.extend(page.get("results", []))
            if not page.get("has_more"):
                break
            cursor = page.get("next_cursor")
            if not cursor:
                break
        return out

    def build_email_to_user_id(self) -> dict[str, str]:
        """Map lowercased email → user_id for every person user.

        Raises NotionError if the integration lacks the user-email capability
        (i.e. every person user is returned without a `person.email` field).
        """
        users = self.list_all_users()
        mapping: dict[str, str] = {}
        saw_person = False
        for u in users:
            if u.get("type") != "person":
                continue
            saw_person = True
            email = (u.get("person") or {}).get("email")
            if not email:
                continue
            mapping[email.lower()] = u["id"]
        if saw_person and not mapping:
            raise NotionError(
                "Integration lacks user-email capability. See Setup prerequisites "
                "step 2 in PRD.md."
            )
        return mapping

    # ---- Data source query (dedup) ----------------------------------------

    def fetch_existing_source_urls(
        self, data_source_id: str, url_property: str = "Link to source"
    ) -> dict[str, str]:
        """Return {source_url: page_id} for every page where url_property is set."""
        existing: dict[str, str] = {}
        cursor: str | None = None
        payload_base = {
            "filter": {
                "property": url_property,
                "url": {"is_not_empty": True},
            },
            "page_size": 100,
        }
        while True:
            payload = dict(payload_base)
            if cursor:
                payload["start_cursor"] = cursor
            try:
                page = self._request(
                    "POST", f"/data_sources/{data_source_id}/query", json=payload
                )
            except NotionError as e:
                if e.status in (401, 403):
                    raise NotionError(
                        "Integration does not have access to the Customer "
                        "Interactions database. See Setup prerequisites step 3 "
                        "in PRD.md."
                    ) from e
                raise
            for row in page.get("results", []):
                props = row.get("properties") or {}
                prop = props.get(url_property) or {}
                url = prop.get("url")
                if url:
                    existing[url] = row.get("id", "")
            if not page.get("has_more"):
                break
            cursor = page.get("next_cursor")
            if not cursor:
                break
        return existing

    def query_data_source(
        self,
        data_source_id: str,
        filter: dict | None = None,
        sorts: list[dict] | None = None,
        page_size: int = 100,
    ) -> list[dict]:
        """Paginated query returning every matching row (full page objects)."""
        results: list[dict] = []
        cursor: str | None = None
        while True:
            payload: dict = {"page_size": page_size}
            if filter is not None:
                payload["filter"] = filter
            if sorts is not None:
                payload["sorts"] = sorts
            if cursor:
                payload["start_cursor"] = cursor
            page = self._request(
                "POST", f"/data_sources/{data_source_id}/query", json=payload
            )
            results.extend(page.get("results", []))
            if not page.get("has_more"):
                break
            cursor = page.get("next_cursor")
            if not cursor:
                break
        return results

    # ---- Page creation / update -------------------------------------------

    def create_page(
        self,
        data_source_id: str,
        properties: dict,
        children: list[dict] | None = None,
    ) -> dict:
        """Create a page under a data-source parent. Returns the created page object."""
        body: dict = {
            "parent": {"type": "data_source_id", "data_source_id": data_source_id},
            "properties": properties,
        }
        if children:
            body["children"] = children
        return self._request("POST", "/pages", json=body)

    def update_page(self, page_id: str, properties: dict) -> dict:
        """PATCH /pages/{id} with the given properties payload."""
        return self._request(
            "PATCH", f"/pages/{page_id}", json={"properties": properties}
        )

    def get_block_children(self, block_id: str) -> list[dict]:
        """Paginated GET /blocks/{id}/children. Returns all child block objects."""
        out: list[dict] = []
        cursor: str | None = None
        while True:
            params: dict = {"page_size": 100}
            if cursor:
                params["start_cursor"] = cursor
            page = self._request(
                "GET", f"/blocks/{block_id}/children", params=params
            )
            out.extend(page.get("results", []))
            if not page.get("has_more"):
                break
            cursor = page.get("next_cursor")
            if not cursor:
                break
        return out

    def append_block_children(self, block_id: str, children: list[dict]) -> list[dict]:
        """Append children to a block in batches of ≤100 (Notion's per-call cap).

        Returns the list of created child block objects (in order), so callers
        that need the new blocks' IDs — e.g. to nest further children under a
        freshly-appended toggle — don't need a second round-trip.
        """
        created: list[dict] = []
        for batch in _chunks(children, BLOCK_CHILDREN_PER_REQUEST):
            resp = self._request(
                "PATCH", f"/blocks/{block_id}/children", json={"children": batch}
            )
            created.extend(resp.get("results", []))
        return created


def _chunks(seq: list, n: int) -> Iterator[list]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]
