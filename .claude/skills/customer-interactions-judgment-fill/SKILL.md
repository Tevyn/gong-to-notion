---
name: customer-interactions-judgment-fill
description: >-
  Fill Purpose (and Agencies as a fallback) on Customer Interactions pages in
  Notion that the gong-to-notion importer and its deterministic fill step left
  blank. Trigger when the user asks to "fill judgment fields", "classify call
  purpose", "fill purpose", "fill missing agencies", or similar.
---

# Customer Interactions — Judgment-Field Filler (LLM pass)

The [gong-to-notion](../) importer runs a **deterministic** Agency + Agency Staff
fill in code (see `src/gong_to_notion/agency_and_staff_fill.py` and the
`backfill-agency-and-staff` subcommand). That covers everything it can resolve from
participant emails + Agency email-domain lookups, and creates Staff rows as
needed.

This skill is the LLM fallback for whatever the deterministic step can't do:

- **Purpose** — always (no deterministic path).
- **Agencies** — only when still blank after the deterministic pass (e.g. a
  call where every external participant is on a free-mail domain, or where the
  Agency's Email Domains aren't populated).

**Agency Staff is out of scope.** Code owns it. Do not create Agency Staff rows
from this skill.

This skill **only fills blanks** — it never overwrites an existing value.

## Databases

| What | Data source ID | Notes |
|---|---|---|
| Customer Interactions (the calls) | `collection://c9db2d38-cf18-4758-985a-99aadc826665` | DB URL: notion.so/83624243ef2a494e9b99a7b309372360 |
| Agencies | `collection://b9945686-eb26-42bb-9020-1e8075466f42` | DB URL: notion.so/445da779d0b7409e9bd0c1df7f508c68. Title prop = `Name`. Do **not** create new rows here. |

## Fields this skill writes

On each Customer Interactions page, only these two (and only when blank):

- **Agencies** — relation to Agencies DB. Reason over the call's title, Summary, and external speaker names. If no row clearly matches, leave blank.
- **Purpose** — multi-select, closed list (below). If nothing fits clearly, leave blank.

Everything else on the page (Agency Staff, Planned Topics, Research Insights, Features, Considerations / Decisions, To Dos, Feature Groups, Teams, Agenda sent, etc.) is **not in scope** — do not touch.

## Purpose — closed list

Pick only from these names (case- and punctuation-exact):

```
Generative Research, User Testing, Customer Council, Demo, QBR, Training,
Implementation / Onboarding, Design Review, Customer Success Check-in,
Support / Troubleshooting, Feedback Session, Customer Training, EBR,
Pre-sales, Exec Visit, Market Sounding Response
```

If you produce a value not in this list, drop it before writing.

### Disambiguation rubric

- **Demo vs Pre-sales** — Demo if the customer already exists; Pre-sales if it's a prospect or sales-cycle conversation aimed at landing/expanding a deal.
- **EBR vs QBR** — EBR is annual/executive; QBR is the regular quarterly cadence with the working team.
- **Training vs Customer Training** — treat as effectively the same; pick one (Customer Training) when it's clearly Swiftly teaching the agency how to use the product.
- **Implementation / Onboarding vs Customer Training** — Implementation/Onboarding is initial setup of a new account; Customer Training is teaching established users (could be later in lifecycle).
- **Customer Success Check-in vs Support / Troubleshooting** — Check-in is recurring/relationship; Support is reactive to a specific problem.
- **Generative Research vs User Testing vs Feedback Session** — Generative is open-ended discovery; User Testing has the user driving a prototype/flow; Feedback Session is the customer reacting to something we already built or proposed.
- **Customer Council** — only if the call is explicitly part of a council program (look for "council" in the title or summary).
- **Market Sounding Response** — only when the call is a vendor-side reply to a formal market-sounding/RFI from an agency.

A call can have multiple Purposes (e.g. a Demo that includes a Pre-sales discussion). Don't force a single pick — but don't add a value just because it's plausible either.

## Tools you'll use

The Notion MCP connector. Function names in this conversation will look like `mcp__<server-id>__notion-...`:

- `notion-fetch` — page content / DB schema by id or URL
- `notion-query-data-sources` — SQL query against one or more data sources
- `notion-update-page` — set properties on an existing page

## Workflow

### Step 1 — Get the time window

If the user didn't specify, ask for one. The window applies to the Conversations page's `Created` (Notion `created_time`), not the call `Date`. Examples: "last 7 days", "since 2026-04-09", "from 2026-04-01 to 2026-04-15".

Convert to ISO-8601 with timezone (UTC is fine if unspecified).

### Step 2 — Find candidate pages

One SQL query against the Customer Interactions data source. Filter:

- `Created >= <window_start>` AND `Created < <window_end>`
- AND at least one of `Agencies` or `Purpose` is empty.

Select: `url`, `Conversation Title`, `Summary`, `Agencies`, `Purpose`, `Swiftlets Involved`.

Hold the result set in memory — it's the source of truth for which fields are empty per page. Re-check before each write.

### Step 3 — Load the Agencies listing (once)

```sql
SELECT url, Name, Classification, "Region/City"
FROM "collection://b9945686-eb26-42bb-9020-1e8075466f42"
```

Build a compact `[{url, name, classification, region}]` table.

### Step 4 — For each candidate page

#### 4a. Fetch page content

`notion-fetch` on the page URL. Read the transcript toggle for context, and the Participants toggle for external speaker emails/names.

#### 4b. Decide what's empty

From the Step 2 row, check `Agencies` and `Purpose`. Empty = NULL / `[]` / `""`. Skip non-empty fields entirely — do not include them in the update payload.

#### 4c. Resolve Agencies (if empty)

Reason over: call title, Summary, external speaker names, external email domains, against the Agencies listing.

Output: zero or more Agency `url`s. Constraints:

- Pick only Agencies that already exist in the listing. Never invent.
- If unsure → pick none.
- One call can be about multiple agencies.

#### 4d. Classify Purpose (if empty)

Reason over title + Summary + transcript. Pick zero or more from the closed list using the disambiguation rubric. Validate every value against the closed list before proceeding — drop any that don't match exactly.

If nothing clearly fits → empty. Don't guess.

#### 4e. Write

Build a single `notion-update-page` payload containing **only** the fields you filled. Examples:

- Filled both:
  ```json
  {
    "Agencies": "[\"<agency-url>\"]",
    "Purpose": "[\"Demo\", \"Pre-sales\"]"
  }
  ```
- Only Purpose was empty:
  ```json
  { "Purpose": "[\"QBR\"]" }
  ```
- Nothing to write (both filled, or neither confidently resolvable): skip the page entirely.

### Step 5 — Report

Print a concise summary: pages seen, pages updated, per-page what was filled.

## Guardrails

- **Fill blanks only.** Re-check emptiness from the Step 2 row right before writing each page. Never include a property in the update payload that already had a value.
- **Closed list.** Purpose values not in the closed list are silently dropped, not written.
- **No new Agencies.** If no Agency clearly matches, leave Agencies blank.
- **Agency Staff is code-owned.** Do not create Staff rows from this skill; the deterministic pass handles that.
- **Idempotence.** Re-running the same window should produce `Pages updated: 0`.
- **Failures don't cascade.** If one page errors, log it and continue.

## What this skill does NOT do

- Touch Agency Staff — the deterministic fill (`backfill-agency-and-staff` subcommand) owns that.
- Touch any field other than Agencies and Purpose.
- Modify the Agencies DB.
- Re-fetch from Gong.
