---
name: customer-interactions-judgment-fill
description: >-
  Fill the Agencies, Agency Staff, and Purpose fields on Customer Interactions
  pages in Notion that the gong-to-notion importer left blank. Trigger when
  the user asks to "fill judgment fields", "classify call purpose", "link
  agencies on calls", "fill in the customer pages", "fill agency staff",
  or anything that names the Customer Interactions DB and the words
  Agencies / Purpose / Staff.
---

# Customer Interactions — Judgment-Field Filler

Post-pass for the deterministic [gong-to-notion](../) importer. The importer lands
calls in the Customer Interactions DB but leaves the three judgment fields
below blank. This skill fills them using the Notion connector you already have
wired up.

The skill **only fills blanks** — it never overwrites a value a human (or a
prior run) already set.

## Databases

| What | Data source ID | Notes |
|---|---|---|
| Customer Interactions (the calls) | `collection://c9db2d38-cf18-4758-985a-99aadc826665` | DB URL: notion.so/83624243ef2a494e9b99a7b309372360 |
| Agencies | `collection://b9945686-eb26-42bb-9020-1e8075466f42` | DB URL: notion.so/445da779d0b7409e9bd0c1df7f508c68. Title prop = `Name`. Do **not** create new rows here. |
| Agency Staff | `collection://664ccf5e-8cdf-43a4-863c-cfe8ccdef26b` | DB URL: notion.so/bdee03e76c01404abe12db03ac1d8a54. Title prop = `Name`. New rows allowed. |

## Fields this skill writes

On each Customer Interactions page, only these three:

- **Agencies** — relation to Agencies DB. Resolved by reasoning over the call's title, Summary, and external speaker names against the loaded Agencies listing. **Never create new Agencies rows.** If no row clearly matches, leave blank.
- **Agency Staff** — relation to Agency Staff DB. For each external speaker on the call: match by normalized name to the existing Staff listing; if no match, create a new Staff row (Name + Agency link if exactly one Agency was resolved this page) and link it.
- **Purpose** — multi-select. Closed list (see below). Pick zero or more values. If nothing fits clearly, leave blank.

Everything else on the page (Planned Topics, Research Insights, Features, Considerations / Decisions, To Dos, Feature Groups, Teams, Agenda sent, etc.) is **not in scope** — do not touch.

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
- `notion-update-page` — set properties on an existing page (use this for the Customer Interactions writes)
- `notion-create-pages` — create new Agency Staff rows

## Workflow

### Step 1 — Get the time window

If the user didn't specify, ask for one. The window applies to the Conversations page's `Created` (Notion `created_time`), not the call `Date`. Examples: "last 7 days", "since 2026-04-09", "from 2026-04-01 to 2026-04-15".

Convert to ISO-8601 with timezone (UTC is fine if unspecified).

### Step 2 — Find candidate pages

One SQL query against the Customer Interactions data source. Filter:

- `Created >= <window_start>` AND `Created < <window_end>`
- AND at least one of `Agencies`, `Agency Staff`, `Purpose` is empty (NULL or `[]` or `""`).

Select: `url`, `Contact title`, `Summary`, `Agencies`, `Agency Staff`, `Purpose`, `Swiftlets Involved`.

Hold the result set in memory. It's the source of truth for which fields are empty per page — re-check from this list before each write so you don't clobber human edits.

### Step 3 — Load the Agencies listing (once)

```sql
SELECT url, Name, Classification, "Region/City"
FROM "collection://b9945686-eb26-42bb-9020-1e8075466f42"
```

Build a compact `[{url, name, classification, region}]` table. Keep this in your context for Step 5c.

### Step 4 — Load the Agency Staff listing (once)

```sql
SELECT url, Name, Email, Agency
FROM "collection://664ccf5e-8cdf-43a4-863c-cfe8ccdef26b"
```

Build a normalized-name lookup: `normalize(name) → page_url`, where `normalize` lowercases, collapses whitespace, and strips honorifics/suffixes (Mr/Ms/Dr/Jr/Sr/PhD).

### Step 5 — For each candidate page

#### 5a. Fetch page content

`notion-fetch` on the page URL. Read the transcript toggle in the body. Extract speaker names (each turn is `**Speaker Name** (timestamp)`).

External speakers = speakers whose name does not match a `Swiftlets Involved` person on the page. Swiftlets Involved is a list of Notion users (user IDs); match by display name from the user listing if needed, or just trust the heuristic that internal Swiftly people are the ones in `Swiftlets Involved` and treat everyone else who spoke as external.

#### 5b. Decide what's empty

From the Step 2 row, check each of `Agencies`, `Agency Staff`, `Purpose`. Empty = NULL / `[]` / `""`. Skip non-empty fields entirely — do not include them in the update payload.

#### 5c. Resolve Agencies (if empty)

Reasoning over: call title, Summary, external speaker names, external speaker email domains (if you can spot them in the transcript), against the Agencies listing.

Output: zero or more Agency `url`s. Constraints:

- Pick only Agencies that already exist in the listing. Never invent.
- If unsure → pick none. Empty is the right default; a wrong link is worse than a blank.
- One call can be about multiple agencies (e.g. a multi-agency council session) — that's fine.

#### 5d. Resolve Agency Staff (if empty)

For each external speaker name:

1. Normalize the name; look up in the Step 4 map.
2. If matched → collect the staff `url`.
3. If not matched → create a new Agency Staff row via `notion-create-pages`:
   - parent: `{"data_source_id": "664ccf5e-8cdf-43a4-863c-cfe8ccdef26b"}`
   - properties: `{"Name": "<full name as spoken>"}` plus `"Agency": "[\"<agency-url>\"]"` **only if** Step 5c resolved exactly one Agency for this page. Otherwise leave Agency blank on the new row.
   - Do not invent emails, roles, or other fields. Anything not derivable from the transcript stays blank.
4. Collect the new row's `url`.

The page's Agency Staff relation = matched urls + newly-created urls.

#### 5e. Classify Purpose (if empty)

Reason over title + Summary + transcript. Pick zero or more from the closed list using the disambiguation rubric. Validate every value against the closed list before proceeding — drop any that don't match exactly.

If nothing clearly fits → empty. Don't guess.

#### 5f. Write

Build a single `notion-update-page` payload containing **only** the fields you filled. Examples:

- Filled all three:
  ```json
  {
    "Agencies": "[\"<agency-url>\"]",
    "Agency Staff": "[\"<staff-url>\", \"<staff-url>\"]",
    "Purpose": "[\"Demo\", \"Pre-sales\"]"
  }
  ```
- Only Purpose was empty (Agencies + Agency Staff already had values):
  ```json
  { "Purpose": "[\"QBR\"]" }
  ```
- Nothing to write (all three already filled, or you decided to leave all three blank): skip the page entirely, no API call.

### Step 6 — Report

Print a concise summary. Format:

```
Judgment-fill run report
  Window (Created time): <start> → <end>
  Candidates:            N
  Pages updated:         N
  Pages fully skipped:   N  (all 3 already filled)
  New Staff rows:        N
  Pages where nothing was confidently fillable: N

Per page:
  - <Contact title> — <notion url>
      Agencies: +N (Names) | Staff: +N (M created) | Purpose: +N (Values)
  - <Contact title> — <notion url>
      Agencies: skipped | Staff: +1 (1 created: Jane Doe) | Purpose: blank (no clear match)
  ...

New Staff rows created:
  - Jane Doe → <notion url> (Agency: LA Metro)
  - John Smith → <notion url> (Agency: blank)
```

## Guardrails

- **Fill blanks only.** Re-check emptiness from the Step 2 row right before writing each page. Never include a property in the update payload that already had a value.
- **Closed list.** Purpose values not in the closed list are silently dropped, not written.
- **No new Agencies.** If no Agency clearly matches, leave Agencies blank. Period.
- **Idempotence.** Re-running the same window should produce a report with `Pages updated: 0` (everything already filled).
- **Failures don't cascade.** If one page errors, log it and continue with the rest. End the run with the per-page summary.

## What this skill does NOT do

- Touch any field other than Agencies / Agency Staff / Purpose.
- Modify the Agencies DB.
- Re-fetch from Gong (the Conversations page is the source of truth here).
- Pre-fill obvious cases deterministically (no email-domain → Agency shortcut). Reasoning over the loaded Agencies listing is the whole strategy. If accuracy is poor in practice, revisit then.
