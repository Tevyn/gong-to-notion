# gong-to-notion

A small Python CLI that pulls Gong call transcripts and metadata into a Notion "Customer Interactions" database. For each external-customer call in the requested window, it creates one Notion page with properties (title, date, facilitator, participants, Gong URL) plus participant and transcript toggle blocks. Runs are deterministic and deduped against the existing database by Gong source URL.

## Requirements

- Python >= 3.11
- [uv](https://docs.astral.sh/uv/)

## Setup

```sh
git clone <repo-url> gong-to-notion
cd gong-to-notion
uv sync
cp .env.example .env
```

Then fill in `.env`:

- `NOTION_TOKEN` - internal integration token with access to the target database
- `NOTION_DATABASE_ID` - the Customer Interactions database (data source) ID
- `GONG_ACCESS_KEY` / `GONG_ACCESS_KEY_SECRET` - Gong API credentials
- `GONG_BASE_URL` - your Gong API base URL (e.g. `https://api.gong.io`)

## Usage

Import the last 7 days:

```sh
uv run python -m gong_to_notion --since 7d
```

Import a specific window (UTC; bare dates are treated as `T00:00:00Z`):

```sh
uv run python -m gong_to_notion --start 2026-04-01 --end 2026-04-15
```

Preview without writing to Notion:

```sh
uv run python -m gong_to_notion --since 24h --dry-run
```

Dump the exact Notion request payloads to a JSON file (works with or without `--dry-run`):

```sh
uv run python -m gong_to_notion --since 7d --dry-run --dump run.json
```

`--since` accepts `Nd` or `Nh` and is mutually exclusive with `--start`/`--end`. Exit code is `1` if any page fails to create, else `0`.

After creating each page, `run` also performs the deterministic Agency + Agency Staff fill (resolving Agencies by Salesforce Account ID, then by participant email domain, and creating Staff rows as needed). Pass `--skip-fill` to disable it.

### Agency Staff identity

Staff rows are matched by email address. A person often appears in Gong under
more than one address (agency domain migrations like `scmtd.com` → `scmetro.org`,
personal vs work, typo domains Gong picked up from a calendar invite), which used
to create a second Staff row per address.

The `Other Emails` property on Agency Staff holds every *additional* address a
person is known by, as free text. `load_fill_caches` indexes it alongside `Email`,
so a call from any known address resolves to the existing row. Create the property
once with:

```sh
uv run python scripts/add_staff_other_emails_property.py
```

When a call resolves through `Other Emails` and that call is the most recent one
linked to the person (compared against the `Last Contacted` formula), the address
it used becomes the primary `Email` and the previous primary moves into
`Other Emails`. The row keeps matching both either way, so this only keeps the
displayed address current. Runs report the count as `Primary emails set`.

Notion's `Last edited` is deliberately not used for this: on the current data 80
of 120 duplicate-cluster rows were last edited by this importer rather than by a
human, so it tracks our own writes rather than which address is current.

`Other Emails` was populated for people who already had two rows by a one-time
merge pass over the whole table: gather facts, validate decisions, apply, verify.
It merged 59 duplicate clusters (2,192 rows down to 2,133) and left 2 flagged for
a human.

That pass was a one-off, so its three scripts and the reviewer-facing
`STAFF_MERGE_GUIDELINES.md` were removed once it finished. Both live in git
history at commit `8ad8726` and can be restored with
`git checkout 8ad8726 -- scripts/staff_merge_gather.py` (and likewise for
`staff_merge_apply.py`, `staff_merge_verify.py`, `STAFF_MERGE_GUIDELINES.md`).

Two constraints in there are worth knowing before touching this data again:
`GET /v1/pages/{id}` truncates relation values at 25 entries and sets
`has_more`, so unioning relations from page objects silently drops links; and
`Agency` / `Customer Conversations` are `dual_property` relations, so writing the
surviving row updates every Customer Interaction and Agency page by itself.

Rows identified only by a first name ("Andy", "Nick") are deliberately never
clustered, so a few visible near-duplicates remain where that is all we have.

### Run report and gaps

At the end of a run the report lists Created / Already existed / Failed, plus a **"Gaps in created pages"** section flagging rows a human should follow up on: no Agency linked, no Agency Staff linked, or no Purpose set. Each gap line explains what couldn't be resolved (e.g. an SF Account ID not present in Notion, or attendee domains with no matching Agency). The Purpose gaps are filled by the `customer-interactions-judgment-fill` skill (an LLM pass), not by this CLI.

## Filtering

External-customer filtering and private-call exclusion are automatic: only calls with at least one `External`-affiliated participant are kept, and calls flagged private in Gong are dropped before anything is written to Notion.

## Subcommands

`run` is the default and is assumed when no subcommand is given. The others maintain the Agencies / Agency Staff side of the database:

```sh
# Deterministic Agency/Staff/Purpose fill over existing Customer Interactions pages.
uv run python -m gong_to_notion backfill-agency-and-staff --since 30d [--dry-run]

# Derive each Agency's Email Domains from its Website URL.
uv run python -m gong_to_notion seed-agency-domains-from-website [--dry-run]
```

Both support `--dry-run` to print a plan without writing.

A third subcommand, `seed-agency-domains`, derived Email Domains from each Agency's
existing Staff emails. It was removed: Staff rosters routinely include contractor and
operator addresses (Transdev, RATP Dev, Vontas, Clever Devices, MTM, Connexionz), so it
wrote shared vendor domains onto individual Agencies. Because a contested domain resolves
by "keeping first" in `load_fill_caches`, that silently mapped any future call with a
vendor attendee to an arbitrary Agency. The Website variant is the supported path: it
derives from the Agency's own URL and holds back collisions and cross-claims for review.

## scripts/

`scripts/` holds one-off migration and diagnostic tools that are not part of the importer's normal operation:

- `fill_agency_account_ids.py`: upsert Agencies from a Salesforce Accounts Report xlsx (needs the `scripts` extra).
- `seed_agency_domains_from_calls.py`: seed Agency Email Domains from observed call attendance.
- `fill_staff_agency_by_domain.py`: backfill blank `Agency` relations on Staff rows by email domain.
- `check_staff_duplicates.py`: read-only report of duplicate Agency Staff rows.

The xlsx importer needs `openpyxl`, declared as an optional dependency:

```sh
uv sync --extra scripts
uv run --extra scripts python scripts/fill_agency_account_ids.py PATH.xlsx --apply
```

Run the others from the repo root, e.g. `uv run python scripts/check_staff_duplicates.py`.
