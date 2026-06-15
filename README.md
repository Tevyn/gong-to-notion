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

### Run report and gaps

At the end of a run the report lists Created / Already existed / Failed, plus a **"Gaps in created pages"** section flagging rows a human should follow up on: no Agency linked, no Agency Staff linked, or no Purpose set. Each gap line explains what couldn't be resolved (e.g. an SF Account ID not present in Notion, or attendee domains with no matching Agency). The Purpose gaps are filled by the `customer-interactions-judgment-fill` skill (an LLM pass), not by this CLI.

## Filtering

External-customer filtering and private-call exclusion are automatic: only calls with at least one `External`-affiliated participant are kept, and calls flagged private in Gong are dropped before anything is written to Notion.

## Subcommands

`run` is the default and is assumed when no subcommand is given. The others maintain the Agencies / Agency Staff side of the database:

```sh
# Deterministic Agency/Staff/Purpose fill over existing Customer Interactions pages.
uv run python -m gong_to_notion backfill-agency-and-staff --since 30d [--dry-run]

# Derive each Agency's Email Domains from its existing Staff emails.
uv run python -m gong_to_notion seed-agency-domains [--dry-run]

# Derive each Agency's Email Domains from its Website URL.
uv run python -m gong_to_notion seed-agency-domains-from-website [--dry-run]
```

All three support `--dry-run` to print a plan without writing.

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
