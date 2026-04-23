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

## Filtering

External-customer filtering and private-call exclusion are automatic: only calls with at least one `External`-affiliated participant are kept, and calls flagged private in Gong are dropped before anything is written to Notion.
