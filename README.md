# Datadog Service Catalog Sync

Syncs services from Datadog Events into a Reference Table and then upserts
Service Catalog entries.

## What it does

1. Query Events via `POST /api/v2/events/search` (application/json)
2. Extract unique services (prefers `attributes.attributes.service`, falls back to `service:*` tags)
3. Read Reference Table rows for those services via:
   `GET /api/v2/reference-tables/tables/{table_id}/rows?row_id=...` (JSON:API)
4. Create missing rows in the Reference Table via `POST .../rows` (JSON:API)
5. Upsert Service Catalog definitions via `POST /api/v2/services/definitions`

## Requirements

- Python 3.10+
- Datadog API and App keys with access to Events, Reference Tables, and Service Catalog

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` file (or export variables in your shell):

```bash
DD_API_KEY=...
DD_APP_KEY=...
DD_SITE=datadoghq.com
REF_TABLE_NAME=
REF_TABLE_ID=
REF_TABLE_COL_1=service
REF_TABLE_COL_2=team
```

## Usage

```bash
python sync_services.py --days 7 --page-limit 100 --verbose
```

Only events with the tag `*` are included by default. Override with:

```bash
python sync_services.py --query "demo:your-tag"
```

List Reference Tables:

```bash
python sync_services.py --list-tables
```


Dry run (no writes):

```bash
python sync_services.py --dry-run
```

## Notes

- Reference Tables in this org expect JSON:API headers.
- Rows are read by `row_id`, so the row ID must match the service name.
- If inserts fail, check the response body for required schema fields.
