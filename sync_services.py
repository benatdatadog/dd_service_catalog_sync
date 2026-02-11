#!/usr/bin/env python3
"""
Sync Datadog Events -> Reference Tables -> Service Catalog

What this does:
1) Query Events via POST /api/v2/events/search (application/json)
2) Extract unique services (prefers attributes.attributes.service, falls back to service:* tags)
3) Read Reference Table rows for those services via GET
   /api/v2/reference-tables/tables/{table_id}/rows?row_id=...
   (JSON:API content-type)
4) Create missing rows in the reference table via POST .../rows (JSON:API)
5) Upsert Service Catalog definition via POST /api/v2/services/definitions
   with top-level service definition document (NOT wrapped in {"data":...})

Env:
  DD_API_KEY, DD_APP_KEY
  DD_SITE (default datadoghq.com)
  REF_TABLE_NAME (default reference_table)
  REF_TABLE_COL_1 (default service)
  REF_TABLE_COL_2 (default team)
"""

import argparse
import datetime as dt
import os
from typing import Dict, Iterable, List, Set, Tuple

import requests
from dotenv import load_dotenv


# -----------------------------
# Base / headers
# -----------------------------
def build_api_base(site: str) -> str:
    site = (site or "").strip()
    if site.startswith("http://") or site.startswith("https://"):
        return site.rstrip("/")
    if site.startswith("api."):
        return f"https://{site}"
    return f"https://api.{site}"


def auth_headers_json(api_key: str, app_key: str) -> Dict[str, str]:
    # Events + Service Catalog
    return {
        "DD-API-KEY": api_key,
        "DD-APPLICATION-KEY": app_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def auth_headers_jsonapi(api_key: str, app_key: str) -> Dict[str, str]:
    # Reference Tables (your org expects JSON:API)
    return {
        "DD-API-KEY": api_key,
        "DD-APPLICATION-KEY": app_key,
        "Content-Type": "application/vnd.api+json",
        "Accept": "application/vnd.api+json",
    }


# -----------------------------
# Small helpers
# -----------------------------
def assign_dummy_teams(services: Iterable[str]) -> Dict[str, str]:
    assigned: Dict[str, str] = {}
    teams = ["team 1", "team 2"]
    for idx, service in enumerate(sorted(set(s for s in services if s))):
        assigned[service] = teams[idx % len(teams)]
    return assigned


def _extract_row_values(item: Dict) -> Dict[str, str]:
    attrs = item.get("attributes", {}) or {}
    return attrs.get("values") or attrs.get("value") or attrs.get("columns") or attrs


# -----------------------------
# Events
# -----------------------------
def list_services_from_events(
    base_url: str,
    headers_json: Dict[str, str],
    start: dt.datetime,
    end: dt.datetime,
    query: str,
    limit: int = 100,
    max_pages: int | None = None,
    verbose: bool = False,
) -> Set[str]:
    services: Set[str] = set()
    url = f"{base_url}/api/v2/events/search"
    body = {
        "filter": {
            "from": start.isoformat(),
            "to": end.isoformat(),
            "query": query,
        },
        "page": {"limit": limit},
    }

    cursor: str | None = None
    page = 0

    while True:
        page += 1
        if max_pages is not None and page > max_pages:
            if verbose:
                print(f"Stopping at page {page - 1} (max_pages reached).")
            break

        if cursor:
            body["page"]["cursor"] = cursor
        elif "cursor" in (body.get("page") or {}):
            body["page"].pop("cursor", None)

        if verbose:
            print(f"Fetching events page {page}...")

        resp = requests.post(url, headers=headers_json, json=body, timeout=(10, 30))
        if verbose:
            print("EVENTS:", resp.request.url, resp.status_code, resp.text[:200])

        if resp.status_code == 401:
            raise SystemExit("Unauthorized (401) for events/search. Check API+APP keys permissions.")
        resp.raise_for_status()

        data = resp.json()
        for event in data.get("data", []) or []:
            attrs = event.get("attributes", {}) or {}

            # Your sample: attributes.attributes.service
            nested = attrs.get("attributes", {}) or {}
            svc = nested.get("service")
            if svc:
                services.add(str(svc))
                continue

            # fallback: tags list contains service:<name>
            tags = attrs.get("tags", []) or []
            for tag in tags:
                if tag.startswith("service:"):
                    services.add(tag.split("service:", 1)[1])

        cursor = (data.get("meta", {}) or {}).get("page", {}).get("after")
        if not cursor:
            break

    return services


# -----------------------------
# Reference Tables
# -----------------------------
def list_reference_tables(base_url: str, headers_jsonapi: Dict[str, str]) -> List[Dict[str, str]]:
    url = f"{base_url}/api/v2/reference-tables/tables"
    params = {"page[limit]": "100"}
    out: List[Dict[str, str]] = []

    while True:
        resp = requests.get(url, headers=headers_jsonapi, params=params, timeout=30)
        if resp.status_code == 401:
            raise SystemExit("Unauthorized (401) listing reference tables.")
        resp.raise_for_status()

        payload = resp.json()
        for item in payload.get("data", []) or []:
            attrs = item.get("attributes", {}) or {}
            out.append(
                {
                    "id": item.get("id") or "",
                    "name": attrs.get("table_name") or attrs.get("name") or "",
                    "description": attrs.get("description") or "",
                }
            )

        next_link = payload.get("links", {}).get("next")
        if not next_link:
            break
        url = next_link
        params = None

    return out


def get_reference_table_id(
    base_url: str,
    headers_jsonapi: Dict[str, str],
    table_name: str,
    verbose: bool = False,
) -> str:
    url = f"{base_url}/api/v2/reference-tables/tables"
    params = {"page[limit]": "100"}
    available: List[str] = []

    while True:
        resp = requests.get(url, headers=headers_jsonapi, params=params, timeout=30)
        if resp.status_code == 401:
            raise SystemExit("Unauthorized (401) reading reference tables.")
        if resp.status_code == 404:
            break
        resp.raise_for_status()

        payload = resp.json()
        for item in payload.get("data", []) or []:
            attrs = item.get("attributes", {}) or {}
            api_name = (attrs.get("table_name") or attrs.get("name") or "").strip()
            if api_name:
                available.append(api_name)

            if api_name.lower() == table_name.lower():
                return item.get("id") or ""

            if (item.get("id") or "").lower() == table_name.lower():
                return item.get("id") or ""

        next_link = payload.get("links", {}).get("next")
        if not next_link:
            break
        url = next_link
        params = None

    if verbose and available:
        sample = ", ".join(sorted(set(available))[:30])
        raise SystemExit(f"Reference table not found. Available (sample): {sample}")

    raise SystemExit(f"Reference table not found: {table_name}")


def get_reference_table_rows_endpoint(base_url: str, table_id: str) -> str:
    return f"{base_url}/api/v2/reference-tables/tables/{table_id}/rows"


def get_reference_table_rows_by_id(
    rows_url: str,
    headers_jsonapi: Dict[str, str],
    row_ids: Iterable[str],
    service_col: str,
    team_col: str,
    verbose: bool = False,
) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    row_list = [r for r in row_ids if r]
    if not row_list:
        return mapping

    chunk_size = 100
    for start in range(0, len(row_list), chunk_size):
        chunk = row_list[start : start + chunk_size]
        params = [("row_id", rid) for rid in chunk]

        resp = requests.get(rows_url, headers=headers_jsonapi, params=params, timeout=30)
        if verbose:
            print("ROWS:", resp.request.url, resp.status_code, resp.text[:200])

        if resp.status_code == 401:
            raise SystemExit("Unauthorized (401) reading reference table rows.")
        if resp.status_code == 404:
            # Some orgs return 404 when rows are missing but include meta.not_found
            try:
                payload = resp.json()
            except ValueError:
                payload = {}
            if payload.get("meta", {}).get("not_found") is not None:
                continue
            raise SystemExit("Rows endpoint not found. Verify rows URL/path.")
        if resp.status_code >= 400:
            raise SystemExit(f"Reference table rows request failed: {resp.status_code} {resp.text}")

        payload = resp.json()
        for item in payload.get("data", []) or []:
            values = _extract_row_values(item)
            service = (values.get(service_col) or "").strip()
            team = (values.get(team_col) or "").strip()
            if service:
                mapping[service] = team

    return mapping


def create_reference_table_rows(
    rows_url: str,
    headers_jsonapi: Dict[str, str],
    service_col: str,
    team_col: str,
    rows: Dict[str, str],
) -> Tuple[int, List[str]]:
    created = 0
    failures: List[str] = []

    for service, team in rows.items():
        payload = {
            "data": [
                {
                    "type": "row",
                    "id": service,
                    "attributes": {
                        "values": {
                            service_col: service,
                            team_col: team,
                        }
                    },
                }
            ]
        }
        resp = requests.post(rows_url, headers=headers_jsonapi, json=payload, timeout=30)
        if resp.status_code in (200, 201):
            created += 1
            continue
        if resp.status_code == 409:
            continue
        failures.append(f"{service}: {resp.status_code} {resp.text}")

    return created, failures


# -----------------------------
# Service Catalog (Service Definitions)
# -----------------------------
def upsert_service_definition(
    base_url: str,
    headers_json: Dict[str, str],
    service: str,
    team: str,
    verbose: bool = False,
) -> Tuple[bool, str]:
    url = f"{base_url}/api/v2/services/definitions"
    payload = {
        "schema-version": "v2.2",
        "dd-service": service,
        "team": team,
    }

    resp = requests.post(url, headers=headers_json, json=payload, timeout=30)

    if resp.status_code in (200, 201):
        return True, "created_or_updated"
    return False, f"{resp.status_code} {resp.text}"


# -----------------------------
# CLI / main
# -----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync Datadog Events services to Service Catalog using a Reference Table mapping.")
    p.add_argument("--table", default=None, help="Reference table name (defaults to REF_TABLE_NAME env)")
    p.add_argument("--service-col", default=None, help="Reference table service column (defaults to REF_TABLE_COL_1)")
    p.add_argument("--team-col", default=None, help="Reference table team column (defaults to REF_TABLE_COL_2)")
    p.add_argument("--days", type=int, default=7, help="Days back to query Events")
    p.add_argument("--query", default="*", help="Datadog Events query string")
    p.add_argument("--page-limit", type=int, default=100, help="Events page size (max 100)")
    p.add_argument("--max-pages", type=int, default=None, help="Stop after this many pages")
    p.add_argument("--verbose", action="store_true", help="Verbose logging")
    p.add_argument("--list-tables", action="store_true", help="List reference tables then exit")
    p.add_argument("--dry-run", action="store_true", help="Do not write reference table rows or service definitions")
    return p.parse_args()


def main() -> None:
    load_dotenv()

    api_key = os.getenv("DD_API_KEY")
    app_key = os.getenv("DD_APP_KEY")
    site = os.getenv("DD_SITE", "datadoghq.com")

    table_name = os.getenv("REF_TABLE_NAME", "reference_table")
    table_id_override = os.getenv("REF_TABLE_ID")

    service_col = os.getenv("REF_TABLE_COL_1", "service")
    team_col = os.getenv("REF_TABLE_COL_2", "team")

    if not api_key or not app_key:
        raise SystemExit("DD_API_KEY and DD_APP_KEY must be set in your environment/.env")

    base_url = build_api_base(site)
    headers_json = auth_headers_json(api_key, app_key)
    headers_jsonapi = auth_headers_jsonapi(api_key, app_key)

    args = parse_args()
    table_name = args.table or table_name
    service_col = args.service_col or service_col
    team_col = args.team_col or team_col

    if args.list_tables:
        tables = list_reference_tables(base_url, headers_jsonapi)
        if not tables:
            print("No reference tables found.")
            return
        print("Reference tables:")
        for t in tables:
            print(f"- {t['id']}  name={t['name'] or '(no name)'}")
        return

    end = dt.datetime.now(dt.UTC)
    start = end - dt.timedelta(days=args.days)

    services = list_services_from_events(
        base_url=base_url,
        headers_json=headers_json,
        start=start,
        end=end,
        query=args.query,
        limit=args.page_limit,
        max_pages=args.max_pages,
        verbose=args.verbose,
    )

    if not services:
        print("No services found in events for the given query/time window.")
        return

    table_id = table_id_override or get_reference_table_id(
        base_url=base_url,
        headers_jsonapi=headers_jsonapi,
        table_name=table_name,
        verbose=args.verbose,
    )

    rows_url = get_reference_table_rows_endpoint(base_url, table_id)

    mapping = get_reference_table_rows_by_id(
        rows_url=rows_url,
        headers_jsonapi=headers_jsonapi,
        row_ids=services,
        service_col=service_col,
        team_col=team_col,
        verbose=args.verbose,
    )

    # Create missing mappings (dummy teams)
    missing_services = [s for s in services if s not in mapping]
    dummy_rows = assign_dummy_teams(missing_services)

    if dummy_rows:
        if args.dry_run:
            if args.verbose:
                print(f"DRY RUN: would create {len(dummy_rows)} reference table rows")
            mapping.update(dummy_rows)
        else:
            created, failures = create_reference_table_rows(
                rows_url=rows_url,
                headers_jsonapi=headers_jsonapi,
                service_col=service_col,
                team_col=team_col,
                rows=dummy_rows,
            )
            if args.verbose:
                print(f"Reference table rows created: {created}")
            if failures:
                print("Reference table insert failures:")
                for f in failures:
                    print(f"- {f}")
            mapping.update(dummy_rows)

    updated = 0
    skipped = 0
    failures = 0
    missing_team: List[str] = []

    for service in sorted(services):
        team = (mapping.get(service) or "").strip()
        if not team:
            skipped += 1
            missing_team.append(service)
            continue

        if args.dry_run:
            updated += 1
            continue

        ok, msg = upsert_service_definition(base_url, headers_json, service, team, verbose=args.verbose)
        if ok:
            updated += 1
        else:
            failures += 1
            print(f"FAILED {service}: {msg}")

    print(f"Services found: {len(services)}")
    print(f"Updated: {updated}")
    print(f"Skipped (no mapping): {skipped}")
    print(f"Failures: {failures}")

    if missing_team:
        print("Missing mappings (no team):")
        for s in missing_team:
            print(f"- {s}")


if __name__ == "__main__":
    main()
