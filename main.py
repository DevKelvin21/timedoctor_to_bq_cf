import os
import json
import time
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import functions_framework
import requests
from google.cloud import bigquery

# ---- ENV / SECRETS ----
PROJECT_ID        = os.getenv("GCP_PROJECT", "")
DATASET           = os.getenv("TD_DATASET", "timedoctor")
TABLE             = os.getenv("TD_TABLE", "hours_worked")
COMPANY_ID        = os.getenv("TD_COMPANY_ID")
TD_CLIENT_ID      = os.getenv("TD_CLIENT_ID")        # secret
TD_CLIENT_SECRET  = os.getenv("TD_CLIENT_SECRET")    # secret
TD_REFRESH_TOKEN  = os.getenv("TD_REFRESH_TOKEN")    # secret
API_BASE          = "https://webapi.timedoctor.com/v1.1"
OAUTH_BASE        = "https://webapi.timedoctor.com/oauth/v2"
MAX_RANGE_DAYS    = int(os.getenv("TD_MAX_RANGE_DAYS", "60"))

# ---- FINAL & STAGING SCHEMAS (same as before) ----
from google.cloud import bigquery
FINAL_SCHEMA = [
    bigquery.SchemaField("run_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("company_id", "STRING"),
    bigquery.SchemaField("employee_fullname", "STRING"),
    bigquery.SchemaField("shift_date", "DATE"),
    bigquery.SchemaField("actual_hours_worked_seconds", "INT64"),
    bigquery.SchemaField("actual_hours_worked_hours", "FLOAT64"),
    bigquery.SchemaField("actual_hours_worked_str", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("source_actual_start", "STRING"),
    bigquery.SchemaField("source_actual_end", "STRING"),
    bigquery.SchemaField("shift_starts_ts", "INT64"),
]
STAGING_SCHEMA = [
    bigquery.SchemaField("company_id", "STRING"),
    bigquery.SchemaField("employee_fullname", "STRING"),
    bigquery.SchemaField("shift_date", "DATE"),
    bigquery.SchemaField("actual_hours_worked_seconds", "INT64"),
    bigquery.SchemaField("actual_hours_worked_hours", "FLOAT64"),
    bigquery.SchemaField("actual_hours_worked_str", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("source_actual_start", "STRING"),
    bigquery.SchemaField("source_actual_end", "STRING"),
    bigquery.SchemaField("shift_starts_ts", "INT64"),
]

# ---- DATE HELPERS ----
def _yesterday_iso() -> str:
    return (dt.date.today() - dt.timedelta(days=1)).isoformat()

def _parse_dates(request) -> Tuple[str, str, str]:
    start = request.args.get("start_date")
    end = request.args.get("end_date")
    if not (start and end):
        try:
            body = request.get_json(silent=True) or {}
        except Exception:
            body = {}
        start = start or body.get("start_date")
        end = end or body.get("end_date")
    if start and end:
        mode = "custom"
    else:
        y = _yesterday_iso()
        start, end, mode = y, y, "daily-default"
    try:
        ds = dt.date.fromisoformat(start)
        de = dt.date.fromisoformat(end)
    except Exception:
        raise ValueError("start_date and end_date must be YYYY-MM-DD")
    if ds > de:
        raise ValueError("start_date cannot be after end_date")
    if (de - ds).days + 1 > MAX_RANGE_DAYS:
        raise ValueError(f"Date range too large (> {MAX_RANGE_DAYS} days).")
    return start, end, mode

# ---- TRANSFORM HELPERS ----
def _to_date(schedule: Dict[str, Any]) -> Optional[dt.date]:
    ts = schedule.get("shiftStartsTs")
    if ts not in (None, ""):
        try:
            return dt.datetime.utcfromtimestamp(int(ts)).date()
        except Exception:
            pass
    human = schedule.get("shiftStartDate")
    if human:
        for fmt in ("%b %d, %Y",):
            try:
                return dt.datetime.strptime(human, fmt).date()
            except Exception:
                continue
    return None

def _flatten(company_id: str, payload: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for person in payload or []:
        fullname = person.get("fullname") or person.get("fullName")
        for sch in person.get("schedules") or []:
            d = _to_date(sch)
            if not d:
                continue
            seconds = sch.get("actualSecondsWorked")
            try:
                seconds = int(seconds) if seconds not in (None, "") else None
            except Exception:
                seconds = None
            hours_f = sch.get("actualHoursWorkedInt")
            try:
                hours_f = float(hours_f) if hours_f not in (None, "") else None
            except Exception:
                hours_f = None
            if hours_f is None and seconds is not None:
                hours_f = seconds / 3600.0
            shift_ts = sch.get("shiftStartsTs")
            try:
                shift_ts = int(shift_ts) if shift_ts not in (None, "") else None
            except Exception:
                shift_ts = None
            rows.append({
                "company_id": str(company_id),
                "employee_fullname": fullname if fullname else None,
                "shift_date": d.isoformat(),
                "actual_hours_worked_seconds": seconds,
                "actual_hours_worked_hours": hours_f,
                "actual_hours_worked_str": sch.get("actualHoursWorked") or None,
                "status": sch.get("status") or None,
                "source_actual_start": sch.get("actualStart") or None,
                "source_actual_end": sch.get("actualEndTime") or None,
                "shift_starts_ts": shift_ts,
            })
    return rows

# ---- OAUTH: refresh to get an access token ----
def _get_access_token(client_id: str, client_secret: str, refresh_token: str) -> str:
    if not (client_id and client_secret and refresh_token):
        raise RuntimeError("Missing TD_CLIENT_ID / TD_CLIENT_SECRET / TD_REFRESH_TOKEN.")
    token_url = f"{OAUTH_BASE}/token"
    params = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    r = requests.get(token_url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    token = data.get("access_token") or data.get("accessToken")
    if not token:
        raise RuntimeError(f"Token response missing access_token: {data}")
    return token

# ---- API CALL ----
def _call_td(company_id: str, access_token: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    url = f"{API_BASE}/companies/{company_id}/absent-and-late"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"_format": "json", "start_date": start_date, "end_date": end_date}
    r = requests.get(url, headers=headers, params=params, timeout=60)
    if r.status_code == 401:
        # For safety, surface the 401 explicitly (CF caller can retry which will refetch token)
        raise requests.HTTPError("Unauthorized", response=r)
    r.raise_for_status()
    return r.json()

# ---- BQ HELPERS ----
def _bq_table(project_id: str, dataset: str, table: str) -> str:
    return f"{project_id}.{dataset}.{table}"

def _stage_name(table_id: str) -> str:
    return f"{table_id}_stg_{int(time.time())}"

def _ensure_final_table(client: bigquery.Client, table_id: str) -> None:
    try:
        client.get_table(table_id)
    except Exception:
        tbl = bigquery.Table(table_id, schema=FINAL_SCHEMA)
        # Optional partitioning:
        # tbl.time_partitioning = bigquery.TimePartitioning(field="shift_date")
        client.create_table(tbl)

def _upsert(client: bigquery.Client, table_id: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    _ensure_final_table(client, table_id)
    staging_id = _stage_name(table_id)
    load_job = client.load_table_from_json(
        rows,
        staging_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=STAGING_SCHEMA),
    )
    load_job.result()
    merge_sql = f"""
    MERGE `{table_id}` T
    USING `{staging_id}` S
    ON  T.company_id = S.company_id
    AND T.employee_fullname = S.employee_fullname
    AND T.shift_date = S.shift_date
    WHEN MATCHED THEN UPDATE SET
      run_timestamp               = CURRENT_TIMESTAMP(),
      actual_hours_worked_seconds = S.actual_hours_worked_seconds,
      actual_hours_worked_hours   = S.actual_hours_worked_hours,
      actual_hours_worked_str     = S.actual_hours_worked_str,
      status                      = S.status,
      source_actual_start         = S.source_actual_start,
      source_actual_end           = S.source_actual_end,
      shift_starts_ts             = S.shift_starts_ts
    WHEN NOT MATCHED THEN INSERT (
      run_timestamp, company_id, employee_fullname, shift_date,
      actual_hours_worked_seconds, actual_hours_worked_hours,
      actual_hours_worked_str, status, source_actual_start,
      source_actual_end, shift_starts_ts
    ) VALUES (
      CURRENT_TIMESTAMP(), S.company_id, S.employee_fullname, S.shift_date,
      S.actual_hours_worked_seconds, S.actual_hours_worked_hours,
      S.actual_hours_worked_str, S.status, S.source_actual_start,
      S.source_actual_end, S.shift_starts_ts
    );
    """
    client.query(merge_sql).result()
    client.delete_table(staging_id, not_found_ok=True)
    return len(rows)

# ---- ENTRYPOINT ----
@functions_framework.http
def main(request):
    project = PROJECT_ID or bigquery.Client().project

    # Required secrets/env
    company_id = COMPANY_ID or request.args.get("company_id")
    if not company_id:
        return ("Missing TD_COMPANY_ID env var or company_id param.", 400)
    if not (TD_CLIENT_ID and TD_CLIENT_SECRET and TD_REFRESH_TOKEN):
        return ("Missing TD_CLIENT_ID / TD_CLIENT_SECRET / TD_REFRESH_TOKEN.", 500)

    # Dates
    try:
        start_date, end_date, mode = _parse_dates(request)
    except ValueError as e:
        return (str(e), 400)

    # OAuth: get an access token using the refresh token
    try:
        access_token = _get_access_token(TD_CLIENT_ID, TD_CLIENT_SECRET, TD_REFRESH_TOKEN)
    except requests.HTTPError as e:
        return (f"Time Doctor token error: {e.response.status_code} {e.response.text}", 502)
    except Exception as e:
        return (f"Time Doctor token error: {e}", 502)

    # API call
    try:
        payload = _call_td(str(company_id), access_token, start_date, end_date)
    except requests.HTTPError as e:
        # Bubble up useful detail
        body = getattr(e.response, "text", "")
        code = getattr(e.response, "status_code", 500)
        return (f"Time Doctor API error: {code} {body}", 502)
    except Exception as e:
        return (f"Time Doctor request failed: {e}", 502)

    rows = _flatten(str(company_id), payload)

    # Load & merge
    try:
        client = bigquery.Client(project=project)
        table_id = _bq_table(project, DATASET, TABLE)
        count = _upsert(client, table_id, rows)
    except Exception as e:
        return (f"BigQuery load/merge failed: {e}", 500)

    return {
        "mode": mode,
        "project": project,
        "dataset": DATASET,
        "table": TABLE,
        "company_id": str(company_id),
        "start_date": start_date,
        "end_date": end_date,
        "rows_upserted": count,
    }
