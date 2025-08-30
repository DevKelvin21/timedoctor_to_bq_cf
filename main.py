import os
import json
import time
import datetime as dt
from typing import Any, Dict, List, Optional

import functions_framework
import requests
from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT", "")
DATASET = os.getenv("TD_DATASET", "timedoctor")
TABLE = os.getenv("TD_TABLE", "hours_worked")
COMPANY_ID = os.getenv("TD_COMPANY_ID")
TD_ACCESS_TOKEN = os.getenv("TD_ACCESS_TOKEN")
API_BASE = "https://webapi.timedoctor.com/v1.1"
MAX_RANGE_DAYS = int(os.getenv("TD_MAX_RANGE_DAYS", "60"))

def _yesterday_iso() -> str:
    return (dt.date.today() - dt.timedelta(days=1)).isoformat()

def _parse_dates(request) -> (str, str, str):
    """
    Resolves date range from query params or JSON body.
    Defaults to yesterday→yesterday.
    Returns (start_date, end_date, mode) where mode is 'custom' or 'daily-default'.
    """
    start = request.args.get("start_date")
    end = request.args.get("end_date")

    if not (start and end):
        # allow JSON body too
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

    # validation
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
    rows = []
    for person in payload or []:
        fullname = person.get("fullname") or person.get("fullName")
        for sch in person.get("schedules") or []:
            d = _to_date(sch)
            if not d:
                continue
            seconds = sch.get("actualSecondsWorked")
            hours_f = sch.get("actualHoursWorkedInt")
            hours_s = sch.get("actualHoursWorked")
            try:
                seconds = int(seconds) if seconds is not None else None
            except Exception:
                seconds = None
            try:
                hours_f = float(hours_f) if hours_f is not None else (seconds / 3600.0 if seconds is not None else None)
            except Exception:
                hours_f = (seconds / 3600.0) if seconds is not None else None

            rows.append({
                "company_id": str(company_id),
                "employee_fullname": fullname,
                "shift_date": d.isoformat(),
                "actual_hours_worked_seconds": seconds,
                "actual_hours_worked_hours": hours_f,
                "actual_hours_worked_str": hours_s,
                "status": sch.get("status"),
                "source_actual_start": sch.get("actualStart"),
                "source_actual_end": sch.get("actualEndTime"),
                "shift_starts_ts": sch.get("shiftStartsTs"),
            })
    return rows

def _call_td(company_id: str, token: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    url = f"{API_BASE}/companies/{company_id}/absent-and-late"
    params = {"access_token": token, "_format": "json", "start_date": start_date, "end_date": end_date}
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def _bq_table(project_id: str, dataset: str, table: str) -> str:
    return f"{project_id}.{dataset}.{table}"

def _stage_name(table_id: str) -> str:
    return f"{table_id}_stg_{int(time.time())}"

def _upsert(client: bigquery.Client, table_id: str, rows: List[Dict[str, Any]]):
    if not rows:
        return 0
    staging_id = _stage_name(table_id)
    load_job = client.load_table_from_json(
        rows, staging_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True),
    )
    load_job.result()
    merge_sql = f"""
    MERGE `{table_id}` T
    USING `{staging_id}` S
    ON T.company_id = S.company_id
       AND T.employee_fullname = S.employee_fullname
       AND T.shift_date = S.shift_date
    WHEN MATCHED THEN UPDATE SET
      run_timestamp = CURRENT_TIMESTAMP(),
      actual_hours_worked_seconds = S.actual_hours_worked_seconds,
      actual_hours_worked_hours = S.actual_hours_worked_hours,
      actual_hours_worked_str = S.actual_hours_worked_str,
      status = S.status,
      source_actual_start = S.source_actual_start,
      source_actual_end = S.source_actual_end,
      shift_starts_ts = S.shift_starts_ts
    WHEN NOT MATCHED THEN INSERT (
      run_timestamp, company_id, employee_fullname, shift_date,
      actual_hours_worked_seconds, actual_hours_worked_hours, actual_hours_worked_str,
      status, source_actual_start, source_actual_end, shift_starts_ts
    ) VALUES (
      CURRENT_TIMESTAMP(), S.company_id, S.employee_fullname, S.shift_date,
      S.actual_hours_worked_seconds, S.actual_hours_worked_hours, S.actual_hours_worked_str,
      S.status, S.source_actual_start, S.source_actual_end, S.shift_starts_ts
    );
    """
    client.query(merge_sql).result()
    client.delete_table(staging_id, not_found_ok=True)
    return len(rows)

@functions_framework.http
def main(request):
    # Config checks
    project = PROJECT_ID or bigquery.Client().project
    if not TD_ACCESS_TOKEN:
        return ("Missing TD_ACCESS_TOKEN (secret).", 500)
    company_id = COMPANY_ID or request.args.get("company_id")
    if not company_id:
        return ("Missing TD_COMPANY_ID env var or company_id param.", 400)

    # Resolve date range
    try:
        start_date, end_date, mode = _parse_dates(request)
    except ValueError as e:
        return (str(e), 400)

    # Pull data
    try:
        payload = _call_td(str(company_id), TD_ACCESS_TOKEN, start_date, end_date)
    except requests.HTTPError as e:
        return (f"Time Doctor API error: {e.response.status_code} {e.response.text}", 502)
    except Exception as e:
        return (f"Time Doctor request failed: {e}", 502)

    rows = _flatten(str(company_id), payload)

    # Upsert
    try:
        client = bigquery.Client(project=project)
        table_id = _bq_table(project, DATASET, TABLE)
        count = _upsert(client, table_id, rows)
    except Exception as e:
        return (f"BigQuery load/merge failed: {e}", 500)

    return {
        "mode": mode,  # 'custom' or 'daily-default'
        "project": project,
        "dataset": DATASET,
        "table": TABLE,
        "company_id": str(company_id),
        "start_date": start_date,
        "end_date": end_date,
        "rows_upserted": count,
    }
