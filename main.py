import os
import json
import time
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import functions_framework
import requests
from google.cloud import bigquery
from google.cloud import secretmanager

# ---------- Config ----------
PROJECT_ID = os.getenv("GCP_PROJECT", "")
DATASET = os.getenv("TD_DATASET", "timedoctor")
TABLE = os.getenv("TD_TABLE", "hours_worked")
COMPANY_ID = os.getenv("TD_COMPANY_ID")

# OAuth / API
TD_CLIENT_ID = os.getenv("TD_CLIENT_ID", "")
TD_CLIENT_SECRET = os.getenv("TD_CLIENT_SECRET", "")
TD_REDIRECT_URI = os.getenv("TD_REDIRECT_URI", "")  # must EXACTLY match the registered redirect, incl. trailing slash
TD_REFRESH_SECRET_RES = os.getenv("TD_REFRESH_SECRET_RES", "")  # e.g. projects/12345/secrets/TD_REFRESH_TOKEN
API_BASE = "https://webapi.timedoctor.com/v1.1"
OAUTH_TOKEN_URL = "https://webapi.timedoctor.com/oauth/v2/token"

MAX_RANGE_DAYS = int(os.getenv("TD_MAX_RANGE_DAYS", "60"))
HTTP_TIMEOUT = int(os.getenv("TD_HTTP_TIMEOUT", "60"))
HTTP_RETRIES = int(os.getenv("TD_HTTP_RETRIES", "4"))

# ---------- Helpers ----------
def _yesterday_iso() -> str:
    return (dt.date.today() - dt.timedelta(days=1)).isoformat()

def _parse_dates(request) -> Tuple[str, str, str]:
    """
    Resolve date range from query params or JSON body.
    Defaults to yesterday→yesterday.
    Returns (start_date, end_date, mode) where mode is 'custom' or 'daily-default'.
    """
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

def _to_date(schedule: Dict[str, Any]) -> Optional[dt.date]:
    # Prefer numeric epoch if present
    ts = schedule.get("shiftStartsTs")
    if ts not in (None, ""):
        try:
            return dt.datetime.utcfromtimestamp(int(ts)).date()
        except Exception:
            pass
    # Fall back to human string like "Aug 25, 2025"
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
        for sch in (person.get("schedules") or []):
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

def _bq_table(project_id: str, dataset: str, table: str) -> str:
    return f"{project_id}.{dataset}.{table}"

def _stage_name(table_id: str) -> str:
    return f"{table_id}_stg_{int(time.time())}"

def _upsert(client: bigquery.Client, table_id: str, rows: List[Dict[str, Any]]):
    if not rows:
        return 0
    staging_id = _stage_name(table_id)
    load_job = client.load_table_from_json(
        rows,
        staging_id,
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

# ---------- Secret Manager ----------
_sm_client: Optional[secretmanager.SecretManagerServiceClient] = None

def _sm() -> secretmanager.SecretManagerServiceClient:
    global _sm_client
    if _sm_client is None:
        _sm_client = secretmanager.SecretManagerServiceClient()
    return _sm_client

def _read_secret_value(secret_res: str) -> str:
    # Reads the *latest* version by default
    name = f"{secret_res}/versions/latest" if "/versions/" not in secret_res else secret_res
    resp = _sm().access_secret_version(request={"name": name})
    return resp.payload.data.decode("utf-8")

def _add_secret_version(secret_res: str, value: str) -> None:
    _sm().add_secret_version(
        request={
            "parent": secret_res,
            "payload": {"data": value.encode("utf-8")}
        }
    )

# ---------- OAuth: refresh on every run & rotate stored refresh token if server rotates ----------
def _refresh_access_token() -> Tuple[str, Optional[str]]:
    """
    Returns (access_token, new_refresh_token_if_any).
    Raises on failure.
    """
    if not TD_CLIENT_ID or not TD_CLIENT_SECRET or not TD_REDIRECT_URI or not TD_REFRESH_SECRET_RES:
        raise RuntimeError("Missing TD_CLIENT_ID / TD_CLIENT_SECRET / TD_REDIRECT_URI / TD_REFRESH_SECRET_RES env vars.")

    current_refresh = _read_secret_value(TD_REFRESH_SECRET_RES).strip()

    # Token refresh
    params = {
        "client_id": TD_CLIENT_ID,
        "client_secret": TD_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": current_refresh,
    }
    r = requests.get(OAUTH_TOKEN_URL, params=params, timeout=HTTP_TIMEOUT)
    if r.status_code == 429:
        # Basic retry for rate limit on the token endpoint
        for i in range(1, HTTP_RETRIES + 1):
            time.sleep(min(2 ** i, 10))
            r = requests.get(OAUTH_TOKEN_URL, params=params, timeout=HTTP_TIMEOUT)
            if r.status_code != 429:
                break
    r.raise_for_status()
    data = r.json()
    access_token = data.get("access_token")
    new_refresh = data.get("refresh_token")

    if not access_token:
        raise RuntimeError(f"Token refresh failed: {data}")

    # If TD rotates the refresh token, persist a new Secret version
    if new_refresh and new_refresh != current_refresh:
        _add_secret_version(TD_REFRESH_SECRET_RES, new_refresh)

    return access_token, new_refresh

# ---------- HTTP with retries ----------
def _get_with_retries(url: str, headers: Dict[str, str], params: Dict[str, str]) -> requests.Response:
    backoff = 1.0
    for attempt in range(HTTP_RETRIES + 1):
        resp = requests.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
        if resp.status_code not in (429, 500, 502, 503, 504):
            return resp
        if attempt == HTTP_RETRIES:
            return resp
        time.sleep(backoff)
        backoff = min(backoff * 2.0, 10.0)
    return resp  # pragma: no cover

def _call_td(company_id: str, access_token: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    url = f"{API_BASE}/companies/{company_id}/absent-and-late"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"_format": "json", "start_date": start_date, "end_date": end_date}
    r = _get_with_retries(url, headers=headers, params=params)
    r.raise_for_status()
    return r.json()

# ---------- Entry ----------
@functions_framework.http
def main(request):
    # Resolve project and company
    project = PROJECT_ID or bigquery.Client().project
    company_id = (COMPANY_ID or request.args.get("company_id") or "").strip()
    if not company_id:
        return ("Missing TD_COMPANY_ID env var or company_id param.", 400)

    # Resolve date range
    try:
        start_date, end_date, mode = _parse_dates(request)
    except ValueError as e:
        return (str(e), 400)

    # Refresh token (and rotate stored refresh token if needed)
    try:
        access_token, _ = _refresh_access_token()
    except Exception as e:
        return (f"OAuth token refresh failed: {e}", 502)

    # Pull data
    try:
        payload = _call_td(str(company_id), access_token, start_date, end_date)
    except requests.HTTPError as e:
        return (f"Time Doctor API error: {e.response.status_code} {e.response.text}", 502)
    except Exception as e:
        return (f"Time Doctor request failed: {e}", 502)

    rows = _flatten(str(company_id), payload)

    # Upsert to BigQuery
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
