import os
import pytz
import time
import pandas as pd
import requests
from datetime import datetime, timedelta
from src.config import visitor_houses_with_realtime_tracking

# Define constants
VEMCOUNT_API_BASE_URL = "https://vemcount.app/api/v3"
VEMCOUNT_API_KEY = os.environ["VEMCOUNT_API_KEY"]

def get_token(api_key: str) -> str:
    """Exchange the API key for a short-lived bearer token (valid ~6h)."""
    resp = requests.post(
        f"{VEMCOUNT_API_BASE_URL}/auth/login",
        json={"api_key": api_key},
        headers={"Accept": "application/json"},
    )
    resp.raise_for_status()
    bearer_token = resp.json()["access_token"]
    
    return bearer_token

def get_current_berlin_date_and_hour_range() -> dict:
    """
    Get the current date and the current+next hour, in Europe/Berlin local
    time (automatically handling CET/CEST daylight saving), formatted for
    this API's expected schema:
      - date_from / date_to: "YYYY-MM-DD" (form_date_from / form_date_to)
      - hour_from / hour_to: "HH:00"      (show_hours_from / show_hours_to)

    Example: if it is currently 09:59 on 2025-01-03 in Berlin, returns:
        {
            "date_from": "2025-01-03",
            "date_to": "2025-01-03",
            "hour_from": "09:00",
            "hour_to": "10:00",
        }
    """
    now_berlin = datetime.now(pytz.timezone('Europe/Berlin'))
    hour_from = now_berlin.replace(minute=0, second=0, microsecond=0)
    hour_to = hour_from + timedelta(hours=1)

    return {
        "date_from": hour_from.strftime("%Y-%m-%d"),
        "date_to": hour_to.strftime("%Y-%m-%d"),
        "hour_from": hour_from.strftime("%H:00"),
        "hour_to": hour_to.strftime("%H:00"),
    }


def get_vemcount_counts(token: str, location_ids: list[int], start_date: str, end_date: str, start_hour: str, end_hour: str, period_step: str = "hour") -> dict:
    """Fetch count_in/count_out/inside for the given locations, bucketed by period_step, for
    today (in each location's own timezone). period_step also accepts e.g. "15min" for finer
    detail."""

    resp = requests.post(
        f"{VEMCOUNT_API_BASE_URL}/report",
        json={
            "source": "locations",
            "data": location_ids,
            "data_output": ["count_in", "count_out", "inside"],
            "period": "date",
            "period_step": period_step,
            "form_date_from": start_date,
            "form_date_to": end_date,
            "show_hours_from": start_hour,
            "show_hours_to": end_hour,
        },
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    resp.raise_for_status()
    return resp.json()

def get_vemcount_counts_with_retry(
    *args,
    max_retries: int = 5,
    backoff_seconds: float = 5.0,
    **kwargs,
) -> dict:
    """
    Wraps get_vemcount_counts with retry logic for transient server errors
    (500/502/503/504) and rate limiting (429). For 429s, honors the
    Retry-After response header if the API provides one; otherwise falls
    back to exponential backoff.
    """
    for attempt in range(1, max_retries + 1):
        try:
            return get_vemcount_counts(*args, **kwargs)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None

            if status == 429:
                retry_after = e.response.headers.get("Retry-After")
                wait_seconds = float(retry_after) if retry_after else backoff_seconds * attempt
                print(f"Rate limited (429). Waiting {wait_seconds}s before retry {attempt}/{max_retries}...")
                time.sleep(wait_seconds)
                continue

            if status in (500, 502, 503, 504) and attempt < max_retries:
                print(f"Server error ({status}). Waiting {backoff_seconds}s before retry {attempt}/{max_retries}...")
                time.sleep(backoff_seconds)
                continue

            raise

    raise RuntimeError(f"Exceeded max retries ({max_retries}) for get_vemcount_counts")

def to_dataframe(report_json: dict, names_by_id: dict[str, str]) -> pd.DataFrame:
    """Flatten the nested {period -> location -> dates -> data} response into a tidy DataFrame
    with one row per location per time bucket."""
    rows = []
    period_block = next(iter(report_json["data"].values()))  # the single "today" block
    for location_id, location_block in period_block.items():
        for bucket in location_block["dates"].values():
            d = bucket["data"]
            rows.append(
                {
                    "location_id": int(location_id),
                    "location_name": names_by_id[location_id],
                    "datetime": d["dt"],
                    "count_in": int(d["count_in"]),
                    "count_out": int(d["count_out"]),
                    "inside": int(d["inside"]),  # From the API calculated number of people still inside a respective house (in case the house has no reset set up, this number can be even lower than 0)
                }
            )
    df = pd.DataFrame(rows)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values(["location_id", "datetime"]).reset_index(drop=True)

    return df

def get_vemcount_counts_chunked(
    token: str,
    location_ids: list[int],
    start_date: str,
    end_date: str,
    start_hour: str,
    end_hour: str,
    period_step: str = "hour",
    chunk_days: int = 30,
    delay_between_requests: float = 1.0,
) -> pd.DataFrame:
    """
    Same as get_vemcount_counts, but splits a long date range into smaller
    windows and concatenates the results. Needed because the API returns
    500/502 errors when asked for too much data (many locations x long
    range x fine period_step) in a single request.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    all_dataframes = []
    chunk_start = start

    while chunk_start <= end:
        chunk_end = min(chunk_start + timedelta(days=chunk_days - 1), end)

        chunk_start_str = chunk_start.strftime("%Y-%m-%d")
        chunk_end_str = chunk_end.strftime("%Y-%m-%d")

        print(f"Fetching {chunk_start_str} to {chunk_end_str}...")

        report_json = get_vemcount_counts_with_retry(
            token=token,
            location_ids=location_ids,
            start_date=chunk_start_str,
            end_date=chunk_end_str,
            start_hour=start_hour,
            end_hour=end_hour,
            period_step=period_step,
        )
        chunk_df = to_dataframe(report_json, visitor_houses_with_realtime_tracking)
        all_dataframes.append(chunk_df)

        chunk_start = chunk_end + timedelta(days=1)

        if chunk_start <= end:
            time.sleep(delay_between_requests)  # proactive pacing, not just reactive retry

    return pd.concat(all_dataframes, ignore_index=True)


if __name__ == "__main__":
    token = get_token(VEMCOUNT_API_KEY)

    location_ids = list(visitor_houses_with_realtime_tracking.keys())

    # Get realtime counts for today
    date_dict_now = get_current_berlin_date_and_hour_range()

    report_json = get_vemcount_counts(
        token=token,
        location_ids=location_ids,
        start_date=date_dict_now["date_from"],
        end_date=date_dict_now["date_to"],
        start_hour=date_dict_now["hour_from"],
        end_hour=date_dict_now["hour_to"],
    )

    df = to_dataframe(report_json, visitor_houses_with_realtime_tracking)

    print(df)

    # Get historic counts
    print("This is now historic counts for for the maximum lifetime of the Data Hub for all realtime tracking locations.")
    df = get_vemcount_counts_chunked(
        token=token,
        location_ids=location_ids,
        start_date="2025-12-02",
        end_date=date_dict_now["date_to"],
        start_hour="00:00",
        end_hour="23:00",
        period_step="hour",
        chunk_days=120,
    )

    print(df)

    df.to_csv("historic_visitor_counts.csv", index=False)