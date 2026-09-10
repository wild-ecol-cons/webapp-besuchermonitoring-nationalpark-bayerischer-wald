# Description: Pulls today's hourly visitor in/out counts for every location registered under
# the Vemcount API key, and assembles them into a single pandas DataFrame. Vemcount is the
# people-counting sensor platform used at some park visitor centers (currently Bavarian Forest
# sites).
#
# Requires the VEMCOUNT_API_KEY environment variable to be set (get one from the Vemcount app
# under user settings > API key). Never hardcode the key here.

import os

import pandas as pd
import requests
from datetime import datetime, timedelta
import pytz

BASE_URL = "https://vemcount.app/api/v3"

API_KEY = os.environ["VEMCOUNT_API_KEY"]  # raises KeyError with a clear message if unset

# Dictionary of house names and their respective location IDs in Vemcount
visitor_houses_with_realtime_tracking = {
    "33955": "Hans-Eisenmann-Haus",
    "33320": "Nationalparkverwaltung Bayerischer Wald - Haus zur Wildnis",
    "33951": "Waldgeschichtliches Museum St. Oswald"
}

def get_token(api_key: str) -> str:
    """Exchange the API key for a short-lived bearer token (valid ~6h)."""
    resp = requests.post(
        f"{BASE_URL}/auth/login",
        json={"api_key": api_key},
        headers={"Accept": "application/json"},
    )
    resp.raise_for_status()
    bearer_token = resp.json()["access_token"]

    print("Got bearer token:", bearer_token)
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
        f"{BASE_URL}/report",
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


if __name__ == "__main__":
    token = get_token(API_KEY)

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

    # keep only buckets from 09:00 onward, in case the API ever returns earlier ones
    df = df[df["datetime"].dt.time >= pd.Timestamp("09:00").time()]

    print(df)

    # Get historic counts
    print("This is now historic counts for one house with ID 33955")
    report_json = get_vemcount_counts(
        token=token,
        location_ids=location_ids,
        start_date="2023-07-01",
        end_date="2026-09-10",
        start_hour="00:00",
        end_hour="23:00",
    )

    df = to_dataframe(report_json, visitor_houses_with_realtime_tracking)

    print(df)

    df.to_csv("historic_visitor_counts.csv", sep=",", encoding="utf-8", index=False)