import pandas as pd
import requests
from datetime import datetime

def get_open_holidays(start_date, end_date, country_code, subdivision=None, holiday_type="PublicHolidays"):
    """Fetch data from OpenHolidays API."""
    base_url = f"https://openholidaysapi.org/{holiday_type}"
    params = {
        "countryIsoCode": country_code,
        "validFrom": start_date,
        "validTo": end_date,
        "languageIsoCode": "EN"
    }
    if subdivision:
        params["subdivisionCode"] = subdivision
        
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    return []

def build_calendar_df(
        start_date: datetime, end_date: datetime
):
    # Create the base range of dates
    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    df = pd.DataFrame({"Date": dates})

    # Convert dates to strings
    start_date = start_date.strftime("%Y-%m-%d")  # → "2023-01-01"
    end_date = end_date.strftime("%Y-%m-%d")  # → "2023-01-01"

    # Fetch Data from API
    # Public Holidays
    by_pub = get_open_holidays(start_date, end_date, "DE", "DE-BY", "PublicHolidays")
    cz_pub = get_open_holidays(start_date, end_date, "CZ", None, "PublicHolidays")
    
    # School Vacations
    by_sch = get_open_holidays(start_date, end_date, "DE", "DE-BY", "SchoolHolidays")
    cz_sch = get_open_holidays(start_date, end_date, "CZ", None, "SchoolHolidays")

    # 3. Helper to extract sets of dates from API response
    def extract_dates(api_data):
        date_set = set()
        for item in api_data:
            # API provides 'startDate' and 'endDate'
            start = pd.to_datetime(item['startDate'])
            end = pd.to_datetime(item['endDate'])
            # Add every day in the range to our set
            for d in pd.date_range(start, end):
                date_set.add(d.date())
        return date_set
    
    # 4. Map to Columns
    by_pub_dates = extract_dates(by_pub)
    cz_pub_dates = extract_dates(cz_pub)
    by_sch_dates = extract_dates(by_sch)
    cz_sch_dates = extract_dates(cz_sch)

    # Assign boolean values 0 and 1 to rows
    df['Feiertag_Bayern'] = df['Date'].dt.date.isin(by_pub_dates).astype(int)
    df['Feiertag_CZ'] = df['Date'].dt.date.isin(cz_pub_dates).astype(int)
    df['Schulferien_Bayern'] = df['Date'].dt.date.isin(by_sch_dates).astype(int)
    df['Schulferien_CZ'] = df['Date'].dt.date.isin(cz_sch_dates).astype(int)

    return df

# Test function
fetched_vacation_df = build_calendar_df(
    start_date = datetime(2023, 1, 1),
    end_date = datetime(2025, 12, 31)
)

# Example: Checking early January
print(fetched_vacation_df)