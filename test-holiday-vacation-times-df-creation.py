import pandas as pd
import requests
from datetime import datetime

def get_open_holidays(year, country_code, subdivision=None, holiday_type="PublicHolidays"):
    """Fetch data from OpenHolidays API."""
    base_url = f"https://openholidaysapi.org/{holiday_type}"
    params = {
        "countryIsoCode": country_code,
        "validFrom": f"{year}-01-01",
        "validTo": f"{year}-12-31",
        "languageIsoCode": "EN"
    }
    if subdivision:
        params["subdivisionCode"] = subdivision
        
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    return []

def build_calendar_df(year):
    # 1. Create the base range of dates
    dates = pd.date_range(start=f"{year}-01-01", end=f"{year}-12-31", freq="D")
    df = pd.DataFrame({"Date": dates})

    # 2. Fetch Data from API
    # Public Holidays
    by_pub = get_open_holidays(year, "DE", "DE-BY", "PublicHolidays")
    cz_pub = get_open_holidays(year, "CZ", None, "PublicHolidays")
    
    # School Vacations
    by_sch = get_open_holidays(year, "DE", "DE-BY", "SchoolHolidays")
    cz_sch = get_open_holidays(year, "CZ", None, "SchoolHolidays")

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

    df['Is holiday in Bavaria?'] = df['Date'].dt.date.isin(by_pub_dates)
    df['Is holiday in Czech Republic?'] = df['Date'].dt.date.isin(cz_pub_dates)
    df['Is school vacation in Bavaria?'] = df['Date'].dt.date.isin(by_sch_dates)
    df['Is school vacation in Czech Republic?'] = df['Date'].dt.date.isin(cz_sch_dates)

    return df

# Generate for 2025
df_2026 = build_calendar_df(2026)

# Example: Checking early January
print(df_2026.head(50))