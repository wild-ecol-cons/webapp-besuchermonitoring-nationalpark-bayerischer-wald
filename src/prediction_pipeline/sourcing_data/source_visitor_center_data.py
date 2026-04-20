import pandas as pd
from src.utils import read_dataframe_from_azure, query_azure_with_duck_db
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
    df = pd.DataFrame({"Datum": dates})

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
    df['Feiertag_Bayern'] = df['Datum'].dt.date.isin(by_pub_dates).astype(int)
    df['Feiertag_CZ'] = df['Datum'].dt.date.isin(cz_pub_dates).astype(int)
    df['Schulferien_Bayern'] = df['Datum'].dt.date.isin(by_sch_dates).astype(int)
    df['Schulferien_CZ'] = df['Datum'].dt.date.isin(cz_sch_dates).astype(int)

    return df

def source_temporal_features(
    start_date: datetime,
    end_date: datetime
):
    
    # Fetch information from OpenHolidays API to build vacation calendar
    fetched_vacation_df = build_calendar_df(
        start_date=start_date,
        end_date=end_date
    )

    return fetched_vacation_df

def source_preprocessed_hourly_visitor_center_data():

    """
    Load the preprocessed hourly visitor center data from the cloud.
    """

    print("Sourcing the historic preprocessed_hourly_visitor_center_data")

    # Load visitor count data from the cloud
    preprocessed_hourly_visitor_center_data = read_dataframe_from_azure(
        file_name="visitor_centers_hourly_2017_to_2026.parquet",
        file_format="parquet",
        source_folder="preprocessed_data",
    )

    print(f"The historic preprocessed_hourly_visitor_center_data is: {preprocessed_hourly_visitor_center_data}")

    return preprocessed_hourly_visitor_center_data