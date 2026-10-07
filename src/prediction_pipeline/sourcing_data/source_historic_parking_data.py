# Install libraries
import pandas as pd
import requests
import json
import os
from functools import reduce
from datetime import datetime
from src.config import parking_sensors

########################################################################################
# Global variables
########################################################################################

# Load Bayern Cloud API key from environment variables
BAYERN_CLOUD_API_KEY = os.getenv('BAYERN_CLOUD_API_KEY') 

OUTPUT_DIR = './outputs/parking_data_final/'

########################################################################################
# Functions
########################################################################################

def get_historical_data_for_location(
    location_id: str,
    location_slug: str,
    data_type: str,
    api_endpoint_suffix: str,
    column_name: str,
    save_file_path: str = 'outputs'
):
    """
    Fetch historical data from the BayernCloud API and save it as a CSV file.

    Args:
        location_id (str): The ID of the location for which the data is to be fetched.
        location_slug (str): A slug (a URL-friendly string) representing the location.
        data_type (str): The type of data being fetched (e.g., 'occupancy', 'occupancy_rate', 'capacity').
        api_endpoint_suffix (str): The specific suffix of the API endpoint for the data type (e.g., 'dcls_occupancy', 'dcls_occupancy_rate').
        column_name (str): The name of the column to store the fetched data in the DataFrame.
        save_file_path (str, optional): The base directory where the CSV file will be saved (default is 'outputs').

    Returns:
        historical_df (pd.DataFrame): A Pandas DataFrame containing the historical data for a location.
    """
    # Construct the API endpoint URL
    API_endpoint = f'https://data.bayerncloud.digital/api/v4/things/{location_id}/{api_endpoint_suffix}/'

    # Set request parameters
    request_params = {
        'token': BAYERN_CLOUD_API_KEY
    }

    # Send the GET request to the API
    response = requests.get(API_endpoint, params=request_params)
    response_json = response.json()

    # Convert the response to a Pandas DataFrame
    historical_df = pd.DataFrame(response_json['data'], columns=['time', column_name])

    # Preprocess data to match expected 1h-frequency
    # Parse as timezone-aware, convert to local wall-clock time, then strip tz
    historical_df["time"] = (
        pd.to_datetime(historical_df["time"], utc=True, format="mixed")  # parse → UTC-aware
        .dt.tz_convert("Europe/Berlin")                  # convert to CET/CEST
        .dt.tz_localize(None)                            # drop tz → naive local time
    )

    historical_df = historical_df.set_index("time")

    return historical_df


def process_all_locations(
        parking_sensors: dict = parking_sensors,
        specify_timerange: bool = False,
        start_time: datetime = None,
        end_time: datetime = None
        ) -> pd.DataFrame:
    """
    Process and fetch all types of historical data for each location in the parking sensors dictionary.

    Args:
        parking_sensors (dict): Dictionary containing location slugs as keys and location IDs as values.
        specify_timerange (bool, optional): Whether to specify a specific timeframe. Defaults to False.
        start_time (datetime, optional): Start time for the timeframe. Defaults to None.
        end_time (datetime, optional): End time for the timeframe. Defaults to None.

    Returns:
        overall_historic_parking_data (pd.DataFrame): A Pandas DataFrame containing the processed historical data for all locations (either all data or a specific timeframe).
    """

    data_types = [
        ('occupancy', 'dcls_occupancy', 'occupancy'),
        ('occupancy_rate', 'dcls_occupancy_rate', 'occupancy_rate'),
        ('capacity', 'dcls_capacity', 'capacity')
    ]

    overall_historic_parking_data = pd.DataFrame(columns=["general_time_index"])

    for key, value in parking_sensors.items():
        historical_data = []
        for data_type, api_suffix, column_name in data_types:
            print(f"Loading historical {data_type} data for location: {key} with location_id: {value["location_id"]}")

            parking_df  = get_historical_data_for_location(
                location_id=value["location_id"],
                location_slug=key,
                data_type=data_type,
                api_endpoint_suffix=api_suffix,
                column_name=column_name
            )
            historical_data.append(parking_df)
        merged_df = reduce(lambda x, y: pd.merge(x, y, on='time'), historical_data)
        
        # Resample to 1-hour buckets
        resampled_parking_df = merged_df.resample("1h").agg(
            occupancy=("occupancy", "mean"),
            occupancy_rate=("occupancy_rate", "mean"),
            capacity=("capacity", "mean"),               
        ).reset_index(names="general_time_index")

        # Rename columns explicitely to avoid duplicate column names and know for which parking sensor the data is
        resampled_parking_df = resampled_parking_df.rename(columns={
            col: f"{col}_{key}"
            for col in resampled_parking_df.columns
            if col != "general_time_index"
        })

        # Merge each resampled DataFrame into a single DataFrame
        overall_historic_parking_data = pd.merge(
            left=overall_historic_parking_data,
            right=resampled_parking_df,
            how="outer",
            on="general_time_index",
        )

    if specify_timerange:
        overall_historic_parking_data = overall_historic_parking_data[
            (overall_historic_parking_data["general_time_index"] >= start_time) &
            (overall_historic_parking_data["general_time_index"] <= end_time)
        ]
    
    return overall_historic_parking_data


def main():

    # Fetch historical data for all locations
    process_all_locations(
        parking_sensors,
        specify_timerange=True,
        start_time="2025-02-26 09:00:00",
        end_time="2025-03-04 23:00:00"
    )

if __name__ == '__main__':
    main()