# import the necessary libraries
import pandas as pd
import requests
from datetime import datetime, timedelta
import os
from meteostat import Hourly, Point
import src.streamlit_app.pre_processing.process_real_time_parking_data as prtpd
import src.streamlit_app.pre_processing.process_forecast_weather_data as prfwd
import streamlit as st
from src.streamlit_app.pages_in_dashboard.visitors.language_selection_menu import TRANSLATIONS
from src.prediction_pipeline.sourcing_data.source_weather import get_hourly_data
from src.config import visitor_sensors_with_realtime_tracking, parking_sensors
import pytz


########################################################################################
# Bayern Cloud setup
########################################################################################

# Load Bayern Cloud API key from environment variables
BAYERN_CLOUD_API_KEY = os.getenv('BAYERN_CLOUD_API_KEY')


########################################################################################
# Weather Data Sourcing - METEOSTAT API
########################################################################################

# Get the start time as todays date (forecasted weather)
START_TIME = datetime.now()
END_TIME = (START_TIME + pd.Timedelta(days=7))

# Coordinates of the Bavarian Forest (Haselbach)
# # These coordinates are based on the weather recommendation by Google for a Bavarian Forest Weather search
# LATITUDE = 49.31452390542327
# LONGITUDE = 12.711573421032

# Update: New Coordinates for BFNP
LATITUDE = 48.96119
LONGITUDE = 13.36234

def get_realtime_occupancy_data_for_location(
    location_slug: str,
) -> int:
    """
    Fetches the real-time occupancy data for a given location from the Bayern Cloud API.

    Args:
        location_slug (str): The slug identifier for the location.
    """
    API_endpoint = f'https://data.bayerncloud.digital/api/v4/endpoints/list_occupancy/{location_slug}'

    request_params = {
        'token': BAYERN_CLOUD_API_KEY
    }

    response = requests.get(API_endpoint, params=request_params)
    response_json = response.json()

    # Access the first item in the @graph list
    graph_item = response_json["@graph"][0]

    # Get and preprocess the current occupancy data from the response for one location
    realtime_occupancy = int(graph_item.get("dcls:currentOccupancy", 0.0))

    # Get data collection timestamp of the current occupancy data
    realtime_occupancy_timestamp = graph_item.get("dcls:latestTimeseriesTimestamp", None)
    realtime_occupancy_timestamp = datetime.fromisoformat(realtime_occupancy_timestamp).strftime("%d.%m.%Y %H:%M Uhr")

    return realtime_occupancy, realtime_occupancy_timestamp

########################################################################################
# Parking functions
########################################################################################


def source_parking_data_from_cloud(
        location_slug: str,
        location_name: str) -> pd.DataFrame:
    """Sources the current occupancy data from the Bayern Cloud API.
    
    Args:
        location_slug (str): The location slug of the parking sensor.
    
    Returns:
        parking_df_with_spatial_info (pd.DataFrame): A DataFrame containing the current occupancy data, occupancy rate, capacity and spatial coordinates.
    """
    
    API_endpoint = f'https://data.bayerncloud.digital/api/v4/endpoints/list_occupancy/{location_slug}'

    request_params = {
        'token': BAYERN_CLOUD_API_KEY
    }


    response = requests.get(API_endpoint, params=request_params)
    response_json = response.json()

    # Access the first item in the @graph list
    graph_item = response_json["@graph"][0]

    # Extract the current occupancy, capacity and data collection timestamp
    current_occupancy = graph_item.get("dcls:currentOccupancy", None)
    current_capacity = graph_item.get("dcls:currentCapacity", None)
    current_occupancy_rate = graph_item.get("dcls:currentOccupancyRate", None)

    # Get data collection timestamp of the current occupancy data
    realtime_occupancy_timestamp = graph_item.get("dcls:latestTimeseriesTimestamp", None)
    realtime_occupancy_timestamp = datetime.fromisoformat(realtime_occupancy_timestamp).strftime("%d.%m.%Y %H:%M Uhr")

    # Make a dataframe with the three values and the current time stamp in the datetime format
    parking_data = pd.DataFrame({
        "timestamp": datetime.now(), 
        "location" : [location_slug],
        "location_name": [location_name],
        "current_occupancy": [current_occupancy],
        "current_capacity": [current_capacity],
        "current_occupancy_rate": [current_occupancy_rate],
        "realtime_occupancy_timestamp": [realtime_occupancy_timestamp]
    })
    
    parking_data.reset_index(drop=True, inplace=True)

    # adding spatial information to the dataframe
    parking_df_with_spatial_info = add_spatial_info_to_parking_sensors(parking_data)

    return parking_df_with_spatial_info

def add_spatial_info_to_parking_sensors(parking_data_df):

    """
    Add spatial information to the parking dataframe.

    Args:
        parking_data_df (pd.DataFrame): DataFrame containing parking sensor data (occupancy, capacity, occupancy rate).
    
    Returns:
        parking_data_df (pd.DataFrame): DataFrame containing parking sensor data with spatial information.
    """

    for location_slug in parking_sensors.keys():
        if location_slug in parking_data_df['location'].values:
            parking_data_df['latitude'] = parking_sensors[location_slug]["coordinates"][0]
            parking_data_df['longitude'] = parking_sensors[location_slug]["coordinates"][1]

            return parking_data_df

            
def merge_all_df_from_list(df_list):
    """
    Merge all the dataframes in the list into a single dataframe.

    Args:
        df_list (list): A list of pandas DataFrames to merge.

    Returns:
        merged_dataframe (pd.DataFrame): The merged DataFrame.
    """
    # Merge all the dataframes in the list with the 'time' column as index
    merged_dataframe = pd.concat(df_list, axis=0, ignore_index=True)
    return merged_dataframe


@st.cache_data(max_entries=1)
def source_and_preprocess_realtime_parking_data(current_timestamp):

    """
    Source and preprocess the real-time parking data. Returns the timestamp of when the function was run.

    Args:
        current_timestamp (datetime): The timestamp of when the function was run.

    Returns:
        processed_parking_data (pd.DataFram): Preprocessed real-time parking data.
    """
    print(f"Fetching and saving real-time parking occupancy data at '{current_timestamp}'...")
    
    # Source the parking data from bayern cloud
    all_parking_dataframes = []
    for location_slug in parking_sensors.keys():
        print(f"Fetching and saving real-time occupancy data for location '{location_slug}'...")
        parking_df = source_parking_data_from_cloud(
            location_slug=location_slug,
            location_name=parking_sensors[location_slug]["pretty_name"])
        all_parking_dataframes.append(parking_df)

    all_parking_data = merge_all_df_from_list(all_parking_dataframes)

    print("Parking data sourced successfully!")

    # Preprocess the parking data
    processed_parking_data = prtpd.process_real_time_parking_data(all_parking_data)

    print("Parking data processed and cleaned!")

    # Return the timestamp in German time indicating the time zone Berlin

    print(f"Parking data processed and cleaned at {current_timestamp}, Europe/Berlin time.")

    return processed_parking_data

@st.cache_data(max_entries=1)
def source_and_preprocess_realtime_visitor_occupancy(current_timestamp: datetime) -> pd.DataFrame:

    """
    Source and preprocess the real-time visitor occupancy data from five different locations that have real-time tracking to Bayern Cloud enabled.. Returns the timestamp of when the function was run.

    Args:
        current_timestamp (datetime): The timestamp of when the function was run.

    Returns:
        processed_visitor_occupancy_data (pd.DataFrame): Preprocessed real-time visitor occupancy data.
    """
    print(f"Fetching real-time visitor occupancy data at '{current_timestamp}'...")

    preprocessed_realtime_visitor_occupancy = pd.DataFrame()

    for sensor, sensor_data in visitor_sensors_with_realtime_tracking.items():
        realtime_sensor_occupancy, realtime_sensor_occupancy_timestamp = get_realtime_occupancy_data_for_location(sensor)

        # Build dataframe of sourced and preprocessed visitor occupancy
        sensor_data_df = pd.DataFrame({"location": [sensor_data["sensor_name"]], "latitude": [sensor_data["coordinates"][0]], "longitude": [sensor_data["coordinates"][1]], "current_occupancy": [realtime_sensor_occupancy], "timestamp_data_collected": [realtime_sensor_occupancy_timestamp]})

        preprocessed_realtime_visitor_occupancy = pd.concat([preprocessed_realtime_visitor_occupancy, sensor_data_df], ignore_index=True)

    # Return the timestamp in German time indicating the time zone Berlin
    print(f"Visitor occupancy data sourced and processed at {current_timestamp}, Europe/Berlin time.")

    return preprocessed_realtime_visitor_occupancy

########################################################################################
# Weather functions
########################################################################################


def source_weather_data(start_time: datetime):
    """
    Source forecasted weather data from the Meteostat API for the Bavarian Forest National Park in the next 7 days in hourly intervals.

    Args:
        start_time (datetime): The start time of the weather data.

    Returns:
        weather_hourly (pd.DataFrame): Hourly weather data for the Bavarian Forest National Park for the next 7 days
    """

    # Create a Point object for the Bavarian Forest National Park entry
    bavarian_forest = Point(lat=LATITUDE, lon=LONGITUDE)
    # Include the 10 nearest weather stations
    bavarian_forest.max_count = 10

    # Convert start_time to datetime format in utc
    start_time = start_time.astimezone(pytz.UTC).replace(tzinfo=None)

    # Add 7 days to start_time
    end_time = start_time + timedelta(days=7)

    # Fetch hourly data for the location
    weather_hourly = get_hourly_data(bavarian_forest, start_time, end_time)

    # Drop unnecessary columns
    weather_hourly = weather_hourly.drop(columns=['dwpt', 'snow', 'wdir', 'wpgt', 'pres', 'tsun'])

    # Convert the 'Time' column to datetime format again in Europe/Berlin time
    weather_hourly['time'] = pd.to_datetime(weather_hourly['time'], utc=True).dt.tz_convert('Europe/Berlin')
    return weather_hourly