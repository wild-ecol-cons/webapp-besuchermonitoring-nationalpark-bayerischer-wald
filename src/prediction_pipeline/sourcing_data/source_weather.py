# Import necessary libraries
import warnings
from datetime import datetime
import pandas as pd
from meteostat import Point, Hourly
import streamlit as st


# Ignore warnings
warnings.filterwarnings('ignore')


############################################################################################################
# Define global variables
############################################################################################################
# Uncomment this if you want to get the weather data for the next 7 days starting from today
# Get dates for the data sourcing
# START_TIME = datetime.now()
# END_TIME = (START_TIME + pd.Timedelta(days=7))

# Set time period for sourcing the data
START_TIME = datetime(2023, 1, 1)
END_TIME = datetime(2024, 7, 22 )

# Coordinates of the Bavarian Forest (Haselbach)
# These coordinates are based on the weather recommendation by Google for a Bavarian Forest Weather search
# LATITUDE = 49.31452390542327
# LONGITUDE = 12.711573421032

# Update: New Coordinates for BFNP
LATITUDE = 48.96119
LONGITUDE = 13.36234

# Define weather condition code mapping
coco_to_coco_2_mapping = {
    1: 1,  # Clear
    2: 1,  # Fair
    3: 2,  # Cloudy
    4: 2,  # Overcast
    5: 2,  # Fog
    6: 5,  # Freezing Fog
    7: 3,  # Light Rain
    8: 3,  # Rain
    9: 3,  # Heavy Rain
    10: 5, # Freezing Rain
    11: 5, # Heavy Freezing Rain
    12: 5, # Sleet
    13: 5, # Heavy Sleet
    14: 4, # Light Snowfall
    15: 4, # Snowfall
    16: 4, # Heavy Snowfall
    17: 3, # Rain Shower
    18: 3, # Heavy Rain Shower
    19: 3, # Sleet Shower
    20: 5, # Heavy Sleet Shower
    21: 4, # Snow Shower
    22: 4, # Heavy Snow Shower
    23: 6, # Lightning
    24: 6, # Hail
    25: 6, # Thunderstorm
    26: 6, # Heavy Thunderstorm
    27: 6  # Storm
}

############################################################################################################
# Define functions
############################################################################################################

def get_hourly_data(region, start_time, end_time):
    """
    Fetch hourly weather data for a specified region and date range.

    This function retrieves hourly weather data from the Meteostat API or another defined source,
    returning it as a pandas DataFrame.

    Args:
        region (Point): A `Point` object representing the geographical location (latitude, longitude).
        start_date (datetime): The start date for data retrieval.
        end_date (datetime): The end date for data retrieval.

    Returns:
        pandas.DataFrame: A DataFrame containing hourly weather data with the following columns:
            - time: Datetime of the record.
            - temp: Temperature in degrees Celsius.
            - dwpt: Dew point in degrees Celsius.
            - prcp: Precipitation in millimeters.
            - wdir: Wind direction in degrees.
            - wspd: Wind speed in km/h.
            - wpgt: Wind gust in km/h.
            - pres: Sea-level air pressure in hPa.
            - tsun: Sunshine duration in minutes.
            - snow: Snowfall in millimeters.
            - rhum: Relative humidity in percent.
            - coco: Weather condition code.
    """
    # Fetch hourly data
    data = Hourly(region, start_time, end_time).fetch()
    
    # Reset the index
    data.reset_index(inplace=True)
    return data


def process_hourly_data(data):
    """
    Process raw hourly weather data by cleaning and formatting.

    This function drops unnecessary columns, renames the remaining columns to more descriptive names,
    and converts the 'time' column to a datetime format.

    Args:
        data (pandas.DataFrame): A DataFrame containing raw hourly weather data.

    Returns:
        pandas.DataFrame: A DataFrame containing the processed hourly weather data with the following columns:
            - Time: Datetime of the record.
            - Temperature (°C): Temperature in degrees Celsius.
            - Wind Speed (km/h): Wind speed in km/h.
            - Relative Humidity (%): Relative humidity in percent.
            - coco_2: Weather condition code.
            - Precipitation (mm): Precipitation in millimeters.
    """
        # Drop unnecessary columns
    data = data.drop(columns=['dwpt', 'wdir', 'wpgt', 'pres','snow', 'tsun'])

    # Rename columns for clarity
    data = data.rename(columns={
        'time': 'Time',
        'temp': 'Temperature (°C)',
        'wspd': 'Wind Speed (km/h)',
        'rhum': 'Relative Humidity (%)',
        'coco': 'coco_2',
        'prcp': 'Precipitation (mm)'
    })


    # Convert the 'Time' column to datetime format
    data['Time'] = pd.to_datetime(data['Time'])
    # Map weather condition codes to new codes
    data['coco_2'] = data['coco_2'].map(coco_to_coco_2_mapping)

    return data

@st.cache_data(max_entries=1)
def source_weather_data(start_time, end_time):
    """
    This function creates a point over the Bavarian Forest National Park, retrieves hourly weather data
    for the specified time period, processes the data to extract necessary weather parameters,
    and saves the processed data to a CSV file.
    """
    print(f"Sourcing weather data for {start_time} to {end_time} at {datetime.now()}...")

    # Create a Point object for the Bavarian Forest National Park entry
    bavarian_forest = Point(lat=LATITUDE, lon=LONGITUDE)
    # Include the 10 nearest weather stations
    bavarian_forest.max_count = 10

    # Fetch hourly data for the location
    hourly_data = get_hourly_data(bavarian_forest, start_time, end_time)

    # Process the hourly data to extract and format necessary weather parameters
    sourced_hourly_data = process_hourly_data(hourly_data)
    

    return sourced_hourly_data



