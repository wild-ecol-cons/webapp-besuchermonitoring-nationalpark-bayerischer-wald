# Install libraries
import pandas as pd
import requests
import json
import os
from functools import reduce

########################################################################################
# Global variables
########################################################################################

# Load Bayern Cloud API key from environment variables
BAYERN_CLOUD_API_KEY = os.getenv('BAYERN_CLOUD_API_KEY')

# We are not using 'parkplatz-fredenbruecke-1' and 'skiwanderzentrum-zwieslerwaldhaus-2'
# because of inconsistency in sending data to the cloud
parking_sensors = {
    "parkplatz-graupsaege-1":"e42069a6-702f-4ef4-b3b5-04e310d97ca0",
    # "parkplatz-fredenbruecke-1":"fac08b6b-e9cb-40cd-a106-b9f2cbfc7447",
    "p-r-spiegelau-1":"ee0490b2-3cc5-4adb-a527-95267257598e",
    # "skiwanderzentrum-zwieslerwaldhaus-2": "dd3734c2-c4fb-4e1d-a57c-9bbed8130d8f",
    "parkplatz-zwieslerwaldhaus-1": "6c9b765e-1ff9-401d-98bc-b0302ee65c62",
    "parkplatz-zwieslerwaldhaus-nord-1": "4bbb3b5c-edc2-4b00-a923-91c1544aa29d",
    "parkplatz-nationalparkzentrum-falkenstein-2" : "a93b64e9-35fb-4b3e-8348-81ba8f1c0d6f",
    "scheidt-bachmann-parkplatz-1" : "144e1868-3051-4140-a83c-41d4b79a6d14",
    "parkplatz-nationalparkzentrum-lusen-p2" : "454b0f50-130b-4c21-9db2-b163e158c847",
    "parkplatz-waldhaeuser-kirche-1" : "454b0f50-130b-4c21-9db2-b163e158c847",
    "parkplatz-waldhaeuser-ausblick-1" : "a14d8ebd-9261-49f7-875b-6a924fe34990",
    "parkplatz-skisportzentrum-finsterau-1": "ea474092-1064-4ae7-955e-8db099955c16"} 

historic_parking_data_path = os.path.join('data', 'raw', 'historic_parking_data')

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

    # Convert the response to a Pandas DataFrame and preprocess it
    historical_df = pd.DataFrame(response_json['data'], columns=['time', column_name])
    historical_df["time"] = pd.to_datetime(historical_df["time"])
    # historical_df.set_index("time", inplace=True)

    return historical_df


def process_all_locations(parking_sensors):
    """
    Process and fetch all types of historical data for each location in the parking sensors dictionary.

    Args:
        parking_sensors (dict): Dictionary containing location slugs as keys and location IDs as values.
    """

    data_types = [
        ('occupancy', 'dcls_occupancy', 'occupancy'),
        ('occupancy_rate', 'dcls_occupancy_rate', 'occupancy_rate'),
        ('capacity', 'dcls_capacity', 'capacity')
    ]

    for key, value in parking_sensors.items():
        historical_data = []
        for data_type, api_suffix, column_name in data_types:
            print(f"Loading historical {data_type} data for location: {key} with location_id: {value}")

            parking_df  = get_historical_data_for_location(
                location_id=value,
                location_slug=key,
                data_type=data_type,
                api_endpoint_suffix=api_suffix,
                column_name=column_name
            )
            historical_data.append(parking_df)
        merged_df = reduce(lambda x, y: pd.merge(x, y, on='time'), historical_data)
        
        # Create a filename based on the location name
        filename = f"{key}_historical_parking_data.csv"

        # make the output directory if it doesn't exist
        os.makedirs(historic_parking_data_path, exist_ok=True)
        output_path = os.path.join(historic_parking_data_path, filename)

        merged_df.to_csv(output_path, index=False)

        print(f"Saved historical parking data for location: {key} to {output_path}")


def main():

    # Fetch historical data for all locations
    process_all_locations(parking_sensors)

if __name__ == '__main__':
    main()