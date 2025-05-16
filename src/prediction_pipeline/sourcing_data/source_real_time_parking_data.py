# Install libraries
import pandas as pd
import requests
import json
import os

########################################################################################
# Global variables
########################################################################################

# Load Bayern Cloud API key from environment variables
BAYERN_CLOUD_API_KEY = os.getenv('BAYERN_CLOUD_API_KEY')

parking_data_dir = os.path.join('data','raw','real_time_parking_data')
# We are not using 'parkplatz-fredenbruecke-1' and 'skiwanderzentrum-zwieslerwaldhaus-2' because of inconsistency in sending data to the cloud
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

########################################################################################
# Functions
########################################################################################

def load_and_save_occupancy_data_for_location(
    location_slug: str,
    save_file_path: str 
):
    API_endpoint = f'https://data.bayerncloud.digital/api/v4/endpoints/list_occupancy/{location_slug}'

    request_params = {
        'token': BAYERN_CLOUD_API_KEY
    }

    response = requests.get(API_endpoint, params=request_params)
    response_json = response.json()

    os.makedirs(save_file_path, exist_ok=True)

    # Save response to JSON file
    with open(f'{save_file_path}/occupancy_data_{location_slug}.json', 'w') as outfile:
        json.dump(response_json, outfile)

    print(f"Occupancy data for location '{location_slug}' saved successfully.")
    return


def main():
    # Fetch and save real-time parking data for each location
    # check if the directory exists
    os.makedirs(parking_data_dir, exist_ok=True)
    for location_slug in parking_sensors.keys():
        print(f"Fetching and saving real-time occupancy data for location '{location_slug}'...")
        load_and_save_occupancy_data_for_location(location_slug, parking_data_dir)

    return


if __name__ == '__main__':
    main()