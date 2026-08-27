import requests
import os

# Load Bayern Cloud API key from environment variables
BAYERN_CLOUD_API_KEY = os.getenv('BAYERN_CLOUD_API_KEY')

def get_realtime_occupancy_data_for_location(
    location_slug: str,
):
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
    response_json = response.json()["@graph"][0]["dcls:currentOccupancy"]

    print(f"Real-time visitor count from Bayern Cloud API sensor '{location_slug}': {response_json}")


visitor_sensors_with_realtime_tracking = [
    "tfg-lusen-1",
    "tfg-lusen-2",
    "tfg-lusen-3",
    "tfg-falkenstein-1",
    "tfg-falkenstein-2",
]

for sensor in visitor_sensors_with_realtime_tracking:
    get_realtime_occupancy_data_for_location(sensor)