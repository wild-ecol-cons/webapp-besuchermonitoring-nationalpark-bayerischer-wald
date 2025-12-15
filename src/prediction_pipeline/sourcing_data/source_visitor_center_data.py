import pandas as pd
from src.utils import read_dataframe_from_azure


def source_visitor_center_data():
    # Source data - this is the preprocessed data
    sourced_visitor_count_data = read_dataframe_from_azure(
        file_name="national-park-vacation-times-houses-opening-times-visitors.xlsx",
        file_format="xlsx",
        source_folder="raw-data",
    )

    return sourced_visitor_count_data

def source_preprocessed_hourly_visitor_center_data():

    """
    Load the preprocessed hourly visitor center data from the cloud.
    """

    print("Sourcing the historic preprocessed_hourly_visitor_center_data")

    # Load visitor count data from the cloud
    preprocessed_hourly_visitor_center_data = read_dataframe_from_azure(
        file_name="visitor_centers_hourly_2017_to_2025.parquet",
        file_format="parquet",
        source_folder="preprocessed_data",
    )

    print(f"The historic preprocessed_hourly_visitor_center_data is: {preprocessed_hourly_visitor_center_data}")

    return preprocessed_hourly_visitor_center_data