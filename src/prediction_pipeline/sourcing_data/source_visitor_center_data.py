import awswrangler as wr
import pandas as pd
from src.config import aws_s3_bucket
import os

visitor_center_data_path = os.path.join("data","raw","visitor_center","national-park-vacation-times-houses-opening-times-visitors.xlsx")
saved_path_visitor_center_query = os.path.join("data","processed","visitor_center","visitor_centers_2017_to_2024.parquet")

def source_visitor_center_data():
    # Source data - this is the preprocessed data
    sourced_visitor_count_data = pd.read_excel(visitor_center_data_path)

    return sourced_visitor_count_data

def source_preprocessed_hourly_visitor_center_data():

    """
    Load the preprocessed hourly visitor center data from AWS S3.
    """

    # Load visitor count data from AWS S3
    preprocessed_hourly_visitor_center_data = pd.read_parquet(saved_path_visitor_center_query)

    return preprocessed_hourly_visitor_center_data