import awswrangler as wr
import pandas as pd
from src.config import aws_s3_bucket

visitor_center_data_path = f"s3://{aws_s3_bucket}/raw-data/national-park-vacation-times-houses-opening-times-visitors.xlsx"

def source_data_from_aws_s3(path: str, **kwargs) -> pd.DataFrame:
    """Loads individual or multiple CSV files from an AWS S3 bucket.
    Args:
        path (str): The path to the CSV files on AWS S3.
        **kwargs: Additional arguments to pass to the read_csv function.
    Returns:
        pd.DataFrame: The DataFrame containing the data from the CSV files.
    """
    df = wr.s3.read_excel(path=path, **kwargs)
    return df
def source_visitor_center_data():
    # Source data - this is the preprocessed data
    sourced_visitor_count_data = source_data_from_aws_s3(visitor_center_data_path)

    return sourced_visitor_count_data

def source_preprocessed_hourly_visitor_center_data():

    """
    Load the preprocessed hourly visitor center data from AWS S3.
    """

    print("Sourcing the historic preprocessed_hourly_visitor_center_data")

    # Load visitor count data from AWS S3
    preprocessed_hourly_visitor_center_data = wr.s3.read_parquet(
        path=f"s3://{aws_s3_bucket}/preprocessed_data/visitor_centers_hourly_2017_to_2025.parquet"
    )

    print(f"The historic preprocessed_hourly_visitor_center_data is: {preprocessed_hourly_visitor_center_data}")

    return preprocessed_hourly_visitor_center_data