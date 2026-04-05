import pandas as pd
from datetime import datetime
from src.config import data_upload_categories_to_azure_folders
from src.utils import query_azure_with_duck_db


def get_min_date_from_queried_data(data_categories: list[str]) -> datetime:
    """
    Get the first selectable date (the earliest date available) from the queried data.

    Args:
        data_categories (list[str]): List of data categories to query.

    Returns:
        datetime: The first selectable date (the earliest date available).
    """
    dataset_min_times = pd.DataFrame()

    for category in data_categories:

        min_date = query_azure_with_duck_db(
                directory=f"data-hub/preprocessed-data/{data_upload_categories_to_azure_folders[category]}",
                columns=["general_time_index"],
                order_by="general_time_index ASC",
                limit=1
            )
        
        dataset_min_times = pd.concat([dataset_min_times, min_date])

    min_date = dataset_min_times.min().iloc[0]
    return min_date

def query_and_preprocess_data(data_categories_to_query: list[str], specify_timerange: bool = False, start_time: datetime = None, end_time: datetime = None) -> pd.DataFrame:
    """
    Query and preprocess data based on the selected data categories and timeframe.

    Args:
        data_categories_to_query (list[str]): List of data categories to query.
        specify_timerange (bool, optional): Whether to specify a specific timeframe. Defaults to False.
        start_time (datetime, optional): Start time for the timeframe. Defaults to None.
        end_time (datetime, optional): End time for the timeframe. Defaults to None.

    Returns:
        pd.DataFrame: Preprocessed data.
    """

    overall_queried_data = pd.DataFrame(columns=["general_time_index"])
    
    # Query the selected data with Duck DB
    for category in data_categories_to_query:

        if specify_timerange:
            queried_single_category_data = query_azure_with_duck_db(
                directory=f"data-hub/preprocessed-data/{data_upload_categories_to_azure_folders[category]}",
                filters = f"general_time_index >= '{start_time}' AND general_time_index <= '{end_time}'"
            )
        else:
            queried_single_category_data = query_azure_with_duck_db(directory=f"data-hub/preprocessed-data/{data_upload_categories_to_azure_folders[category]}")

        # Drop duplicate rows
        ## First, order by data_upload_time
        queried_single_category_data.sort_values(by="data_upload_time", ascending=True, inplace=True)
        ## Then, drop duplicates when the same general_time_index is encountered (keep the last, so the latest uploaded version is kept)
        queried_single_category_data.drop_duplicates(subset="general_time_index", keep="last", inplace=True)

        # Drop data_upload_time column, as it was only needed for duplicate removal
        queried_single_category_data = queried_single_category_data.drop(columns=["data_upload_time"])

        if category == "Hütten: Zählungen, Wetterstationsdaten,Öffnungszeiten & Feiertage":
            daily_value_cols_to_be_filled = queried_single_category_data.columns.difference(['general_time_index'])

        # Do a full outer join between the current state of the overall queried data and the queried data of the current category, resulting again in the overall queried data
        overall_queried_data = pd.merge(
            left=overall_queried_data,
            right=queried_single_category_data,
            how="outer",
            on="general_time_index",
            suffixes=(None, f"_{category}"),
        )

    # Fill missing values for daily data
    if "Hütten: Zählungen, Wetterstationsdaten,Öffnungszeiten & Feiertage" in data_categories_to_query:
        # overlap_eco_counter_huetten["general_time_index"] = pd.to_datetime(overlap_eco_counter_huetten["general_time_index"])

        overall_queried_data[daily_value_cols_to_be_filled] = overall_queried_data.groupby(overall_queried_data['general_time_index'].dt.date)[daily_value_cols_to_be_filled].ffill()
    
    # Order queried data by time
    overall_queried_data.sort_values(by="general_time_index", ascending=True, inplace=True)

    return overall_queried_data