import pandas as pd
import numpy as np
import hashlib
import re
import io
import zipfile
import streamlit as st
from datetime import datetime
from src.config import data_upload_categories_to_azure_folders
from src.utils import query_azure_with_duck_db, upload_dataframe_to_azure, read_dataframe_from_azure
from src.prediction_pipeline.sourcing_data.source_historic_parking_data import process_all_locations
from src.prediction_pipeline.sourcing_data.source_weather import source_weather_data
from src.streamlit_app.pages_in_dashboard.visitors.language_selection_menu import TRANSLATIONS


def log_queried_data_to_azure(queried_data: pd.DataFrame) -> str:
    """
    If not already done before for the same data, log the queried data to Azure.

    Args:
        queried_data (pd.DataFrame): The data to log to Azure.

    Returns:
        str: The file name of the exported data
    """
    with st.spinner(TRANSLATIONS[st.session_state.selected_language]['spinner_msg_logging_data_to_azure'], show_time=True):
        # Create a unique key for this specific dataset preview
        data_hash = hashlib.md5(queried_data.to_csv().encode()).hexdigest()

        data_download_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file_name_data_export = f"{data_download_time}_Data_Hub_Datenexport.csv"

        # Check if we have already logged this specific version of the data
        if st.session_state.get("last_uploaded_hash") != data_hash:
            upload_dataframe_to_azure(
                df=queried_data,
                file_name=file_name_data_export,
                target_folder="data-hub/exported-data",
                file_format="csv",
            )
    # Mark this hash as uploaded so it doesn't repeat on every rerun
    st.session_state.last_uploaded_hash = data_hash
    st.toast(TRANSLATIONS[st.session_state.selected_language]['toast_msg_data_logged_to_azure'], icon="☁️")

    return file_name_data_export

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

        if category in ["Parkplatzzählungen", "Wetterdaten", "Schulferien & Feiertage (BY & CZ)"]:
            continue
        else:
            min_date = query_azure_with_duck_db(
                    directory=f"data-hub/preprocessed-data/{data_upload_categories_to_azure_folders[category]}",
                    columns=["general_time_index"],
                    order_by="general_time_index ASC",
                    limit=1
                )
            dataset_min_times = pd.concat([dataset_min_times, min_date])

    if len(dataset_min_times) == 0:
        return None
    else:
        min_date = dataset_min_times.min().iloc[0]
        return min_date

def aggregate_to_daily(df: pd.DataFrame, daily_value_cols: list[str]) -> pd.DataFrame:
    """
    Aggregate hourly data to daily data.

    - Columns in daily_value_cols (already daily, e.g. from Hütten category): take first value per day
    - All other numeric columns: sum per day
    - Non-numeric columns: take first value per day

    Args:
        df (pd.DataFrame): Hourly data with 'general_time_index' column.
        daily_value_cols (list[str]): Column names that are already on a daily frequency and must not be summed.

    Returns:
        pd.DataFrame: Daily aggregated data.
    """
    df = df.copy()
    df['general_time_index'] = pd.to_datetime(df['general_time_index'])
    df['_date'] = df['general_time_index'].dt.normalize()

    existing_daily_cols = [c for c in daily_value_cols if c in df.columns]
    numeric_cols = df.select_dtypes(include='number').columns.tolist()
    non_numeric_cols = [c for c in df.columns if c not in numeric_cols and c not in ['general_time_index', '_date']]

    agg_dict = {}
    for col in numeric_cols:
        agg_dict[col] = 'first' if col in existing_daily_cols else 'sum'
    for col in non_numeric_cols:
        agg_dict[col] = 'first'

    daily_df = df.groupby('_date').agg(agg_dict).reset_index()
    daily_df = daily_df.rename(columns={'_date': 'general_time_index'})

    return daily_df.sort_values(by='general_time_index').reset_index(drop=True)


def query_and_preprocess_data(data_categories_to_query: list[str], specify_timerange: bool = False, start_time: datetime = None, end_time: datetime = None, time_frequency: str = "hourly") -> pd.DataFrame:
    """
    Query and preprocess data based on the selected data categories and timeframe.

    Args:
        data_categories_to_query (list[str]): List of data categories to query.
        specify_timerange (bool, optional): Whether to specify a specific timeframe. Defaults to False.
        start_time (datetime, optional): Start time for the timeframe. Defaults to None.
        end_time (datetime, optional): End time for the timeframe. Defaults to None.
        time_frequency (str, optional): Output time frequency – "hourly" or "daily". Defaults to "hourly".

    Returns:
        pd.DataFrame: Preprocessed data.
    """

    overall_queried_data = pd.DataFrame(columns=["general_time_index"])
    # Tracks columns from the Hütten category (already daily – must not be summed during daily aggregation)
    daily_value_cols: list[str] = []
    
    # Query the selected data with Duck DB
    for category in data_categories_to_query:

        if category == "Schulferien & Feiertage (BY & CZ)":
            continue
        elif category == "Wetterdaten":
            if not specify_timerange:
                # Stop the streamlit execution
                st.warning("⚠️ Wetterdaten können nur für einen bestimmten Zeitraum abgefragt werden. Bitte gebe ein Start- und Enddatum an.")
                st.stop() # Beendet die Skriptausführung an dieser Stelle (der Loop wird nie erreicht)
            else:
                queried_single_category_data = source_weather_data(
                start_time=start_time,
                end_time=end_time)
                queried_single_category_data = queried_single_category_data.rename(columns={"Time": "general_time_index"})
        elif category == "Parkplatzzählungen":
            queried_single_category_data = process_all_locations(
                specify_timerange=specify_timerange,
                start_time=start_time,
                end_time=end_time)
        else:
            if specify_timerange:
                queried_single_category_data = query_azure_with_duck_db(
                    directory=f"data-hub/preprocessed-data/{data_upload_categories_to_azure_folders[category]}",
                    filters = f"general_time_index >= '{start_time}' AND general_time_index <= '{end_time}'"
                )
            else:
                queried_single_category_data = query_azure_with_duck_db(directory=f"data-hub/preprocessed-data/{data_upload_categories_to_azure_folders[category]}")

            # Drop duplicate rows if not empty query
            if len(queried_single_category_data) > 0:
                ## First, order by data_upload_time
                queried_single_category_data = queried_single_category_data.sort_values(by="data_upload_time", ascending=True)
                ## Then, drop duplicates when the same general_time_index is encountered (keep the last, so the latest uploaded version is kept)
                queried_single_category_data = queried_single_category_data.drop_duplicates(subset="general_time_index", keep="last")

                # Drop data_upload_time column, as it was only needed for duplicate removal
                queried_single_category_data = queried_single_category_data.drop(columns=["data_upload_time"])

        # Convert empty strings to NaN and drop empty columns (before merge or preview)
        queried_single_category_data = queried_single_category_data.replace("", np.nan).dropna(axis=1, how='all')
        
        if category == "Hütten: Zählungen, Wetterstationsdaten,Öffnungszeiten & Feiertage":
            daily_value_cols_to_be_filled = queried_single_category_data.columns.difference(['general_time_index'])
            daily_value_cols = list(daily_value_cols_to_be_filled)

        # Do a full outer join between the current state of the overall queried data and the queried data of the current category, resulting again in the overall queried data
        if len(queried_single_category_data) > 0:
            overall_queried_data = pd.merge(
                left=overall_queried_data,
                right=queried_single_category_data,
                how="outer",
                on="general_time_index",
                suffixes=(None, f"_{category}"),
            )
        else:
            continue

    # Fill missing values for daily data
    if "Hütten: Zählungen, Wetterstationsdaten,Öffnungszeiten & Feiertage" in data_categories_to_query:
        # overlap_eco_counter_huetten["general_time_index"] = pd.to_datetime(overlap_eco_counter_huetten["general_time_index"])

        overall_queried_data[daily_value_cols_to_be_filled] = overall_queried_data.groupby(overall_queried_data['general_time_index'].dt.date)[daily_value_cols_to_be_filled].ffill()
    
    # Order queried data by time
    overall_queried_data = overall_queried_data.sort_values(by="general_time_index", ascending=True)

    if time_frequency == "daily":
        overall_queried_data = aggregate_to_daily(overall_queried_data, daily_value_cols)

    return overall_queried_data

def normalize_excel_sheet_name(name: str) -> str:
    """
    Normalizes a string the same way Excel does when creating sheet names:
    - Removes forbidden characters (: \\ / ? * [ ])
    - Strips leading/trailing whitespace
    - Truncates to 31 characters (Excel's sheet name limit)

    Args:
        name (str): The original sheet name.

    Returns:
        str: The normalized sheet name.
    """
    # Remove Excel-forbidden characters
    name = re.sub(r'[:\\/?*\[\]]', '', name)
    # Strip whitespace and truncate to 31 chars
    return name.strip()[:31]

def filter_sheets_by_name(
    all_sheets: dict[str, pd.DataFrame],
    desired_sheets: list[str],
    match_chars: int = 15
) -> dict[str, pd.DataFrame]:
    """
    Filters a dict of sheets by matching against desired sheet names,
    accounting for Excel's sheet name transformations.

    Args:
        all_sheets (dict): All sheets loaded from Excel.
        desired_sheets (list[str]): The sheet names you want to load.
        match_chars (int): Number of leading characters to compare. Defaults to 15.

    Returns:
        dict[str, pd.DataFrame]: Filtered sheets that matched a desired name.
    """
    normalized_desired = [normalize_excel_sheet_name(s)[:match_chars] for s in desired_sheets]

    return {
        name: df for name, df in all_sheets.items()
        if normalize_excel_sheet_name(name)[:match_chars] in normalized_desired
    }

def build_download_zip(df: pd.DataFrame, csv_filename: str, queried_data_categories: list) -> bytes:
    """
    Builds a ZIP file in memory containing:
    - The queried data as a CSV
    - The respective data dictionary as an Excel file

    Args:
        df (pd.DataFrame): The queried data to export.
        csv_filename (str): The filename for the CSV inside the ZIP.

    Returns:
        bytes: The ZIP file as bytes.
    """
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:

        # --- 1. Add Queried Data as CSV ---
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        zip_file.writestr(csv_filename, csv_bytes)

        # --- 2. Add Excel data dictionary ---
        # Load the Data Dictionary from Azure
        overall_data_dictionary = read_dataframe_from_azure(
            file_name="Data Dictionary für Data Hub.xlsx",
            file_format="xlsx",
            source_folder="data-hub",
            read_options={
                "sheet_name": None
            }
        )

        # Filter the data dictionary only for needed sheets
        selected_sheets_for_data_dictionary = filter_sheets_by_name(
            all_sheets=overall_data_dictionary,
            desired_sheets=queried_data_categories,
            match_chars=15
        )   
        
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
            for sheet_name, sheet_df in selected_sheets_for_data_dictionary.items():
                sheet_df.to_excel(writer, index=False, sheet_name=sheet_name)

        zip_file.writestr("data_dictionary.xlsx", excel_buffer.getvalue())

    return zip_buffer.getvalue()