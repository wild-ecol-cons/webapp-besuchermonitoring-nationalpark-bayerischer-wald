# imports libraries
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from numpy.random import default_rng as rng
from src.streamlit_app.pages_in_dashboard.password import check_password
from src.streamlit_app.pages_in_dashboard.visitors.language_selection_menu import TRANSLATIONS
from src.prediction_pipeline.pre_processing.preprocess_historic_visitor_count_data import parse_german_dates
from src.utils import upload_file_to_azure, upload_dataframe_to_azure, query_azure_with_duck_db

# Mapping data upload categories to specific folders in Azure Blob Storage
data_upload_categories_to_azure_folders = {
    "Permanente Besucherzählung (Eco-Counter)": "visitor-counts-eco-counter",
    "Hütten: Zählungen, Wetterstationsdaten,Öffnungszeiten & Feiertage": "huts-counts-openings-weather-station-holidays",
    "Sonderzählungen": "special-counts",
}

data_upload_categories_time_cols_freq = {
    "Permanente Besucherzählung (Eco-Counter)": {"col": "Time", "freq": "1 hour"},
    "Hütten: Zählungen, Wetterstationsdaten,Öffnungszeiten & Feiertage": {"col": "Datum", "freq": "1 day"},
    "Sonderzählungen": {"col": None, "freq": None}
}

def warning_for_new_columns(df: pd.DataFrame, already_existing_columns: list) -> None:
    """
    Warns the user about new columns that are not in the already listed as features in the Data Hub.

    Args:
        df (pd.DataFrame): DataFrame to check for new columns
        already_existing_columns (list): List of already existing columns
    """
    # List all columns that are not in the already existing columns
    new_columns = [col for col in df.columns if col not in already_existing_columns]

    if new_columns:
        st.warning(f'Achtung! Es wurden die folgenden, neuen Spaltennamen in der hochgeladenen Datei gefunden: {", ".join(new_columns)}', icon="⚠️")

def retrieve_already_existing_features(category_to_upload_data_to: str) -> list:
    """
    Retrieve already in the Data Hub existing features for a specific data category.

    Args:
        category_to_upload_data_to (str): Category to upload data to

    Returns:
        list: List of already existing features
    """

    queried_data = query_azure_with_duck_db(
        directory=f"data-hub/preprocessed-data/{data_upload_categories_to_azure_folders[category_to_upload_data_to]}",
        limit=1
    )

    already_existing_columns = queried_data.columns.to_list()

    with st.expander("Information zu aktuell vorhandenen Spaltennamen"):
        st.info(f'Aktuell vorhandene Spaltennamen in der Datenkategorie "{category_to_upload_data_to} sind: {", ".join(already_existing_columns)}', icon="ℹ️")

    return already_existing_columns

def save_preprocessed_data_to_cloud(preprocessed_data: pd.DataFrame, category_to_upload_data_to: str) -> None:
    """
    Save preprocessed data to Azure Cloud with adding the current timestamp as a new feature.
    
    Args:
        preprocessed_data (pd.DataFrame): Preprocessed data to upload
        category_to_upload_data_to (str): Category to upload data to
    """
    data_upload_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Add column to df with current timestamp
    preprocessed_data["data_upload_time"] = data_upload_time
    # Save preprocessed data (df) to Azure

    file_name = f"preprocessed-{data_upload_categories_to_azure_folders[category_to_upload_data_to]}-{data_upload_time}"

    try:
        upload_dataframe_to_azure(
            df=preprocessed_data,
            file_name=file_name,
            target_folder=f"data-hub/preprocessed-data/{data_upload_categories_to_azure_folders[category_to_upload_data_to]}",
            file_format="parquet",
        )
        st.success(f"Die Datei wurde erfolgreich vorverarbeitet und als {file_name} erfolgreich in der Cloud gespeichert!")
    except Exception as e:
        st.error(f"Beim Hochladen der verarbeitetten Datei {uploaded_file.name} ist ein Fehler aufgetreten: {e}")

def save_raw_data_to_cloud(raw_file_to_upload: object, category_to_upload_data_to: str) -> None:
    """
    Save raw data file provided via Data Hub to Azure Cloud as it is.
    
    Args:
        raw_file_to_upload (object): Raw data file to upload
        category_to_upload_data_to (str): Category to upload data to
    """
    # IMPORTANT: Rewind the file if you read it earlier in your code
    raw_file_to_upload.seek(0)

    success = upload_file_to_azure(
        file_obj=raw_file_to_upload,
        target_folder=f"data-hub/raw-data/{data_upload_categories_to_azure_folders[category_to_upload_data_to]}",
        filename=raw_file_to_upload.name
    )
    if success:
        st.success(f"Die Rohdatei {raw_file_to_upload.name} wurde erfolgreich zur Cloud hochgeladen!")

def validate_time_frequency(df, time_col, freq_string, category, file_id):
    """
    Validates if the dataframe follows a specific frequency.
    freq_string: e.g., '1 hour', '1 day', '30 minutes'
    """
    # Convert to datetime
    if category == "Permanente Besucherzählung (Eco-Counter)":
        df = parse_german_dates(df=df, date_column_name=time_col)
    else:
        df[time_col] = pd.to_datetime(df[time_col], format="mixed")
    
    # Sort to ensure we're checking chronological continuity
    df = df.sort_values(by=time_col)

    if freq_string is None:
        # Ask user to specify the time column via an text input field
        freq_string = st.text_input(
            label="Bitte trage hier die Frequenz der Zeitspalte ein (z.B. ""1 hour"", ""1 day"", ""30 minutes""). Hinweis: Falls die Zeitspalte keine regelmässige Frequenz hat, bitte den Wert ""no_time_frequency"" eingeben:",
            key=f"text_input_freq_string_{uploaded_file.file_id}"
        )
    
    if freq_string == "no_time_frequency":
        return df
    else:
        # Calculate the expected timedelta
        try:
            expected_delta = pd.to_timedelta(freq_string)
        except ValueError:
            st.error(f"Ungültiges Frequenz-Format: '{freq_string}'")
            st.stop()

        # Calculate differences between consecutive rows
        time_diffs = df[time_col].diff().dropna()
        
        # Check if the data mostly follows the expected frequency
        most_prevelant_time_diff = time_diffs.value_counts().index[0]

        if not most_prevelant_time_diff == expected_delta:
            st.error(f"Error: Die Datenfrequenz entspricht nicht '{freq_string}'. "
                    "Bitte überprüfe die Datei.")
            st.stop()
        
        return df

def process_and_validate_upload(uploaded_file, category):
    # Check extension and read file
    if uploaded_file.name.endswith('.csv'):
        if category == "Permanente Besucherzählung (Eco-Counter)":
            df = pd.read_csv(uploaded_file, skiprows=2)
        else:
            df = pd.read_csv(uploaded_file)
    elif uploaded_file.name.endswith(('.xls', '.xlsx')):
        df = pd.read_excel(uploaded_file)
    else:
        st.error("Error: Bitte nur Excel (.xlsx, .xls) oder CSV Dateien hochladen.")
        st.stop()

    # Map category to expected time column name
    time_col = data_upload_categories_time_cols_freq[category]["col"]
    
    if time_col is None:
        # Ask user to specify the time column via an text input field
        time_col = st.text_input(
            label=f"Bitte trage hier den exakten Namen der Zeitspalte der Datei {uploaded_file.name} ein:",
            key=f"text_input_time_col_{uploaded_file.file_id}"
        )
    
    if time_col not in df.columns:
        st.error(f"Error: Die Zeitspalte '{time_col}' der Datei {uploaded_file.name} konnte nicht in der hochgeladenen Datei gefunden werden. Der Upload wird abgebrochen.")
        st.stop()
    
    else:

        preprocessed_df = validate_time_frequency(
            df,
            time_col,
            freq_string=data_upload_categories_time_cols_freq[category]["freq"],
            category=category,
            file_id=uploaded_file.file_id
        )

        # Drop entirely empty columns
        preprocessed_df.dropna(how="all", axis="columns", inplace=True)

        # Save time column as general time index
        preprocessed_df["general_time_index"] = preprocessed_df[time_col]

        return preprocessed_df

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

        # Do a full outer join between the current state of the overall queried data and the queried data of the current category, resulting again in the overall queried data
        overall_queried_data = pd.merge(
            left=overall_queried_data,
            right=queried_single_category_data,
            how="outer",
            on="general_time_index",
            suffixes=(None, f"_{category}"),
        )

    # Order queried data by time
    overall_queried_data.sort_values(by="general_time_index", ascending=True, inplace=True)

    return overall_queried_data


# Initialize language in session state if it doesn't exist
if 'selected_language' not in st.session_state:
    st.session_state.selected_language = 'German'  # Default language

# Define the page layout of the Streamlit app

st.set_page_config(
page_title=TRANSLATIONS[st.session_state.selected_language]['page_title_data_hub'],
page_icon="🌲",
layout="wide",
initial_sidebar_state="expanded")

# Password-protect the page
if not check_password(
    type_of_password="admin"
):
    st.stop()  # Do not continue if check_password is not True.

# get_upload_and_download_section()
st.markdown("## Data Hub ☁️")

st.markdown(
    TRANSLATIONS[st.session_state.selected_language]['page_description_data_hub']
)

# Tabs for Upload and Download
tab_query_download_data, tab_upload_data = st.tabs(["Query/Download Data", "Upload Data"])

with tab_query_download_data:
    # Select one or multiple data categories
    available_data_categories = st.multiselect(
        "Ausgewählte Datenkategorien:",
        data_upload_categories_to_azure_folders.keys(),

            # "Schulferien & Feiertage (BY & CZ)", # TODO: Add this at the end of the project if time allows
            # "Parkplatzzählungen", # TODO: First focus on manually collected data, then on this API-fetched data
            # "Wetterdaten" # TODO: First focus on manually collected data, then on this API-fetched data
    )

    # Select entire timeframe or a specific start and end date
    ## Checkbox: All data?
    specify_timerange = st.toggle("Zeitraum eingrenzen")

    ## time Selection
    if specify_timerange:
        # Get the first selectable date (the earliest date available) 
        first_selectable_date = get_min_date_from_queried_data(data_categories=available_data_categories)

        col_left, col_right = st.columns(2)

        with col_left:
            start_time = st.datetime_input(
                "Start:",
                value=None,
                min_value=first_selectable_date,
                max_value="now",
                step=3600
            )

        with col_right:
            end_time = st.datetime_input(
                "Ende:",
                value=None,
                min_value=first_selectable_date,
                max_value="now",
                step=3600
            )

    # Button to query data
    if st.button(
        label="Query Data",
        help="Query the data based on the selected data categories and timeframe.",
        type="primary"
    ):

        overall_queried_data = query_and_preprocess_data(
            data_categories_to_query=available_data_categories,
            specify_timerange=specify_timerange,
            start_time=start_time,
            end_time=end_time
        )
        
        # Preview queried data before download
        st.markdown(f"#### Preview der Daten")
        st.dataframe(overall_queried_data.head())

        data_download_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file_name_data_export = f"{data_download_time}_Data_Hub_Datenexport.csv"

        # Button to download data
        if st.download_button(
            label="Download Data",
            data=overall_queried_data.to_csv(index=False).encode('utf-8'),
            file_name=file_name_data_export,
            icon=":material/download:",
        ):

            upload_dataframe_to_azure(
                df=overall_queried_data,
                file_name=file_name_data_export,
                target_folder="data-hub/exported-data",
                file_format="csv",
            )

with tab_upload_data:

    st.markdown("## Upload Data")

    # Select category that it is being uploaded to
    category_to_upload_data_to = st.radio(
    "Wähle die Datenkategorie aus:",
    [
        "Permanente Besucherzählung (Eco-Counter)",
        "Hütten: Zählungen, Wetterstationsdaten,Öffnungszeiten & Feiertage",
        "Sonderzählungen",
    ],
    )
    
    already_existing_columns = retrieve_already_existing_features(category_to_upload_data_to)

    # Select local file to upload
    uploaded_files = st.file_uploader(
    "Upload data", accept_multiple_files=True, type=["csv", "xlsx", "xls"]
    )
    for uploaded_file in uploaded_files:
        
        df = process_and_validate_upload(uploaded_file, category_to_upload_data_to)

        warning_for_new_columns(df, already_existing_columns)

        # Preview file before upload
        st.markdown(f"#### Preview: `{uploaded_file.name}`")
        st.dataframe(df.head())

        # Confirm upload
        if st.button(
            "Upload Data",
            key=f"data_upload_button_{uploaded_file.file_id}",
            type="primary"
        ):
            save_raw_data_to_cloud(uploaded_file, category_to_upload_data_to)

            save_preprocessed_data_to_cloud(df, category_to_upload_data_to)