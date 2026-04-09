import pandas as pd
import streamlit as st
from datetime import datetime
from src.utils import query_azure_with_duck_db, upload_dataframe_to_azure, upload_file_to_azure
from src.prediction_pipeline.pre_processing.preprocess_historic_visitor_count_data import parse_german_dates
from src.config import data_upload_categories_to_azure_folders, data_upload_categories_time_cols_freq


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

def save_preprocessed_data_to_cloud(preprocessed_data: pd.DataFrame, category_to_upload_data_to: str, uploaded_file: object) -> None:
    """
    Save preprocessed data to Azure Cloud with adding the current timestamp as a new feature.
    
    Args:
        preprocessed_data (pd.DataFrame): Preprocessed data to upload
        category_to_upload_data_to (str): Category to upload data to
        uploaded_file (object): Uploaded file
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

def validate_time_frequency(df, time_col, freq_string, category, uploaded_file):
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
            uploaded_file=uploaded_file.file_id
        )

        # Drop entirely empty columns
        preprocessed_df.dropna(how="all", axis="columns", inplace=True)

        # Save time column as general time index
        preprocessed_df["general_time_index"] = preprocessed_df[time_col]

        return preprocessed_df