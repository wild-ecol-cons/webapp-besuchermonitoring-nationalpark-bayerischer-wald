import duckdb
import pandas as pd
import streamlit as st

from src.config import CONTAINER_NAME, storage_options, CONNECTION_STRING
from typing import Dict, Any, Optional, List
from azure.storage.blob import BlobServiceClient


def read_dataframe_from_azure(
    file_name: str,
    file_format: str = "csv",
    source_folder: str = "",
    read_options: Optional[Dict[str, Any]] = None,
    container_name: str = CONTAINER_NAME,
    storage_options: dict = storage_options,
) -> pd.DataFrame:
    """
    Reads a Pandas DataFrame from Azure Blob Storage from a CSV, Parquet, or xlsx file.

    Args:
        file_name (str): The name of the file to read.
        file_format (str, optional): The format of the file to read. Must be 'csv', 'parquet', or 'xlsx'. Defaults to 'csv'.
        source_folder (str, optional): The folder path within the container. Defaults to an empty string.
        read_options (dict, optional): Additional options for the read operation. Defaults to None.
        container_name (str, optional): The name of the container in Azure Blob Storage. Defaults to CONTAINER_NAME.
        storage_options (dict, optional): Options for connecting to Azure Blob Storage. Defaults to storage_options.

    Returns:
        pd.DataFrame: The DataFrame loaded from Azure Blob Storage.

    Raises:
        ValueError: If file_format is not 'csv', 'parquet', or 'xlsx'.
        Exception: If the read operation fails.
    """
    
    read_options = read_options or {} # Ensure read_options is always a dictionary
    
    # Standardize and validate folder path
    if source_folder and not source_folder.endswith("/"):
        source_folder += "/"
        
    # Standardize and validate file format
    file_format = file_format.lower()
    valid_formats = ["csv", "parquet", "xlsx"]
    if file_format not in valid_formats:
        raise ValueError(f"Unsupported file format: {file_format}. Must be one of {valid_formats}.")

    # 2. --- Construct Full Azure URL ---
    # Ensure file_name has the correct extension, or append it
    file_extension = f".{file_format}"
    if not file_name.endswith(file_extension):
        file_name_with_ext = file_name + file_extension
    else:
        file_name_with_ext = file_name
        
    # Construct the full path: az://<container>/<folder>/<file_name>.<ext>
    full_azure_path = f"az://{container_name}/{source_folder}{file_name_with_ext}"

    print(f"\n🔎 Attempting to read DataFrame from: **{full_azure_path}**")

    # 3. --- Read based on format ---
    try:
        if file_format == "csv":
            df = pd.read_csv(
                full_azure_path,
                storage_options=storage_options,
                **read_options
            )
        elif file_format == "parquet":
            df = pd.read_parquet(
                full_azure_path,
                storage_options=storage_options,
                **read_options
            )
        elif file_format == "xlsx":
            df = pd.read_excel(
                full_azure_path,
                storage_options=storage_options,
                **read_options
            )
        
        print(f"✅ Successfully loaded DataFrame from **{file_format.upper()}**.")
        print(f"DataFrame shape: {df.shape}")
        print(df.head())
        return df

    except Exception as e:
        print(f"❌ An error occurred while reading from Azure Blob Storage: {e}")
        raise e


def upload_dataframe_to_azure(
    df: pd.DataFrame,
    file_name: str,
    target_folder: str = "",
    file_format: str = "parquet",
    index: bool = False,
    write_options: Optional[Dict[str, Any]] = None,
    container_name: str = CONTAINER_NAME,
    storage_options: dict = storage_options,
) -> None:
    """
    Uploads a Pandas DataFrame to Azure Blob Storage as either a CSV or Parquet file.

    Args:
        df (pd.DataFrame): The DataFrame to upload.
        file_name (str): The name of the file to upload.
        target_folder (str, optional): The folder path within the container. Defaults to an empty string.
        file_format (FileFormat, optional): The format of the file to upload. Must be 'csv' or 'parquet'. Defaults to 'parquet'.
        write_options (dict, optional): Additional options for the write operation. Defaults to None.
        container_name (str, optional): The name of the container in Azure Blob Storage. Defaults to CONTAINER_NAME.
        storage_options (dict, optional): Options for connecting to Azure Blob Storage. Defaults to storage_options.

    Raises:
        ValueError: If file_format is not 'csv' or 'parquet'.
        Exception: If the upload fails.
    """
    
    write_options = write_options or {} # Ensure write_options is always a dictionary

    # Standardize and validate folder path
    if target_folder and not target_folder.endswith("/"):
        target_folder += "/"
        
    # Standardize and validate file format
    file_format = file_format.lower()
    if file_format not in ["csv", "parquet"]:
        raise ValueError(f"Unsupported file format: {file_format}. Must be 'csv' or 'parquet'.")

    # 2. --- Construct Full Azure URL ---
    # Ensure file_name has the correct extension, or append it
    file_extension = f".{file_format}"
    if not file_name.endswith(file_extension):
        file_name_with_ext = file_name + file_extension
    else:
        file_name_with_ext = file_name
        
    # Construct the full path: az://<container>/<folder>/<file_name>.<ext>
    full_azure_path = f"az://{container_name}/{target_folder}{file_name_with_ext}"
    
    print(f"\n🚀 Attempting to upload DataFrame to: **{full_azure_path}**")

    # 3. --- Upload based on format ---
    try:
        if file_format == "csv":
            df.to_csv(
                full_azure_path,
                index=index,
                storage_options=storage_options,
                **write_options
            )
        elif file_format == "parquet":
            df.to_parquet(
                full_azure_path,
                index=index,
                storage_options=storage_options,
                **write_options
            )
            
        print(f"✅ Successfully saved DataFrame as **{file_format.upper()}** to Azure Blob Storage.")

    except Exception as e:
        print(f"❌ An error occurred while writing to Azure Blob Storage: {e}")
        raise e
    

def upload_file_to_azure(file_obj: object, target_folder: str, filename: str) -> bool:
    """
    Uploads a file object to Azure Blob Storage.

    Args:
        file_obj (object): The file object to upload.
        target_folder (str): The folder path within the container.
        filename (str): The name of the file to upload (including the file extension).

    Returns:
        bool: True if the upload is successful, False otherwise.
    """
    
    # Standardize and validate folder path
    if target_folder and not target_folder.endswith("/"):
        target_folder += "/"
    
    try:
        # Create the BlobServiceclient
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=f"{target_folder}{filename}")

        # Upload the file object directly
        blob_client.upload_blob(file_obj, overwrite=True)
        return True
    except Exception as e:
        st.error(f"An error occurred when trying to upload the file: {e}")
        return False
    

def set_up_duck_db_connection() -> duckdb.DuckDBPyConnection:
    """
    Set up a DuckDB connection via files on Azure Blob Storage to a transient in-memory database. This connection is used to query data with SQL queries.

    Returns:
        duckdb.DuckDBPyConnection: A DuckDB connection
    """
    # Connect to a transient in-memory DuckDB
    conn = duckdb.connect(database=':memory:')

    # Install and load the Azure extension (one-time per session)
    conn.execute("INSTALL azure; LOAD azure;")

    # Authenticate with Azure
    secret_query = f"""
    CREATE OR REPLACE SECRET (
        TYPE AZURE,
        CONNECTION_STRING '{CONNECTION_STRING}'
    );
    """
    conn.execute(secret_query)

    # Set parameter to solve certificate issue
    conn.execute("SET azure_transport_option_type = 'curl';")
    return conn

def query_azure_with_duck_db(
    directory: str,
    columns: List[str] = ["*"],
    filters: Optional[str] = None,
    limit: Optional[int] = 10,
    select_string: Optional[str] = None,
    order_by: Optional[str] = None
) -> pd.DataFrame:
    """
    Queries Parquet files on Azure with optional filtering and selection.

    Args:
        directory (str): The directory path within the container
        columns (List[str], optional): A list of column names to select. Defaults to ["*"].
        filters (Optional[str], optional): An SQL WHERE clause to apply to the query. Defaults to None.
        limit (Optional[int], optional): The maximum number of rows to return. Defaults to 10.
        select_string (Optional[str], optional): A custom SQL SELECT clause to use. Defaults to None.
        order_by (Optional[str], optional): An SQL ORDER BY clause to apply to the query. Defaults to None.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the results of the query
    """
    conn = set_up_duck_db_connection()
    
    if select_string:
        col_selector = select_string
    else:
        # 1. Construct the column string
        col_selector = ", ".join(columns)
    
    # 2. Base URL
    path = f"az://webapp-besuchermonitoring-data-dev/{directory}/*.parquet"
    
    # 3. Build the SQL string dynamically
    query = f"SELECT {col_selector} FROM read_parquet('{path}')"
    
    # 4. Append WHERE clause if filters are provided
    if filters:
        query += f" WHERE {filters}"

    if order_by:
        query += f" ORDER BY {order_by}"
    
    # 6. Append LIMIT
    if limit:
        query += f" LIMIT {limit}"
    
    try:
        return conn.execute(query).df()
    except Exception as e:
        print(f"Query failed: {e}")
        return pd.DataFrame()