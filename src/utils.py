import pandas as pd
from src.config import CONTAINER_NAME, storage_options
from typing import Dict, Any, Optional

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
        container_name (str, optional): The name of the container in Azure Blob Storage. Defaults to CONTAINER_NAME.
        storage_options (dict, optional): Options for connecting to Azure Blob Storage. Defaults to storage_options.

    Raises:
        ValueError: If file_format is not 'csv' or 'parquet'.
        Exception: If the upload fails.
    """
    
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
                index=False,
                storage_options=storage_options
            )
        elif file_format == "parquet":
            df.to_parquet(
                full_azure_path,
                index=False,
                storage_options=storage_options,
            )
            
        print(f"✅ Successfully saved DataFrame as **{file_format.upper()}** to Azure Blob Storage.")

    except Exception as e:
        print(f"❌ An error occurred while writing to Azure Blob Storage: {e}")
        raise e