import pandas as pd
from src.config import CONTAINER_NAME, storage_options


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