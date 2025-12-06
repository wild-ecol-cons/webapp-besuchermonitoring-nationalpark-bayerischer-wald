import pandas as pd
import os

# Get Azure account name and key from environment variables
AZURE_ACCOUNT_NAME = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_ACCOUNT_KEY = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")

# Define Azure Blob Storage configuration
storage_options = {
    "account_name": AZURE_ACCOUNT_NAME,
    "account_key": AZURE_ACCOUNT_KEY
}

# Define Azure Blob Storage container name
CONTAINER_NAME = "webapp-besuchermonitoring-data-dev"

# --- CATEGORY CSV ---

# Method 1: Read CSV from Azure Blob Storage
FILE_PATH = "raw-data/observations_humans_via_camera_sensors_on_trails.csv"
AZURE_FILE_URL = f"az://{CONTAINER_NAME}/{FILE_PATH}"

try:
    print(f"Attempting to read CSV from: {AZURE_FILE_URL}")
    
    df = pd.read_csv(
        AZURE_FILE_URL,
        storage_options=storage_options
    )
    
    # --- 4. Verify Data ---
    print("\nSuccessfully loaded DataFrame:")
    print(df.head())
    print(f"\nDataFrame shape: {df.shape}")

except Exception as e:
    print(f"\nAn error occurred while reading from Azure Blob Storage: {e}")

# Method 2: Write CSV to Azure Blob Storage
FILE_PATH = "test-folder/observations_humans_via_camera_sensors_on_trails.csv"
AZURE_FILE_URL = f"az://{CONTAINER_NAME}/{FILE_PATH}"

try:
    print(f"\nAttempting to write CSV to: {AZURE_FILE_URL}")
    
    df.to_csv(
        AZURE_FILE_URL,
        index=False,
        storage_options=storage_options
    )
    
    print("\nSuccessfully saved DataFrame to Azure Blob Storage.")
except Exception as e:
    print(f"\nAn error occurred while writing to Azure Blob Storage: {e}")


# --- CATEGORY PARQUET ---

# Method 1: Read Parquet from Azure Blob Storage
FILE_PATH = "preprocessed_data/visitor_centers_hourly_2017_to_2025.parquet"
AZURE_FILE_URL = f"az://{CONTAINER_NAME}/{FILE_PATH}"

try:
    print(f"Attempting to read Parquet File from: {AZURE_FILE_URL}")
    
    df = pd.read_parquet(
        AZURE_FILE_URL,
        storage_options=storage_options
    )
    
    # --- 4. Verify Data ---
    print("\nSuccessfully loaded DataFrame:")
    print(df.head())
    print(f"\nDataFrame shape: {df.shape}")

except Exception as e:
    print(f"\nAn error occurred while reading from Azure Blob Storage: {e}")

# Method 2: Write Parquet File to Azure Blob Storage
FILE_PATH = "test-folder/visitor_centers_hourly_2017_to_2025.parquet"
AZURE_FILE_URL = f"az://{CONTAINER_NAME}/{FILE_PATH}"

try:
    print(f"\nAttempting to write Parquet File to: {AZURE_FILE_URL}")
    
    df.to_csv(
        AZURE_FILE_URL,
        index=False,
        storage_options=storage_options
    )
    
    print("\nSuccessfully saved DataFrame to Azure Blob Storage.")
except Exception as e:
    print(f"\nAn error occurred while writing to Azure Blob Storage: {e}")