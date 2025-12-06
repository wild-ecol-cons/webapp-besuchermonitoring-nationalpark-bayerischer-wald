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
    
    df.to_parquet(
        AZURE_FILE_URL,
        index=False,
        storage_options=storage_options
    )
    
    print("\nSuccessfully saved DataFrame to Azure Blob Storage.")
except Exception as e:
    print(f"\nAn error occurred while writing to Azure Blob Storage: {e}")


# --- CATEGORY EXCEL ---

# Method 1: Read Excel from Azure Blob Storage
FILE_PATH = "raw-data/03112025-visitor-centers-no-visitors-opening-times-2017-2025.xlsx"
AZURE_FILE_URL = f"az://{CONTAINER_NAME}/{FILE_PATH}"

try:
    print(f"Attempting to read Excel File from: {AZURE_FILE_URL}")
    
    df = pd.read_excel(
        AZURE_FILE_URL,
        storage_options=storage_options
    )
    
    # --- 4. Verify Data ---
    print("\nSuccessfully loaded DataFrame:")
    print(df.head())
    print(f"\nDataFrame shape: {df.shape}")

except Exception as e:
    print(f"\nAn error occurred while reading from Azure Blob Storage: {e}")


# --- CATEGORY PICKLED MODELS ---

# Method 1: Read pickled ML models from Azure Blob Storage
import pickle
from azure.storage.blob import BlobClient
import io

# Construct the connection string
CONNECTION_STRING = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={AZURE_ACCOUNT_NAME};"
    f"AccountKey={AZURE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)

def load_latest_models_azure(connection_string, container_name, folder_prefix, models_names):
    """
    Load the models from an Azure Blob Storage container.

    Parameters:
    - connection_string (str): The connection string for the Azure Storage Account.
    - container_name (str): The name of the Blob Storage container.
    - folder_prefix (str): The folder/virtual path prefix within the container.
    - models_names (list): List of model names.

    Returns:
    - dict: A dictionary containing the loaded models.
    """

    # Dictionary to store loaded models
    loaded_models = {}

    # Loop through each model
    for model in models_names:
        
        # Construct the full blob name (key)
        blob_name = folder_prefix + model + '.pkl'
        print(f"Retrieving the trained model {model} saved under Azure container {container_name} with blob name {blob_name}")
        
        # 1. Create a BlobClient
        # This client is used to interact with a specific blob.
        blob_client = BlobClient.from_connection_string(
            conn_str=connection_string, 
            container_name=container_name, 
            blob_name=blob_name
        )
        
        # 2. Download the blob content
        # download_blob() returns a BlobLeaseClient, from which you can read the data.
        download_stream = blob_client.download_blob()
        
        # Read all data into a byte stream
        bytes_data = download_stream.readall()

        # 3. Load the pickled model from the byte stream
        # Using the standard 'pickle' module (or joblib if installed/preferred)
        # We wrap the bytes in io.BytesIO to simulate a file object for pickle.load()
        loaded_model = pickle.load(io.BytesIO(bytes_data))
        
        # Store the loaded model in the dictionary
        loaded_models[f'{model}'] = loaded_model
    
    return loaded_models

# Note: joblib.load() and pickle.load() are both compatible with io.BytesIO.
# For consistency with your original snippet, you could also use:
# loaded_model = joblib.load(io.BytesIO(bytes_data))

loaded_models = load_latest_models_azure(
    connection_string=CONNECTION_STRING,
    container_name=CONTAINER_NAME,
    folder_prefix="models/models_trained/1483317c-343a-4424-88a6-bd57459901d1/",
    models_names=["extra_trees_Rachel-Spiegelau IN"])

print(loaded_models)