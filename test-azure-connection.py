import pandas as pd
import os
from src.utils import upload_dataframe_to_azure, read_dataframe_from_azure

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

# Construct the connection string
CONNECTION_STRING = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={AZURE_ACCOUNT_NAME};"
    f"AccountKey={AZURE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)

# --- TRYING OUT UTILS FUNCTION READ_DATAFRAME_FROM_AZURE ---

df_from_csv = read_dataframe_from_azure(
    file_name="observations_humans_via_camera_sensors_on_trails",
    file_format="csv",
    source_folder="raw-data",
)

df_from_parquet = read_dataframe_from_azure(
    file_name="test-upload-with-utils-function",
    file_format="parquet",
    source_folder="test-folder",
)

df_from_excel = read_dataframe_from_azure(
    file_name="03112025-visitor-centers-no-visitors-opening-times-2017-2025.xlsx",
    file_format="xlsx",
    source_folder="raw-data/",
)

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

# FOR BOTH CSV + PARQUET - load function from utils + upload DF to Azure
upload_dataframe_to_azure(
    df=df,
    file_name="test-upload-with-utils-function.csv",
    target_folder="test-folder",
    file_format="csv",
)

upload_dataframe_to_azure(
    df=df,
    file_name="test-upload-with-utils-function",
    target_folder="test-folder/",
    file_format="parquet",
)

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


# Method 2: Write pickled ML models to Azure Blob Storage
import os
import pickle
import io
from azure.storage.blob import BlobClient

def save_model_to_azure_blob(model, container_name: str, folder_prefix: str, uuid: str, model_name: str, connection_string: str) -> None:
    """Pickles a model and uploads it to an Azure Blob Storage container.

    Args:
        model: The ML model object to save.
        container_name (str): The name of the Azure Blob Storage container.
        folder_prefix (str): The virtual folder path within the container (e.g., 'models/prod/').
        model_name (str): The name for the model file (e.g., 'extra_trees_classifier').
        connection_string (str): The connection string for the Azure Storage Account.

    Returns:
        None
    """

    # 1. Construct the full blob name (path in Azure)    
    blob_name = folder_prefix + uuid + "/" + model_name + '.pkl'

    print(f"Preparing to save model {model_name} to Azure container {container_name} with blob name {blob_name}")
    
    # 2. Serialize the model object into an in-memory byte stream (pickle)
    # This avoids saving a temporary file to the local disk.
    buffer = io.BytesIO()
    # Using the standard pickle module for serialization
    pickle.dump(model, buffer)
    buffer.seek(0) # Rewind the buffer to the beginning

    try:
        # 3. Create a BlobClient using the connection string
        blob_client = BlobClient.from_connection_string(
            conn_str=CONNECTION_STRING, 
            container_name=CONTAINER_NAME, 
            blob_name=blob_name
        )

        # 4. Upload the byte stream to Azure
        # upload_blob() takes the byte stream directly. overwrite=True allows updates.
        blob_client.upload_blob(buffer, overwrite=True)
        
        print(f"Successfully saved model {model_name} to Azure Blob Storage at: {container_name}/{blob_name}")
        
    except Exception as e:
        print(f"Error saving model to Azure Blob Storage: {e}")
        
    return

save_model_to_azure_blob(
    model=loaded_models["extra_trees_Rachel-Spiegelau IN"],
    container_name=CONTAINER_NAME,
    folder_prefix="test-folder/models/models_trained/",
    uuid="1483317c-343a-4424-88a6-bd57459901d1",
    model_name="extra_trees_Rachel-Spiegelau IN",
    connection_string=CONNECTION_STRING
)


# --- CATEGORY UTILS ---

# Method 1: List file names in Azure Blob Storage

from azure.storage.blob import BlobServiceClient

def list_files_in_azure_blob(container_name: str, connection_string: str, folder_prefix: str) -> list:
    """Lists all files in an Azure Blob Storage container.

    Args:
        container_name (str): The name of the Azure Blob Storage container.
        connection_string (str): The connection string for the Azure Storage Account.

    Returns:
        list: A list of file names in the container.
    """

    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    blob_list = container_client.list_blob_names(name_starts_with=folder_prefix)

    return [name for name in blob_list]

list_of_test_files = list_files_in_azure_blob(container_name=CONTAINER_NAME, connection_string=CONNECTION_STRING, folder_prefix="test-folder")

print(list_of_test_files)


# Method 2: Check whether an explicit file path exists in Azure Blob Storage

from azure.storage.blob import BlobClient    

test_blob_name = "test-folder/models/models_trained/1483317c-343a-4424-88a6-bd57459901d1/extra_trees_Rachel-Spiegelau IN.pkl"

blob_client = BlobClient.from_connection_string(
    conn_str=CONNECTION_STRING, 
    container_name=CONTAINER_NAME, 
    blob_name=test_blob_name
)

if blob_client.exists():
    print(f"The file {test_blob_name} exists in the container {CONTAINER_NAME} in Azure Blob Storage.")
else:
    print(f"The file {test_blob_name} does not exist in the container {CONTAINER_NAME} in Azure Blob Storage.")