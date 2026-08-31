import pandas as pd
import streamlit as st
import joblib
import pickle
import io
import io
from pycaret.regression import load_model
from sklearn.preprocessing import MinMaxScaler
from src.config import regions, CONTAINER_NAME, CONNECTION_STRING
from src.utils import upload_dataframe_to_azure
from azure.storage.blob import BlobClient


# Folder where models are stored
folder_prefix = 'models/models_trained/520593b1-6a90-4f50-8fbe-a5e82d37ee74/'  # If you have a specific folder


target_vars_et  = ['traffic_abs', 'sum_IN_abs', 'sum_OUT_abs', 
                    'Lusen-Mauth-Finsterau IN', 'Lusen-Mauth-Finsterau OUT', 
                    'Nationalparkzentrum Lusen IN', 'Nationalparkzentrum Lusen OUT',
                    'Rachel-Spiegelau IN', 'Rachel-Spiegelau OUT', 
                    'Falkenstein-Schwellhäusl IN', 'Falkenstein-Schwellhäusl OUT',
                    'Scheuereck-Schachten-Trinkwassertalsperre IN', 'Scheuereck-Schachten-Trinkwassertalsperre OUT', 
                    'Nationalparkzentrum Falkenstein IN', 'Nationalparkzentrum Falkenstein OUT']


# model names 
model_names = [f'extra_trees_{var}' for var in target_vars_et]

@st.cache_resource(max_entries=1)
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

        # 3. Load the model from the byte stream using JOBLIB.LOAD
        # Joblib is generally recommended for models with large NumPy arrays (like scikit-learn models).
        # It's highly likely this is how the models were saved.
        
        # We wrap the bytes in io.BytesIO to simulate a file object for joblib.load()
        loaded_model = joblib.load(io.BytesIO(bytes_data))
        
        # Store the loaded model
        loaded_models[f'{model}'] = loaded_model
        
        # Optional: Print the type to confirm the fix
        print(f"Successfully loaded model '{model}'. Type: {type(loaded_model)}")
    
    return loaded_models



def predict_with_models(loaded_models, df_features):
    """
    Given a dictionary of models and a DataFrame of features, this function predicts the target
    values using each model and saves the inference predictions to the cloud (to be further loaded from Streamlit).
    
    Parameters:
    - loaded_models (dict): A dictionary of models where keys are model names and values are the trained models.
    - df_features (pd.DataFrame): A DataFrame containing the features to make predictions on.

    Returns:
    - pd.DataFrame: A DataFrame containing the predictions of all models per region.
    """

    overall_predictions = pd.DataFrame()

    # Iterate through the loaded models
    for model_name, model in loaded_models.items():
        # Check if the model has a predict method
        if hasattr(model, 'predict'):
            # Make predictions
            predictions = model.predict(df_features)
            
            # Create a new DataFrame for the predictions with the time column
            df_predictions = pd.DataFrame(predictions, columns=['predictions'])

            # Make the index column 'Time'
            df_predictions['Time'] = df_features.index

            # Make sure predictions are integers and not floats
            df_predictions['predictions'] = df_predictions['predictions'].astype(int)
    
            # save the prediction dataframe as a parquet file
            upload_dataframe_to_azure(
                df=df_predictions,
                file_name=model_name,
                target_folder="models/inference_data_outputs",
                file_format="parquet"
            )

            print(f"Predictions for {model_name} stored successfully")
            df_predictions["region"] = model_name.split('extra_trees_')[1].split('.parquet')[0]

            # Append the predictions to the overall_predictions DataFrame
            overall_predictions = pd.concat([overall_predictions, df_predictions])

        else:
           print(f"Error: {model_name} is not a valid model. It is of type {type(model)}")
    
    return overall_predictions

def calculate_relative_traffic(df, column, type_of_aggregation):
    # Create a weekly relative traffic column with sklearn min-max scaling
    scaler = MinMaxScaler(feature_range=(0.03, 1))
    df[f'{type_of_aggregation}_relative_traffic_{column}'] = scaler.fit_transform(df[[column]])

    # Create a new column for color coding based on traffic thresholds
    df[f'{type_of_aggregation}_relative_traffic_color_{column}'] = df[f'{type_of_aggregation}_relative_traffic_{column}'].apply(
        lambda x: 'red' if x > 0.40 else 'green' if x < 0.05 else 'blue'
    )
    return df

def preprocess_overall_inference_predictions(overall_predictions: pd.DataFrame) -> pd.DataFrame:
    # Pivot the dataframe to wide format
    hourly_overall_predictions_with_relative_traffic = overall_predictions.pivot(index='Time', columns='region', values='predictions').reset_index()

    # Convert the 'Time' column to datetime format
    hourly_overall_predictions_with_relative_traffic['Time'] = pd.to_datetime(hourly_overall_predictions_with_relative_traffic['Time'], errors='coerce')

    # Create a new column to combine both date and day for radio buttons
    hourly_overall_predictions_with_relative_traffic['day_date'] = hourly_overall_predictions_with_relative_traffic['Time'].dt.strftime('%d-%m-%Y')

    # Calculate the traffic rate per region on an hourly basis
    for key, value in regions.items():
        # Summing up the IN and OUT columns
        hourly_overall_predictions_with_relative_traffic[key] = hourly_overall_predictions_with_relative_traffic[value[0]] + hourly_overall_predictions_with_relative_traffic[value[1]]

        # Calculate the relative traffic rate per region on an hourly basis
        hourly_overall_predictions_with_relative_traffic = calculate_relative_traffic(hourly_overall_predictions_with_relative_traffic, key, 'hourly')

    # Calculate the relative traffic rate per region on a daily basis
    cols_for_aggregation = list(regions.keys()) + ['day_date']
    daily_overall_predictions_with_relative_traffic = hourly_overall_predictions_with_relative_traffic[cols_for_aggregation].groupby('day_date').sum().reset_index()

    for key, value in regions.items():
        daily_overall_predictions_with_relative_traffic = calculate_relative_traffic(daily_overall_predictions_with_relative_traffic, key, 'daily')

    # Convert day_date to datetime and sort
    daily_overall_predictions_with_relative_traffic['day_date'] = pd.to_datetime(daily_overall_predictions_with_relative_traffic['day_date'], format='%d-%m-%Y')
    daily_overall_predictions_with_relative_traffic = daily_overall_predictions_with_relative_traffic.sort_values('day_date')

    return hourly_overall_predictions_with_relative_traffic, daily_overall_predictions_with_relative_traffic


def visitor_predictions(inference_data):

    loaded_models = load_latest_models_azure(
        connection_string=CONNECTION_STRING,
        container_name=CONTAINER_NAME,
        folder_prefix=folder_prefix,
        models_names=model_names
    )

    print("Models loaded successfully")
    
    overall_inference_predictions = predict_with_models(loaded_models, inference_data)

    hourly_inference_predictions, daily_inference_predictions = preprocess_overall_inference_predictions(overall_inference_predictions)

    return hourly_inference_predictions, daily_inference_predictions

