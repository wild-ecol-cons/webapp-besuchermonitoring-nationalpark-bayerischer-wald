import awswrangler as wr
import pandas as pd
import streamlit as st
import boto3
import joblib
import io
from pycaret.regression import load_model
from sklearn.preprocessing import MinMaxScaler
from src.config import regions, aws_s3_bucket
import os
import random

# change this to change the model to be used for inference
chosen_uuid = "1483317c-343a-4424-88a6-bd57459901d1"

target_vars_et  = ['traffic_abs', 'sum_IN_abs', 'sum_OUT_abs', 
                    'Lusen-Mauth-Finsterau IN', 'Lusen-Mauth-Finsterau OUT', 
                    'Nationalparkzentrum Lusen IN', 'Nationalparkzentrum Lusen OUT',
                    'Rachel-Spiegelau IN', 'Rachel-Spiegelau OUT', 
                    'Falkenstein-Schwellhäusl IN', 'Falkenstein-Schwellhäusl OUT',
                    'Scheuereck-Schachten-Trinkwassertalsperre IN', 'Scheuereck-Schachten-Trinkwassertalsperre OUT', 
                    'Nationalparkzentrum Falkenstein IN', 'Nationalparkzentrum Falkenstein OUT']


# model names 
model_names = [f'extra_trees_{var}' for var in target_vars_et]

def get_model_folder_path():
    models_base_folder = "models"
    # Change the folder uuid according to which we want to train or pick any folder uuid
    models_folder = os.path.join("models",chosen_uuid)

    # Check if the specified folder exists
    if not os.path.isdir(models_folder):
        print(f"Specified model folder '{models_folder}' not found. Selecting the last modified folder.")

        # Get all subdirectories in models_base_folder
        subdirs = [
            os.path.join(models_base_folder, d)
            for d in os.listdir(models_base_folder)
            if os.path.isdir(os.path.join(models_base_folder, d))
        ]

        if not subdirs:
            raise FileNotFoundError("No model folders found in the 'models' directory.")

        # Find the most recently modified subdirectory
        models_folder = max(subdirs, key=os.path.getmtime)
        print(f"Using last modified model folder: {models_folder}")
    else:
        print(f"Using specified model folder: {models_folder}")

    return models_folder

@st.cache_resource(max_entries=1)
def load_latest_models_local(model_folder, models_names):
    """
    Load the model pickle files from a local folder.

    Parameters:
    - local_folder_path (str): The path to the local folder containing the model files.
    - models_names (list): List of model file names without the '.pkl' extension.

    Returns:
    - dict: A dictionary containing the loaded models with model names as keys.
    """

    loaded_models = {}

    for model in models_names:
        file_path = os.path.join(model_folder, model + '.pkl')
        print(f"Loading model '{model}' from: {file_path}")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Model file not found: {file_path}")

        loaded_model = joblib.load(file_path)
        loaded_models[model] = loaded_model

    return loaded_models



def predict_with_models(loaded_models, df_features):
    """
    Given a dictionary of models and a DataFrame of features, this function predicts the target
    values using each model and saves the inference predictions to AWS S3 (to be further loaded from Streamlit).
    
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
    
            # save the prediction dataframe as a parquet file in aws
            wr.s3.to_parquet(df_predictions,path = f"s3://{aws_s3_bucket}/models/inference_data_outputs/{model_name}.parquet")

            print(f"Predictions for {model_name} stored successfully")
            df_predictions["region"] = model_name.split('extra_trees_')[1].split('.parquet')[0]

            # Append the predictions to the overall_predictions DataFrame
            overall_predictions = pd.concat([overall_predictions, df_predictions])

        else:
           print(f"Error: {model_name} is not a valid model. It is of type {type(model)}")
    
    return overall_predictions

@st.cache_data(max_entries=1)
def preprocess_overall_inference_predictions(overall_predictions: pd.DataFrame) -> pd.DataFrame:
    # Pivot the dataframe to wide format
    overall_predictions_wide = overall_predictions.pivot(index='Time', columns='region', values='predictions').reset_index()

    # Convert the 'Time' column to datetime format
    overall_predictions_wide['Time'] = pd.to_datetime(overall_predictions_wide['Time'], errors='coerce')

    # Create a new column to combine both date and day for radio buttons
    overall_predictions_wide['day_date'] = overall_predictions_wide['Time'].dt.strftime('%d-%m-%Y')

        # Calculate the traffic rate per region
    for key, value in regions.items():
        overall_predictions_wide[key] = overall_predictions_wide[value[0]] + overall_predictions_wide[value[1]]

        # Create a weekly relative traffic column with sklearn min-max scaling
        scaler = MinMaxScaler()
        overall_predictions_wide[f'weekly_relative_traffic_{key}'] = scaler.fit_transform(overall_predictions_wide[[key]])

        # Create a new column for color coding based on traffic thresholds
        overall_predictions_wide[f'traffic_color_{key}'] = overall_predictions_wide[f'weekly_relative_traffic_{key}'].apply(
            lambda x: 'red' if x > 0.40 else 'green' if x < 0.05 else 'blue'
        )

    return overall_predictions_wide


@st.cache_data(max_entries=1)
def visitor_predictions(inference_data):
    model_folder = get_model_folder_path()
    loaded_models = load_latest_models_local(model_folder, model_names)

    print("Models loaded successfully")
    
    overall_inference_predictions = predict_with_models(loaded_models, inference_data)

    preprocessed_overall_inference_predictions = preprocess_overall_inference_predictions(overall_inference_predictions)

    return preprocessed_overall_inference_predictions

