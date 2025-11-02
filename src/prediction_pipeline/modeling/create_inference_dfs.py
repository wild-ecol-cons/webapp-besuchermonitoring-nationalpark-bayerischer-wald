import awswrangler as wr
import pandas as pd
import streamlit as st
import boto3
import joblib
import io
from pycaret.regression import load_model
from sklearn.preprocessing import MinMaxScaler
from src.config import regions, aws_s3_bucket


# Your AWS bucket and folder details where models are stored
folder_prefix = 'models/models_trained/1483317c-343a-4424-88a6-bd57459901d1/'  # If you have a specific folder


target_vars_et  = ['traffic_abs', 'sum_IN_abs', 'sum_OUT_abs', 
                    'Lusen-Mauth-Finsterau IN', 'Lusen-Mauth-Finsterau OUT', 
                    'Nationalparkzentrum Lusen IN', 'Nationalparkzentrum Lusen OUT',
                    'Rachel-Spiegelau IN', 'Rachel-Spiegelau OUT', 
                    'Falkenstein-Schwellhausl IN', 'Falkenstein-Schwellhausl OUT',
                    'Scheuereck-Schachten-Trinkwassertalsperre IN', 'Scheuereck-Schachten-Trinkwassertalsperre OUT', 
                    'Nationalparkzentrum Falkenstein IN', 'Nationalparkzentrum Falkenstein OUT']


# model names 
model_names = [f'extra_trees_{var}' for var in target_vars_et]

@st.cache_resource(max_entries=1)
def load_latest_models(bucket_name, folder_prefix, models_names):
    """
    Load the latest files from an S3 folder based on the model names, 
    and dynamically create variables with 'loaded_' as prefix.

    Parameters:
    - bucket_name (str): The name of the S3 bucket.
    - folder_prefix (str): The folder path within the bucket.
    - models (list): List of model names without the 'extra_trees_' prefix.

    Returns:
    - dict: A dictionary containing the loaded models with keys prefixed by 'loaded_'.
    """

    # Dictionary to store loaded models
    loaded_models = {}

    # Loop through each model to get the latest pickle (.pkl) file
    for model in models_names:
        
        # Create an S3 client
        s3 = boto3.client('s3')

        s3_key = folder_prefix + model + '.pkl'
        print(f"Retrieving the trained model {model} saved under AWS S3 in bucket {bucket_name} with key {s3_key}")

        # Get the object from S3
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)

        # Load the pickled model from the response object using joblib
        loaded_regressor_model = joblib.load(io.BytesIO(response['Body'].read()))
        
        # Store the loaded model in the dictionary
        loaded_models[f'{model}'] = loaded_regressor_model
    
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

    loaded_models = load_latest_models(aws_s3_bucket, folder_prefix, model_names)

    print("Models loaded successfully")
    
    overall_inference_predictions = predict_with_models(loaded_models, inference_data)

    preprocessed_overall_inference_predictions = preprocess_overall_inference_predictions(overall_inference_predictions)

    return preprocessed_overall_inference_predictions

