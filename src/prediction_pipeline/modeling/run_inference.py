import pytz
import pandas as pd
import streamlit as st
from datetime import datetime

# imports for inference dataframe
from src.prediction_pipeline.modeling.preprocess_inference_features import source_preprocess_inference_data
from src.prediction_pipeline.modeling.create_inference_dfs import visitor_predictions
from src.prediction_pipeline.sourcing_data.source_weather import source_weather_data
from src.prediction_pipeline.sourcing_data.source_temporal_features import source_temporal_features
from src.prediction_pipeline.pre_processing.preprocess_temporal_features import process_temporal_features


@st.fragment(run_every="3h")
def run_inference():

    """
    Run the inference pipeline. Fetches the latest weather forecasts, preprocesses data, and makes predictions.

    Returns:
        None
    """

    # get the weather data for inference
    def get_today_midnight_berlin():
        # Set the timezone to Berlin (CET or CEST)
        berlin_tz = pytz.timezone('Europe/Berlin')
        
        # Get the current time in Berlin
        now_berlin = datetime.now(berlin_tz)
        
        # Replace the hour, minute, second, and microsecond with 0 to get today at 00:00
        day_today_berlin = now_berlin.date()

        # Convert day_today_berlin to datetime
        datetime_today_berlin = datetime.combine(day_today_berlin, datetime.min.time())
        
        return datetime_today_berlin, day_today_berlin

    time_now, today = get_today_midnight_berlin()
    start_inference_time = time_now - pd.Timedelta(days=10)
    end_inference_time = time_now + pd.Timedelta(days=7)
    print(f"Running inference part from {start_inference_time} to {end_inference_time}...")

    # Fetch and preprocess the temporal features
    temporal_features_df = source_temporal_features(
        start_date=today,
        end_date=end_inference_time.date()
    )
    processed_temporal_features_df = process_temporal_features(temporal_features_df)

    # Fetch the weather data
    weather_data_inference = source_weather_data(start_time=start_inference_time, end_time=end_inference_time)
    print(f"The overall weather_data_inference is: {weather_data_inference}")

    # preprocess the inference data
    inference_df = source_preprocess_inference_data(weather_data_inference, processed_temporal_features_df, start_time=time_now, end_time=end_inference_time)

    print(f"The overall inference_df is: {inference_df}")

    # make predictions
    hourly_inference_predictions, daily_inference_predictions = visitor_predictions(inference_df) 

    return hourly_inference_predictions, daily_inference_predictions