import pytz
import pandas as pd
import streamlit as st
from datetime import datetime

# imports for inference dataframe
from src.prediction_pipeline.modeling.preprocess_inference_features import source_preprocess_inference_data
from src.prediction_pipeline.modeling.create_inference_dfs import visitor_predictions
from src.prediction_pipeline.sourcing_data.source_weather import source_weather_data


@st.fragment(run_every="3h")
def run_inference(preprocessed_hourly_visitor_center_data):

    """
    Run the inference pipeline. Fetches the latest weather forecasts, preprocesses data, and makes predictions.

    Args:
        preprocessed_hourly_visitor_center_data (pd.DataFrame): The preprocessed hourly visitor center data.

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
        day_today_berlin = datetime.combine(day_today_berlin, datetime.min.time())
        
        return day_today_berlin

    today = get_today_midnight_berlin()
    start_inference_time = today - pd.Timedelta(days=10)
    end_inference_time = today + pd.Timedelta(days=7)
    print(f"Running inference part from {start_inference_time} to {end_inference_time}...")

    weather_data_inference = source_weather_data(start_time=start_inference_time, end_time=end_inference_time)

    print(f"The overall weather_data_inference is: {weather_data_inference}")

    # preprocess the inference data
    inference_df = source_preprocess_inference_data(weather_data_inference, preprocessed_hourly_visitor_center_data, start_time=today, end_time=end_inference_time)

    print(f"The overall inference_df is: {inference_df}")

    # make predictions
    overall_visitor_predictions = visitor_predictions(inference_df) 

    return overall_visitor_predictions