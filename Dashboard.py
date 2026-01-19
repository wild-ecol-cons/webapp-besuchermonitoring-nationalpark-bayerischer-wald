import streamlit as st
from PIL import Image
import pandas as pd
from datetime import datetime
import pytz

# get the streamlit app modules
import src.streamlit_app.pages_in_dashboard.visitors.page_layout_config as page_layout_config
import src.streamlit_app.pages_in_dashboard.visitors.language_selection_menu as lang_sel_menu
import src.streamlit_app.pages_in_dashboard.visitors.weather as weather
import src.streamlit_app.pages_in_dashboard.visitors.parking as parking 
import src.streamlit_app.pages_in_dashboard.visitors.visitor_count as visitor_count
import src.streamlit_app.pages_in_dashboard.visitors.recreational_activities as recreation
import src.streamlit_app.pages_in_dashboard.visitors.other_information as other_info
from src.streamlit_app.pages_in_dashboard.visitors.language_selection_menu import TRANSLATIONS
from src.streamlit_app.pages_in_dashboard.password import check_password

# imports for the sourcing and preprocessing pipeline
from src.prediction_pipeline.sourcing_data.source_visitor_center_data import source_preprocessed_hourly_visitor_center_data
from src.prediction_pipeline.sourcing_data.source_historic_visitor_count import source_historic_visitor_count 
from src.prediction_pipeline.pre_processing.preprocess_historic_visitor_count_data import preprocess_visitor_count_data
from src.prediction_pipeline.sourcing_data.source_visitor_center_data import source_visitor_center_data
from src.prediction_pipeline.pre_processing.preprocess_visitor_center_data import process_visitor_center_data
from src.prediction_pipeline.sourcing_data.source_weather import source_weather_data
from src.prediction_pipeline.pre_processing.preprocess_weather_data import process_weather_data
from src.prediction_pipeline.pre_processing.join_sensor_weather_visitorcenter import get_joined_dataframe
from src.prediction_pipeline.pre_processing.features_zscoreweather_distanceholidays import get_zscores_and_nearest_holidays

# imports for training pipeline
from src.prediction_pipeline.modeling.source_and_feature_selection import get_features
from src.prediction_pipeline.modeling.train_regressor import train_regressor

# imports for inference pipeline
from src.prediction_pipeline.modeling.run_inference import run_inference

# Initialize language in session state if it doesn't exist
if 'selected_language' not in st.session_state:
    st.session_state.selected_language = 'German'  # Default language

# Set the page layout - it is a two column layout
col1, col2 = page_layout_config.get_page_layout()

def create_dashboard_main_page(inference_predictions):

    """
    Creates the dashboard page for the Bavarian Forest National Park visitor information. This includes the visitor count, parking, weather, recreation, and other information.

    Args:
        inference_predictions (pd.DataFrame): The inference predictions for region-wise visitor counts.
    """
    
    with col1:

        # Display the logo and title of the column
        logo = Image.open("src/streamlit_app/assets/logo-bavarian-forest-national-park.png")
        st.image(logo, width=300)
        st.title(TRANSLATIONS[st.session_state.selected_language]['title'])

        # Get the visitor count section
        visitor_count.get_visitor_counts_section(inference_predictions)

        # get the parking section
        parking.get_parking_section()


    with col2:
        # get the language selection menu
        lang_sel_menu.get_language_selection_menu()
        
        # get the weather section
        weather.get_weather_section()
        

        # create recreational section
        recreation.get_recreation_section()

        # Get the other information section
        other_info.get_other_information()


def run_training():

    """
    Runs the training pipeline. This includes sourcing and preprocessing the data, training the model, and saving the model.
    """

    # source and preprocess the historic visitor count data
    sourced_visitor_count_df = source_historic_visitor_count()
    processed_visitor_count_df = preprocess_visitor_count_data(sourced_visitor_count_df)

    # source and preprocess the visitor center data
    sourced_vc_data_df = source_visitor_center_data()
    processed_vc_df_hourly,_ = process_visitor_center_data(sourced_vc_data_df)

     # get the weather data for training and inference
    # training data
    train_start_date = datetime(2023, 1, 1)
    train_end_date = datetime(2024, 7, 21)
    weather_data = source_weather_data(start_time=train_start_date, end_time=train_end_date)
    processed_weather_df = process_weather_data(weather_data)

    # join the dataframes
    joined_df = get_joined_dataframe(processed_weather_df, processed_visitor_count_df, processed_vc_df_hourly)

    # Feature engineering: add features such as zscore weather features and nearest holidays
    weather_columns_for_zscores = [ 'Temperature (°C)','Relative Humidity (%)','Wind Speed (km/h)']
    with_zscores_and_nearest_holidays_df = get_zscores_and_nearest_holidays(joined_df, weather_columns_for_zscores)

    # get the features for training
    feature_df = get_features(with_zscores_and_nearest_holidays_df,train_start_date, train_end_date)

    # train the model
    train_regressor(feature_df)


if __name__ == "__main__":

    # Password-protect the page
    if not check_password(
        type_of_password="general_access"
    ):
        st.stop()  # Do not continue if check_password is not True.

    preprocessed_hourly_visitor_center_data = source_preprocessed_hourly_visitor_center_data()

    # call the sourcing and processing pipeline
    inference_predictions = run_inference(preprocessed_hourly_visitor_center_data)

    # create the dashboard
    create_dashboard_main_page(inference_predictions)