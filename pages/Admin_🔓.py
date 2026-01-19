# import the necessary libraries
import streamlit as st
from src.streamlit_app.pages_in_dashboard.password import check_password
from src.streamlit_app.pages_in_dashboard.admin.visitor_count import visitor_prediction_graph
from src.streamlit_app.pages_in_dashboard.admin.parking import get_parking_section
from src.streamlit_app.source_data import source_and_preprocess_realtime_parking_data
from src.streamlit_app.pages_in_dashboard.visitors.language_selection_menu import TRANSLATIONS
from src.prediction_pipeline.sourcing_data.source_visitor_center_data import source_preprocessed_hourly_visitor_center_data
from src.prediction_pipeline.modeling.run_inference import run_inference
from datetime import datetime
import pytz

# Initialize language in session state if it doesn't exist
if 'selected_language' not in st.session_state:
    st.session_state.selected_language = 'German'  # Default language

# Title of the page - page layout
st.write(f"# {TRANSLATIONS[st.session_state.selected_language]['admin_page_title']}")

if not check_password(
    type_of_password="admin"
):
    st.stop()  # Do not continue if check_password is not True.

def get_visitor_predictions_section():
    """
    Build the visitor predictions section by running/loading the inference pipeline and displaying the predictions in actual number of visitors.
    """

    preprocessed_hourly_visitor_center_data = source_preprocessed_hourly_visitor_center_data()

    inference_predictions = run_inference(preprocessed_hourly_visitor_center_data)

    visitor_prediction_graph(inference_predictions)


@st.fragment(run_every="15min")
def get_latest_parking_data_and_visualize_it():

    """
    Display the parking section of the dashboard with a map showing the real-time parking occupancy 
    and interactive metrics with actual numbers of visitors. It will update every 15 minutes.
    """
    print("Rendering parking section for the visitor dashboard...")

    def get_current_15min_interval():
        """
        Get the current 15-minute interval in the format "HH:MM:00".

        Returns:
            str: The current 15-minute interval in the format "HH:MM:00".
        """
        current_time = datetime.now(pytz.timezone('Europe/Berlin'))
        minutes = (current_time.minute // 15) * 15
  
        # Replace the minute value with the truncated value and set seconds and microseconds to 0
        timestamp_latest_parking_data_fetch = current_time.replace(minute=minutes, second=0, microsecond=0)

        # If you want to format it as a string in the "%Y-%m-%d %H:%M:%S" format
        timestamp_latest_parking_data_fetch_str = timestamp_latest_parking_data_fetch.strftime("%Y-%m-%d %H:%M:%S")

        return timestamp_latest_parking_data_fetch_str
    
    timestamp_latest_parking_data_fetch = get_current_15min_interval()

    # Source and preprocess the parking data
    processed_parking_data = source_and_preprocess_realtime_parking_data(timestamp_latest_parking_data_fetch)

    get_parking_section(processed_parking_data)

get_visitor_predictions_section()

get_latest_parking_data_and_visualize_it()