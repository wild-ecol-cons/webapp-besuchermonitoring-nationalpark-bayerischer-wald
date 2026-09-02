# import the necessary libraries
import streamlit as st
from src.streamlit_app.pages_in_dashboard.password import check_password
from src.streamlit_app.pages_in_dashboard.admin.visitor_count import visitor_prediction_graph
from src.streamlit_app.source_data import source_and_preprocess_realtime_parking_data
from src.streamlit_app.pages_in_dashboard.visitors.language_selection_menu import TRANSLATIONS
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

    hourly_inference_predictions, daily_inference_predictions = run_inference()

    visitor_prediction_graph(hourly_inference_predictions)

get_visitor_predictions_section()