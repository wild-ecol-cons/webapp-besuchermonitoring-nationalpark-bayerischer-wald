# imports libraries
import streamlit as st
import altair as alt
from src.streamlit_app.pages_in_dashboard.data_accessibility.query_box import get_query_section
from src.streamlit_app.pages_in_dashboard.data_accessibility.upload import upload_section
from src.streamlit_app.pages_in_dashboard.data_accessibility.download import download_section
from src.streamlit_app.pages_in_dashboard.password import check_password

# Initialize language in session state if it doesn't exist
if 'selected_language' not in st.session_state:
    st.session_state.selected_language = 'German'  # Default language

# Define the page layout of the Streamlit app

st.set_page_config(
page_title='Access the park data🌲',
page_icon="🌲",
layout="wide",
initial_sidebar_state="expanded")
alt.themes.enable("dark")

# Password-protect the page
if not check_password(
    type_of_password="admin"
):
    st.stop()  # Do not continue if check_password is not True.

# Define the app layout
col1, col2 = st.columns((1.5,2), gap='medium')
with col1:
    # get_upload_and_download_section()
    st.markdown("## Data Access Point")

    # Tabs for Upload and Download
    tab1, tab2 = st.tabs(["Upload Data", "Download Data"])
    
    with tab1:
        upload_section()
    with tab2:
        download_section()

with col2:
    get_query_section()