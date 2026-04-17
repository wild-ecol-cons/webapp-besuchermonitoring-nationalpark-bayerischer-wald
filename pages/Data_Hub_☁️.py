# imports libraries
import streamlit as st
from datetime import datetime
from src.config import data_upload_categories_to_azure_folders
from src.streamlit_app.pages_in_dashboard.password import check_password
from src.streamlit_app.pages_in_dashboard.visitors.language_selection_menu import TRANSLATIONS
from src.utils import upload_dataframe_to_azure
from src.streamlit_app.pages_in_dashboard.data_hub.query_and_download_data import get_min_date_from_queried_data, query_and_preprocess_data, log_queried_data_to_azure
from src.streamlit_app.pages_in_dashboard.data_hub.upload_data import retrieve_already_existing_features, process_and_validate_upload, warning_for_new_columns, save_raw_data_to_cloud, save_preprocessed_data_to_cloud


# Initialize language in session state if it doesn't exist
if 'selected_language' not in st.session_state:
    st.session_state.selected_language = 'German'  # Default language

# Define the page layout of the Streamlit app

st.set_page_config(
page_title=TRANSLATIONS[st.session_state.selected_language]['page_title_data_hub'],
page_icon="🌲",
layout="wide",
initial_sidebar_state="expanded")

# Password-protect the page
if not check_password(
    type_of_password="admin"
):
    st.stop()  # Do not continue if check_password is not True.

st.markdown("## Data Hub ☁️")

st.markdown(
    TRANSLATIONS[st.session_state.selected_language]['page_description_data_hub']
)

# Tabs for Upload and Download
tab_query_download_data, tab_upload_data = st.tabs([TRANSLATIONS[st.session_state.selected_language]['query_tab_data_hub'], TRANSLATIONS[st.session_state.selected_language]['upload_tab_data_hub']])

with tab_query_download_data:
    # Select one or multiple data categories
    available_data_categories = st.multiselect(
        TRANSLATIONS[st.session_state.selected_language]['selected_data_categories'],
        data_upload_categories_to_azure_folders.keys(),
        placeholder=TRANSLATIONS[st.session_state.selected_language]['choose_data_categories']
    )

    # Select entire timeframe or a specific start and end date
    ## Checkbox: All data?
    specify_timerange = st.toggle(TRANSLATIONS[st.session_state.selected_language]['specify_time_range'])

    ## time Selection
    if specify_timerange:
        # Get the first selectable date (the earliest date available) 
        first_selectable_date = get_min_date_from_queried_data(data_categories=available_data_categories)

        col_left, col_right = st.columns(2)

        with col_left:
            start_time = st.datetime_input(
                TRANSLATIONS[st.session_state.selected_language]['start_time'],
                value=None,
                min_value=first_selectable_date,
                max_value="now",
                step=3600
            )

        with col_right:
            end_time = st.datetime_input(
                TRANSLATIONS[st.session_state.selected_language]['end_time'],
                value=None,
                min_value=first_selectable_date,
                max_value="now",
                step=3600
            )
    else:
        start_time = None
        end_time = None

    # Button to query data
    if st.button(
        label=TRANSLATIONS[st.session_state.selected_language]['button_query_data'],
        help=TRANSLATIONS[st.session_state.selected_language]['query_button_hover_text'],
        type="primary"
    ):
        with st.spinner(TRANSLATIONS[st.session_state.selected_language]['spinner_msg_querying_data'], show_time=True):
            overall_queried_data = query_and_preprocess_data(
                data_categories_to_query=available_data_categories,
                specify_timerange=specify_timerange,
                start_time=start_time,
                end_time=end_time
            )
            
        # Preview queried data before download
        st.markdown(f"#### {TRANSLATIONS[st.session_state.selected_language]['preview_data_title']}")
        st.dataframe(overall_queried_data.head())

        file_name_data_export = log_queried_data_to_azure(queried_data=overall_queried_data)

        st.download_button(
            label=TRANSLATIONS[st.session_state.selected_language]['button_download_data'],
            data=overall_queried_data.to_csv(index=False).encode('utf-8'),
            file_name=file_name_data_export,
            icon=":material/download:",
        )

with tab_upload_data:

    # Select category that it is being uploaded to
    category_to_upload_data_to = st.radio(
    TRANSLATIONS[st.session_state.selected_language]['title_data_upload_data_category_selection'],
    [
        "Permanente Besucherzählung (Eco-Counter)",
        "Hütten: Zählungen, Wetterstationsdaten,Öffnungszeiten & Feiertage",
        "Sonderzählungen",
    ],
    captions=[
        TRANSLATIONS[st.session_state.selected_language]['upload_tip_1_hour_frequency'],
        TRANSLATIONS[st.session_state.selected_language]['upload_tip_1_day_frequency'],
        TRANSLATIONS[st.session_state.selected_language]['upload_tip_1_hour_frequency'],
    ],
    )

    st.warning(TRANSLATIONS[st.session_state.selected_language]['warning_time_col_in_berlin_timezone'], icon="⚠️")
    
    already_existing_columns = retrieve_already_existing_features(category_to_upload_data_to)

    # Select local file to upload
    uploaded_files = st.file_uploader(
        TRANSLATIONS[st.session_state.selected_language]['data_upload_title'],
        accept_multiple_files=True,
        type=["csv", "xlsx", "xls"]
    )
    for uploaded_file in uploaded_files:
        
        df = process_and_validate_upload(uploaded_file, category_to_upload_data_to)

        warning_for_new_columns(df, already_existing_columns)

        # Preview file before upload
        st.markdown(f"#### Preview: `{uploaded_file.name}`")
        st.dataframe(df.head())

        # Confirm upload
        if st.button(
            TRANSLATIONS[st.session_state.selected_language]['button_upload_data'],
            key=f"data_upload_button_{uploaded_file.file_id}",
            type="primary"
        ):
            save_raw_data_to_cloud(uploaded_file, category_to_upload_data_to)

            save_preprocessed_data_to_cloud(df, category_to_upload_data_to, uploaded_file)