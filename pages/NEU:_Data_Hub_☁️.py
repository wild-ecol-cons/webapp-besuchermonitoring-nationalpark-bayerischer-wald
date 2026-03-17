# imports libraries
import streamlit as st
from datetime import datetime
from src.config import data_upload_categories_to_azure_folders
from src.streamlit_app.pages_in_dashboard.password import check_password
from src.streamlit_app.pages_in_dashboard.visitors.language_selection_menu import TRANSLATIONS
from src.utils import upload_dataframe_to_azure
from src.streamlit_app.pages_in_dashboard.data_hub.query_and_download_data import get_min_date_from_queried_data, query_and_preprocess_data
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

# get_upload_and_download_section()
st.markdown("## Data Hub ☁️")

st.markdown(
    TRANSLATIONS[st.session_state.selected_language]['page_description_data_hub']
)

# Tabs for Upload and Download
tab_query_download_data, tab_upload_data = st.tabs(["Query/Download Data", "Upload Data"])

with tab_query_download_data:
    # Select one or multiple data categories
    available_data_categories = st.multiselect(
        "Ausgewählte Datenkategorien:",
        data_upload_categories_to_azure_folders.keys(),

            # "Schulferien & Feiertage (BY & CZ)", # TODO: Add this at the end of the project if time allows
            # "Parkplatzzählungen", # TODO: First focus on manually collected data, then on this API-fetched data
            # "Wetterdaten" # TODO: First focus on manually collected data, then on this API-fetched data
    )

    # Select entire timeframe or a specific start and end date
    ## Checkbox: All data?
    specify_timerange = st.toggle("Zeitraum eingrenzen")

    ## time Selection
    if specify_timerange:
        # Get the first selectable date (the earliest date available) 
        first_selectable_date = get_min_date_from_queried_data(data_categories=available_data_categories)

        col_left, col_right = st.columns(2)

        with col_left:
            start_time = st.datetime_input(
                "Start:",
                value=None,
                min_value=first_selectable_date,
                max_value="now",
                step=3600
            )

        with col_right:
            end_time = st.datetime_input(
                "Ende:",
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
        label="Query Data",
        help="Query the data based on the selected data categories and timeframe.",
        type="primary"
    ):

        overall_queried_data = query_and_preprocess_data(
            data_categories_to_query=available_data_categories,
            specify_timerange=specify_timerange,
            start_time=start_time,
            end_time=end_time
        )
        
        # Preview queried data before download
        st.markdown(f"#### Preview der Daten")
        st.dataframe(overall_queried_data.head())

        data_download_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file_name_data_export = f"{data_download_time}_Data_Hub_Datenexport.csv"

        # Button to download data
        if st.download_button(
            label="Download Data",
            data=overall_queried_data.to_csv(index=False).encode('utf-8'),
            file_name=file_name_data_export,
            icon=":material/download:",
        ):

            upload_dataframe_to_azure(
                df=overall_queried_data,
                file_name=file_name_data_export,
                target_folder="data-hub/exported-data",
                file_format="csv",
            )

with tab_upload_data:

    st.markdown("## Upload Data")

    # Select category that it is being uploaded to
    category_to_upload_data_to = st.radio(
    "Wähle die Datenkategorie aus:",
    [
        "Permanente Besucherzählung (Eco-Counter)",
        "Hütten: Zählungen, Wetterstationsdaten,Öffnungszeiten & Feiertage",
        "Sonderzählungen",
    ],
    )
    
    already_existing_columns = retrieve_already_existing_features(category_to_upload_data_to)

    # Select local file to upload
    uploaded_files = st.file_uploader(
    "Upload data", accept_multiple_files=True, type=["csv", "xlsx", "xls"]
    )
    for uploaded_file in uploaded_files:
        
        df = process_and_validate_upload(uploaded_file, category_to_upload_data_to)

        warning_for_new_columns(df, already_existing_columns)

        # Preview file before upload
        st.markdown(f"#### Preview: `{uploaded_file.name}`")
        st.dataframe(df.head())

        # Confirm upload
        if st.button(
            "Upload Data",
            key=f"data_upload_button_{uploaded_file.file_id}",
            type="primary"
        ):
            save_raw_data_to_cloud(uploaded_file, category_to_upload_data_to)

            save_preprocessed_data_to_cloud(df, category_to_upload_data_to)