# imports libraries
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from numpy.random import default_rng as rng
from src.streamlit_app.pages_in_dashboard.password import check_password
from src.streamlit_app.pages_in_dashboard.visitors.language_selection_menu import TRANSLATIONS
from src.prediction_pipeline.pre_processing.preprocess_historic_visitor_count_data import parse_german_dates

data_upload_categories_time_cols_freq = {
    "Permanente Besucherzählung (Eco-Counter)": {"col": "Time", "freq": "1 hour"},
    "Hütten: Zählungen, Wetterstationsdaten,Öffnungszeiten & Feiertage": {"col": "Datum", "freq": "1 day"},
    "Sonderzählungen": {"col": None, "freq": None}
}

def validate_time_frequency(df, time_col, freq_string, category):
    """
    Validates if the dataframe follows a specific frequency.
    freq_string: e.g., '1 hour', '1 day', '30 minutes'
    """
    # Convert to datetime
    if category == "Permanente Besucherzählung (Eco-Counter)":
        df = parse_german_dates(df=df, date_column_name=time_col)
    else:
        df[time_col] = pd.to_datetime(df[time_col], format="mixed")
    
    # Sort to ensure we're checking chronological continuity
    df = df.sort_values(by=time_col)

    if freq_string is None:
        # Ask user to specify the time column via an text input field
        freq_string = st.text_input("Bitte trage hier die Frequenz der Zeitspalte ein (z.B. ""1 hour"", ""1 day"", ""30 minutes""):")
    
    # Calculate the expected timedelta
    try:
        expected_delta = pd.to_timedelta(freq_string)
    except ValueError:
        st.error(f"Ungültiges Frequenz-Format: '{freq_string}'")
        st.stop()

    # Calculate differences between consecutive rows
    time_diffs = df[time_col].diff().dropna()
    
    # Check if the data mostly follows the expected frequency
    most_prevelant_time_diff = time_diffs.value_counts().index[0]

    if not most_prevelant_time_diff == expected_delta:
        st.error(f"Error: Die Datenfrequenz entspricht nicht '{freq_string}'. "
                 "Bitte überprüfe die Datei.")
        st.stop()
    
    return df

def process_and_validate_upload(uploaded_file, category):
    # Check extension and read file
    if uploaded_file.name.endswith('.csv'):
        if category == "Permanente Besucherzählung (Eco-Counter)":
            df = pd.read_csv(uploaded_file, skiprows=2)
        else:
            df = pd.read_csv(uploaded_file)
    elif uploaded_file.name.endswith(('.xls', '.xlsx')):
        df = pd.read_excel(uploaded_file)
    else:
        st.error("Error: Bitte nur Excel (.xlsx, .xls) oder CSV Dateien hochladen.")
        st.stop()

    # Map category to expected time column name
    time_col = data_upload_categories_time_cols_freq[category]["col"]
    
    if time_col is None:
        # Ask user to specify the time column via an text input field
        time_col = st.text_input("Bitte trage hier den exakten Namen der Zeitspalte ein:")
    
    if time_col not in df.columns:
        st.error(f"Error: Die Zeitspalte '{time_col}' konnte nicht in der hochgeladenen Datei gefunden werden. Der Upload wird abgebrochen.")
        st.stop()
    
    else:

        preprocessed_df = validate_time_frequency(
            df,
            time_col,
            freq_string=data_upload_categories_time_cols_freq[category]["freq"],
            category=category
        )

        return preprocessed_df

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
        [
            "Permanente Besucherzählung (Eco-Counter)",
            "Hütten: Zählungen, Wetterstationsdaten,Öffnungszeiten & Feiertage",
            "Schulferien & Feiertage (BY & CZ)",
            "Parkplatzzählungen",
            "Sonderzählungen",
            "Wetterdaten"
        ]
    )

    # Select entire timeframe or a specific start and end date
    ## Checkbox: All data?
    on = st.toggle("Zeitraum eingrenzen")

    ## time Selection
    if on:
        # TODO: The first selectable date should be the first day of the available data
        col_left, col_right = st.columns(2)

        with col_left:
            start_time = st.datetime_input("Start:", value=None, max_value="now", step=3600)

        with col_right:
            end_time = st.datetime_input("Ende:", value=None, max_value="now", step=3600)

    # Button to query data
    if st.button(
        label="Query Data",
        help="Query the data based on the selected data categories and timeframe.",
        type="primary"
    ):
        # Preview data (now: dummy data)
        st.markdown("# Data Preview:")
        st.markdown(" ⚠️ Für Testzwecke, werden hier Dummy-Daten präsentiert.")

        def create_dummy_data() -> pd.DataFrame:
            np.random.seed(42)

            n_rows = 8
            base_date = datetime.today()

            df = pd.DataFrame(
                {

                    # Emoji “rating”
                    "mood": [
                        "😀",
                        "😐",
                        "😢",
                        "🤩",
                        "😴",
                        "😡",
                        "🤔",
                        "😂",
                    ],
                    # Boolean / categorical
                    "active": [True, False, True, True, False, True, False, True],
                    "category": ["A", "B", "C", "A", "B", "C", "A", "B"],
                    # Numbers
                    "score": np.round(np.random.uniform(0, 100, n_rows), 1),
                    "category": [
                        ["exploration", "visualization"],
                        ["llm", "visualization"],
                        ["exploration"],
                        ["llm", "visualization"],
                        ["llm"],
                        ["llm", "exploration"],
                        ["llm"],
                        ["exploration", "visualization"],
                    ],
                    "progress": np.round(np.random.rand(n_rows), 2),
                    # Dates and times
                    "date": [base_date.date() + timedelta(days=i) for i in range(n_rows)],
                    "timestamp": [base_date + timedelta(hours=i * 3) for i in range(n_rows)],
                    # URL column
                    "link": [
                        "https://streamlit.io",
                        "https://docs.streamlit.io",
                        "https://github.com/streamlit/streamlit",
                        "https://discuss.streamlit.io",
                        "https://streamlit.io/gallery",
                        "https://streamlit.io/cloud",
                        "https://blog.streamlit.io",
                        "https://streamlit.io/components",
                    ],
                    # Image URLs (can be any public images)
                    "logo": [
                        "https://storage.googleapis.com/s4a-prod-share-preview/default/st_app_screenshot_image/5435b8cb-6c6c-490b-9608-799b543655d3/Home_Page.png",
                        "https://storage.googleapis.com/s4a-prod-share-preview/default/st_app_screenshot_image/ef9a7627-13f2-47e5-8f65-3f69bb38a5c2/Home_Page.png",
                        "https://storage.googleapis.com/s4a-prod-share-preview/default/st_app_screenshot_image/31b99099-8eae-4ff8-aa89-042895ed3843/Home_Page.png",
                        "https://storage.googleapis.com/s4a-prod-share-preview/default/st_app_screenshot_image/6a399b09-241e-4ae7-a31f-7640dc1d181e/Home_Page.png",
                        "https://storage.googleapis.com/s4a-prod-share-preview/default/st_app_screenshot_image/5435b8cb-6c6c-490b-9608-799b543655d3/Home_Page.png",
                        "https://storage.googleapis.com/s4a-prod-share-preview/default/st_app_screenshot_image/ef9a7627-13f2-47e5-8f65-3f69bb38a5c2/Home_Page.png",
                        "https://storage.googleapis.com/s4a-prod-share-preview/default/st_app_screenshot_image/31b99099-8eae-4ff8-aa89-042895ed3843/Home_Page.png",
                        "https://storage.googleapis.com/s4a-prod-share-preview/default/st_app_screenshot_image/6a399b09-241e-4ae7-a31f-7640dc1d181e/Home_Page.png",
                    ],
                    # Per-row mini time series for chart columns
                    "trend_line": [
                        np.random.randn(10).cumsum().tolist() for _ in range(n_rows)
                    ],
                    "trend_area": [
                        (np.random.rand(10) * 100).tolist() for _ in range(n_rows)
                    ],
                }
            )

            return df

        # --- Rich st.dataframe with column_config ------------------------------
        st.markdown("#### Dummy Data Preview als normale Tabelle")

        dummy_data = create_dummy_data()
        st.dataframe(dummy_data)

        # Button to download data
        st.download_button(
            label="Download Data",
            data=dummy_data.to_csv(index=False).encode('utf-8'),
            file_name="dummy_data.csv",
            icon=":material/download:",
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

    already_existing_columns = [
        "Datum",
        "Besucherzahlen"
    ]

    st.info(f'Aktuell vorhandene Spaltennamen in der Datenkategorie "{category_to_upload_data_to} sind: {", ".join(already_existing_columns)}', icon="ℹ️")

    # Select local file to upload
    uploaded_files = st.file_uploader(
    "Upload data", accept_multiple_files=True, type=["csv", "xlsx", "xls"]
    )
    for uploaded_file in uploaded_files:
        
        df = process_and_validate_upload(uploaded_file, category_to_upload_data_to)

        # List all columns that are not in the already existing columns
        new_columns = [col for col in df.columns if col not in already_existing_columns]

        if new_columns:
            st.warning(f'Achtung! Es wurden die folgenden, neuen Spaltennamen in der hochgeladenen Datei gefunden: {", ".join(new_columns)}', icon="⚠️")

        # Preview file before upload
        st.markdown(f"#### Preview: `{uploaded_file.name}`")
        st.dataframe(df.head())

        # Confirm upload
        if st.button(
            "Upload Data",
            key=uploaded_file.file_id,
            type="primary"
        ):
            st.success(f"`{uploaded_file.name}` wurde erfolgreich hochgeladen!")