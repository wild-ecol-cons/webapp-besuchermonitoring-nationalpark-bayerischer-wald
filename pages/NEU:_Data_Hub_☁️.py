# imports libraries
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from numpy.random import default_rng as rng
from src.streamlit_app.pages_in_dashboard.password import check_password
from src.streamlit_app.pages_in_dashboard.visitors.language_selection_menu import TRANSLATIONS


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
        ["Permanente Besucherzählung (Eco-Counter)",
         "Häuser: Öffnungszeiten & Zählungen",
         "Schulferien & Feiertage (BY & CZ)",
         "Parkplatzzählungen",
         "Sonderzählungen",
         "Wetterdaten"],
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
        st.markdown("# Preview of data:")
        st.markdown(" ⚠️ For testing purposes, dummy data is being presented.")

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
        st.markdown("#### Dummy Data: Rich `st.dataframe` with diverse data types")

        def preview_rich_dataframe(df: pd.DataFrame) -> None:
            st.dataframe(
                df,
                use_container_width=True,
                column_config={

                    "name": st.column_config.TextColumn(
                        "Name",
                        help="Plain text name",
                        max_chars=20,
                    ),
                    "mood": st.column_config.TextColumn(
                        "Mood",
                        help="Emoji mood indicator",
                        width="small",
                    ),
                    "active": st.column_config.CheckboxColumn(
                        "Active?",
                        help="Boolean flag rendered as checkbox",
                        default=False,
                    ),
                    "category": st.column_config.SelectboxColumn(
                        "Category",
                        help="Categorical selectbox inside the dataframe",
                        options=["A", "B", "C"],
                    ),
                    "score": st.column_config.NumberColumn(
                        "Score",
                        help="Numeric score between 0 and 100",
                        format="%.1f",
                        min_value=0,
                        max_value=100,
                    ),
                    "category": st.column_config.MultiselectColumn(
                        "App Categories",
                        help="The categories of the app",
                        options=[
                            "exploration",
                            "visualization",
                            "llm",
                        ],
                        color=["#ffa421", "#803df5", "#00c0f2"],
                        format_func=lambda x: x.capitalize(),
                    ),
                    "progress": st.column_config.ProgressColumn(
                        "Progress",
                        help="Progress bar between 0 and 1",
                        min_value=0.0,
                        max_value=1.0,
                        format="%.0f%%",
                    ),
                    "date": st.column_config.DateColumn(
                        "Date",
                        help="Date column",
                    ),
                    "timestamp": st.column_config.DatetimeColumn(
                        "Timestamp",
                        help="Date & time column",
                    ),
                    "link": st.column_config.LinkColumn(
                        "Link",
                        help="Clickable URL that opens in a new tab",
                        display_text="Open",
                    ),
                    "logo": st.column_config.ImageColumn(
                        "Picture",
                        help="Logo rendered directly in the cell",
                        width="small",
                    ),
                    "trend_line": st.column_config.LineChartColumn(
                        "Line trend (10 pts)",
                        help="Mini line chart per row",
                        y_min=-10,
                        y_max=10,
                    ),
                    "trend_area": st.column_config.AreaChartColumn(
                        "Area trend (10 pts)",
                        help="Mini area chart per row",
                        y_min=0,
                        y_max=100,
                    ),
                },
                # Optional: choose a column order to highlight structured → rich → unstructured
                column_order=[
                    "id",
                    "name",
                    "description",
                    "mood",
                    "active",
                    "category",
                    "score",
                    "progress",
                    "date",
                    "timestamp",
                    "logo",
                    "trend_line",
                    "trend_area",
                    "link",
                ],
            )

        dummy_data = create_dummy_data()
        preview_rich_dataframe(dummy_data)

        # Button to download data
        st.download_button(
            label="Download Data",
            data=dummy_data.to_csv(index=False).encode('utf-8'),
            file_name="dummy_data.csv",
            icon=":material/download:",
        )
with tab_upload_data:
    # Select category that it is being uploaded to
    # Select local file to upload
    # Preview file before upload
    # Button to upload data

    st.markdown("## Upload Data")