# import the required libraries
import streamlit as st
import altair as alt


def get_page_layout():
    """
    Set the page layout for the Streamlit app.

    Returns:
        col1, col2: The two columns of the page layout.
    """
    st.set_page_config(
    page_title='Plan your trip🌲',
    page_icon="🌲",
    layout="wide",
    initial_sidebar_state="expanded")

    alt.themes.enable("dark")

    # Define the app layout

    col1, col2 = st.columns((2.2,1), gap="large")

    return col1, col2