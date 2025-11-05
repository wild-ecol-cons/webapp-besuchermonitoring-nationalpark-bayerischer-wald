# Import necessary libraries
import streamlit as st
import pydeck as pdk
import pandas as pd
import pytz
from src.streamlit_app.source_data import source_and_preprocess_realtime_parking_data
from src.streamlit_app.pages_in_dashboard.visitors.language_selection_menu import TRANSLATIONS
from datetime import datetime

def get_fixed_size():
    """
    Get a fixed size value for the map markers.
    """
    return 450  

def calculate_color_based_on_occupancy_rate(occupancy_rate) -> dict:
    """
    Calculate the color of the marker based on the occupancy rate.
    Returns a named tuple with the RGB values and a CSS gradient color value.



    Args:
        occupancy_rate (float): The occupancy rate of the parking section.

    Returns:
        list: A list of RGB values representing the color of the marker.
    """
    occupancy_rate = float(occupancy_rate)

    if occupancy_rate >= 80:
        return {"color_markers_map_visualization": [230, 39, 39],
                "color_bar_occupancy_rate": "red"} # red
    elif occupancy_rate >= 60:
        return {"color_markers_map_visualization": [250, 232, 8],
                "color_bar_occupancy_rate": "yellow"} # yellow
    else:
        return {"color_markers_map_visualization": [109, 249, 2],
                "color_bar_occupancy_rate": "green"} # green


def get_occupancy_status(occupancy_rate):
    """
    Get the occupancy status (High, Medium, Low) based on the occupancy rate.

    Args:
        occupancy_rate (float): The occupancy rate of the parking section.

    Returns:
        str: The occupancy status ("High", "Medium", "Low").
    """
    if occupancy_rate >= 80:
        return TRANSLATIONS[st.session_state.selected_language]["parking_status_high"]
    elif occupancy_rate >= 60:
        return TRANSLATIONS[st.session_state.selected_language]["parking_status_moderate"]
    else:
        return TRANSLATIONS[st.session_state.selected_language]["parking_status_low"]

def render_occupancy_bar(occupancy_rate):
    """
    Render a color bar representing the occupancy rate using HTML and CSS.

    Args:
        occupancy_rate (float): The occupancy rate of the parking section.

    Returns:
        None
    """
    # Ensure occupancy rate is between minimum value and 100
    minimum_value_of_occupancy = 5
    occupancy_rate = min(max(float(occupancy_rate), minimum_value_of_occupancy), 100)
    
    # Define the color based on occupancy
    bar_color = calculate_color_based_on_occupancy_rate(occupancy_rate)["color_bar_occupancy_rate"]

    # Create an HTML div with the appropriate width based on occupancy rate
    st.markdown(f"""
    <div style="width: 100%; background-color: lightgrey; border-radius: 5px; padding: 3px;">
        <div style="width: {occupancy_rate}%; background-color: {bar_color}; height: 25px; border-radius: 5px;"></div>
    </div>
    """, unsafe_allow_html=True)

@st.fragment(run_every="15min")
def get_parking_section():
    """
    Display the parking section of the dashboard with a map showing the real-time parking occupancy 
    and interactive metrics.

    Args:
        processed_parking_data (pd.DataFrame): Processed parking data.

    Returns:
        None
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

    st.markdown(f"### {TRANSLATIONS[st.session_state.selected_language]['real_time_parking_occupancy']}")
    
    # Set a fixed size for all markers
    processed_parking_data['size'] = get_fixed_size()
    processed_parking_data['color'] = processed_parking_data['current_occupancy_rate'].apply(lambda occupancy_rate: calculate_color_based_on_occupancy_rate(occupancy_rate)["color_markers_map_visualization"])

    # Convert the occupancy rate to numeric and handle errors
    processed_parking_data['current_occupancy_rate'] = pd.to_numeric(processed_parking_data['current_occupancy_rate'], errors='coerce')

    # Map occupancy rate to status (High, Medium, Low)
    processed_parking_data['occupancy_status'] = processed_parking_data['current_occupancy_rate'].apply(get_occupancy_status)

    # Calculate center of the map based on the average of latitudes and longitudes
    avg_latitude = processed_parking_data['latitude'].mean()
    avg_longitude = processed_parking_data['longitude'].mean()

    # PyDeck Map Configuration with adjusted view_state
    view_state = pdk.ViewState(
        latitude=avg_latitude,  # Center map at the average latitude
        longitude=avg_longitude,  # Center map at the average longitude
        zoom=10,  # Zoom level increased for a closer view
        pitch=50
    )

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=processed_parking_data,
        get_position=["longitude", "latitude"],
        get_radius="size",
        get_fill_color="color",
        pickable=True,
    )

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            "text": "{location}\n" + f"{TRANSLATIONS[st.session_state.selected_language]['occupancy_status']}: " + "{occupancy_status}"
        },
        map_style="road"
    )
    st.pydeck_chart(deck)

    # Interactive Metrics
    selected_location = st.selectbox(
        TRANSLATIONS[st.session_state.selected_language]['select_parking_section'], 
        processed_parking_data['location'].unique(),
        key="selectbox_parking_section",
        width=400
    )

    # Display selected location details
    if selected_location:
        selected_data = processed_parking_data[processed_parking_data['location'] == selected_location].iloc[0]

        col1, col2, col3 = st.columns(3)
        col1.metric(label=TRANSLATIONS[st.session_state.selected_language]['capacity'], value=f"{selected_data['current_capacity']} 🚗")
        
        # Display occupancy status and bar
        with col2:
            st.metric(label = TRANSLATIONS[st.session_state.selected_language]['occupancy_status'], value=f"{selected_data['occupancy_status']}")
        with col3:
            st.markdown(f"**{TRANSLATIONS[st.session_state.selected_language]['occupancy_rate']}**")
            render_occupancy_bar(selected_data['current_occupancy_rate'])

