# import libraries
import streamlit as st
import pydeck as pdk
import pandas as pd
from src.streamlit_app.pages_in_dashboard.visitors.language_selection_menu import TRANSLATIONS

# TODO: Normalize the numbers to get different sized markers according to the occupancy rate of the parking sections
def get_fixed_size():
    """
    Get a fixed size value for the map markers.
    """
    return 300  

def calculate_color(occupancy_rate):
    """
    Calculate the color of the marker based on the occupancy rate.

    Args:
        occupancy_rate (float): The occupancy rate of the parking section.

    Returns:
        list: A list of RGB values representing the color of the marker.
    """
    occupancy_rate = float(occupancy_rate)

    if occupancy_rate >= 80:
        return [230, 39, 39] #red

    elif occupancy_rate >= 60:
        return [244, 251, 81] #yellow
    
    else:
        return [109, 249, 2] #green


def get_parking_section(
        processed_parking_data):
    """
    
    Display the parking section of the dashboard with a map showing the real-time parking occupancy 
    and interactive metrics.

    Args:
        processed_parking_data (pd.DataFrame): Processed parking data.

    Returns:
        None
    """
    st.markdown(f"### {TRANSLATIONS[st.session_state.selected_language]['real_time_parking_occupancy']}")

    # Set a fixed size for all markers
    processed_parking_data['size'] = get_fixed_size()
    processed_parking_data['color'] = processed_parking_data['current_occupancy_rate'].apply(calculate_color)

    processed_parking_data['current_occupancy_rate'] = pd.to_numeric(processed_parking_data['current_occupancy_rate'], errors='coerce')  # Convert to float
    processed_parking_data['current_occupancy_rate'] = processed_parking_data['current_occupancy_rate'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")


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
        pickable=True
    )

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            "text": "{location}\n" + f"{TRANSLATIONS[st.session_state.selected_language]['available_spaces']}: " + "{current_availability} 🚗\n" + f"{TRANSLATIONS[st.session_state.selected_language]['occupancy_rate']}: " + "{current_occupancy_rate}%"
        },  # Updated tooltip text with two decimal points for occupancy rate
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
        col1.metric(label=TRANSLATIONS[st.session_state.selected_language]['available_spaces'], value=f"{selected_data['current_availability']} 🚗")
        col2.metric(label=TRANSLATIONS[st.session_state.selected_language]['capacity'], value=f"{selected_data['current_capacity']} 🚗")
        col3.metric(label=TRANSLATIONS[st.session_state.selected_language]['occupancy_rate'], value=f"{selected_data['current_occupancy_rate']}%")

