# Import necessary libraries
import streamlit as st
import pydeck as pdk
import pandas as pd
import geopandas as gpd
import joblib
import io
import pytz
from src.streamlit_app.source_data import source_and_preprocess_realtime_parking_data
from src.streamlit_app.pages_in_dashboard.visitors.language_selection_menu import TRANSLATIONS
from src.config import CONTAINER_NAME, CONNECTION_STRING
from azure.storage.blob import BlobClient
from datetime import datetime

# BKG WMTS endpoint for TopPlusOpen, addressed like a standard XYZ tile
# source. "web_light_grau" is the "TopPlusOpen Light Grau" variant.
#
# NOTE: this is loaded as a pydeck TileLayer (a normal layer, in the
# `layers` array) rather than as a `map_style` dict. A TileLayer is just
# another layer, serialized the same way as the ScatterplotLayer below.
TOPPLUS_LAYER = "web_light_grau"
TOPPLUS_TILE_URL = (
    f"https://sgx.geodatenzentrum.de/wmts_topplus_open/tile/1.0.0/"
    f"{TOPPLUS_LAYER}/default/WEBMERCATOR/{{z}}/{{y}}/{{x}}.png"
)

REGIONS_GEOJSON_AZURE_PATH = 'raw-data/geodata/ecocounter_regionen_v2.geojson'

# Qualitative palette for regions (RGB)
REGION_COLOR_PALETTE = [
    [158, 29, 201],   # Purple#
    [29, 143, 201],   # Sky blue
    [29, 72, 201],    # Blue
    [72, 29, 201],    # Indigo/violet
    [29, 201, 186],   # Teal/cyan
    [201, 29, 158],   # Magenta/pink
]
@st.cache_data
def load_regions(path: str) -> gpd.GeoDataFrame:
    """
    Load the visitor forecast regions GeoJSON and reproject it from
    EPSG:25832 (as provided) to EPSG:4326 (lat/lon, what pydeck expects).
    """
            
    # Construct the full blob name (key)
    print(f"Retrieving the region GeoJSON saved in Azure with blob name {path}")
    
    # 1. Create a BlobClient
    blob_client = BlobClient.from_connection_string(
        conn_str=CONNECTION_STRING, 
        container_name=CONTAINER_NAME, 
        blob_name=path
    )
    
    # 2. Download the blob content
    download_stream = blob_client.download_blob()
    
    # Read all data into a byte stream
    geojson_bytes = download_stream.readall()
    
    # Convert the byte stream to a GeoDataFrame
    buffer = io.BytesIO(geojson_bytes)
    regions = gpd.read_file(buffer)
 
    if regions.crs is None:
        # Fall back to the CRS declared in the file's "crs" block if GeoPandas
        # didn't pick it up automatically.
        regions = regions.set_crs(epsg=25832, allow_override=True)

    regions = regions.to_crs(epsg=4326)

    # Stable per-region color assignment, keyed by Region_ID so colors don't
    # shuffle between reruns.
    region_ids = sorted(regions["Region_ID"].unique())
    color_map = {
        region_id: REGION_COLOR_PALETTE[i % len(REGION_COLOR_PALETTE)]
        for i, region_id in enumerate(region_ids)
    }
    regions["fill_color_base"] = regions["Region_ID"].map(color_map)

    return regions


def style_regions_for_display(regions: gpd.GeoDataFrame, highlighted_names: list) -> gpd.GeoDataFrame:
    """
    Set fill/line color + opacity per region based on which region names are
    currently highlighted via the legend. If nothing is selected, show all
    regions at full color (default state).
    """
    regions = regions.copy()
    show_all = len(highlighted_names) == 0

    def fill_color(row):
        r, g, b = row["fill_color_base"]
        if show_all or row["Name"] in highlighted_names:
            return [r, g, b, 130]
        return [190, 190, 190, 40]  # dimmed grey for non-selected regions

    def line_color(row):
        r, g, b = row["fill_color_base"]
        if show_all or row["Name"] in highlighted_names:
            return [r, g, b, 255]
        return [160, 160, 160, 120]

    def line_width(row):
        return 100 if (row["Name"] in highlighted_names) else 1

    regions["fill_color"] = regions.apply(fill_color, axis=1)
    regions["line_color"] = regions.apply(line_color, axis=1)
    regions["line_width"] = regions.apply(line_width, axis=1)

    # Compute region-wise tooltip message
    regions['tooltip_line1_name'] = "Region: " + regions['Name']
    regions['tooltip_line2_occupancy'] = ""  # TODO: Later add here the real-time occupancy rates of that entire region

    return regions


def render_regions_legend(regions: gpd.GeoDataFrame) -> list:
    """
    Interactive legend: one swatch + region name per row. Selection drives
    which regions are highlighted on the map. Returns the list of selected
    region names.
    """
    lang = st.session_state.selected_language
    legend_title = TRANSLATIONS[lang].get("visitor_forecast_regions", "Visitor Forecast Regions")

    with st.expander(legend_title, expanded=False):
        options = regions.sort_values("Name")[["Name", "fill_color_base"]].drop_duplicates("Name")

        selected = st.multiselect(
            TRANSLATIONS[lang].get("highlight_regions", "Highlight region(s)"),
            options=options["Name"].tolist(),
            default=[],
            key="selected_regions_multiselect",
        )

        for _, row in options.iterrows():
            r, g, b = row["fill_color_base"]
            swatch_col, label_col = st.columns([0.08, 0.92])
            with swatch_col:
                st.markdown(
                    f'<div style="width:16px;height:16px;border-radius:3px;'
                    f'background-color:rgb({r},{g},{b});margin-top:4px;"></div>',
                    unsafe_allow_html=True,
                )
            with label_col:
                st.markdown(row["Name"])

    return selected

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
        return {"color_markers_map_visualization": [211, 47, 47],
                "color_bar_occupancy_rate": "red"} # red
    elif occupancy_rate >= 60:
        return {"color_markers_map_visualization": [255, 160, 0],
                "color_bar_occupancy_rate": "yellow"} # yellow
    else:
        return {"color_markers_map_visualization": [56, 142, 60],
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
    Display the parking section of the dashboard with a map showing:
      - the BKG TopPlusOpen Light Grau basemap
      - visitor forecast regions (with an interactive legend)
      - real-time parking occupancy markers
    in a fixed bird's-eye view fit to the data, still pannable/zoomable.
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

    # Rename parking locations to be more user-friendly
    processed_parking_data['location'] = processed_parking_data['location'].replace({
        "parkplatz-graupsaege-1": "P+R Graupsäge",
        "p-r-spiegelau-1": "P+R Spiegelau",
        "parkplatz-zwieslerwaldhaus-1": "Parkplatz Zwieslerwaldhaus",
        "parkplatz-nationalparkzentrum-falkenstein-2": "Parkplatz Nationalparkzentrum Falkenstein",
        "scheidt-bachmann-parkplatz-1": "Scheidt-Bachmann-Parkplatz",
        "parkplatz-nationalparkzentrum-lusen-p2": "Parkplatz Nationalparkzentrum Lusen",
        "parkplatz-waldhaeuser-kirche-1": "Parkplatz Waldhäuser Kirche",
        "parkplatz-waldhaeuser-ausblick-1": "Parkplatz Waldhäuser Ausblick",
        "parkplatz-skisportzentrum-finsterau-1": "Parkplatz Finsterau Ski- und Sportstadion",
    })

    # Compute parking place tooltip message
    processed_parking_data['tooltip_line1_name'] = processed_parking_data['location']
    processed_parking_data['tooltip_line2_occupancy'] = "Belegungsstatus: " + processed_parking_data['occupancy_status']

    # --- Regions: load + legend (drives highlight state) -----------------
    regions = load_regions(path=REGIONS_GEOJSON_AZURE_PATH)
    highlighted_regions = render_regions_legend(regions)
    styled_regions = style_regions_for_display(regions, highlighted_regions)

    # PyDeck Map Configuration with adjusted view_state
    view_state = pdk.ViewState(
        latitude=48.98788792657768,  # Center map at the average latitude
        longitude=13.388472800551007,  # Center map at the average longitude
        zoom=9.6,  # Zoom level increased for a closer view
        pitch=0,  # Set the pitch to 0 for a top-down view
        bearing=0,  # To set the initial bearing to 0 (0 being aligned to true north)
    )

    # --- Layers ------------------------------------------------------------
    regions_layer = pdk.Layer(
        "GeoJsonLayer",
        data=styled_regions.__geo_interface__,
        stroked=True,
        filled=True,
        get_fill_color="properties.fill_color",
        get_line_color="properties.line_color",
        get_line_width="properties.line_width",
        line_width_min_pixels=1,
        pickable=True,
        auto_highlight=True,
    )

    tile_layer = pdk.Layer(
        "TileLayer",
        data=TOPPLUS_TILE_URL,
        min_zoom=0,
        max_zoom=19,
        tile_size=256,
    )
 
    parking_layer = pdk.Layer(
        "ScatterplotLayer",
        data=processed_parking_data,
        get_position=["longitude", "latitude"],
        get_radius=12,
        radius_units="'pixels'", # for dynamic scaling relative to zoom
        radius_min_pixels=8,
        radius_max_pixels=25,
        get_fill_color="color",
        get_line_color=[0,0,0],
        get_line_width=10,
        stroked=True,
        pickable=True,
    )

    deck = pdk.Deck(
        layers=[tile_layer, regions_layer, parking_layer],
        initial_view_state=view_state,
        map_style=None,        # basemap comes from tile_layer, not a Mapbox/MapLibre style
        map_provider=None,     # no basemap provider needed — avoids any API-key/token requirement
        tooltip={
        "html": "{tooltip_line1_name}<br/>{tooltip_line2_occupancy}"
        },
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

