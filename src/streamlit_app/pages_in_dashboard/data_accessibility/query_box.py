# Import libraries
import streamlit as st
import datetime
from src.streamlit_app.pages_in_dashboard.data_accessibility.data_retrieval import get_data_from_query
from src.streamlit_app.pages_in_dashboard.data_accessibility.query_viz_and_download import get_visualization_section


def select_category():
    """
    Select the category of data to access using st.selectbox from Streamlit.

    Returns: 
        category (str): The category selected by the user.
    """
    # select the dropdown for the category
    category = st.selectbox("Select data category", 
                            ["weather", "visitor_sensors", "parking", "visitor_centers"],
                            index=0)
    
    return category

def select_date():
    """
    Select the start and end date for data access using date inputs in Streamlit.

    Returns: 
        tuple: The selected start and end date in the format "MM-DD-YYYY".
    """
    
    # Define the default start and end dates of the 01.01.2023 to 31.12.2023
    default_start = datetime.datetime(2023, 1, 1)
    default_end = datetime.datetime(2023, 12, 31)

    # Create the date input widget with start date
    d = st.date_input(
        "Select the start date",
        default_start,
        format="DD.MM.YYYY",
    )
    # Create the date input widget with end date
    e = st.date_input(
        "Select the end date",
        default_end,
        format="DD.MM.YYYY",
    )
    # capture the selected date
    start_date = d.strftime("%m-%d-%Y")
    end_date = e.strftime("%m-%d-%Y")

    # prompt if the end date is chosen before start date
    if start_date > end_date:
        st.error("Error: End date must fall after start date.")
        st.stop()

    return start_date, end_date

def select_filters(category, start_date, end_date):

    """
    Select additional filters such as sensors, weather values, or parking values.

    Args:
        category (str): The category selected by the user. Can be one of:
                        "weather", "parking", "visitor_sensors", "visitor_centers".

    Returns:
        tuple: A tuple containing selected_properties and selected_sensors.
    """

    st.markdown("### More Filters")
        
    # Select the sensors or weather values or parking values
    category_based_filters = {
        "weather" : [
            'Temperature (°C)', 'Precipitation (mm)', 'Wind Speed (km/h)', 'Relative Humidity (%)', 'Sunshine Duration (min)'
            ],
        "parking" : {'sensors':[
                                'p-r-spiegelau-1','parkplatz-fredenbruecke-1','parkplatz-graupsaege-1',
                                'parkplatz-nationalparkzentrum-falkenstein-2','parkplatz-nationalparkzentrum-lusen-p2',
                                'parkplatz-skisportzentrum-finsterau-1','parkplatz-waldhaeuser-ausblick-1',
                                'parkplatz-waldhaeuser-kirche-1','parkplatz-zwieslerwaldhaus-1',
                                'parkplatz-zwieslerwaldhaus-nord-1','scheidt-bachmann-parkplatz-1',
                                'skiwanderzentrum-zwieslerwaldhaus-2'],

                     'properties':[
                         'occupancy', 'capacity', 'occupancy_rate'],
                    },

        'visitor_sensors'  :[
                                        'Bayerisch Eisenstein', 'Bayerisch Eisenstein IN',
                                        'Bayerisch Eisenstein OUT', 'Bayerisch Eisenstein Fußgänger',
                                        'Bayerisch Eisenstein Fahrräder',
                                        'Bayerisch Eisenstein Fußgänger IN',
                                        'Bayerisch Eisenstein Fußgänger OUT',
                                        'Bayerisch Eisenstein Fahrräder IN',
                                        'Bayerisch Eisenstein Fahrräder OUT', 'Brechhäuslau IN',
                                        'Brechhäuslau OUT', 'Brechhäuslau', 'Brechhäuslau Fußgänger IN',
                                        'Brechhäuslau Fußgänger OUT', 'Bucina_Multi', 'Bucina_Multi OUT',
                                        'Bucina_Multi Fußgänger', 'Bucina_Multi Fahrräder',
                                        'Bucina_Multi Fußgänger IN', 'Bucina_Multi Fahrräder IN',
                                        'Bucina_Multi Fahrräder OUT', 'Bucina_Multi Fußgänger OUT',
                                        'Deffernik', 'Deffernik IN', 'Deffernik OUT',
                                        'Deffernik Fußgänger', 'Deffernik Fahrräder',
                                        'Deffernik Fahrräder IN', 'Deffernik Fahrräder OUT',
                                        'Deffernik Fußgänger IN', 'Deffernik Fußgänger OUT',
                                        'Diensthüttenstraße', 'Diensthüttenstraße Fußgänger IN',
                                        'Diensthüttenstraße Fußgänger OUT', 'Felswandergebiet',
                                        'Felswandergebiet IN', 'Felswandergebiet OUT', 'Ferdinandsthal',
                                        'Ferdinandsthal IN', 'Ferdinandsthal OUT', 'Fredenbrücke',
                                        'Fredenbrücke Fußgänger IN', 'Fredenbrücke Fußgänger OUT', 'Gfäll',
                                        'Gfäll Fußgänger IN', 'Gfäll Fußgänger OUT', 'Gsenget',
                                        'Gsenget IN', 'Gsenget OUT', 'Gsenget Fußgänger',
                                        'Gsenget Fahrräder', 'Gsenget IN.1', 'Gsenget OUT.1',
                                        'Gsenget Fahrräder IN', 'Gsenget Fahrräder OUT',
                                        'Klingenbrunner Wald', 'Klingenbrunner Wald IN',
                                        'Klingenbrunner Wald OUT', 'Klingenbrunner Wald Fußgänger',
                                        'Klingenbrunner Wald Fahrräder',
                                        'Klingenbrunner Wald Fußgänger IN',
                                        'Klingenbrunner Wald Fußgänger OUT',
                                        'Klingenbrunner Wald Fahrräder IN',
                                        'Klingenbrunner Wald Fahrräder OUT', 'Klosterfilz',
                                        'Klosterfilz IN', 'Klosterfilz OUT', 'Klosterfilz Fußgänger',
                                        'Klosterfilz Fahrräder', 'Klosterfilz Fußgänger IN',
                                        'Klosterfilz Fußgänger OUT', 'Klosterfilz Fahrräder IN',
                                        'Klosterfilz Fahrräder OUT', 'Racheldiensthütte',
                                        'Racheldiensthütte IN', 'Racheldiensthütte OUT',
                                        'Racheldiensthütte Fußgänger', 'Racheldiensthütte Fahrräder',
                                        'Racheldiensthütte Fahrräder IN', 'Racheldiensthütte Cyclist OUT',
                                        'Racheldiensthütte Pedestrian IN',
                                        'Racheldiensthütte Pedestrian OUT', 'Sagwassersäge',
                                        'Sagwassersäge Fußgänger IN', 'Sagwassersäge Fußgänger OUT',
                                        'Scheuereck', 'Scheuereck IN', 'Scheuereck OUT', 'Schillerstraße',
                                        'Schillerstraße IN', 'Schillerstraße OUT', 'Schwarzbachbrücke',
                                        'Schwarzbachbrücke Fußgänger IN',
                                        'Schwarzbachbrücke Fußgänger OUT', 'TFG_Falkenstein_1',
                                        'TFG_Falkenstein_1 Fußgänger zum Parkplatz',
                                        'TFG_Falkenstein_1 Fußgänger zum HZW', 'TFG_Falkenstein_2',
                                        'TFG_Falkenstein_2 Fußgänger In Richtung Parkplatz',
                                        'TFG_Falkenstein_2 Fußgänger In Richtung TFG', 'TFG_Lusen_1',
                                        'TFG_Lusen_1 Fußgänger Richtung TFG',
                                        'TFG_Lusen_1 Fußgänger Richtung Parkplatz', 'TFG_Lusen_2',
                                        'TFG_Lusen_2 Fußgänger Richtung Vögel am Waldrand',
                                        'TFG_Lusen_2 Fußgänger Richtung Parkplatz', 'TFG_Lusen_3',
                                        'TFG_Lusen_3 TFG Lusen 3 IN', 'TFG_Lusen_3 TFG Lusen 3 OUT',
                                        'Trinkwassertalsperre_MULTI', 'Trinkwassertalsperre_MULTI IN',
                                        'Trinkwassertalsperre_MULTI OUT',
                                        'Trinkwassertalsperre_MULTI Fußgänger',
                                        'Trinkwassertalsperre_MULTI Fußgänger IN',
                                        'Trinkwassertalsperre_MULTI Fußgänger OUT',
                                        'Trinkwassertalsperre_MULTI Fahrräder',
                                        'Trinkwassertalsperre_MULTI Fahrräder IN',
                                        'Trinkwassertalsperre_MULTI Fahrräder OUT', 'Waldhausreibe',
                                        'Waldhausreibe IN', 'Waldhausreibe OUT', 'Waldspielgelände_1',
                                        'Waldspielgelände_1 IN', 'Waldspielgelände_1 OUT', 'Wistlberg',
                                        'Wistlberg Fußgänger IN', 'Wistlberg Fußgänger OUT'],
                                
                                  
        'visitor_centers' : [
                            'Besuchszahlen_HEH',
                            'Besuchszahlen_HZW', 'Besuchszahlen_WGM', 'Parkpl_HEH_PKW',
                            'Parkpl_HEH_BUS', 'Parkpl_HZW_PKW', 'Parkpl_HZW_BUS'
                            ]
    }
    if category == "weather":
        selected_properties = st.multiselect("Select the weather properties", category_based_filters[category], default=None)
        selected_sensors = None

    elif category == "parking":
        selected_properties = st.multiselect("Select the parking values", category_based_filters[category]['properties'], default=None)  
        selected_sensors = st.selectbox("Select the parking sensor you want to find the values for?", category_based_filters[category]['sensors'])

    elif category == "visitor_sensors":
        visitor_sensors_data = get_data_from_query(
            selected_category=category,
            selected_query=None,
            selected_query_type=None,
            start_date=start_date,
            end_date=end_date,
            selected_sensors=None)
        
        visitor_sensor_options = list(set(visitor_sensors_data.columns.tolist()) - set(["month", "year", "season"]))

        selected_sensors = st.multiselect("Select the visitor sensor you want to find the count for?", visitor_sensor_options, default=None)
        selected_properties = None

    elif category == "visitor_centers":
        selected_sensors = st.multiselect("Select the visitor center you want to find the count for?", category_based_filters[category], default=None)
        selected_properties = None

    else:
        selected_properties = None
        selected_sensors = None

    return selected_properties, selected_sensors

def get_queries_for_parking(start_date, end_date, selected_properties, selected_sensors):
    
    """
    Generate queries for parking data based on selected date range, properties, and sensors.

    Args:
        start_date (str): The start date for the query.
        end_date (str): The end date for the query.
        selected_properties (list): List of parking properties to include in the query (e.g., occupancy, capacity).
        selected_sensors (list): List of parking sensors to query data for.

    Returns:
        dict: A dictionary with keys "type1", "type2", and "type3" containing queries for the date range.
    """
          
    queries = {}

    if selected_sensors:
        for property in selected_properties:
            queries.setdefault("type1", []).append(
                f"What is the {property} value for the sensor {selected_sensors} from {start_date} to {end_date}?"
            )

    return queries

def get_queries_for_weather(start_date, end_date, selected_properties):
    queries = {}

    # Queries for the date range (type1)
    for property in selected_properties:
        queries.setdefault("type4", []).append(
            f"What is the {property} value from {start_date} to {end_date}?"
        )

    return queries

def get_queries_for_visitor_centers(start_date, end_date, selected_sensors):
    
    """
    Generate queries for visitor center data based on selected date range,and sensors.

    Args:
        start_date (str): The start date for the query.
        end_date (str): The end date for the query.
        selected_sensors (list): List of visitor center sensors to query data for.

    Returns:
        dict: A dictionary with keys "type4", "type5", and "type6" containing queries for the date range.
    """

    queries = {}

    # Queries for the date range (type1)
    for sensor in selected_sensors:
        queries.setdefault("type4", []).append(
            f"What is the {sensor} value from {start_date} to {end_date}?"
        )

    return queries


def get_queries_for_visitor_sensors(start_date, end_date, selected_sensors):
    
    """
    Generate queries for visitor sensor data based on selected date range and sensors.

    Args:
        start_date (str): The start date for the query.
        end_date (str): The end date for the query.
        selected_sensors (list): List of visitor sensors to query data for.

    Returns:
        dict: A dictionary with keys "type4", "type5", and "type6" containing queries for the date range.
    """
    
    queries = {}

    # Queries for the date range (type1)
    for sensor in selected_sensors:
        queries.setdefault("type4", []).append(
            f"What is the {sensor} value from {start_date} to {end_date}?"
        )

    return queries


def generate_queries(category, start_date, end_date, selected_properties, selected_sensors):
    
    """
    Generate queries based on the selected category and date range.

    Args:
        category (str): The category of data (e.g., 'parking', 'weather', 'visitor_sensors', 'visitor_centers').
        start_date (str): The start date for the queries.
        end_date (str): The end date for the queries.
        selected_properties (list): List of selected properties relevant to the category.
        selected_sensors (list): List of selected sensors relevant to the category.

    Returns:
        dict: A dictionary containing generated queries based on the specified category and filters.
    """

    if category == 'parking':
        queries = get_queries_for_parking(start_date, end_date,selected_properties, selected_sensors)
    if category == 'weather':
        queries = get_queries_for_weather(start_date, end_date, selected_properties)
    if category == 'visitor_sensors':
        queries = get_queries_for_visitor_sensors(start_date, end_date, selected_sensors)
    if category == 'visitor_centers':
        queries = get_queries_for_visitor_centers(start_date, end_date, selected_sensors)
    return queries

def get_query_section():
    
    """
    Get the query section for data selection and execution.

    This function displays a user interface for selecting data categories, date ranges, 
    and additional filters. It allows users to generate 
    specific queries and execute them to retrieve data.

    Returns:
        None: This function does not return any values but updates the Streamlit UI
              with the selected query results and visualizations.
    """
    # display the query box
    st.markdown("## Data query")

    col1, col2 = st.columns((1,1))

    with col1:
        selected_category = select_category()
        print(selected_category)

    with col2:
        start_date, end_date = select_date()
        print(start_date, end_date)
       
    selected_properties, selected_sensors = select_filters(selected_category, start_date, end_date)
    
    # Give options to select your queries in form of a dropdown
    queries_dict = generate_queries(selected_category, start_date, end_date, selected_properties,selected_sensors)

    # get all the values of the all the keys in the dictionary queries

    queries = [query for query_list in queries_dict.values() for query in query_list]

    selected_query_type = None

    with st.form("Select a query"):

        selected_query = st.selectbox("Select a query", queries)
        
        # get the type of the selected query from the queries dictionary
        for key, value in queries_dict.items():
            if selected_query in value:
                selected_query_type = key

        submitted = st.form_submit_button(":green[Run Query]")
    if submitted:
        # get_data_from_query(selected_query,selected_query_type,selected_category)
        queried_df = get_data_from_query(selected_category,selected_query,selected_query_type, start_date, end_date, selected_sensors)
        
        # handle error if the queried df is an empty dataframe
        if queried_df.empty:
            st.error("Error: The query returned an empty dataframe. Please try again.")
            st.stop()
        else:
            st.write("Query executed successfully!")

            # get visualization for the queried data
            get_visualization_section(queried_df)


  


