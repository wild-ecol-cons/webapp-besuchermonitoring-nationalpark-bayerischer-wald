import os


# ------ AZURE CONFIG -----
# Define Azure Blob Storage container where data from this project is stored
CONTAINER_NAME = "webapp-besuchermonitoring-data-dev"

# Get Azure account name and key from secrets
AZURE_ACCOUNT_NAME = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_ACCOUNT_KEY = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")

# Define Azure Blob Storage configuration
storage_options = {
    "account_name": AZURE_ACCOUNT_NAME,
    "account_key": AZURE_ACCOUNT_KEY
}

# Construct the connection string
CONNECTION_STRING = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={AZURE_ACCOUNT_NAME};"
    f"AccountKey={AZURE_ACCOUNT_KEY};"
    f"EndpointSuffix=core.windows.net"
)


# ------ FURTHER PROJECT CONFIG -----
# Categorize sub-regions to user-friendly region-names
regions = {
    'Bayerischer Wald Total': ['sum_IN_abs', 'sum_OUT_abs'],
    'Nationalparkzentrum Falkenstein': ['Nationalparkzentrum Falkenstein IN', 'Nationalparkzentrum Falkenstein OUT'],
    'Nationalparkzentrum Lusen': ['Nationalparkzentrum Lusen IN', 'Nationalparkzentrum Lusen OUT'],
    'Falkenstein-Schwellhäusl': ['Falkenstein-Schwellhäusl IN', 'Falkenstein-Schwellhäusl OUT'],
    'Scheuereck-Schachten-Trinkwassertalsperre': ['Scheuereck-Schachten-Trinkwassertalsperre IN', 'Scheuereck-Schachten-Trinkwassertalsperre OUT'],
    'Lusen-Mauth-Finsterau': ['Lusen-Mauth-Finsterau IN', 'Lusen-Mauth-Finsterau OUT'],
    'Rachel-Spiegelau': ['Rachel-Spiegelau IN', 'Rachel-Spiegelau OUT'],
}

# Mapping data upload categories to specific folders in Azure Blob Storage
data_upload_categories_to_azure_folders = {
    "Permanente Besucherzählung (Eco-Counter)": "visitor-counts-eco-counter",
    "(legacy) Hütten: Zählungen, Wetterstationsdaten,Öffnungszeiten & Feiertage": "huts-counts-openings-weather-station-holidays",
    "Sonderzählungen": "special-counts",
    "Parkplatzzählungen": None,
    "Wetterdaten": None,
    "Häuserzählungen der Vemcount API": None,
    # "Schulferien & Feiertage (BY & CZ)", # TODO: Add this at the end of the project if time allows
}

data_upload_categories_time_cols_freq = {
    "Permanente Besucherzählung (Eco-Counter)": {"col": "Time", "freq": "1 hour"},
    "(legacy) Hütten: Zählungen, Wetterstationsdaten,Öffnungszeiten & Feiertage": {"col": "Datum", "freq": "1 day"},
    "Sonderzählungen": {"col": None, "freq": "1 hour"},
}

# Define sensor renaming dictionary: key = old sensor name, value = new sensor name
sensor_renaming_dictionary = {
    'Bucina IN': 'Bucina_Multi IN',
    'Bucina OUT': 'Bucina_Multi OUT',
    'Falkenstein 1 IN': 'TFG_Falkenstein_1 zum HZW',
    'Falkenstein 1 OUT': 'TFG_Falkenstein_1 zum Parkplatz',
    'Falkenstein 2 IN': 'TFG_Falkenstein_2 In Richtung TFG',
    'Falkenstein 2 OUT': 'TFG_Falkenstein_2 zum Parkplatz',
    'Lusen 1 IN': 'TFG_Lusen_1 IN',
    'Lusen 1 OUT': 'TFG_Lusen_1 Richtung Parkplatz',
    'Lusen 2 IN': 'TFG_Lusen_2 Richtung Vögel am Waldrand',
    'Lusen 2 OUT': 'TFG_Lusen_2 Richtung Parkplatz',
    'Lusen 3 IN': 'TFG_Lusen_3 In Richtung TFG',
    'Lusen 3 OUT': 'TFG_Lusen_3 In Richtung Parkplatz',
    'TFG_Lusen_3 TFG Lusen 3 IN': 'TFG_Lusen_3 In Richtung TFG',
    'TFG_Lusen_3 TFG Lusen 3 OUT': 'TFG_Lusen_3 In Richtung Parkplatz',
    'Trinkwassertalsperre IN': 'Trinkwassertalsperre_MULTI IN',
    'Trinkwassertalsperre OUT': 'Trinkwassertalsperre_MULTI OUT',
    'Waldspielgelände IN': 'Waldspielgelände_1 IN (Ins WSG)',
    'Waldspielgelände OUT': 'Waldspielgelände_1 OUT (aus dem WSG)',
    'Waldspielgelände_1 IN': 'Waldspielgelände_1 IN (Ins WSG)',
    'Waldspielgelände_1 OUT': 'Waldspielgelände_1 OUT (aus dem WSG)',
    'Gsenget IN.1': 'Gsenget IN',
    'Gsenget OUT.1': 'Gsenget OUT',
}

sensor_mapping_to_traffic_metrics = {
    'abs_col': [
        "Bayerisch Eisenstein", 
        "Brechhäuslau",
        "Bucina_Multi",
        "Deffernik",
        "Diensthüttenstraße",
        "Felswandergebiet",
        "Ferdinandsthal",
        "Fredenbrücke",
        "Gfäll",
        "Gsenget",
        "Klingenbrunner Wald",
        "Klosterfilz",
        "Racheldiensthütte",
        "Sagwassersäge",
        "Scheuereck",
        "Schillerstraße",
        "Schwarzbachbrücke",
        "TFG_Falkenstein_1",
        "TFG_Falkenstein_2",
        "TFG_Lusen_1",
        "TFG_Lusen_2",
        "TFG_Lusen_3",
        "Trinkwassertalsperre_MULTI",
        "Waldhausreibe",
        "Waldspielgelände_1",
        "Wistlberg"],

    'in_col': [
        "Bayerisch Eisenstein IN",
        "Brechhäuslau IN",
        "Bucina_Multi IN",
        "Deffernik IN",
        "Diensthüttenstraße IN",
        "Felswandergebiet IN",
        "Ferdinandsthal IN",
        "Fredenbrücke IN",
        "Gfäll IN",
        "Gsenget IN",
        "Klingenbrunner Wald IN",
        "Klosterfilz IN",
        "Racheldiensthütte IN",
        "Sagwassersäge IN",
        "Scheuereck IN",
        "Schillerstraße IN",
        "Schwarzbachbrücke IN",
        "TFG_Falkenstein_1 zum HZW",
        'TFG_Falkenstein_2 In Richtung TFG',
        "TFG_Lusen_1 IN",
        "TFG_Lusen_2 Richtung Vögel am Waldrand",
        "TFG_Lusen_3 In Richtung TFG",
        "Trinkwassertalsperre_MULTI IN",
        "Waldhausreibe IN",
        "Waldspielgelände_1 IN (Ins WSG)",
        "Wistlberg IN"],
    'out_col': [
        "Bayerisch Eisenstein OUT",
        "Brechhäuslau OUT",
        "Bucina_Multi OUT",
        "Deffernik OUT",
        "Diensthüttenstraße OUT",
        "Felswandergebiet OUT",
        "Ferdinandsthal OUT",
        "Fredenbrücke OUT",
        "Gfäll OUT",
        "Gsenget OUT",
        "Klingenbrunner Wald OUT",
        "Klosterfilz OUT",
        "Racheldiensthütte OUT",
        "Sagwassersäge OUT",
        "Scheuereck OUT",
        "Schillerstraße OUT",
        "Schwarzbachbrücke OUT",
        "TFG_Falkenstein_1 zum Parkplatz",
        'TFG_Falkenstein_2 zum Parkplatz',
        "TFG_Lusen_1 Richtung Parkplatz",
        "TFG_Lusen_2 Richtung Parkplatz",
        "TFG_Lusen_3 In Richtung Parkplatz",
        "Trinkwassertalsperre_MULTI OUT",
        "Waldhausreibe OUT",
        "Waldspielgelände_1 OUT (aus dem WSG)",
        "Wistlberg OUT"]
}

# Slug names and coordinates of the visitor sensors that have real-time tracking of visitor occupancy to Bayern Cloud (defined in EPSG:4326 WGS84)
visitor_sensors_with_realtime_tracking = {
    "tfg-lusen-1": {
        "sensor_name": "Tierfreigelände Lusen - Sensor 1",
        "coordinates": (48.892878, 13.4875722),
    },
    "tfg-lusen-2": {
        "sensor_name": "Tierfreigelände Lusen - Sensor 2",
        "coordinates": (48.8928588, 13.4895289),
    },
    "tfg-lusen-3": {
        "sensor_name": "Tierfreigelände Lusen - Sensor 3",
        "coordinates": (48.90411, 13.4712889),
    },
    "tfg-falkenstein-1": {
        "sensor_name": "Tierfreigelände Falkenstein - Sensor 1",
        "coordinates": (49.0605989, 13.2421349),
    },
    "tfg-falkenstein-2": {
        "sensor_name": "Tierfreigelände Falkenstein - Sensor 2",
        "coordinates": (49.0595505, 13.238188),
    },
}

# Dictionary of house names and their respective location IDs in Vemcount
visitor_houses_with_realtime_tracking = {
    "33955": "Hans-Eisenmann-Haus",
    "33320": "Nationalparkverwaltung Bayerischer Wald - Haus zur Wildnis",
    "33951": "Waldgeschichtliches Museum St. Oswald"
}

# Coordinates (lat, lon) for each house, keyed by the same location IDs as
# visitor_houses_with_realtime_tracking. St. Oswald is missing pending coords.
visitor_house_coordinates = {
    "33955": (48.8901888, 13.4866217),   # Hans-Eisenmann-Haus
    "33320": (49.0604494, 13.2434616),   # Nationalparkverwaltung Bayerischer Wald - Haus zur Wildnis
    "33951": (48.891487, 13.427821),     # Waldgeschichtliches Museum St. Oswald
}

# Dictionary of parking sensors and their respective location IDs in Bayern Cloud, coordinates, and prettified names
parking_sensors = {
     "parkplatz-graupsaege-1":{
         "location_id":"e42069a6-702f-4ef4-b3b5-04e310d97ca0",
         "coordinates":(48.92414,13.44515),
         "pretty_name": "P+R Graupsäge"
     },
     "parkplatz-fredenbruecke-1":{
         "location_id":"fac08b6b-e9cb-40cd-a106-b9f2cbfc7447",
         "coordinates":(48.93759, 13.45431),
         "pretty_name": "Fredenbrücke"
     },
     "p-r-spiegelau-1":{
         "location_id":"ee0490b2-3cc5-4adb-a527-95267257598e",
         "coordinates":(48.9178,13.35544),
         "pretty_name": "P+R Spiegelau P+R"
     },
     "parkplatz-zwieslerwaldhaus-1":{
         "location_id":"6c9b765e-1ff9-401d-98bc-b0302ee65c62",
         "coordinates":(49.08802, 13.24647),
         "pretty_name": "Parkplatz Zwieslerwaldhaus (P1)"
     },
     "parkplatz-nationalparkzentrum-falkenstein-2":{
         "location_id":"a93b64e9-35fb-4b3e-8348-81ba8f1c0d6f",
         "coordinates":(49.06042,13.23583),
         "pretty_name": "Parkplatz Nationalparkzentrum Falkenstein"
     },
     "parkplatz-nationalparkzentrum-lusen-p2":{
         "location_id":"454b0f50-130b-4c21-9db2-b163e158c847",
         "coordinates":(48.89060, 13.48939),
         "pretty_name": "Parkplatz Nationalparkzentrum Lusen (P2)"
     },
     "parkplatz-waldhaeuser-kirche-1":{
         "location_id":"454b0f50-130b-4c21-9db2-b163e158c847",
         "coordinates":(48.92842,13.4624),
         "pretty_name": "Parkplatz Waldhäuser Kirche"   
     },
     "parkplatz-waldhaeuser-ausblick-1":{
         "location_id":"a14d8ebd-9261-49f7-875b-6a924fe34990",
         "coordinates":(48.92796,13.47076),
         "pretty_name": "Parkplatz Waldhäuser Ausblick"
     },
     "parkplatz-skisportzentrum-finsterau-1":{
         "location_id":"ea474092-1064-4ae7-955e-8db099955c16",
         "coordinates":(48.94129,13.57491),
         "pretty_name": "Parkplatz Finsterau Ski-/Sportstadion"
     },
     "parkplatz-zwieslerwaldhaus-nord-1": {
         "location_id":"4bbb3b5c-edc2-4b00-a923-91c1544aa29d",
         "coordinates":(49.09685, 13.23761),
         "pretty_name": "Parkplatz Zwieslerwaldhaus Nord"
     },
     "parkplatz-schillerstrasse": {
         "location_id":"eba1578b-9ca9-4a74-8855-09ed598331c8",
         "coordinates":(49.08805, 13.24864),
         "pretty_name": "Parkplatz Schillerstraße (P1)"
     },
     "skiwanderzentrum-zwieslerwaldhaus-2": {
         "location_id":"dd3734c2-c4fb-4e1d-a57c-9bbed8130d8f",
         "coordinates":(49.08676, 13.24436),
         "pretty_name": "Parkplatz Skiwanderzentrum Zwieslerwaldhaus"
     },
     "parkplatz-wistlberg-1": {
         "location_id":"13f76ce2-4b07-4e62-9623-cb19091d9a95",
         "coordinates":(48.94149, 13.57114),
         "pretty_name": "Parkplatz Wistlberg"
     },
     "scheidt-bachmann-parkplatz-1": {
         "location_id":"144e1868-3051-4140-a83c-41d4b79a6d14",
         "coordinates":(48.89186, 13.48963),
         "pretty_name": "Parkplatz Nationalparkzentrum Lusen (P1)"
     }
}