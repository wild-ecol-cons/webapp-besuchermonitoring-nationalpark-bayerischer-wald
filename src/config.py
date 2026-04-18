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
    'Falkenstein-Schwellhäusl': ['Falkenstein-Schwellhausl IN', 'Falkenstein-Schwellhausl OUT'],
    'Scheuereck-Schachten-Trinkwassertalsperre': ['Scheuereck-Schachten-Trinkwassertalsperre IN', 'Scheuereck-Schachten-Trinkwassertalsperre OUT'],
    'Lusen-Mauth-Finsterau': ['Lusen-Mauth-Finsterau IN', 'Lusen-Mauth-Finsterau OUT'],
    'Rachel-Spiegelau': ['Rachel-Spiegelau IN', 'Rachel-Spiegelau OUT'],
}

# Mapping data upload categories to specific folders in Azure Blob Storage
data_upload_categories_to_azure_folders = {
    "Permanente Besucherzählung (Eco-Counter)": "visitor-counts-eco-counter",
    "Hütten: Zählungen, Wetterstationsdaten,Öffnungszeiten & Feiertage": "huts-counts-openings-weather-station-holidays",
    "Sonderzählungen": "special-counts",
    "Parkplatzzählungen": None,
    "Wetterdaten": None,
    # "Schulferien & Feiertage (BY & CZ)", # TODO: Add this at the end of the project if time allows
}

data_upload_categories_time_cols_freq = {
    "Permanente Besucherzählung (Eco-Counter)": {"col": "Time", "freq": "1 hour"},
    "Hütten: Zählungen, Wetterstationsdaten,Öffnungszeiten & Feiertage": {"col": "Datum", "freq": "1 day"},
    "Sonderzählungen": {"col": None, "freq": "1 hour"},
}

# Define sensor renaming dictionary: key = old sensor name, value = new sensor name
sensor_mapping_dictioanry = {
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

individual_sensor_region_mapping = {
    'Bayerisch Eisenstein IN': 'Falkenstein-Schwellhäusl',
    'Bayerisch Eisenstein OUT': 'Falkenstein-Schwellhäusl', 
    'Brechhäuslau IN': 'Falkenstein-Schwellhäusl', 
    'Brechhäuslau OUT': 'Falkenstein-Schwellhäusl', 
    'Deffernik IN': 'Falkenstein-Schwellhäusl',
    'Deffernik OUT': 'Falkenstein-Schwellhäusl',
    'Falkenstein 1 IN': 'Nationalparkzentrum Falkenstein', 
    'Falkenstein 1 OUT': 'Nationalparkzentrum Falkenstein',
    'Falkenstein 2 IN': 'Nationalparkzentrum Falkenstein', 
    'Falkenstein 2 OUT': 'Nationalparkzentrum Falkenstein',
    'Ferdinandsthal IN': 'Falkenstein-Schwellhäusl', 
    'Ferdinandsthal OUT': 'Falkenstein-Schwellhäusl', 
    'Gsenget IN': 'Scheuereck-Schachten-Trinkwassertalsperre', 
    'Gsenget OUT': 'Scheuereck-Schachten-Trinkwassertalsperre', 
    'Scheuereck IN': 'Scheuereck-Schachten-Trinkwassertalsperre',
    'Scheuereck OUT': 'Scheuereck-Schachten-Trinkwassertalsperre', 
    'Schillerstraße IN': 'Falkenstein-Schwellhäusl', 
    'Schillerstraße OUT': 'Falkenstein-Schwellhäusl', 
    'Trinkwassertalsperre IN': 'Scheuereck-Schachten-Trinkwassertalsperre',
    'Trinkwassertalsperre OUT': 'Scheuereck-Schachten-Trinkwassertalsperre',
    'Bucina IN': 'Lusen-Mauth-Finsterau',
    'Bucina OUT': 'Lusen-Mauth-Finsterau', 
    'Diensthüttenstraße IN': 'Rachel-Spiegelau', 
    'Diensthüttenstraße OUT': 'Rachel-Spiegelau',
    'Felswandergebiet IN': 'Lusen-Mauth-Finsterau', 
    'Felswandergebiet OUT': 'Lusen-Mauth-Finsterau',
    'Fredenbrücke IN': 'Lusen-Mauth-Finsterau', 
    'Fredenbrücke OUT': 'Lusen-Mauth-Finsterau', 
    'Gfäll IN': 'Rachel-Spiegelau', 
    'Gfäll OUT': 'Rachel-Spiegelau', 
    'Klingenbrunner Wald IN': 'Rachel-Spiegelau', 
    'Klingenbrunner Wald OUT': 'Rachel-Spiegelau', 
    'Klosterfilz IN': 'Rachel-Spiegelau', 
    'Klosterfilz OUT': 'Rachel-Spiegelau',
    'Lusen 1 IN': 'Nationalparkzentrum Lusen', 
    'Lusen 1 OUT': 'Nationalparkzentrum Lusen', 
    'Lusen 2 IN': 'Nationalparkzentrum Lusen',
    'Lusen 2 OUT': 'Nationalparkzentrum Lusen', 
    'Lusen 3 IN': 'Nationalparkzentrum Lusen', 
    'Lusen 3 OUT': 'Nationalparkzentrum Lusen',
    'Racheldiensthütte IN': 'Rachel-Spiegelau', 
    'Racheldiensthütte OUT': 'Rachel-Spiegelau',
    'Schwarzbachbrücke IN': 'Lusen-Mauth-Finsterau', 
    'Schwarzbachbrücke OUT': 'Lusen-Mauth-Finsterau', 
    'Waldhausreibe IN': 'Lusen-Mauth-Finsterau', 
    'Waldhausreibe OUT': 'Lusen-Mauth-Finsterau', 
    'Waldspielgelände IN': 'Rachel-Spiegelau', 
    'Waldspielgelände OUT': 'Rachel-Spiegelau', 
    'Wistlberg IN': 'Lusen-Mauth-Finsterau', 
    'Wistlberg OUT': 'Lusen-Mauth-Finsterau', 
    'Sagwassersäge IN': 'Lusen-Mauth-Finsterau',
    'Sagwassersäge OUT': 'Lusen-Mauth-Finsterau'
}