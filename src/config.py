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