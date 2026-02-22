import pandas as pd
from src.utils import read_dataframe_from_azure, upload_dataframe_to_azure
from src.config import CONNECTION_STRING

# ----- UPLOAD TEST DATA -----

# def source_preprocessed_hourly_visitor_center_data():

#     """
#     Load the preprocessed hourly visitor center data from the cloud.
#     """

#     print("Sourcing the historic preprocessed_hourly_visitor_center_data")

#     # Load visitor count data from the cloud
#     preprocessed_hourly_visitor_center_data = read_dataframe_from_azure(
#         file_name="visitor_centers_hourly_2017_to_2026.parquet",
#         file_format="parquet",
#         source_folder="preprocessed_data",
#     )

#     print(f"The historic preprocessed_hourly_visitor_center_data is: {preprocessed_hourly_visitor_center_data}")

#     return preprocessed_hourly_visitor_center_data

# visitor_center_data = source_preprocessed_hourly_visitor_center_data()
# print(f"The total number of entries is: {len(visitor_center_data)}")

# # partition the visitor center data in 4 random splits and
# file_1 = visitor_center_data[visitor_center_data["Time"] < "2021-01-01"]
# file_2 = visitor_center_data[visitor_center_data["Time"] < "2024-01-01"]
# file_3 = visitor_center_data[visitor_center_data["Time"] < "2019-01-01"]
# file_4 = visitor_center_data
# print(f"The number of all entries from file_1 to file_4 is: {len(file_1) + len(file_2) + len(file_3) + len(file_4)}")

# for i in range(4):
#     upload_dataframe_to_azure(
#         df=eval(f'file_{i + 1}'),
#         file_name="visitor_centers_hourly_file_" + str(i + 1) + ".parquet",
#         target_folder="duck-db-test/visitor_centers_data",
#         file_format="parquet",
#     )

# ---- TEST DUCK DB ----

import duckdb

# Connect to a transient in-memory DuckDB
conn = duckdb.connect(database=':memory:')

# Install and load the Azure extension (one-time per session)
conn.execute("INSTALL azure; LOAD azure;")

# Using the modern Secret syntax is more reliable than SET variables
secret_query = f"""
CREATE SECRET (
    TYPE AZURE,
    CONNECTION_STRING '{CONNECTION_STRING}'
);
"""
conn.execute(secret_query)

# 2. Querying directly on the Blob URL
container_url = "az://webapp-besuchermonitoring-data-dev/duck-db-test/visitor_centers_data/*.parquet"

query = f"""
    SELECT Time, Jahr, Jahreszeit 
    FROM '{container_url}'
"""

try:
    queried_data = conn.execute(query)
    print(queried_data)
except Exception as e:
    print(e)