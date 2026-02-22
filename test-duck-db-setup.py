import pandas as pd
from src.utils import read_dataframe_from_azure, upload_dataframe_to_azure
from src.config import CONNECTION_STRING, AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY

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
from typing import List, Optional


def set_up_duck_db_connection() -> duckdb.DuckDBPyConnection:
    """
    Set up a DuckDB connection via files on Azure Blob Storage to a transient in-memory database. This connection is used to query data with SQL queries.

    Returns:
        duckdb.DuckDBPyConnection: A DuckDB connection
    """
    # Connect to a transient in-memory DuckDB
    conn = duckdb.connect(database=':memory:')

    # Install and load the Azure extension (one-time per session)
    conn.execute("INSTALL azure; LOAD azure;")

    # Authenticate with Azure
    secret_query = f"""
    CREATE OR REPLACE SECRET (
        TYPE AZURE,
        CONNECTION_STRING '{CONNECTION_STRING}'
    );
    """
    conn.execute(secret_query)

    # Set parameter to solve certificate issue
    conn.execute("SET azure_transport_option_type = 'curl';")
    return conn

def query_azure_with_duck_db(
    conn: duckdb.DuckDBPyConnection,
    directory: str,
    columns: List[str] = ["*"],
    filters: Optional[str] = None,
    limit: Optional[int] = 10
) -> pd.DataFrame:
    """
    Queries Parquet files on Azure with optional filtering and selection.

    Args:
        conn (duckdb.DuckDBPyConnection): A DuckDB connection
        directory (str): The directory path within the container
        columns (List[str], optional): A list of column names to select. Defaults to ["*"].
        filters (Optional[str], optional): An SQL WHERE clause to apply to the query. Defaults to None.
        limit (Optional[int], optional): The maximum number of rows to return. Defaults to 10.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the results of the query
    """
    # 1. Construct the column string
    col_selector = ", ".join(columns)
    
    # 2. Base URL
    path = f"az://webapp-besuchermonitoring-data-dev/{directory}/*.parquet"
    
    # 3. Build the SQL string dynamically
    query = f"SELECT {col_selector} FROM read_parquet('{path}')"
    
    # 4. Append WHERE clause if filters are provided
    if filters:
        query += f" WHERE {filters}"
    
    # 5. Append LIMIT
    if limit:
        query += f" LIMIT {limit}"
    
    try:
        return conn.execute(query).df()
    except Exception as e:
        print(f"Query failed: {e}")
        return pd.DataFrame()


DIRECTORY = "duck-db-test/visitor_centers_data"
conn = set_up_duck_db_connection()

test_data = query_azure_with_duck_db(
    conn=conn,
    directory=DIRECTORY,
    columns=["Time", "Jahr"],
    filters="Time > '2021-01-01' AND Time < '2024-01-01'",
    limit=20
)

print(test_data)