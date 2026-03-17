import pandas as pd
from src.utils import read_dataframe_from_azure, upload_dataframe_to_azure, query_azure_with_duck_db

# Mapping data upload categories to specific folders in Azure Blob Storage
data_upload_categories_to_azure_folders = {
    "Permanente Besucherzählung (Eco-Counter)": "visitor-counts-eco-counter",
    "Hütten: Zählungen, Wetterstationsdaten,Öffnungszeiten & Feiertage": "huts-counts-openings-weather-station-holidays",
    "Sonderzählungen": "special-counts",
}

dataset_min_times = pd.DataFrame()

for category, folder in data_upload_categories_to_azure_folders.items():

    min_date = query_azure_with_duck_db(
            directory=f"data-hub/preprocessed-data/{folder}",
            columns=["general_time_index"],
            order_by="general_time_index ASC",
            limit=1
        )
    
    dataset_min_times = pd.concat([dataset_min_times, min_date])


print(dataset_min_times.min().iloc[0])


# test_data = read_dataframe_from_azure(
#     source_folder="data-hub/preprocessed-data/huts-counts-openings-weather-station-holidays",
#     file_name="preprocessed-huts-counts-openings-weather-station-holidays-2026-03-17 08:42:17.parquet",
#     file_format="parquet",
# )

# print(test_data.head())