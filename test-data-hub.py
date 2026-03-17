from src.utils import upload_file_to_azure, upload_dataframe_to_azure, query_azure_with_duck_db

# Mapping data upload categories to specific folders in Azure Blob Storage
data_upload_categories_to_azure_folders = {
    "Permanente Besucherzählung (Eco-Counter)": "visitor-counts-eco-counter",
    "Hütten: Zählungen, Wetterstationsdaten,Öffnungszeiten & Feiertage": "huts-counts-openings-weather-station-holidays",
    "Sonderzählungen": "special-counts",
}


for category, folder in data_upload_categories_to_azure_folders.items():

    queried_data = query_azure_with_duck_db(
            directory=f"data-hub/preprocessed-data/{folder}",
            select_string="ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_index"
        )

    print(f"Data for {category}:\n")
    print(queried_data.head())