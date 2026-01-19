import streamlit as st
import pandas as pd
from src.config import CONNECTION_STRING, CONTAINER_NAME
from src.utils import read_dataframe_from_azure
from azure.storage.blob import BlobServiceClient


# Setup
base_folder = "raw-data/bf_raw_files"

def list_files_in_azure_folder(category: str) -> list:
    """Lists files in Azure for a given category and returns only file names."""

    folder_prefix = f"{base_folder}/{category.replace(' ', '_')}/"

    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    blob_list = container_client.list_blob_names(name_starts_with=folder_prefix)

    return [name.split('/')[-1] for name in blob_list]


def download_section():

    st.header("Download Data")
        
    # Select category
    category = st.selectbox(
        "Select the data category to browse",
        ["visitor_count_sensors", "visitors_count_centers", "other"]
    )
    
    # List files based on category
    if category:
        files = list_files_in_azure_folder(category)
        selected_file = st.selectbox("Select a file to preview", files) if files else None

        if selected_file:
            # Preview button (enabled only when a file is selected)
            preview_confirm = st.button(
                label="Preview Data",
                disabled=not selected_file,
                help="Preview the data before confirming download"
            )

            if preview_confirm:
                # Load and preview selected file
                st.write(f"Preview of {selected_file}")
                data = read_dataframe_from_azure(
                    file_name=selected_file,
                    file_format="csv",
                    source_folder=f"{base_folder}/{category.replace(' ', '_')}"
                )
                st.dataframe(data)

                # Download button
                st.download_button(
                    label="Download selected file as CSV",
                    data=data.to_csv(index=False),
                    file_name=selected_file,
                    mime='text/csv',
                )