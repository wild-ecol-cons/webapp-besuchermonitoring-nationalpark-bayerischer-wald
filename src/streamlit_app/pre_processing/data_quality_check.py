import pandas as pd
from src.streamlit_app.pre_processing.gen_config_for_visitor_sensors_and_centers import visitor_centers, visitor_sensors
import numpy as np
import re
import awswrangler as wr
import streamlit as st
import os
from src.config import aws_s3_bucket, CONNECTION_STRING, CONTAINER_NAME
from src.utils import upload_dataframe_to_azure, read_dataframe_from_azure
from azure.storage.blob import BlobClient    


raw_folder = "raw-data/bf_raw_files"
preprocessed_folder = "preprocessed_data/bf_preprocessed_files"
invalid_upload_folder = "invalid-data/bf_invalid_upload_files"

def convert_sensor_dictionary_to_excel_file(
        sensor_dict: dict,
        output_file_path: str) -> None:
    """
    Convert sensor dictionary to a Pandas Dataframe and save it as an Excel file.

    Info: This function is not used as of now, but might be useful in the future for handling changes to the sensor configuration.

    Args:
        sensor_dict (dict): A dictionary containing sensor data.
        output_file_path (str): The path to the output Excel file.

    Returns:
        None
    """
    regions = []
    sensor_names = []
    sensor_directions = []
    possible_sensor_ids = []

    for region, sensors in sensor_dict.items():
        for sensor_name, directions in sensors.items():
            for direction, sensor_ids in directions.items():
                regions.append(region)
                sensor_names.append(sensor_name)
                sensor_directions.append(direction)
                # Join the list into a comma-separated string
                possible_sensor_ids.append(",".join(sensor_ids))

    df = pd.DataFrame({
        "region": regions,
        "sensor_name": sensor_names,
        "sensor_direction": sensor_directions,
        "possible_sensor_ids": possible_sensor_ids
    })

    df.to_excel(output_file_path, index=False)
    
def convert_sensor_excel_file_to_dictionary(
        sensor_file_path: str) -> dict:
    """
    Convert Excel file containing sensor configuration data to a dictionary.

    Info: This function is not used as of now, but might be useful in the future for handling changes to the sensor configuration.

    Args:
        sensor_file_path (str): The path to the Excel file.

    Returns:
        dict: A dictionary containing sensor configuration.
    """
    df = pd.read_excel(sensor_file_path)
    
    sensor_dict = {}
    
    for index, row in df.iterrows():
        region = row["region"]
        sensor_name = row["sensor_name"]
        sensor_direction = row["sensor_direction"]
        possible_sensor_ids = row["possible_sensor_ids"].split(",")

        if region not in sensor_dict:
            sensor_dict[region] = {}

        if sensor_name not in sensor_dict[region]:
            sensor_dict[region][sensor_name] = {}

        if sensor_direction not in sensor_dict[region][sensor_name]:
            sensor_dict[region][sensor_name][sensor_direction] = []

        sensor_dict[region][sensor_name][sensor_direction] = possible_sensor_ids

    return sensor_dict

def get_and_match_columns(df,dict_to_check):
    
    # Flatten the sensor list from the dictionary
    sensors_list = []
    for _, sensors in dict_to_check.items():
        if isinstance(sensors, dict):  # Handle locations that have sensors stored as dictionaries
            for sensor, columns in sensors.items():
                sensors_list.extend(columns)
        elif isinstance(sensors, list):  # Handle locations like 'Time' that are lists
            sensors_list.extend(sensors)
    # Compare the DataFrame columns with the flattened sensor list
    missing_in_df = []
    extra_in_df = []

    # Check if any sensor in the dictionary is missing in the DataFrame columns
    for sensor in sensors_list:
        if sensor not in df.columns:
            missing_in_df.append(sensor)

    # Check if any column in the DataFrame is not found in the dictionary
    for column in df.columns:
        if column not in sensors_list:
            extra_in_df.append(column)
            # if column name has Unnamed in it and all the values are NaN, then drop the column
            if "Unnamed" in column and df[column].isnull().all():
                df.drop(column, axis=1, inplace=True)
                # update the extra_in_df list
                extra_in_df.remove(column)

    if missing_in_df or extra_in_df:
        # Show a streamlit error message saying which columns are missing and extra

        st.error(f"Missing columns in the DataFrame: {missing_in_df}")
        st.error(f"Extra columns in the DataFrame: {extra_in_df}")
        status = False
    else:
        status = True

    return status

def int_for_all_counts(df):
    """
    Convert all numeric columns in the DataFrame to integer type. Round float values that are not integers,
    and replace NaN values with 0 to allow conversion to integers.
    """
    # Loop through each column in the dataframe
    for column in df.columns:
        # Check if the column is of float type
        if df[column].dtype == "float64":
            # Apply the transformation only to non-NaN values
            df[column] = df[column].apply(lambda x: int(x) if pd.notna(x) and x.is_integer() else (round(x) if pd.notna(x) else np.nan))
            # Replace NaN values with 0 or another placeholder, then convert to integer
            df[column] = df[column].fillna(0).astype('int64')
        elif df[column].dtype == "object":
            pass

    return df

def convert_data_types(df, visitor_centers):
    # Visitor centers count should be integers
    visitor_center_columns = visitor_centers['visitor_centers_count'].values()
    for column_list in visitor_center_columns:
        for column in column_list:
            if column in df.columns:
                # Convert to numeric and fill NaN with 0, then convert to integer
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)

    # Holidays should be binary (0 or 1)
    holiday_columns = visitor_centers['holidays'].values()
    for column_list in holiday_columns:
        for column in column_list:
            if column in df.columns:
                # Convert to numeric and treat non-numeric as NaN, then ensure binary values (0 or 1)
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0)
                df[column] = df[column].apply(lambda x: 1 if x > 0 else 0).astype(int)

    # Opening or closed should be binary (0 or 1)
    opening_columns = visitor_centers['opening_or_closed'].values()
    for column_list in opening_columns:
        for column in column_list:
            if column in df.columns:
                # Convert to numeric and ensure binary (0 or 1)
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0)
                df[column] = df[column].apply(lambda x: 1 if x > 0 else 0).astype(int)

    
    # Retrieve 'Datum' and 'Wochentag' from the dictionary
    time_column = visitor_centers.get('time', [None])[0]  # Get 'time' field, default to None if not found
    day_column = visitor_centers.get('Day', [None])[0]    # Get 'Day' field, default to None if not found

    # Convert 'time' column to datetime
    if time_column and time_column in df.columns:
        df[time_column] = pd.to_datetime(df[time_column], errors='coerce')

    # Convert 'Day' column to string
    if day_column and day_column in df.columns:
        df[day_column] = df[day_column].astype(str)

    # Convert temperature columns to float64, except 'Laubfärbung' to binary
    temperature_columns = visitor_centers['temperature_columns'].values()
    for column_list in temperature_columns:
        for column in column_list:
            if column in df.columns:
                if column == 'Laubfärbung':
                    # Convert Laubfärbung to binary (0 or 1)
                    df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0)
                    df[column] = df[column].apply(lambda x: 1 if x > 0 else 0).astype(int)
                else:
                    # Convert other temperature columns to float64
                    df[column] = pd.to_numeric(df[column], errors='coerce').astype('float64')

    return df

def parse_german_dates(
    df: pd.DataFrame,
    date_column_name: str
) -> pd.DataFrame:
    """
    Parses German dates in the specified date column of the DataFrame using regex,
    including hours and minutes if available.

    Args:
        df (pd.DataFrame): The DataFrame containing the date column.
        date_column_name (str): The name of the date column.

    Returns:
        pd.DataFrame: The DataFrame with parsed German dates.
    """
    
    # Define a mapping of German month names to their numeric values
    month_map = {
        "Jan.": "01",
        "Feb.": "02",
        "März": "03",
        "Apr.": "04",
        "Mai": "05",
        "Juni": "06",
        "Juli": "07",
        "Aug.": "08",
        "Sep.": "09",
        "Okt.": "10",
        "Nov.": "11",
        "Dez.": "12"
    }

    # Create a regex pattern for replacing months and capturing time
    pattern = re.compile(r'(\d{1,2})\.\s*(' + '|'.join(month_map.keys()) + r')\s*(\d{4})\s*(\d{2}):(\d{2})')

    # Function to replace the month in the matched string and keep the time part
    def replace_month(match):
        day = match.group(1)
        month = month_map[match.group(2)]
        year = match.group(3)
        hour = match.group(4)
        minute = match.group(5)
        return f"{year}-{month}-{day} {hour}:{minute}:00"

    # Apply regex replacement and convert to datetime
        # Apply regex replacement and convert to datetime, only for string values
    df[date_column_name] = df[date_column_name].apply(
        lambda x: replace_month(pattern.search(str(x))) if isinstance(x, str) and pattern.search(str(x)) else x
    )
    df[date_column_name] = pd.to_datetime(df[date_column_name], errors='coerce')

    return df

def start_and_end_dates(df,time_column):
    start_date = df[time_column].min()
    end_date = df[time_column].max()

    # get the start and the end dates with the format YYYY-MM-DD without the time
    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    return start_date, end_date

def process_and_upload_data(
        new_processed_df,
        target_folder,
        file_name,
        time_column):
    
    try:
        blob_to_verify = BlobClient.from_connection_string(
            conn_str=CONNECTION_STRING, 
            container_name=CONTAINER_NAME, 
            blob_name=f"{target_folder}/{file_name}.csv"
        )
        
        # Check if the file already exists in Azure
        if blob_to_verify.exists():
            st.info("Existing preprocessed file found. Reading and concatenating with new data...")
            
            # Read the last edited file from the preprocessed folder
            old_processed_df = read_dataframe_from_azure(
                file_name=file_name,
                file_format="csv",
                source_folder=target_folder
            )
            
            # Concatenate the old and the new dataframes
            new_df = pd.concat([old_processed_df, new_processed_df], ignore_index=True)
            
            # Handle duplicate dates (remove rows with the same 'Time' value, keeping the last: as the concatination in the previous step concats as last dataframe the new data, keep='last' will keep the latest data when there are duplicates in the 'Time' column")
            new_df = new_df.drop_duplicates(subset=[time_column], keep='last')

        else:
            st.info("No preprocessed file found. Using the new data only.")
            # If the file does not exist, just use the new data
            new_df = new_processed_df

        # Add upload timestamp to the new data
        new_df["Upload_time"] = pd.to_datetime("today").strftime('%Y-%m-%d %H:%M:%S')

        # Write the new dataframe back to S3
        upload_dataframe_to_azure(
            df=new_df,
            file_name=file_name,
            target_folder=target_folder,
            file_format="csv",
            write_options={"index": False}
        )
        st.success("Data successfully processed and uploaded to the preprocessed folder.")

    except Exception as e:
        st.error(f"Error while processing data: {e}")

def data_quality_check(data,category):

    if category == "visitor_count_sensors":
        configuration_dict = visitor_sensors
    elif category == "visitors_count_centers":
        configuration_dict = visitor_centers
    else:
        status = True # This category does not have a data quality check implemented, so return True
        return status

    # check the column names from the data
    status = get_and_match_columns(data, configuration_dict)
    
    if status:
        # get the time column from the configuration dictionary
        time_column = configuration_dict.get('time', [None])[0]

        fixed_dates_data_df = parse_german_dates(data, time_column)

        # get the start and the end dates
        start_date, end_date = start_and_end_dates(fixed_dates_data_df, time_column)
        
        if category == "visitor_count_sensors":
            new_processed_df = int_for_all_counts(fixed_dates_data_df)
        elif category == "visitors_count_centers":
            new_processed_df = convert_data_types(fixed_dates_data_df, visitor_centers)
                    
        # Add upload times the last column of the dataframe
        new_processed_df["Upload_time"] = pd.to_datetime("today").strftime('%Y-%m-%d %H:%M:%S')

        process_and_upload_data(
            new_processed_df=new_processed_df,
            target_folder=f"{preprocessed_folder}/{category}",
            file_name=f"{category}_preprocessed",
            time_column=time_column
        )

    else:
        print("Data quality check failed. Please check the columns in the uploaded file.")
        upload_time = pd.to_datetime("today").strftime('%Y-%m-%d %H:%M:%S')

        upload_dataframe_to_azure(
            df=data,
            file_name=f"{category}_{upload_time}.csv",
            target_folder=f"{invalid_upload_folder}/{category}",
            file_format="csv",
            write_options={
                "index": False
            }
        )

        st.error("If you have changed any column names, please update and try again. Your file is now uploaded to the invalid folder in the AWS S3 bucket.")
    
    return status
    

if __name__ == "__main__":
    data_quality_check()
