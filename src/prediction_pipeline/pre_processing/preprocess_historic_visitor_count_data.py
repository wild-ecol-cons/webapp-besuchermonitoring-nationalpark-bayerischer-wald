"""
Clean historic sensor data from 2016 to 2024. In the docstring of every function you can check what it does and the assumptions that were made.

Usage:
- Change the global variables section if needed
    - Fill your AWS credentiales


Output:
- Returns the preprocessed data
"""



#import libraries

import pandas as pd
import re
import numpy as np
import os

pd.options.mode.chained_assignment = None  


###########################################################################################
#GLOBAL VARIABLES
###########################################################################################

output_data_folder = os.path.join("data","processed","visitor_sensor")
output_file_name = "preprocessed_visitor_sensor_data.csv"


##############################################################################################

# Setting up AWS

# boto3.setup_default_session(profile_name=aws_profile) 
    
# Functions

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
    df[date_column_name] = df[date_column_name].apply(lambda x: replace_month(pattern.search(x)) if pattern.search(x) else x)
    df[date_column_name] = pd.to_datetime(df[date_column_name], errors='coerce')

    return df


def fix_columns_names(df):
    """
    Processes the given DataFrame by renaming columns, dropping specified columns, and creating a new column for Bucina_Multi IN by summing the Bucina_Multi Fahrräder IN and Bucina_Multi Fußgänger IN columns. .

    Args:
        df (pd.DataFrame): The DataFrame to be modified.
        rename (dict): A dictionary where the keys are existing column names and the values are the new column names.
        drop (list): A list of column names that should be removed from the DataFrame.
        create (str): The name of the new column that will be created by summing the "Bucina_Multi Fahrräder IN" 
                      and "Bucina_Multi Fußgänger IN" columns.

    Returns:
        pd.DataFrame: The modified DataFrame with the specified changes applied.
    """

    #lists and dictionaries for columns that need to be dropped or renamed

    drop = ['Brechhäuslau Fußgänger IN', 'Brechhäuslau Fußgänger OUT', 'Waldhausreibe Channel 1 IN', 'Waldhausreibe Channel 2 OUT'] #Waldhausreibe Channel 1 (IN and OUT) had a total sum of values of 10 and 13. Brechhäuslau columns were duplicated.

    rename = {'Bucina IN': 'Bucina PYRO IN',
          'Bucina OUT': 'Bucina PYRO OUT',
          'Gsenget IN.1': 'Gsenget Fußgänger IN',
          'Gsenget OUT.1': 'Gsenget Fußgänger OUT',
          'Gfäll Fußgänger IN' : 'Gfäll IN',
          'Gfäll Fußgänger OUT': 'Gfäll OUT',
          'Fredenbrücke Fußgänger IN' : 'Fredenbrücke IN',
          'Fredenbrücke Fußgänger OUT': 'Fredenbrücke OUT',
          'Diensthüttenstraße Fußgänger IN': 'Diensthüttenstraße IN' ,
          'Diensthüttenstraße Fußgänger OUT': 'Diensthüttenstraße OUT',
          'Racheldiensthütte Cyclist OUT' : 'Racheldiensthütte Fahrräder OUT',
          'Racheldiensthütte Pedestrian IN' : 'Racheldiensthütte Fußgänger IN',
          'Racheldiensthütte Pedestrian OUT' : 'Racheldiensthütte Fußgänger OUT',
          'Sagwassersäge Fußgänger IN' : 'Sagwassersäge IN',
          'Sagwassersäge Fußgänger OUT': 'Sagwassersäge OUT',
          'Schwarzbachbrücke Fußgänger IN' : 'Schwarzbachbrücke IN',
          'Schwarzbachbrücke Fußgänger OUT' : 'Schwarzbachbrücke OUT',
          'NPZ_Falkenstein IN' : 'Falkenstein 1 PYRO IN',
          'NPZ_Falkenstein OUT' : 'Falkenstein 1 PYRO OUT',
          'TFG_Falkenstein_1 Fußgänger zum Parkplatz' : 'Falkenstein 1 OUT',
          'TFG_Falkenstein_1 Fußgänger zum HZW' : 'Falkenstein 1 IN',
          'TFG_Falkenstein_2 Fußgänger In Richtung Parkplatz' : 'Falkenstein 2 OUT',
          'TFG_Falkenstein_2 Fußgänger In Richtung TFG' : 'Falkenstein 2 IN',
          'TFG_Lusen IN' : 'Lusen 1 PYRO IN',
          'TFG_Lusen OUT' : 'Lusen 1 PYRO OUT',
          'TFG_Lusen_1 Fußgänger Richtung TFG': 'Lusen 1 EVO IN',
          'TFG_Lusen_1 Fußgänger Richtung Parkplatz' : 'Lusen 1 EVO OUT',
          'TFG_Lusen_2 Fußgänger Richtung Vögel am Waldrand': 'Lusen 2 IN',
          'TFG_Lusen_2 Fußgänger Richtung Parkplatz' : 'Lusen 2 OUT',
          'TFG_Lusen_3 TFG Lusen 3 IN': 'Lusen 3 IN',
          'TFG_Lusen_3 TFG Lusen 3 OUT': 'Lusen 3 OUT',
          'Waldspielgelände_1 IN': 'Waldspielgelände IN',
          'Waldspielgelände_1 OUT': 'Waldspielgelände OUT',
          'Wistlberg Fußgänger IN' : 'Wistlberg IN',
          'Wistlberg Fußgänger OUT' : 'Wistlberg OUT',
          'Trinkwassertalsperre IN' : 'Trinkwassertalsperre PYRO IN', 
          'Trinkwassertalsperre OUT' : 'Trinkwassertalsperre PYRO OUT'
          }


    # Rename columns according to the provided mapping
    
    df.rename(columns=rename, inplace=True)
    print(len(rename), ' columns were renamed')

    # Remove the specified columns from the DataFrame
    df.drop(columns=drop, inplace=True)
    print(len(drop), ' repeated columns were dropped')

    # Add Bucina_Multi IN column by summing Fahrraeder and Fussgaenger columns
    df['Bucina_Multi IN'] = df["Bucina_Multi Fahrräder IN"] + df["Bucina_Multi Fußgänger IN"]
    print('Bucina_Multi IN column was created')

    return df


# Fix problems with duplicated values in time column

def correct_and_impute_times(df):
    
    """
    Corrects repeated timestamps caused by a 2-hour interval that is indicative of a daylight saving.

    The function operates under the following assumptions:
    1. By default every interval should be of 1 hour
    2. If any interval differ from this, particularly the repeated timestamp is corrected by subtracting one hour.
    3. The data values for the corrected timestamp are then imputed from the next available row.
    4. 2017 is an odd year where the null row is not the one with the 2 hours interval, but the one with 0. We fixed this manually for this specific rows.

    Args:
        df (pandas.DataFrame): A DataFrame containing a 'Time' column with datetime-like values and other associated data columns.

    Returns:
        pandas.DataFrame: The corrected DataFrame with timestamps set as the index and sorted chronologically.

    Raises:
        ValueError: If the 'Time' column is missing from the DataFrame.
        KeyError: If an index out of range occurs due to imputation attempts beyond the DataFrame bounds.
    """
    # Swap values of specific rows to correct data misalignment
    df.iloc[[54603, 54602]] = df.iloc[[54602, 54603]].values

    # Sort DataFrame by 'Time'
    df.sort_values("Time", ascending=True, inplace=True)

    # Identify intervals where there is a 2 hours gap
    intervals = df.Time.diff().dropna()
    index_wrong_time = intervals[intervals == "0 days 02:00:00"].index

    # Impute values from the next row and adjust 'Time' column
    for idx in index_wrong_time:
        df.loc[idx, 'Time'] = df.loc[idx, 'Time'] - pd.Timedelta(hours=1)  # Adjust for daylight saving
        df.loc[idx, df.columns != 'Time'] = df.loc[idx + 1, df.columns != 'Time']  # Impute values from the next row

    # Set 'Time' as index and sort by index
    df = df.set_index('Time').sort_index()

    return df

def correct_non_replaced_sensors(df):
    """
    Replaces data with NaN for non-replaced sensors in the DataFrame based on specified timestamps. A dictionary is provided where keys are timestamps as strings and values are lists of column names that should be set to NaN if the index is earlier than the timestamp.

    Args:
        df (pd.DataFrame): The DataFrame to be corrected.

    Returns:
        pd.DataFrame: The DataFrame with corrected sensor data.
    """

    dict_non_replaced = {'2020-07-30 00:00:00' : ['Lusen 1 PYRO IN', 'Lusen 1 PYRO OUT'],
                     '2022-12-20 00:00:00' : ['Lusen 3 IN', 'Lusen 3 OUT'],
                     '2022-10-12 00:00:00' : ['Gsenget IN', 'Gsenget OUT']}


    # Iterate over the dictionary of non-replaced sensors
    for key, columns in dict_non_replaced.items():
        # Convert the timestamp key from string to datetime object
        timestamp = pd.to_datetime(key)
        
        # Set values to NaN for specified columns where the index is earlier than the given timestamp
        df.loc[df.index < timestamp, columns] = np.nan

    print("Out of place values were turn to NaN for Lusen 1 PYRO, Lusen 3 and Gsenget")    

    return df


# Fix overlapping values in replaced sensors

def correct_overlapping_sensor_data(df):
    """
    Corrects sensor overlapping data by setting specific values to NaN based on replacement dates. Also filters the DataFrame to include only rows with an index timestamp on or after "2016-05-10 03:00:00". This is 3am after the installing date for the first working sensor.

    Args:
        df (pd.DataFrame): The DataFrame containing sensor data to be corrected.

    Returns:
        pd.DataFrame: The DataFrame with corrected sensor data.
    """
    # Define the replacement dates and columns for different sensor types
    replacement_dates = {
        'trinkwassertalsperre': '2021-06-18 00:00:00',
        'bucina': '2021-05-28 00:00:00',
        'falkenstein 1': '2022-12-22 12:00:00'
    }

    multi_columns_dict = {
        'trinkwassertalsperre': [
            'Trinkwassertalsperre_MULTI Fußgänger IN',
            'Trinkwassertalsperre_MULTI Fußgänger OUT',
            'Trinkwassertalsperre_MULTI Fahrräder IN',
            'Trinkwassertalsperre_MULTI Fahrräder OUT',
            'Trinkwassertalsperre_MULTI IN',
            'Trinkwassertalsperre_MULTI OUT'
        ],
        'bucina': [
            'Bucina_Multi OUT',
            'Bucina_Multi Fußgänger IN',
            'Bucina_Multi Fahrräder IN',
            'Bucina_Multi Fahrräder OUT',
            'Bucina_Multi Fußgänger OUT',
            'Bucina_Multi IN'
        ],
        'falkenstein 1': [
            'Falkenstein 1 OUT',
            'Falkenstein 1 IN'
        ]
    }

    pyro_columns_dict = {
        'trinkwassertalsperre': [
            'Trinkwassertalsperre PYRO IN',
            'Trinkwassertalsperre PYRO OUT'
        ],
        'bucina': [
            'Bucina PYRO IN',
            'Bucina PYRO OUT'
        ],
        'falkenstein 1': [
            'Falkenstein 1 PYRO IN',
            'Falkenstein 1 PYRO OUT'
        ]
    }

    # Process each sensor type based on the predefined dictionaries
    for sensor_type in replacement_dates:
        replacement_date = pd.to_datetime(replacement_dates[sensor_type])
        multi_columns = multi_columns_dict.get(sensor_type, [])
        pyro_columns = pyro_columns_dict.get(sensor_type, [])

        # Set to NaN the values in 'multi_columns' for dates on or before the replacement date
        if multi_columns:
            df.loc[df.index <= replacement_date, multi_columns] = np.nan

        # Set to NaN the values in 'pyro_columns' for dates after the replacement date
        if pyro_columns:
            df.loc[df.index > replacement_date, pyro_columns] = np.nan

    # Slice data before date because  there were no sensors installed
    df = df[df.index >= "2016-05-10 03:00:00"]


    print("Fixed overlapping values for replaced sensors")
    return df


def merge_columns(df):
    """
    Merges columns from replaced sensors in the DataFrame into new combined columns based on a predefined mapping and drops the original columns after merging.

    The function merges multiple related columns into single combined columns using a predefined dictionary (`merge_dict`). 
    For each key-value pair in the dictionary, values from the first column are used, and missing values are filled 
    from the second column. After merging, the original columns used for merging are dropped from the DataFrame.

    Args:
        df (pandas.DataFrame): A DataFrame containing columns to be merged.

    Returns:
        pandas.DataFrame: The modified DataFrame with the new merged columns and original columns removed.
    """
    merge_dict = {
        'Bucina MERGED IN': ['Bucina PYRO IN', 'Bucina_Multi IN'],
        'Bucina MERGED OUT': ['Bucina PYRO OUT', 'Bucina_Multi OUT'],
        'Falkenstein 1 MERGED IN': ['Falkenstein 1 PYRO IN', 'Falkenstein 1 IN'],
        'Falkenstein 1 MERGED OUT': ['Falkenstein 1 PYRO OUT', 'Falkenstein 1 OUT'],
        'Lusen 1 MERGED IN': ['Lusen 1 PYRO IN', 'Lusen 1 EVO IN'],
        'Lusen 1 MERGED OUT': ['Lusen 1 PYRO OUT', 'Lusen 1 EVO OUT'],
        'Trinkwassertalsperre MERGED IN': ['Trinkwassertalsperre PYRO IN', 'Trinkwassertalsperre_MULTI IN'],
        'Trinkwassertalsperre MERGED OUT': ['Trinkwassertalsperre PYRO OUT', 'Trinkwassertalsperre_MULTI OUT']
    }

    # Iterate over each item in the dictionary to merge columns
    for new_col, cols in merge_dict.items():
        # Combine the two columns into one using the first non-null value
        df[new_col] = df[cols[0]].combine_first(df[cols[1]])

    # Drop the original columns used for merging
    cols_to_drop = [col for cols in merge_dict.values() for col in cols]
    df = df.drop(columns=cols_to_drop)

    return df

def handle_outliers(df):
    """
    Transform to NaN every value higher than 800. During exploration we found that values over that are outliers. There were only 6 rows with any count over 800

    Args:
        df (pandas.DataFrame): DataFrame with values to be turned to NaN.

    Returns:
        pandas.DataFrame: The modified DataFrame with values over 800 turned to NaN
    """

    df[df > 800] = np.nan

    return df

def merge_columns(df):
    """
    Merges columns from replaced sensors in the DataFrame into new combined columns based on a predefined mapping
    and drops the original columns after merging. Additionally, drops columns with names containing "Fahrräder" or "Fußgänger" as we will not use that distinction.

    Args:
        df (pandas.DataFrame): A DataFrame containing columns to be merged.

    Returns:
        pandas.DataFrame: The modified DataFrame with the new merged columns, original columns removed, and Fahrräder or Fußgänger columns dropped.
    """
    merge_dict = {
        'Bucina MERGED IN': ['Bucina PYRO IN', 'Bucina_Multi IN'],
        'Bucina MERGED OUT': ['Bucina PYRO OUT', 'Bucina_Multi OUT'],
        'Falkenstein 1 MERGED IN': ['Falkenstein 1 PYRO IN', 'Falkenstein 1 IN'],
        'Falkenstein 1 MERGED OUT': ['Falkenstein 1 PYRO OUT', 'Falkenstein 1 OUT'],
        'Lusen 1 MERGED IN': ['Lusen 1 PYRO IN', 'Lusen 1 EVO IN'],
        'Lusen 1 MERGED OUT': ['Lusen 1 PYRO OUT', 'Lusen 1 EVO OUT'],
        'Trinkwassertalsperre MERGED IN': ['Trinkwassertalsperre PYRO IN', 'Trinkwassertalsperre_MULTI IN'],
        'Trinkwassertalsperre MERGED OUT': ['Trinkwassertalsperre PYRO OUT', 'Trinkwassertalsperre_MULTI OUT']
    }

    # Iterate over each item in the dictionary to merge columns
    for new_col, cols in merge_dict.items():
        # Combine the two columns into one using the first non-null value
        df[new_col] = df[cols[0]].combine_first(df[cols[1]])

    # Drop the original columns used for merging
    cols_to_drop = [col for cols in merge_dict.values() for col in cols]
    df = df.drop(columns=cols_to_drop)

    # Drop columns with names containing "Fahrräder" or "Fußgänger"
    df = df.loc[:, ~df.columns.str.contains("Fahrräder|Fußgänger")]

    return df

def calculate_traffic_metrics_abs(df):
    """
      This function calculates several traffic metrics and adds them to the DataFrame:
    - `traffic_abs`: The sum of all INs and OUTs for every sensor
    - `sum_IN_abs`: The sum of all columns containing 'IN' in their names.
    - `sum_OUT_abs`: The sum of all columns containing 'OUT' in their names.
    - `diff_abs`: The difference between `sum_IN_abs` and `sum_OUT_abs`.
    - `occupancy_abs`: The cumulative sum of `diff_abs`, representing the occupancy over time.

    Args:
        df (pandas.DataFrame): DataFrame containing traffic data.

    Returns:
        pandas.DataFrame: The DataFrame with additional columns for absolute traffic metrics.
    """
    # Calculate total traffic
    df["traffic_abs"] = df.filter(regex='IN|OUT').sum(axis=1)

    # Calculate sum of 'IN' columns
    df["sum_IN_abs"] = df.filter(like='IN').sum(axis=1)

    # Calculate sum of 'OUT' columns
    df["sum_OUT_abs"] = df.filter(like='OUT').sum(axis=1)
    
    return df


def preprocess_visitor_count_data(visitor_counts: pd.DataFrame) -> pd.DataFrame:

    visitor_counts_parsed_dates = parse_german_dates(df=visitor_counts, date_column_name="Time")
    # Remove data before 2016-05-10 03:00:00 as there were no sensors installed
    df = visitor_counts_parsed_dates[visitor_counts_parsed_dates['Time'] >= "2016-05-10 03:00:00"].reset_index(drop=True)
   
    df_mapped = fix_columns_names(df)
    
    df_imputed_timestamps = correct_and_impute_times(df_mapped)

    df_corrected_sensors = correct_non_replaced_sensors(df_imputed_timestamps)

    df_corrected_sensors = correct_overlapping_sensor_data(df_corrected_sensors)

    df_merged_columns = merge_columns(df_corrected_sensors)

    df_no_outliers = handle_outliers(df_merged_columns)
   
    df_traffic_metrics = calculate_traffic_metrics_abs(df_no_outliers)

    df_traffic_metrics.reset_index(inplace=True)

    print("\nVisitor sensors data is preprocessed and overall traffic metrics were created! \n")
    
    # create the output folder if it does not exist
    if not os.path.exists(output_data_folder):
        os.makedirs(output_data_folder)
        print(f"Output folder {output_data_folder} created.")
    
    # Save the preprocessed data to a CSV file locally
    df_traffic_metrics.to_csv(os.path.join(output_data_folder, output_file_name), index=False)


    return df_traffic_metrics
