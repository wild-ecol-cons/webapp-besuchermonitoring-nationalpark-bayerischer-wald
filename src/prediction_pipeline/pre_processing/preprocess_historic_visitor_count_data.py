"""
Clean historic sensor data from 2016 to 2024. In the docstring of every function you can check what it does and the assumptions that were made.

Usage:
- Change the global variables section if needed

Output:
- Returns the preprocessed data
"""



#import libraries

import pandas as pd
import re
import numpy as np
from src.config import sensor_mapping_to_traffic_metrics

pd.options.mode.chained_assignment = None  


###########################################################################################
#GLOBAL VARIABLES
###########################################################################################

output_data_folder = "preprocessed_data"
output_file_name = "preprocessed_visitor_sensor_data.csv"


##############################################################################################
    
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

    rename = {
          'TFG_Lusen_1 Fußgänger Richtung TFG': 'Lusen 1 EVO IN',
          'TFG_Lusen_1 Fußgänger Richtung Parkplatz' : 'Lusen 1 EVO OUT',
          }


    # Rename columns according to the provided mapping
    
    df.rename(columns=rename, inplace=True)
    print(len(rename), ' columns were renamed')

    # Remove the specified columns from the DataFrame
    df.drop(columns=drop, inplace=True, errors='ignore')
    print(len(drop), ' repeated columns were dropped')

    # Add Bucina_Multi IN column by summing Fahrraeder and Fussgaenger columns
    df['Bucina_Multi IN'] = df["Bucina_Multi Fahrräder IN"] + df["Bucina_Multi Fußgänger IN"]
    print('Bucina_Multi IN column was created')

    return df


# Fix problems with duplicated values in time column

def correct_and_impute_times(df: pd.DataFrame) -> pd.DataFrame:
    """
    Corrects DST-related timestamp issues in a DataFrame with a 'Time' column.

    Handles both DST transition cases that occur in the Europe/Berlin timezone:
    - Spring-forward (March): One hour is skipped, creating a gap in the hourly series.
    - Fall-back (October): One hour is repeated, creating duplicate timestamps at 02:00.

    The function localizes timestamps to Europe/Berlin, reindexes to a clean continuous
    hourly range, forward-fills any gaps, and returns a timezone-naive DatetimeIndex
    in Europe/Berlin wall-clock time.

    Args:
        df (pd.DataFrame): DataFrame containing a 'Time' column with naive datetime values.

    Returns:
        pd.DataFrame: Corrected DataFrame with a clean, continuous, timezone-naive hourly
                      DatetimeIndex named 'Time', sorted chronologically.
    """
    # Sort and set 'Time' as index
    df = df.sort_values("Time").set_index("Time")

    # Remove any pre-existing duplicate timestamps before localization
    df = df[~df.index.duplicated(keep='first')]

    # Localize to Europe/Berlin:
    # - ambiguous="NaT": unresolvable DST fall-back timestamps become NaT (dropped below)
    # - nonexistent="shift_forward": DST spring-forward timestamps are shifted to next valid hour
    df.index = df.index.tz_localize(
        "Europe/Berlin",
        ambiguous="NaT",
        nonexistent="shift_forward"
    )

    # Drop any NaT index rows produced by ambiguous DST timestamps
    df = df[df.index.notna()]

    # Reindex to a clean, continuous hourly range in Europe/Berlin time
    full_range = pd.date_range(
        start=df.index.min(),
        end=df.index.max(),
        freq="H",
        tz="Europe/Berlin"
    )
    df = df.reindex(full_range)

    # Forward-fill gaps introduced by spring-forward or dropped NaT rows
    df = df.ffill()

    # Strip timezone info back to naive timestamps (Europe/Berlin wall-clock time preserved)
    df.index = df.index.tz_localize(None)
    df.index.name = "Time"

    # Deduplicate once more — stripping timezone causes DST fall-back hours (02:00)
    # to appear twice again as naive timestamps, so we keep the first occurrence
    df = df[~df.index.duplicated(keep='first')]

    # Validate final result
    dupes = df.index.duplicated().sum()
    if dupes > 0:
        print(f"⚠️ {dupes} duplicate timestamps remain after correction:")
        print(df.index[df.index.duplicated(keep=False)])
    else:
        print("No duplicates found in 'Time' index ✅")

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
                     '2022-12-20 00:00:00' : ['TFG_Lusen_3 In Richtung TFG', 'TFG_Lusen_3 In Richtung Parkplatz'],
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
            'TFG_Falkenstein_1 zum Parkplatz',
            'TFG_Falkenstein_1 zum HZW'
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


def handle_outliers(df):
    """
    Transform to NaN every value of a numeric column higher than 800. During exploration we found that values over that are outliers. There were only 6 rows with any count over 800

    Args:
        df (pandas.DataFrame): DataFrame with values to be turned to NaN.

    Returns:
        pandas.DataFrame: The modified DataFrame with values over 800 turned to NaN
    """
    numeric_cols = df.select_dtypes(include='number').columns
    df[numeric_cols] = df[numeric_cols].where(df[numeric_cols] <= 800, other=np.nan)

    return df

def remove_certain_unnecessary_cols(df):
    """
    Drops columns with names containing "Fahrräder" or "Fußgänger" as we will not use that distinction.

    Args:
        df (pandas.DataFrame): A DataFrame.

    Returns:
        pandas.DataFrame: The modified DataFrame.
    """

    # Drop columns with names containing "Fahrräder" or "Fußgänger"
    df = df.loc[:, ~df.columns.str.contains("Fahrräder|Fußgänger")]

    return df

def calculate_traffic_metrics_abs(df: pd.DataFrame, columns_for_sums: dict = sensor_mapping_to_traffic_metrics) -> pd.DataFrame:
    """
      This function calculates several traffic metrics and adds them to the DataFrame:
    - `traffic_abs`: The sum of all INs and OUTs for every sensor
    - `sum_IN_abs`: The sum of all columns containing 'IN' in their names.
    - `sum_OUT_abs`: The sum of all columns containing 'OUT' in their names.

    Args:
        df (pandas.DataFrame): DataFrame containing traffic data.
        columns_for_sums (dict): A dictionary with keys 'abs_col', 'in_col', and 'out_col'
                                 containing lists of column names to sum for each traffic type.

    Returns:
        pandas.DataFrame: The DataFrame with additional columns for absolute traffic metrics.   
    """

    df['traffic_abs'] = df[columns_for_sums['abs_col']].sum(axis=1)
    df['sum_IN_abs'] = df[columns_for_sums['in_col']].sum(axis=1)
    df['sum_OUT_abs'] = df[columns_for_sums['out_col']].sum(axis=1)
    return df


def preprocess_visitor_count_data(visitor_counts: pd.DataFrame) -> pd.DataFrame:

    # Check for duplicates in visitor_counts.Time
    if visitor_counts['Time'].duplicated().sum() > 0:
        print("⚠️ Duplicates found in 'Time' column of visitor_counts!")
        print("The following duplicated timestamps were found:")
        # Investigate duplicates
        print(visitor_counts[visitor_counts['Time'].duplicated(keep=False)]["Time"].unique())
    else:
        print("No duplicates found in 'Time' column of visitor_counts ✅")

    # Remove data before 2016-05-10 03:00:00 as there were no sensors installed
    df = visitor_counts[visitor_counts['Time'] >= "2016-05-10 03:00:00"].reset_index(drop=True)
   
    df_mapped = fix_columns_names(df)
    
    df_imputed_timestamps = correct_and_impute_times(df_mapped)

    df_corrected_sensors = correct_non_replaced_sensors(df_imputed_timestamps)

    df_corrected_sensors = correct_overlapping_sensor_data(df_corrected_sensors)

    df_removed_columns = remove_certain_unnecessary_cols(df_corrected_sensors)

    # Remove columns that are entirely empty
    df_removed_columns = df_removed_columns.dropna(axis=1, how='all')

    df_no_outliers = handle_outliers(df_removed_columns)
   
    df_traffic_metrics = calculate_traffic_metrics_abs(df_no_outliers)

    df_traffic_metrics.reset_index(inplace=True)

    print("\nVisitor sensors data is preprocessed and overall traffic metrics were created! \n")

    return df_traffic_metrics
