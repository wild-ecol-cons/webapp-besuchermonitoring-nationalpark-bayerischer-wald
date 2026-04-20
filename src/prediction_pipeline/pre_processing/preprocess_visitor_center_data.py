##################################################
# Package Import Section
##################################################
import pandas as pd  # Provides data structures and data analysis tools.
import numpy as np  # Supports large, multi-dimensional arrays and matrices.
import logging
from src.utils import upload_dataframe_to_azure


##########################################################################
##########################################################################
# Import raw data and Functions to Clean Data
##########################################################################
##########################################################################

def change_binary_variables(df_visitcenters):
    # Documentation:
    # - This code converts columns in the DataFrame `df_visitcenters` that contain only binary values (0 and 1) to a boolean type.
    # - `df_visitcenters[column].isin([0, 1, np.nan]).all()` checks if all values in the column are either 0, 1, or NaN.
    # - `astype('bool')` converts the column from float64 type to boolean type, where 0 becomes False and 1 becomes True.
    # Iterate over each column in the DataFrame
    for column in df_visitcenters.columns:
        # Check if all values in the column are either 0, 1, or NaN
        if df_visitcenters[column].isin([0, 1, np.nan]).all():
            # Convert the column to boolean type (binary values: True, False)
            df_visitcenters[column] = df_visitcenters[column].astype('bool')

    return df_visitcenters

def clean_visitor_center_data(df_visitcenters):
    # Remove white spaces as values in all columns
    df_visitcenters = df_visitcenters.replace(r'^\s*$', np.nan, regex=True)
    # Change boolean variables
    df_visitcenters=change_binary_variables(df_visitcenters)

    return df_visitcenters

##########################################################################
##########################################################################
# Functions to Create New Variables/Columns
##########################################################################
##########################################################################

def add_date_variables(df):
    """
    Create new columns for day, month, and year from a date column in the DataFrame.
    
    Parameters:
    df (pandas.DataFrame): DataFrame containing the 'Datum' column with date information.
    
    Returns:
    pandas.DataFrame: DataFrame with additional columns for day, month, and year.
    """
    # Convert 'Datum' column to datetime format
    df['Datum'] = pd.to_datetime(df['Datum'])
    
    # Add new columns for day, month, day of the year, and year
    df['Tag'] = df['Datum'].dt.day
    df['Monat'] = df['Datum'].dt.month
    df['Jahr'] = df['Datum'].dt.year
    df['DayOfTheYear'] = df['Datum'].dt.dayofyear
    
    # Change data types for modeling purposes
    df['Tag'] = df['Tag'].astype('Int64')
    df['Monat'] = df['Monat'].astype('category')
    df['Jahr'] = df['Jahr'].astype('Int64')
    df['DayOfTheYear'] = df['DayOfTheYear'].astype('Int64')
    
    return df

def add_season_variable(df):
    """
    Create a new column 'Jahreszeit' in the DataFrame based on the month variable.
    
    Parameters:
    df (pandas.DataFrame): DataFrame containing the 'Monat' column with month information.
    
    Returns:
    pandas.DataFrame: DataFrame with an additional 'Jahreszeit' column representing the season.
    """
    # Define the seasons based on the month
    df['Jahreszeit'] = df['Monat'].apply(
        lambda x: 'Frühling' if x in [3, 4, 5] else
                  'Sommer' if x in [6, 7, 8] else
                  'Herbst' if x in [9, 10, 11] else
                  'Winter' if x in [12, 1, 2] else
                  np.nan
    )
    
    # Convert the 'Jahreszeit' column to category type
    df['Jahreszeit'] = df['Jahreszeit'].astype('category')
    
    return df

def add_and_translate_day_of_week(df):
    """
    Create a new column 'Wochentag' that represents the day of the week in German.
    
    Parameters:
    df (pandas.DataFrame): DataFrame containing the 'Datum' column with date information.
    
    Returns:
    pandas.DataFrame: DataFrame with updated 'Wochentag' column in German.
    """
    # Create a new column 'Wochentag2' with the day of the week in English
    df['Wochentag2'] = df['Datum'].dt.day_name()
    df['Wochentag2'] = df['Wochentag2'].astype('category')
    
    # Define the translation mapping from English to German
    translation_map = {
        'Monday': 'Montag',
        'Tuesday': 'Dienstag',
        'Wednesday': 'Mittwoch',
        'Thursday': 'Donnerstag',
        'Friday': 'Freitag',
        'Saturday': 'Samstag',
        'Sunday': 'Sonntag'
    }
    
    # Replace the English day names in the 'Wochentag2' column with German names
    df['Wochentag2'] = df['Wochentag2'].replace(translation_map)
    
    # Remove the 'Wochentag' column from the DataFrame
    df = df.drop(columns=['Wochentag'], errors='ignore')
    
    # Rename 'Wochentag2' to 'Wochentag'
    df = df.rename(columns={'Wochentag2': 'Wochentag'})
    
    return df

def add_weekend_variable(df):
    """
    Create a new binary column 'Wochenende' indicating whether the day is a weekend.
    
    Parameters:
    df (pandas.DataFrame): DataFrame containing the 'Wochentag' column with German day names.
    
    Returns:
    pandas.DataFrame: DataFrame with an additional 'Wochenende' column indicating weekend status.
    """
    # Create a new binary column 'Wochenende' where True represents weekend days (Saturday, Sunday)
    df['Wochenende'] = df['Wochentag'].apply(lambda x: x in ['Samstag', 'Sonntag'])
    
    # Convert the 'Wochenende' column to boolean type
    df['Wochenende'] = df['Wochenende'].astype(bool)
    
    return df

def reorder_columns(df):
    """
    Reorder columns in the DataFrame to place date-related variables together.
    
    Parameters:
    df (pandas.DataFrame): DataFrame with various columns including date-related variables.
    
    Returns:
    pandas.DataFrame: DataFrame with columns reordered to place date-related variables next to each other.
    """
    # Define the desired order of columns
    column_order = [
        'Datum', 'Tag', 'Monat', 'Jahr', 'DayOfTheYear','Wochentag', 'Wochenende', 'Jahreszeit', 
        'Schulferien_Bayern', 'Schulferien_CZ', 'Feiertag_Bayern', 'Feiertag_CZ',
    ]
    
    # Reorder columns in the DataFrame
    df = df[column_order]
    
    return df

def add_additional_columns(df_visitcenters):
    # Add date variables
    df_visitcenters=add_date_variables(df_visitcenters)
    # Add season variable
    df_visitcenters=add_season_variable(df_visitcenters)
    # Add day of week variable
    df_visitcenters=add_and_translate_day_of_week(df_visitcenters)
    # Add weekend variable dummy code
    df_visitcenters=add_weekend_variable(df_visitcenters)
    # Reorder columns to group similar variables
    df_visitcenters=reorder_columns(df_visitcenters)
    return df_visitcenters

##########################################################################
##########################################################################
# Functions to Handle Extreme Outliers
##########################################################################
##########################################################################

def detect_outliers_std(df, column, num_sd=7):
    """
    Detect outliers in a specific column of the DataFrame using the standard deviation method.
    
    Parameters:
    df (pandas.DataFrame): DataFrame containing the column to check.
    column (str): Name of the column to check for outliers.
    num_sd (int): Number of standard deviations to define the outlier bounds (default is 7).
    
    Returns:
    pandas.DataFrame: DataFrame containing rows with outliers in the specified column.
    """
    mean = df[column].mean()
    std_dev = df[column].std()
    
    # Define the bounds for outliers
    lower_bound = mean - num_sd * std_dev
    upper_bound = mean + num_sd * std_dev
    
    # Identify outliers
    outliers_mask = (df[column] < lower_bound) | (df[column] > upper_bound)
    return df[outliers_mask][['Datum', column]]

##########################################################################
##########################################################################
# Create an hourly level DataFrame by expanding each day into 24 hours

# This hourly data frame is later joined with other data for predictions
##########################################################################
##########################################################################

def create_hourly_dataframe(df):
    """
    Expands the daily data in the DataFrame to an hourly level using resampling.
    
    Parameters:
    df (pandas.DataFrame): DataFrame containing daily data with a 'Datum' column representing dates.
    
    Returns:
    pandas.DataFrame: New DataFrame with an hourly level where each day is expanded into 24 hourly rows.
    """
    # Rename and set 'Datum' as the index to enable resampling
    df_hourly = df.rename(columns=lambda x: x.strip())
    df_hourly = df_hourly.rename(columns={'Datum': 'Time'})
    df_hourly = df_hourly.set_index('Time')

    # Resample to hourly and forward-fill missing hours
    df_hourly = df_hourly.resample('H').ffill()

    # Restore Time as a column
    df_hourly = df_hourly.reset_index()

    return df_hourly

def rename_and_set_time_as_index(df):
    """
    Rename columns, convert 'time' column to datetime, and set 'time' as the index.
    
    Parameters:
    df (pandas.DataFrame): DataFrame containing data with a 'Datum' column to be renamed and converted.
    
    Returns:
    pandas.DataFrame: The cleaned DataFrame with 'Datum' renamed to 'time', converted to datetime, and 'time' set as index.
    """
    # Rename 'Datum' column to 'time'
    df.rename(columns={'Datum': 'time'}, inplace=True)
    df.index=pd.to_datetime(df.index)

    # Convert 'time' column to datetime
    #df['time'] = pd.to_datetime(df['time'])
    
    # Set 'time' column as index
    #df.set_index('time', inplace=True)
    
    return df

def process_visitor_center_data(sourced_df):
    cleaned_df = clean_visitor_center_data(sourced_df)
    transformed_df = add_additional_columns(cleaned_df)
    hourly_df = create_hourly_dataframe(transformed_df)
    hourly_df = rename_and_set_time_as_index(hourly_df)
    # reset the index
    hourly_df.reset_index(drop=True, inplace=True)
    transformed_df.reset_index(drop=True, inplace=True)

    # Before saving and returning hourly_df, we need to add the hour column
    hourly_df['Hour'] = hourly_df['Time'].dt.hour

    # Save daily data to the cloud for querying
    upload_dataframe_to_azure(
        df=transformed_df,
        file_name="visitor_centers_daily_2017_to_2026.parquet",
        target_folder="preprocessed_data/bf_preprocessed_files/visitor_centers",
        file_format="parquet",
    )
    
    # Save houly data to the cloud for joining/modeling
    upload_dataframe_to_azure(
        df=hourly_df,
        file_name="visitor_centers_hourly_2017_to_2026.parquet",
        target_folder="preprocessed_data",
        file_format="parquet",
    )

    return hourly_df, transformed_df