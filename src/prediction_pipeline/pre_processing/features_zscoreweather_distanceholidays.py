
# Import libraries
import pandas as pd
import numpy as np
from src.utils import upload_dataframe_to_azure

##############################################################################################

# GLOBAL VARIABLES

output_file_name = "holidays_deltaweather_features_df.csv"
output_data_folder = "preprocessed_data"

window_size = 5 # Define the window size in days that you wish to use to calculate z-scores


# Functions
def slice_at_first_non_null(df):
    """
    Slices the DataFrame starting at the first non-null value in the 'Feiertag_Bayern' column.

    We don't have data for holidays in 2016, so the function finds the index of the first non-null 
    value in the 'Feiertag_Bayern' column and returns the DataFrame sliced from that index onward.

    Args:
        df (pandas.DataFrame): DataFrame containing the 'Feiertag_Bayern' column.

    Returns:
        pandas.DataFrame: The sliced DataFrame starting from the first non-null value in 'Feiertag_Bayern'.
    """
    # Find the index of the first non-null value in the 'Feiertag_Bayern' column
    first_non_null_index = df['Feiertag_Bayern'].first_valid_index()

    # Slice the DataFrame from the first non-null index onward and create a copy to avoid warnings
    df = df.loc[first_non_null_index:].copy()

    return df

def add_nearest_holiday_distance(df):
    """
    Add columns to the DataFrame calculating the distance to the nearest public holiday for both 'Feiertag_Bayern' and 'Feiertag_CZ'.

    Args:
        df (pd.DataFrame): DataFrame with 'Time', 'Feiertag_Bayern', and 'Feiertag_CZ' columns.
            - 'Time': Datetime column with timestamps.
            - 'Feiertag_Bayern': Boolean column indicating if the date is a holiday in Bayern.
            - 'Feiertag_CZ': Boolean column indicating if the date is a holiday in CZ.

    Returns:
        pd.DataFrame: DataFrame with two new columns:
            - 'Distance_to_Nearest_Holiday_Bayern': Distance in days to the nearest holiday in Bayern for each day/row.
            - 'Distance_to_Nearest_Holiday_CZ': Distance in days to the nearest holiday in CZ for each day/row.
    """

    # Check if required columns for this function execution exist
    required_columns = ['Time', 'Feiertag_Bayern', 'Feiertag_CZ']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' is missing from the DataFrame!")
    
    # Ensure the Time column is in datetime format
    df['Time'] = pd.to_datetime(df['Time'])

    # Extract date from Time column
    df['Date'] = df['Time'].dt.date

    # make sure Feiertag columns are integers
    df['Feiertag_Bayern'] = df['Feiertag_Bayern'].astype(int)
    df['Feiertag_CZ'] = df['Feiertag_CZ'].astype(int)

    # Extract unique dates for holidays
    bayern_holidays = df[df['Feiertag_Bayern'] == 1]['Date'].unique()
    cz_holidays = df[df['Feiertag_CZ'] == 1]['Date'].unique()

    # Create a DataFrame with unique dates
    dates_df = pd.DataFrame({'Date': df['Date'].unique()})


    def get_nearest_holiday_distance(date, holidays):
        """
        Calculate the distance in days to the nearest holiday.

        Args:
            date (pd.Timestamp): The date for which to calculate the distance.
            holidays (np.ndarray): Array of holiday dates.

        Returns:
            float: Distance in days to the nearest holiday, or NaN if no holidays are provided.
        """
        if len(holidays) == 0:
            return np.nan
        nearest_holiday = min(abs((date - pd.to_datetime(holidays)).days))
        return nearest_holiday

    # Apply the function to calculate distances for both sets of holidays
    dates_df['Distance_to_Nearest_Holiday_Bayern'] = dates_df['Date'].apply(
        lambda x: get_nearest_holiday_distance(pd.to_datetime(x), bayern_holidays)
    )
    dates_df['Distance_to_Nearest_Holiday_CZ'] = dates_df['Date'].apply(
        lambda x: get_nearest_holiday_distance(pd.to_datetime(x), cz_holidays)
    )
    # Merge the distances back with the original DataFrame
    df = df.merge(dates_df, on='Date', how='left')
   

    return df

def add_daily_max_values(df, columns):
    """
    Add columns to the DataFrame that show the maximum daily value for each weather characteristic.

    Args:
        df (pd.DataFrame): DataFrame with 'Time' and multiple weather-related columns.
            - 'Time': Datetime column with timestamps.
            - columns (list of str): List of column names to compute the daily maximum values for.

    Returns:
        pd.DataFrame: DataFrame with new columns that contain the maximum values for each day,
                      repeated for every hour.
    """
    # Ensure the Time column is in datetime format
    df['Time'] = pd.to_datetime(df['Time'])

    # Extract the date from the Time column
    df['Date'] = df['Time'].dt.date

    # Create a DataFrame to store daily max values
    daily_max_df = df.groupby('Date')[columns].max().reset_index()

    # Rename columns to indicate they are daily maximum values
    daily_max_df = daily_max_df.rename(columns={col: f'Daily_Max_{col}' for col in columns})

    # Merge the daily max values back into the original DataFrame
    df = df.merge(daily_max_df, on='Date', how='left')

    return df

def add_daily_precipidation_sum_value(df):
    """
    Adds the daily precipitation sum column to the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame with 'Time' and multiple weather-related columns.

    Returns:
        pd.DataFrame: DataFrame with new daily precipitation sum column.
    """

    daily_sum_prcp_df = df.groupby('Date')['Precipitation (mm)'].sum().reset_index()
    daily_sum_prcp_df = daily_sum_prcp_df.rename(columns={'Precipitation (mm)': 'Daily_Sum_Precipitation (mm)'})

    # Merge the daily sum of precipitation back into the original DataFrame
    df = df.merge(daily_sum_prcp_df, on='Date', how='left')

    return df

def add_moving_z_scores(df, columns, window_size):
    """
    Add moving z-score columns for weather characteristics based on their daily maximum values.

    Args:
        df (pd.DataFrame): DataFrame with 'Time' and daily maximum columns.
            - 'Time': Datetime column with timestamps.
        columns (list of str): List of column names to compute the moving z-scores for.
        window_size (int): Size of the moving window in days.

    Returns:
        pd.DataFrame: DataFrame with new columns that contain the moving z-scores for each column.
    """
    columns = [f'Daily_Max_{col}' for col in columns] + ['Daily_Sum_Precipitation (mm)']

    # Ensure the Time column is in datetime format
    df['Time'] = pd.to_datetime(df['Time'])

    # Extract unique dates with daily max values
    daily_df = df[['Date'] + columns].drop_duplicates()

    # Calculate rolling mean and standard deviation for daily max values
    for col in columns:
        # Calculate rolling mean and standard deviation over the specified window size
        daily_df[f'Rolling_Mean_{col}'] = daily_df[col].rolling(window=window_size, min_periods=window_size).mean()
        daily_df[f'Rolling_Std_{col}'] = daily_df[col].rolling(window=window_size, min_periods=window_size).std()

        # Calculate the z-score
        daily_df[f'ZScore_{col}'] = (
            (daily_df[col] - daily_df[f'Rolling_Mean_{col}']) /
            (daily_df[f'Rolling_Std_{col}'] + 1e-8)  # Add a small value to prevent division by zero
        )

        # Drop the rolling mean and std columns as they are intermediate calculations
        daily_df.drop(columns=[f'Rolling_Mean_{col}', f'Rolling_Std_{col}'], inplace=True)

    # Merge the z-scores back into the original hourly DataFrame
    df = df.merge(daily_df[['Date'] + [f'ZScore_{col}' for col in columns]], on='Date', how='left')

    # Drop the daily max columns from the main DataFrame
    df.drop(columns=columns, inplace=True)

    return df


##############################################################################################

def get_zscores_and_nearest_holidays(df,columns_for_zscores):

    # Reset the index, converting the index back into a 'Time' column
    df.reset_index(inplace=True)

    # Optionally, rename the index column to 'Time' if it's not automatically renamed
    df.rename(columns={'index': 'Time'}, inplace=True)
 
    df_no_null = slice_at_first_non_null(df)

    df_holidays = add_nearest_holiday_distance(df_no_null)

    df_daily_max = add_daily_max_values(df_holidays, columns_for_zscores)

    df_daily_max_with_precip_sum = add_daily_precipidation_sum_value(df_daily_max)

    df_zscores_and_nearest_holidays = add_moving_z_scores(df_daily_max_with_precip_sum, columns_for_zscores, window_size)

    # Remove NaN values (as there will be NaNs in the first rows of the dataframe due to zscore being NaN)
    df_zscores_and_nearest_holidays = df_zscores_and_nearest_holidays.dropna()

    upload_dataframe_to_azure(
        df=df_zscores_and_nearest_holidays,
        file_name=output_file_name,
        target_folder=output_data_folder,
        file_format="csv"
    )

    print("Dataset with new features (distance to holidays, weather z-scores) uploaded to the cloud succesfully!")
    
    return df_zscores_and_nearest_holidays


