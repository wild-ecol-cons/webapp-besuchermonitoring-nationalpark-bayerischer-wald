import pandas as pd
from functools import reduce


###########################################################################################

# Functions

def create_datetimeindex(df):
    """
    Prepare DataFrame by ensuring the index is a DateTimeIndex, resampling to hourly frequency,
    and handling missing values.
    
    Parameters:
    - df: DataFrame containing the data.
    - "Time": Name of the timestamp column to convert and set as the index.
    
    Returns:
    - df: DataFrame resampled to hourly frequency with missing values handled.
    """
    # Ensure the timestamp column is converted to datetime if it's not already the index

    df["Time"] = pd.to_datetime(df["Time"])
    df.set_index("Time", inplace=True)
    
    # Ensure the index is a DateTimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("Index must be a DateTimeIndex.")
    
    return df

def join_dataframes(df_list: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Joins a list of DataFrames using an inner join along the columns. All dataframes are at this point already displaying the Europe/Berlin timezone, however some have the timezone information in the datetimeindex, some not. So a step is needed to remove the timezone information.

    Args:
        df_list (list of pd.DataFrame): A list of pandas DataFrames to join.

    Returns:
        pd.DataFrame: A single DataFrame resulting from concatenating all input DataFrames along columns.
    """

    normalised = []
    for df in df_list:
        df = df.copy()
        df.index = df.index.tz_localize(None)
        normalised.append(df)

    return normalised[0].join(normalised[1:], how="inner")


def get_joined_dataframe(weather_data, visitor_count_data, visitorcenter_data) -> pd.DataFrame:
    """
    Main function to run the data joining pipeline.

    This function loads the visitor count, visitor center and weather data, preprocesses them and joins them into one dataframe.

    Returns:
        pd.DataFrame: The joined data.
    """
    df_list = [weather_data, visitor_count_data, visitorcenter_data]
    for df in df_list:
        create_datetimeindex(df)

    joined_data = join_dataframes(df_list)

    return joined_data
