import pandas as pd
import numpy as np
import warnings


warnings.filterwarnings("ignore")

############################################################################################################
# Global Variables
############################################################################################################

# Columns to use for preprocessing
columns_to_use = [
'Time',  'Bayerisch Eisenstein IN',  'Bayerisch Eisenstein OUT',  'Brechhäuslau IN',  'Brechhäuslau OUT',  
'Deffernik IN',  'Deffernik OUT',  'Diensthüttenstraße IN',  'Diensthüttenstraße OUT',  'Felswandergebiet IN',  
'Felswandergebiet OUT',  'Ferdinandsthal IN',  'Ferdinandsthal OUT',  'Fredenbrücke IN',  'Fredenbrücke OUT',  
'Gfäll IN',  'Gfäll OUT',  'Gsenget IN',  'Gsenget OUT',  'Klingenbrunner Wald IN',  'Klingenbrunner Wald OUT',  
'Klosterfilz IN',  'Klosterfilz OUT',  'Racheldiensthütte IN',  'Racheldiensthütte OUT',  'Sagwassersäge IN',  
'Sagwassersäge OUT',  'Scheuereck IN',  'Scheuereck OUT',  'Schillerstraße IN',  'Schillerstraße OUT',  
'Schwarzbachbrücke IN',  'Schwarzbachbrücke OUT',  'TFG_Falkenstein_2 zum Parkplatz',  'TFG_Falkenstein_2 In Richtung TFG',  'TFG_Lusen_2 Richtung Vögel am Waldrand',  
'TFG_Lusen_2 Richtung Parkplatz',  'TFG_Lusen_3 In Richtung TFG',  'TFG_Lusen_3 In Richtung Parkplatz',  'Waldhausreibe IN',  'Waldhausreibe OUT',  'Waldspielgelände_1 IN (Ins WSG)',  
'Waldspielgelände_1 OUT (aus dem WSG)',  'Wistlberg IN',  'Wistlberg OUT',
'traffic_abs',  'sum_IN_abs',  'sum_OUT_abs',  'Temperature (°C)',  'Relative Humidity (%)',  
'Precipitation (mm)',  'Wind Speed (km/h)',  'Sunshine Duration (min)',  'Tag',  'Monat', 'DayOfTheYear', 
'Wochentag',  'Wochenende',  'Jahreszeit',  'Schulferien_Bayern',  'Schulferien_CZ',  
'Feiertag_Bayern',  'Feiertag_CZ'
]


# Dictionary to map the columns to the location
regionwise_sensor_mapping = {
    'Falkenstein-Schwellhäusl': {
        'IN': [
            'Bayerisch Eisenstein IN', 'Brechhäuslau IN', 'Deffernik IN', 
            'Ferdinandsthal IN', 'Schillerstraße IN'
        ],
        'OUT': [
            'Bayerisch Eisenstein OUT', 'Brechhäuslau OUT', 'Deffernik OUT', 
            'Ferdinandsthal OUT', 'Schillerstraße OUT'
        ]
    },
    'Nationalparkzentrum Falkenstein': {
        'IN': ['TFG_Falkenstein_1 zum HZW', 'TFG_Falkenstein_2 In Richtung TFG'],
        'OUT': ['TFG_Falkenstein_1 zum Parkplatz', 'TFG_Falkenstein_2 zum Parkplatz']
    },
    'Scheuereck-Schachten-Trinkwassertalsperre': {
        'IN': ['Gsenget IN', 'Scheuereck IN', 'Trinkwassertalsperre_MULTI IN'],
        'OUT': ['Gsenget OUT', 'Scheuereck OUT', 'Trinkwassertalsperre_MULTI OUT']
    },
    'Lusen-Mauth-Finsterau': {
        'IN': [
            'Bucina_Multi IN', 'Felswandergebiet IN', 'Fredenbrücke IN', 
            'Schwarzbachbrücke IN', 'Waldhausreibe IN', 'Wistlberg IN', 'Sagwassersäge IN'
        ],
        'OUT': [
            'Bucina_Multi OUT', 'Felswandergebiet OUT', 'Fredenbrücke OUT', 
            'Schwarzbachbrücke OUT', 'Waldhausreibe OUT', 'Wistlberg OUT', 'Sagwassersäge OUT'
        ]
    },
    'Rachel-Spiegelau': {
        'IN': [
            'Diensthüttenstraße IN', 'Gfäll IN', 'Klingenbrunner Wald IN', 
            'Klosterfilz IN', 'Racheldiensthütte IN', 'Waldspielgelände_1 IN (Ins WSG)'
        ],
        'OUT': [
            'Diensthüttenstraße OUT', 'Gfäll OUT', 'Klingenbrunner Wald OUT', 
            'Klosterfilz OUT', 'Racheldiensthütte OUT', 'Waldspielgelände_1 OUT (aus dem WSG)'
        ]
    },
    'Nationalparkzentrum Lusen': {
        'IN': ['TFG_Lusen_1 IN', 'TFG_Lusen_2 Richtung Vögel am Waldrand', 'TFG_Lusen_3 In Richtung TFG'],
        'OUT': ['TFG_Lusen_1 Richtung Parkplatz', 'TFG_Lusen_2 Richtung Parkplatz', 'TFG_Lusen_3 In Richtung Parkplatz']
    }
}



dtype_dict = {
    'datetime64[ns]': [
        'Time'
    ],
    
    'float64': [
        'traffic_abs',
        'Temperature (°C)',
        'Relative Humidity (%)',
        'Wind Speed (km/h)',
        'Precipitation (mm)',
        'Monat',
        'sum_IN_abs',
        'sum_OUT_abs',
        
        # traffic data
        'Falkenstein-Schwellhäusl IN',
        'Rachel-Spiegelau IN',
        'Nationalparkzentrum Falkenstein IN',
        'Nationalparkzentrum Lusen IN',
        'Lusen-Mauth-Finsterau IN',
        'Scheuereck-Schachten-Trinkwassertalsperre IN',
        'Falkenstein-Schwellhäusl OUT',
        'Rachel-Spiegelau OUT',
        'Nationalparkzentrum Falkenstein OUT',
        'Nationalparkzentrum Lusen OUT',
        'Lusen-Mauth-Finsterau OUT',
        'Scheuereck-Schachten-Trinkwassertalsperre OUT',
        'Bayerisch Eisenstein IN', 'Bayerisch Eisenstein OUT',
        'Brechhäuslau IN', 'Brechhäuslau OUT',
        'Deffernik IN', 'Deffernik OUT',
        'Diensthüttenstraße IN', 'Diensthüttenstraße OUT',
        'Felswandergebiet IN', 'Felswandergebiet OUT',
        'Ferdinandsthal IN', 'Ferdinandsthal OUT',
        'Fredenbrücke IN', 'Fredenbrücke OUT',
        'Gfäll IN', 'Gfäll OUT',
        'Gsenget IN', 'Gsenget OUT',
        'Klingenbrunner Wald IN', 'Klingenbrunner Wald OUT',
        'Klosterfilz IN', 'Klosterfilz OUT',
        'Racheldiensthütte IN', 'Racheldiensthütte OUT',
        'Sagwassersäge IN', 'Sagwassersäge OUT',
        'Scheuereck IN', 'Scheuereck OUT',
        'Schillerstraße IN', 'Schillerstraße OUT',
        'Schwarzbachbrücke IN', 'Schwarzbachbrücke OUT',
        'TFG_Falkenstein_1 zum HZW', 'TFG_Falkenstein_1 zum Parkplatz',
        'TFG_Falkenstein_2 In Richtung TFG', 'TFG_Falkenstein_2 zum Parkplatz',
        'TFG_Lusen_1 IN', 'TFG_Lusen_1 Richtung Parkplatz',
        'TFG_Lusen_2 Richtung Vögel am Waldrand', 'TFG_Lusen_2 Richtung Parkplatz',
        'TFG_Lusen_3 In Richtung TFG', 'TFG_Lusen_3 In Richtung Parkplatz',
        'Waldhausreibe IN', 'Waldhausreibe OUT',
        'Waldspielgelände_1 IN (Ins WSG)', 'Waldspielgelände_1 OUT (aus dem WSG)',
        'Wistlberg IN', 'Wistlberg OUT',
        'Bucina_Multi IN', 'Bucina_Multi OUT',
        'Trinkwassertalsperre_MULTI IN', 'Trinkwassertalsperre_MULTI OUT',
        
        # Z-Score data
        'ZScore_Daily_Max_Temperature (°C)',
        'ZScore_Daily_Max_Relative Humidity (%)',
        'ZScore_Daily_Max_Wind Speed (km/h)',
        'ZScore_Daily_Max_Precipitation (mm)',
        'ZScore_Daily_Sum_Precipitation (mm)',
        
        # Distance to nearest holidays
        'Distance_to_Nearest_Holiday_Bayern',
        'Distance_to_Nearest_Holiday_CZ'
    ],
    
    'category': [
        'Wochentag',
        'Wochenende',
        'DayOfTheYear',
        'Jahreszeit',
        'Feiertag_Bayern',
        'Feiertag_CZ',
        'Schulferien_Bayern',
        'Schulferien_CZ',
        'coco_2'
    ]
}

numeric_features_for_modelling = ['Temperature (°C)', 'Relative Humidity (%)', 'Wind Speed (km/h)', 'Precipitation (mm)', 'ZScore_Daily_Max_Precipitation (mm)', 'ZScore_Daily_Sum_Precipitation (mm)', 'ZScore_Daily_Max_Temperature (°C)', 
                    'ZScore_Daily_Max_Relative Humidity (%)','ZScore_Daily_Max_Wind Speed (km/h)',
                    'Distance_to_Nearest_Holiday_Bayern','Distance_to_Nearest_Holiday_CZ','Tag_sin', 'Tag_cos', 'Monat_sin', 'Monat_cos',
                    'Hour_sin', 'Hour_cos','Wochentag_sin', 'Wochentag_cos', 'DayOfTheYear_sin', 'DayOfTheYear_cos']

categorical_features_for_modelling = ['Wochenende','Schulferien_Bayern', 'Schulferien_CZ', 
                        'Feiertag_Bayern', 'Feiertag_CZ','sunny', 'cloudy', 'rainy', 'snowy', 'extreme','stormy','Frühling',
                        'Sommer', 'Herbst', 'Winter']

target_vars_et = ['traffic_abs', 'sum_IN_abs', 'sum_OUT_abs', 'Lusen-Mauth-Finsterau IN', 'Lusen-Mauth-Finsterau OUT', 
               'Nationalparkzentrum Lusen IN', 'Nationalparkzentrum Lusen OUT', 'Rachel-Spiegelau IN', 'Rachel-Spiegelau OUT', 
               'Falkenstein-Schwellhäusl IN', 'Falkenstein-Schwellhäusl OUT', 
               'Scheuereck-Schachten-Trinkwassertalsperre IN', 'Scheuereck-Schachten-Trinkwassertalsperre OUT', 
               'Nationalparkzentrum Falkenstein IN', 'Nationalparkzentrum Falkenstein OUT']

coco_mapping = {
    1: [1, 2],       # Clear, Fair
    2: [3, 4, 5],    # Cloudy, Overcast, Fog
    3: [7, 8, 9, 17, 18, 19],  # Light Rain, Rain, Heavy Rain, Rain Shower, Heavy Rain Shower, Sleet Shower
    4: [14, 15, 16, 21, 22],   # Light Snowfall, Snowfall, Heavy Snowfall, Snow Shower, Heavy Snow Shower
    5: [6, 10, 11, 12, 13, 20], # Freezing Fog, Freezing Rain, Heavy Freezing Rain, Sleet, Heavy Sleet, Heavy Sleet Shower
    6: [23, 24, 25, 26, 27]    # Lightning, Hail, Thunderstorm, Heavy Thunderstorm, Storm
}

###########################################################################################################################
# Functions
###########################################################################################################################


def get_regionwise_IN_and_OUT_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data by summing IN and OUT columns for each region.
    
    Args:
        df (pd.DataFrame): The DataFrame containing the data to preprocess.
        
    Returns:
        pd.DataFrame: The preprocessed DataFrame.
    """

    # Iterate over the regionwise sensor mapping
    for region, sensors in regionwise_sensor_mapping.items():
        # Sum the values for all IN columns of the current region
        if sensors['IN']:
            df[f'{region} IN'] = df[sensors['IN']].sum(axis=1, min_count=1)

        # Sum the values for all OUT columns of the current region
        if sensors['OUT']:
            df[f'{region} OUT'] = df[sensors['OUT']].sum(axis=1, min_count=1)

    return df



# Helper function to convert columns to datetime
def convert_to_datetime(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    for column in columns:
        df[column] = pd.to_datetime(df[column])
    return df

# Merging new features from df_newfeatures into df
def merge_new_features(df: pd.DataFrame, df_newfeatures: pd.DataFrame) -> pd.DataFrame:
    """Merges new features from df_newfeatures into df on 'Time' column."""
    
    # Ensure 'Time' columns are datetime
    df, df_newfeatures = map(lambda x: convert_to_datetime(x, ['Time']), [df, df_newfeatures])

    # Columns to merge
    columns_to_add = [
        'ZScore_Daily_Max_Temperature (°C)',
        'ZScore_Daily_Max_Relative Humidity (%)',
        'ZScore_Daily_Max_Wind Speed (km/h)',
        'ZScore_Daily_Max_Precipitation (mm)',
        'ZScore_Daily_Sum_Precipitation (mm)',
        'Distance_to_Nearest_Holiday_Bayern',
        'Distance_to_Nearest_Holiday_CZ'
    ]

    # Select only existing columns for merging
    selected_columns = [col for col in columns_to_add if col in df_newfeatures.columns]

    # Merge on 'Time' column
    df = df.merge(df_newfeatures[['Time'] + selected_columns], on='Time', how='left')

   #add the hour column additionally
    df['Hour'] = df['Time'].dt.hour

    return df

# Change datatypes based on dtype_dict
def change_datatypes(df: pd.DataFrame, dtype_dict: dict) -> pd.DataFrame:
    """Change column datatypes based on dtype_dict."""
    for dtype, columns in dtype_dict.items():
        df[columns] = df[columns].astype(dtype)

    # set time as the index
    df = df.set_index('Time')
    return df

def apply_cliclic_tranformations(df: pd.DataFrame,cyclic_features: list) -> pd.DataFrame:

    # Convert categorical features to numeric if they are not already
    for feature in cyclic_features:
        if feature in df.columns:
            if pd.api.types.is_categorical_dtype(df[feature]):
                df[feature] = df[feature].cat.codes  # Convert categorical to numeric codes
            
            max_value = df[feature].max()  # Get max value for scaling
            
            # Apply sine and cosine transformations
            df[f'{feature}_sin'] = np.sin(2 * np.pi * df[feature] / max_value)
            df[f'{feature}_cos'] = np.cos(2 * np.pi * df[feature] / max_value)
        else:
            print(f"Warning: Feature '{feature}' not found in DataFrame")
    # Drop the original columns
    columns_to_drop = ['Tag', 'Monat', 'Wochentag', 'Hour', 'DayOfTheYear']
    df = df.drop(columns=columns_to_drop)

    return df

def standardize_numeric_features(df: pd.DataFrame, standardize_features: list) -> pd.DataFrame: 

    # Loop through each numeric feature and apply z-score normalization
    for feature in standardize_features:
        if feature in df.columns:
            mean_value = df[feature].mean()  # Calculate mean
            std_value = df[feature].std()    # Calculate standard deviation
            
            # Apply z-score normalization
            df[feature] = (df[feature] - mean_value) / std_value
        else:
            print(f"Warning: Feature '{feature}' not found in DataFrame")

    return df



       
def get_dummy_encodings(df: pd.DataFrame, columns_to_use: list) -> pd.DataFrame:
    # Create a copy of the original dataframe
    df_copy = df.copy()

    ################ Get dummy encodings for the coco_2 column ####################
    
    # Dummy encode the column *coco_2*
    dummies = pd.get_dummies(df[columns_to_use[1]], drop_first=False)

    # check if the dummy columns are created  and if not fill the column with False
    for col in [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]:
        if col not in dummies.columns:
            dummies[col] = False

    # Rename the dummy columns to more meaningful names
    dummies = dummies.rename(columns={1.0: 'sunny',
                                      2.0: 'cloudy', 
                                      3.0: 'rainy', 
                                      4.0: 'snowy', 
                                      5.0: 'extreme', 
                                      6.0: 'stormy'})
    df_copy = pd.concat([df_copy, dummies], axis=1)

    # 

    ################### dummy encodings for the Jahreszeit column ####################
    # Dummy encode the columns *Jahreszeit*
    season_dummies = pd.get_dummies(df[columns_to_use[0]], drop_first=False)
    
    for col in ['Frühling', 'Sommer', 'Herbst', 'Winter']:
        if col not in season_dummies.columns:
            dummies[col] = False
    
    df_copy = pd.concat([df_copy, season_dummies], axis=1)

    # remove the original columns
    df_copy = df_copy.drop(columns=columns_to_use)
    
    # Return the dataframe with original and new dummy-encoded columns
    return df_copy

def handle_binary_values(df: pd.DataFrame) -> pd.DataFrame:

    boolean_columns = df.select_dtypes(include=['bool']).columns  # Select boolean columns
    df[boolean_columns] = df[boolean_columns].astype(int)  # Convert to int

    # convert the columns with True and False to 1 and 0 and convert the int64 datatype to category
    df = df.replace({True: 1, False: 0})
    # convert the int64 datatype to category
    boolean_columns = df.select_dtypes(include=['int64']).columns
    # Convert these columns to categorical
    df[boolean_columns] = df[boolean_columns].astype('category')

    return df

def process_transformations(df: pd.DataFrame) -> pd.DataFrame:
    """Process the transformations on the DataFrame."""
    df = apply_cliclic_tranformations(df, cyclic_features = ['Tag','Hour', 'Monat', 'Wochentag', 'DayOfTheYear'])
    df = standardize_numeric_features(df, standardize_features = ['Temperature (°C)', 'Relative Humidity (%)', 'Wind Speed (km/h)', 'Precipitation (mm)',
                                                                  'Distance_to_Nearest_Holiday_Bayern','Distance_to_Nearest_Holiday_CZ'])
    df = get_dummy_encodings(df, columns_to_use = ['Jahreszeit', 'coco_2'])
    df = handle_binary_values(df)
    
    return df

def filter_features_for_modelling(df: pd.DataFrame) -> pd.DataFrame:
    """Filter the features for modelling."""
    # Filter the features for modelling
    df = df[numeric_features_for_modelling + categorical_features_for_modelling + target_vars_et]

    return df



def get_features(with_zscores_and_nearest_holidays_df, train_start_date, train_end_date):
    
    # Filter only for certain dates
    sliced_df = with_zscores_and_nearest_holidays_df[(with_zscores_and_nearest_holidays_df['Time'] >= train_start_date) & (with_zscores_and_nearest_holidays_df['Time'] <= train_end_date)]

    # Further feature engineering
    regionwise_df = get_regionwise_IN_and_OUT_columns(sliced_df)
    changed_datatypes_df = change_datatypes(regionwise_df, dtype_dict)
    processed_features_df = process_transformations(changed_datatypes_df)

    # Filter only for the features for modelling
    filtered_features_df = filter_features_for_modelling(processed_features_df)

    return filtered_features_df

def drop_duplicated_datetimeindices(df: pd.DataFrame, threshold: float = 0.0005) -> pd.DataFrame:
    """
    Drops rows with duplicated DatetimeIndex values if the total number of duplicates
    is below a specified threshold relative to the DataFrame size.

    If the proportion of duplicated index values exceeds the threshold, a ValueError
    is raised to avoid silently discarding a significant amount of data.

    Args:
        df (pd.DataFrame): DataFrame with a DatetimeIndex.
        threshold (float): Maximum acceptable proportion of duplicates (default: 0.0005 = 0.05%).

    Returns:
        pd.DataFrame: DataFrame with duplicated index rows removed.

    Raises:
        ValueError: If the proportion of duplicates exceeds the threshold.
    """
    n_duplicates = df.index.duplicated().sum()
    proportion = n_duplicates / len(df)

    if n_duplicates == 0:
        print("No duplicated index values found ✅")
        return df

    if proportion > threshold:
        raise ValueError(
            f"❌ Too many duplicated index values: {n_duplicates} ({proportion:.4%} of rows). "
            f"Exceeds threshold of {threshold:.4%}. Please investigate upstream."
        )

    print(f"⚠️ Dropping {n_duplicates} duplicated index rows ({proportion:.4%} of data).")
    return df[~df.index.duplicated(keep='first')]