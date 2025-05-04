import pandas as pd
from src.prediction_pipeline.modeling.source_and_feature_selection import get_features
from pycaret import *
from pycaret.time_series import *
from pycaret.regression import *
import os
import uuid

# Define target columns
target_vars_et  = ['traffic_abs', 'sum_IN_abs', 'sum_OUT_abs', 'Lusen-Mauth-Finsterau IN', 'Lusen-Mauth-Finsterau OUT', 
               'Nationalparkzentrum Lusen IN', 'Nationalparkzentrum Lusen OUT', 'Rachel-Spiegelau IN', 'Rachel-Spiegelau OUT', 
               'Falkenstein-Schwellhäusl IN', 'Falkenstein-Schwellhäusl OUT', 
               'Scheuereck-Schachten-Trinkwassertalsperre IN', 'Scheuereck-Schachten-Trinkwassertalsperre OUT', 
               'Nationalparkzentrum Falkenstein IN', 'Nationalparkzentrum Falkenstein OUT']

numeric_features = ['Temperature (°C)', 'Relative Humidity (%)', 'Wind Speed (km/h)', 'ZScore_Daily_Max_Temperature (°C)', 
                    'ZScore_Daily_Max_Relative Humidity (%)','ZScore_Daily_Max_Wind Speed (km/h)',
                    'Distance_to_Nearest_Holiday_Bayern','Distance_to_Nearest_Holiday_CZ','Tag_sin', 'Tag_cos', 'Monat_sin', 'Monat_cos',
                    'Hour_sin', 'Hour_cos','Wochentag_sin', 'Wochentag_cos']

categorical_features = ['Wochenende','Laubfärbung', 'Schulferien_Bayern', 'Schulferien_CZ', 
                        'Feiertag_Bayern', 'Feiertag_CZ', 'HEH_geoeffnet', 'HZW_geoeffnet', 'WGM_geoeffnet', 
                        'Lusenschutzhaus_geoeffnet', 'Racheldiensthuette_geoeffnet', 'Falkensteinschutzhaus_geoeffnet', 
                        'Schwellhaeusl_geoeffnet','sunny', 'cloudy', 'rainy', 'snowy', 'extreme','stormy','Frühling',
                        'Sommer', 'Herbst', 'Winter']

def create_uuid() -> str:
    """Creates a unique identifier string.

    Returns:
        str: A unique identifier string.
    """
    unique_id = str(uuid.uuid4())

    return unique_id



def train_regressor(feature_dataframe: pd.DataFrame) -> None:

    uuid = create_uuid()
    print(f"Training Regressor with Run ID: {uuid}")

    for target in target_vars_et:
        print(f"Training Extra Trees Regressor for {target}")
    
        # Ensure the DataFrame has a date-time index
        if isinstance(feature_dataframe.index, pd.DatetimeIndex):
            # Define date ranges for training, testing, and unseen data
            train_start = '2023-01-01'
            train_end = '2024-04-30'
            test_start = '2024-05-01'
            test_end = '2024-07-21'
    
            # Split the data into train, test, and unseen sets based on date ranges
            df_train = feature_dataframe[numeric_features+categorical_features+[target]].loc[train_start:train_end]
            df_test = feature_dataframe[numeric_features+categorical_features+[target]].loc[test_start:test_end]
 
            # Setup PyCaret for the target variable with the combined data
            reg_setup = setup(data=df_train,
                            target=target, 
                            numeric_features=numeric_features, 
                            categorical_features=categorical_features,
                            fold=5,
                            preprocess=False,
                            data_split_shuffle=True,
                            session_id=123,
                            test_data=df_test)  # Use 90% of data for training 
                
            # Train the Extra Trees Regressor model
            extra_trees_model = create_model('et')
                
            # Predict on the unseen data
            predictions = predict_model(extra_trees_model) # predicts on hold-out data defined above

            # Finalize the model
            final_model = finalize_model(extra_trees_model)
            

            save_model_path = os.path.join("models",uuid, f"extra_trees_{target}")
            # save the model locally and create the uuid folder if doesn't exist
            if not os.path.exists(save_model_path):
                os.makedirs(save_model_path)
            # save the model locally
            save_model(final_model, save_model_path, model_only=True)
            print(f"Model with {target} saved locally")
            
            save_path_predictions = os.path.join("models", uuid, "test_data_predictions")
            # create the uuid folder if doesn't exist
            if not os.path.exists(save_path_predictions):
                os.makedirs(save_path_predictions)

            # Save prediction locallly as parquet file
            predictions.to_parquet(os.path.join(save_path_predictions, f"y_test_predicted_{target}.parquet"))
            print(f"Predictions with {target} saved locally")

    return