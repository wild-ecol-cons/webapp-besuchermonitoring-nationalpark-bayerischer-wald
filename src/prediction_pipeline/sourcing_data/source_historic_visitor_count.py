import os
import pandas as pd
import glob

raw_data_folder = "raw-data"
visitor_counts_folder = "hourly-historic-visitor-counts-all-sensors"
visior_count_path = os.path.join("data","raw","visitor_sensor")

# needed columns across all dfs 
common_columns = ['Time',
 'Bayerisch Eisenstein IN',
 'Bayerisch Eisenstein OUT',
 'Bayerisch Eisenstein Fußgänger IN',
 'Bayerisch Eisenstein Fußgänger OUT',
 'Bayerisch Eisenstein Fahrräder IN',
 'Bayerisch Eisenstein Fahrräder OUT',
 'Brechhäuslau IN',
 'Brechhäuslau OUT',
 'Brechhäuslau Fußgänger IN',
 'Brechhäuslau Fußgänger OUT',
 'Bucina IN',
 'Bucina OUT',
 'Bucina_Multi OUT',
 'Bucina_Multi Fußgänger IN',
 'Bucina_Multi Fahrräder IN',
 'Bucina_Multi Fahrräder OUT',
 'Bucina_Multi Fußgänger OUT',
 'Deffernik IN',
 'Deffernik OUT',
 'Deffernik Fahrräder IN',
 'Deffernik Fahrräder OUT',
 'Deffernik Fußgänger IN',
 'Deffernik Fußgänger OUT',
 'Diensthüttenstraße Fußgänger IN',
 'Diensthüttenstraße Fußgänger OUT',
 'Felswandergebiet IN',
 'Felswandergebiet OUT',
 'Ferdinandsthal IN',
 'Ferdinandsthal OUT',
 'Fredenbrücke Fußgänger IN',
 'Fredenbrücke Fußgänger OUT',
 'Gfäll Fußgänger IN',
 'Gfäll Fußgänger OUT',
 'Gsenget IN',
 'Gsenget OUT',
 'Gsenget IN.1',
 'Gsenget OUT.1',
 'Gsenget Fahrräder IN',
 'Gsenget Fahrräder OUT',
 'Klingenbrunner Wald IN',
 'Klingenbrunner Wald OUT',
 'Klingenbrunner Wald Fußgänger IN',
 'Klingenbrunner Wald Fußgänger OUT',
 'Klingenbrunner Wald Fahrräder IN',
 'Klingenbrunner Wald Fahrräder OUT',
 'Klosterfilz IN',
 'Klosterfilz OUT',
 'Klosterfilz Fußgänger IN',
 'Klosterfilz Fußgänger OUT',
 'Klosterfilz Fahrräder IN',
 'Klosterfilz Fahrräder OUT',
 'NPZ_Falkenstein IN',
 'NPZ_Falkenstein OUT',
 'Racheldiensthütte IN',
 'Racheldiensthütte OUT',
 'Racheldiensthütte Fahrräder IN',
 'Racheldiensthütte Cyclist OUT',
 'Racheldiensthütte Pedestrian IN',
 'Racheldiensthütte Pedestrian OUT',
 'Sagwassersäge Fußgänger IN',
 'Sagwassersäge Fußgänger OUT',
 'Scheuereck IN',
 'Scheuereck OUT',
 'Schillerstraße IN',
 'Schillerstraße OUT',
 'Schwarzbachbrücke Fußgänger IN',
 'Schwarzbachbrücke Fußgänger OUT',
 'TFG_Falkenstein_1 Fußgänger zum Parkplatz',
 'TFG_Falkenstein_1 Fußgänger zum HZW',
 'TFG_Falkenstein_2 Fußgänger In Richtung Parkplatz',
 'TFG_Falkenstein_2 Fußgänger In Richtung TFG',
 'TFG_Lusen IN',
 'TFG_Lusen OUT',
 'TFG_Lusen_1 Fußgänger Richtung TFG',
 'TFG_Lusen_1 Fußgänger Richtung Parkplatz',
 'TFG_Lusen_2 Fußgänger Richtung Vögel am Waldrand',
 'TFG_Lusen_2 Fußgänger Richtung Parkplatz',
 'TFG_Lusen_3 TFG Lusen 3 IN',
 'TFG_Lusen_3 TFG Lusen 3 OUT',
 'Trinkwassertalsperre IN',
 'Trinkwassertalsperre OUT',
 'Trinkwassertalsperre_MULTI IN',
 'Trinkwassertalsperre_MULTI OUT',
 'Trinkwassertalsperre_MULTI Fußgänger IN',
 'Trinkwassertalsperre_MULTI Fußgänger OUT',
 'Trinkwassertalsperre_MULTI Fahrräder IN',
 'Trinkwassertalsperre_MULTI Fahrräder OUT',
 'Waldhausreibe IN',
 'Waldhausreibe OUT',
 'Waldhausreibe Channel 1 IN',
 'Waldhausreibe Channel 2 OUT',
 'Waldspielgelände_1 IN',
 'Waldspielgelände_1 OUT',
 'Wistlberg Fußgänger IN',
 'Wistlberg Fußgänger OUT']



def source_historic_visitor_count_local(local_folder_path, common_columns):
    """Source historic visitor count data from a local folder."""

    # Match all CSV files in the local folder
    path_pattern = os.path.join(visior_count_path, "*.csv")
    csv_files = glob.glob(path_pattern)

    # Read and concatenate all CSV files
    df_list = [
        pd.read_csv(file, skiprows=2, usecols=common_columns)
        for file in csv_files
    ]
    visitor_counts = pd.concat(df_list, ignore_index=True)

    return visitor_counts