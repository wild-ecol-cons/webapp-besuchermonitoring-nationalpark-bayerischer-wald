# import libraries
import pandas as pd
import os


#################### Start and End time of processing data from different sensors ####################

parking_sensors = ["parkplatz-graupsaege-1",
    # "parkplatz-fredenbruecke-1",
    "p-r-spiegelau-1",
    # "skiwanderzentrum-zwieslerwaldhaus-2",
    "parkplatz-zwieslerwaldhaus-1",
    "parkplatz-zwieslerwaldhaus-nord-1",
    "parkplatz-nationalparkzentrum-falkenstein-2",
    "scheidt-bachmann-parkplatz-1",
    "parkplatz-nationalparkzentrum-lusen-p2",
    "parkplatz-waldhaeuser-kirche-1",
    "parkplatz-waldhaeuser-ausblick-1",
    "parkplatz-skisportzentrum-finsterau-1"] 

paths_to_parking_data = [os.path.join('./outputs','parking_data_final',f'{sensors}_historical_parking_data.csv') for sensors in parking_sensors]


def fill_missing_values(data):
    """
    Fill missing values in the data

    """
    # Fill all the missing values with linear interpolation
    data.interpolate(method='linear', inplace=True)
    return data
    

def check_missing_data_per_sensor(data,sensor):
    """
    Check missing data per sensor

    """
    missing_data = data.isnull().sum()
    print(f"Missing data for {sensor}:")
    print('---------------------------------')
    print(missing_data)

    if missing_data.sum() == 0:
        print(f"No missing data for {sensor}")
    else:
        data = fill_missing_values(data)
    
    return data

def impute_occupancy_values(data):
    """
    Impute occupancy values where the occupancy is greater than the capacity

    """
    # Impute the occupancy values where the occupancy is greater than the capacity
    data['occupancy'] = data['occupancy'].apply(lambda x: x if x < data['capacity'].max() else data['capacity'].max())
    print('---------------------------------')
    print("Imputed occupancy values:")
    print(data[data['occupancy']>data['capacity']])
    return data
    

def check_data_quality(data,sensor):
    """
    Check data quality - if the occupancy is greater than the capacity of the parking space

    """
    # Check if the capacity is always smaller than the occupancy
    if data['occupancy'].max() > data['capacity'].max():
        # remove the rows where the occupancy is greater than the capacity
        # print the row numbers where the occupancy is greater than the capacity
        print(f"Data quality issue for {sensor}")
        print('---------------------------------')
        print("The occupancy is greater than the capacity")
        print(f"Rows where the occupancy is greater than the capacity:")
        print(data[data['occupancy']>data['capacity']])
        # impute_occupancy_values(data)
    else:
        print(f"No data quality issue for {sensor}")
    

    return data

def save_higher_occupancy_rate(data,sensor):
    """
    Save the rows where the occupancy rate is greater than 100

    """
    save_path = os.path.join('processed','parking_data_quality',f'{sensor}_higher_occupancy_rate.csv')

    # make the output directory if it doesn't exist
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    # Save the rows where the occupancy rate is greater than 100
    data.to_csv(save_path,index=False)
    print(f"Saved the rows where the occupancy rate is greater than 100 for {sensor}")

def check_data_quality_occupancy_rate(data,sensor):

    """
    Check data quality - if the occupancy rate is greater than 100

    """
    # Check if the occupancy rate is greater than 100
    if data['occupancy_rate'].max() > 100.00:
        print(f"Data quality issue for {sensor}")
        print('---------------------------------')
        print("The occupancy rate is greater than 100")
        print(f"Rows where the occupancy rate is greater than 100:")
        print(data[data['occupancy_rate']>100])
        save_higher_occupancy_rate(data[data['occupancy_rate']>100],sensor)

    else:
        print(f"No data quality issue for {sensor}")

def main():
    """
    Main function to run the script

    """
    for sensor, path in zip(parking_sensors, paths_to_parking_data):
        data = pd.read_csv(path)
        data['time'] = pd.to_datetime(data['time'])

        # Missing data for each sensor
        print(f"Checking missing data for {sensor}....................")
        check_missing_data_per_sensor(data,sensor)

        # check if the capacity is always smaller than the occupancy
        check_data_quality_occupancy_rate(data,sensor)



if __name__ == '__main__':
    main()

