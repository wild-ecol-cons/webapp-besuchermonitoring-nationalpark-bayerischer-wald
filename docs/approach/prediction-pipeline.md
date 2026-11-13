# Step 2: Building the Prediction Pipeline

## Overview
In this section, we will describe our data preprocessing and data cleaning steps, the data integration, feature selection and feature engineering, as well as our modeling approach and the selected models.

![E2E Pipeline Step 2 Prediction Pipeline](../asset/E2E_Pipeline_Prediction.png)
---

## Description of the Data Preprocessing and Cleaning Steps

The prediction pipeline uses three data sources: sensor data for visitor counts, weather data, and temporal features (public and school holidays). These datasets come in different formats, so each is cleaned and preprocessed before they are joined. Parking data is shown in the dashboard but is not used as a model input.

### Visitor Count Data Preprocessing

The historic visitor counts are read from the Data Hub (category *Permanente Besucherzählung (Eco-Counter)*, stored in Azure Blob Storage under `data-hub/preprocessed-data/visitor-counts-eco-counter`). New counts that park staff upload through the Data Hub are therefore picked up automatically the next time the model is trained.

For the visitor count data, several crucial steps were taken:

- **Removing Unwanted Data**: We excluded any data prior to "2016-05-10 03:00:00" since no sensors were installed before this date, ensuring our analysis focused on relevant data.

- **Fixing Column Names**: The original dataset contained inconsistently named columns due to sensor replacements and renaming. We implemented a mapping process to unify sensor names, allowing us to aggregate readings under single names. For example, 'Bucina PYRO IN' and 'Bucina_Multi IN' were combined into 'Bucina MERGED IN'.

- **Correcting and Imputing Timestamps**: We adjusted timestamps to account for daylight saving time changes, ensuring the integrity of our time series data. Missing values were filled in from subsequent rows to maintain continuity.

- **Handling Non-Replaced Sensors**: Columns representing non-replaced sensors were set to NaN for values recorded before their replacement dates, as they no longer provided valid data.

- **Correcting Overlapping Sensor Data**: In cases where both old and new sensors reported data during overlapping periods, we set values to NaN for old sensors after their replacement dates to avoid skewing our analysis.

- **Merging Columns**: We merged columns based on predefined mappings to create simplified combined columns, such as 'Bucina MERGED IN'. This step eliminated redundancy and ensured a focus on relevant data.

- **Outlier Handling**: Any visitor count exceeding 800 was transformed to NaN, as these values were deemed outliers and not representative of typical sensor readings.

- **Calculating Traffic Metrics**: We derived key traffic metrics, including:
    - `traffic_abs`: Total counts (in + out) from all sensors.
    - `sum_IN_abs` and `sum_OUT_abs`: Sums of incoming and outgoing counts.
    - `occupancy_abs`: A cumulative measure representing occupancy over time, aiding in trend analysis.

### Weather Data Preprocessing

The weather data was sourced and preprocessed to ensure consistency and reliability for our analysis. The following key steps were taken:

- **Data Sourcing**: 
     - Hourly weather data is retrieved from the Meteostat API each time the pipeline runs. For training, the data covers the full training and test period; for inference, it covers the last 10 days plus the 7-day forecast. The location is a point in the park (latitude 48.96119, longitude 13.36234), and Meteostat combines data from the 10 nearest weather stations.

- **Cleaning and Formatting**: 

    - Unnecessary columns such as dew point, snow, wind direction, and others were removed to simplify the dataset.
    - The remaining columns were renamed for clarity, transforming them into more descriptive titles like 'Temperature (°C)', 'Precipitation (mm)', and 'Relative Humidity (%)'.
    - The 'Time' column was converted into a proper datetime format for seamless time series analysis.

- **Imputation of Missing Values**: 
    - We implemented a function to fill missing values using linear interpolation, ensuring that gaps in the data were appropriately addressed. If a significant percentage of zero values (over 60%) was detected in any parameter, we opted to fill missing entries with zero.
    - This approach helped maintain the integrity of the dataset while providing a more accurate representation of weather conditions.

By carefully processing the weather data, we ensured it was well-prepared for integration with visitor count data and other datasets in our predictive modeling pipeline.

### Temporal Features (Holidays and Calendar)

The temporal features are now fetched automatically from the [OpenHolidays API](https://www.openholidaysapi.org/) for any requested date range, both for training and for inference:

| Feature | Source |
|---|---|
| `Feiertag_Bayern` | Public holidays in Bavaria (`DE-BY`) |
| `Feiertag_CZ` | Public holidays in the Pilsen (`CZ-PL`) or South Bohemian (`CZ-JC`) region |
| `Schulferien_Bayern` | School holidays in Bavaria (`DE-BY`) |
| `Schulferien_CZ` | School holidays in the Pilsen (`CZ-PL`) or South Bohemian (`CZ-JC`) region |

Only the two Czech regions bordering the park are used, not the whole of the Czech Republic.

From the date itself, the pipeline derives the day of the month (`Tag`), month (`Monat`), year (`Jahr`), day of the year (`DayOfTheYear`), weekday (`Wochentag`), a weekend flag (`Wochenende`), and the season (`Jahreszeit`: spring = March–May, summer = June–August, autumn = September–November, winter = December–February). The daily table is then expanded to hourly rows and an `Hour` column is added, so it can be joined with the hourly visitor and weather data.

---

## Data Integration

The preprocessed visitor counts, weather data, and temporal features are joined on their hourly timestamps. All datasets are already in Berlin local time at this point; before the join, the timezone information is removed from the timestamps so they can be matched. The join is an **inner join**, so only hours that exist in all three datasets are kept. A small number of remaining duplicated timestamps (at most 0.05 % of the rows) are dropped before training; if there are more, the pipeline stops with an error.

---

## Feature Selection and Feature Engineering

After joining the data, we conducted various tasks, including feature selection and feature engineering. Below are the relevant aspects of this process:

### Region-wise Mapping

The Bavarian Forest National Park is divided into six regions, and we grouped the sensors accordingly. For each region, we aggregated the IN and OUT values based on the specific sensors that belong to that region:

> **Falkenstein-Schwellhäusl**  
> Sensors: Bayerisch Eisenstein, Brechhäuslau, Deffernik, Ferdinandsthal, Schillerstraße  
>
> **Nationalparkzentrum Falkenstein**  
> Sensors: Falkenstein 1, Falkenstein 2  
>
> **Scheuereck-Schachten-Trinkwassertalsperre**  
> Sensors: Gsenget, Scheuereck, Trinkwassertalsperre  
>
> **Lusen-Mauth-Finsterau**  
> Sensors: Bucina, Felswandergebiet, Fredenbrücke, Schwarzbachbrücke, Waldhausreibe, Wistlberg, Sagwassersäge  
>
> **Rachel-Spiegelau**  
> Sensors: Diensthüttenstraße, Gfäll, Klingenbrunner Wald, Klosterfilz, Racheldiensthütte, Waldspielgelände  
>
> **Nationalparkzentrum Lusen**  
> Sensors: Lusen 1, Lusen 2, Lusen 3  

The readings from these sensors were combined to provide the total IN and OUT values for the region.

### Feature Engineering

The same feature engineering code is used for training and for inference, so the model sees the same kind of features in both cases:

- **Distance to the nearest public holiday** (in days), separately for Bavaria and the Czech border regions.
- **Weather anomalies:** for temperature, relative humidity, wind speed, and precipitation, the daily maximum is calculated, as well as the daily precipitation sum. Each daily value is then expressed as a z-score relative to the mean and standard deviation of a rolling **5-day window**. This shows whether a day is unusually warm, wet, or windy compared with the days before it.
- **Weather condition:** the Meteostat condition code is grouped into six categories (sunny, cloudy, rainy, snowy, extreme, stormy; see the table on the [data sourcing](data.md) page).

### Data Transformation

1. **Cyclic features:** day of the month, hour, month, weekday, and day of the year are each converted into a sine and a cosine component, so that, for example, hour 23 and hour 0 are close to each other.
2. **Numerical features:** temperature, relative humidity, wind speed, precipitation, and the two holiday distances are standardized (z-score: mean 0, standard deviation 1).
3. **Categorical features:** the weather category and the season are one-hot encoded (one 0/1 column per category). All true/false columns are converted to 0/1 and treated as categories.

### Model Inputs and Targets

The model training script (`train_regressor.py`) uses the following inputs:

| Type | Features |
|---|---|
| Numerical (21) | Temperature, relative humidity, wind speed, precipitation; z-scores of the daily maximum of each of these and of the daily precipitation sum; distance to nearest holiday (Bavaria, CZ); sine/cosine of day of month, month, hour, weekday, and day of year |
| Categorical (15) | Weekend; school holidays (Bavaria, CZ); public holidays (Bavaria, CZ); weather category (sunny, cloudy, rainy, snowy, extreme, stormy); season (spring, summer, autumn, winter) |

A separate model is trained for each of **15 target variables**: total traffic (`traffic_abs`), total IN (`sum_IN_abs`) and total OUT (`sum_OUT_abs`) for the whole park, and IN and OUT for each of the six regions listed above.

### Additional Steps

- **Datetime Conversion**: The 'Time' column was converted to a datetime format for easier time-based indexing and analysis.
- **Summation of IN and OUT Values**: We summed the IN and OUT values for each region based on the defined mappings.
- **Data Type Changes**: Data types were adjusted according to predefined specifications to ensure consistency and optimize performance.

This systematic approach to preprocessing and feature engineering sets a solid foundation for subsequent modeling and analysis.

---

## Modeling

### Current Model: LightGBM

The production pipeline trains a **LightGBM** regressor (gradient-boosted decision trees) through PyCaret, one model per target variable. LightGBM replaced the Extra Trees Regressor that was selected in 2024 (see [Earlier Experiments](#earlier-experiments-2024)). Because the file and variable names were kept, the saved models are still called `extra_trees_<target>.pkl`, even though they contain LightGBM models.

**Training setup**

- **Training period:** January 1, 2023, to December 31, 2024.
- **Test period:** January 1, 2025, to December 31, 2025. The end date is a parameter (`test_end_date`) of the pipeline in `Dashboard.py`.
- **PyCaret setup:** the data is not shuffled (to keep the time order), 5-fold cross-validation is used on the training period, and the test period serves as hold-out data.
- After evaluation, the model is finalized (refitted) and saved.

**Running the training**

Training is not run on a schedule. It is started by calling `run_pipeline_and_create_dashboard(should_run_training=True)` in `Dashboard.py`. Each training run gets a unique ID (UUID). The models are saved to Azure Blob Storage under `models/models_trained/<UUID>/`, and the predictions on the test period under `models/test_data_predictions/<UUID>/`.

**Selecting the model used in the dashboard**

The dashboard does not automatically use the newest training run. The run it uses is set in `folder_prefix` in `src/prediction_pipeline/modeling/create_inference_dfs.py`. After a new training run, this value has to be updated to the new UUID.

### Real-Time Inference

The inference pipeline (`run_inference.py`) runs when the dashboard is opened and is refreshed every **3 hours**:

1. Fetch the temporal features (OpenHolidays API) and the weather data from 10 days ago until 7 days ahead. The past days are needed to calculate the rolling 5-day weather z-scores.
2. Apply the same feature engineering and transformations as in training.
3. Load the 15 models of the selected training run from Azure and predict hourly visitor numbers for the **next 7 days**.
4. Aggregate the predictions per region on an **hourly** level and as **daily** sums (IN + OUT). The dashboard charts show these predicted counts.
5. In addition, a relative traffic level per region is calculated with min-max scaling (range 0.03 to 1). These values are not shown in the dashboard at the moment.

The raw predictions of each model are also saved to Azure under `models/inference_data_outputs/`.

### Earlier Experiments (2024)

The following sections describe the models compared during the DSSGx fellowship in 2024. At that time, the data from January 1, 2023, to July 22, 2024, was used, and the Extra Trees Regressor was chosen for real-time inference. The LSTM code (`train_lstm.py`) is still in the repository but is not used in the current pipeline.

#### ExtraTree Regressor

The ExtraTree Regressor (Extremely Randomized Trees) is a powerful ensemble learning algorithm that constructs multiple decision trees during training. Unlike traditional decision trees, where splits are chosen based on the best criteria, Extra Trees randomly select cut points in the features. This approach often reduces variance without significantly increasing bias, making Extra Trees particularly well-suited for datasets with complex patterns and large feature spaces.

##### Why ExtraTree Regressor is Ideal for Our Use Case

- **Robustness to Overfitting**: Due to the random nature of tree splits, ExtraTree Regressor is less likely to overfit the training data, especially in cases where the data has high dimensionality or contains noise.

- **Efficiency**: ExtraTree Regressor requires less computational effort compared to other ensemble methods like Random Forests, making it an efficient choice for large-scale data.

- **Interpretability**: While still complex, the decision-tree-based structure of the ExtraTree Regressor provides a level of interpretability, allowing us to understand which features contribute most significantly to the model’s predictions.

- **Handling of Various Feature Types**: The model can handle both numerical and categorical features, which is crucial for our dataset, containing a mix of these feature types.

##### Model Implementation

**Data Preparation**: The data is split into training (January 1, 2023, to April 30, 2024) and testing (May 1, 2024, to July 22, 2024) sets based on specific date ranges, ensuring the temporal structure of the data is preserved.

**Model Setup**: PyCaret is used for setting up the model, where we define numeric and categorical features, ensure no data shuffling, and use 90% of the data for training with a 5-fold cross-validation.

**Model Training**: The Extra Trees Regressor is trained on the defined training set, and predictions are made on the test set to evaluate performance.

**Model Saving**: The trained models are saved for each target variable, ensuring reproducibility and facilitating later use in inference tasks.

This approach allowed us to capture the complexities in the data and produce reliable forecasts for visitor traffic in the Bavarian Forest National Park.

#### LSTM (Long Short-Term Memory)

Long Short-Term Memory (LSTM) networks are a type of recurrent neural network (RNN) specifically designed to model sequential data and capture long-term dependencies. LSTMs are highly effective for time series forecasting due to their ability to retain information over extended periods, which is crucial for predicting visitor traffic based on past patterns.

##### Why LSTM is Ideal for Our Use Case

- **Handling Sequential Data**: LSTMs are designed to process sequences of data, which is critical for our task of predicting future visitor counts based on historical patterns.

- **Capturing Long-Term Dependencies**: The LSTM architecture effectively captures long-term dependencies, which is crucial for understanding visitor trends that may span over days or weeks.

- **Flexibility in Architecture**: LSTMs allow for various architectural adjustments (e.g., bidirectional layers, stacking) that enable the model to capture more complex visitor traffic patterns.

- **Robustness to Temporal Patterns**: LSTMs are capable of learning and adapting to the temporal structure of the data, making them ideal for forecasting tasks where the order and timing of data points are important.

##### Model Implementation

**Sequence Creation**: To train the LSTM, the data is prepared by creating sequences. Each sequence represents a window of 168 hours (7 days). This approach ensures that the model is trained on weekly patterns, which are crucial for accurate forecasting.

- **Training Sequences**: The training data is split into sequences of 168 hours, with each sequence used to predict the following hour. This ensures that the model learns from both short-term fluctuations and longer-term trends.

**Model Architecture**: The LSTM model architecture is designed to capture complex temporal patterns in the data. Key components of the architecture include:

- **Bidirectional LSTM Layers**: These layers process the sequence data in both forward and backward directions, allowing the model to learn from both past and future contexts within each sequence.

- **Dropout Layers**: Dropout layers are included to prevent overfitting by randomly dropping units during training, encouraging the model to learn more robust features.

- **Dense Layer**: The final Dense layer outputs predictions for the target variable, leveraging the features learned by the preceding LSTM layers.

**Training and Evaluation**: The LSTM model is trained on the prepared sequences with a validation split to monitor performance. Early stopping is implemented to prevent overfitting, stopping the training if the validation loss does not improve for several consecutive epochs.

- **Optimizer**: The Adam optimizer is used, which is effective for handling the complexity of LSTM networks.

- **Loss Function**: The Mean Squared Error (MSE) loss function is employed to minimize large errors by focusing on the difference between predicted and actual values.

- **Early Stopping**: This technique monitors validation loss and restores the best weights if overfitting is detected.

**Model Saving**: After training, the model is saved for each target variable. This ensures that the trained models can be reused for inference and further fine-tuning, making the forecasting process more efficient and scalable.

The LSTM model, with its ability to model complex sequential data, has proven to be a powerful tool in our visitor traffic forecasting pipeline, particularly for capturing the temporal dynamics of visitor flows across different regions of the Bavarian Forest National Park.

#### Model Selection for Real-Time Inference

After experimenting with both the LSTM and Extra Tree Regressor models, we found that both performed admirably in terms of forecasting visitor traffic. However, our final choice of model for real-time inference was influenced by several key factors:

- **Limited Dataset Size**: Given the relatively small size of our dataset, we needed a model that could deliver reliable performance without requiring large amounts of data. While LSTM models are powerful for capturing complex temporal dependencies, they often require larger datasets to fully realize their potential.

- **Model Simplicity and Interpretability**: The Extra Tree Regressor, being an ensemble learning method based on decision trees, offers a straightforward and interpretable approach to forecasting. Its simplicity means that it is less prone to overfitting on smaller datasets and provides easily understandable results, which is advantageous for real-time applications where quick insights are needed.

- **Computational Efficiency**: In real-time inference scenarios, the computational cost and speed of model predictions are critical. The Extra Tree Regressor is computationally efficient compared to LSTM models, which can be resource-intensive due to their sequential nature and complex architecture. This efficiency makes the Extra Tree Regressor a more suitable choice for deploying in a real-time environment where low latency is essential.

- **Robustness to Overfitting**: With fewer data points, there is a higher risk of overfitting, especially with more complex models like LSTM. The Extra Tree Regressor, with its inherent randomness and use of multiple decision trees, is more robust against overfitting, ensuring that the model generalizes better on unseen data.

- **Scalability for Future Datasets**: As more data becomes available over time, the simplicity of the Extra Tree Regressor allows for easier retraining and updating of the model. This scalability is critical as the system evolves and more historical data is collected.

- **Consistency in Performance**: During our model evaluation, the Extra Tree Regressor consistently delivered strong performance across various metrics, making it a reliable choice for ongoing operations.

Based on these considerations, we selected the Extra Tree Regressor as the model for real-time inference. This decision balances the need for accurate predictions with the practical constraints of our current dataset and computational resources, ensuring that we can provide reliable visitor traffic forecasts in the Bavarian Forest National Park.
