# Findings and Final Product

## Overview

In this section, we summarized the findings of the model training as well as the visitor traffic forecasting. Moreover, we give an overview over the final product built.

The analysis of visitor traffic in the Bavarian Forest National Park utilized the Extra Trees Regressor, focusing on predictions across six distinct regions. We examined 15 target variables, including total visitor counts and IN/OUT values for each region, as well as overall metrics for the park.

<div class="doc-outdated" markdown>
**The model results on this page are from the 2024 fellowship.** They describe the Extra Trees Regressor, trained on data from January 2023 to April 2024 and tested on May to July 2024. The current pipeline trains a **LightGBM** model on 2023–2024 data, tests it on 2025, and uses a different set of features (see [Modeling](approach/prediction-pipeline.md#modeling)). The metrics table, the comparison plots, and the feature importance plots below therefore do not describe the model currently used in the dashboard. They should be replaced with the results of the current training run, which are stored in Azure under `models/test_data_predictions/<UUID>/`.
</div>

---

## Model Training

### Model Fit Indices

The performance of the Extra Trees Regressor was evaluated using 5-fold cross-validation for both training and test data. The average results across all folds for the target variables are summarized in the table below:

| Target/Model                                                   | MAE      | MSE       | RMSE     | R²      | RMSLE   | MAPE    |
|---------------------------------------------------------------|----------|-----------|----------|---------|---------|---------|
| Traffic (IN + OUT for entire Park)                            | 46.1162  | 8917.1099 | 92.2189  | 0.8081  | 0.8319  | 1.0861  |
| IN (entire park)                                             | 24.3335  | 2442.0343 | 48.2164  | 0.7974  | 0.7300  | 1.0028  |
| OUT (entire park)                                            | 24.1747  | 2458.5690 | 48.4344  | 0.7931  | 0.8057  | 1.0266  |
| Lusen-Mauth-Finsterau (IN)                                   | 6.7317   | 201.3070  | 14.0569  | 0.5393  | 0.8434  | 1.4217  |
| Lusen-Mauth-Finsterau (OUT)                                  | 8.0701   | 257.9571  | 15.9973  | 0.5997  | 0.9015  | 1.6323  |
| Nationalparkzentrum Lusen (IN)                               | 7.9054   | 287.9660  | 16.6835  | 0.6963  | 0.6837  | 1.1265  |
| Nationalparkzentrum Lusen (OUT)                              | 7.5386   | 260.5668  | 15.9148  | 0.7029  | 0.7600  | 1.1740  |
| Rachel-Spiegelau (IN)                                        | 5.9471   | 147.6946  | 11.5325  | 0.6194  | 0.6604  | 1.0976  |
| Rachel-Spiegelau (OUT)                                       | 5.1613   | 114.6056  | 10.1086  | 0.6211  | 0.6459  | 1.1172  |
| Falkenstein-Schwellhäusl (IN)                                | 6.3214   | 175.2889  | 12.7272  | 0.7207  | 0.6538  | 1.0518  |
| Falkenstein-Schwellhäusl (OUT)                               | 5.9329   | 159.9409  | 12.2524  | 0.7392  | 0.6455  | 1.0008  |
| Scheuereck-Schachten-Trinkwassertalsperre (IN)              | 2.4397   | 57.1253   | 6.9169   | 0.1636  | 0.6783  | 1.0380  |
| Scheuereck-Schachten-Trinkwassertalsperre (OUT)             | 2.1488   | 21.6679   | 4.2865   | 0.3744  | 0.6556  | 1.0186  |
| Nationalparkzentrum Falkenstein (IN)                         | 5.0964   | 116.4658  | 10.7654  | 0.6457  | 0.6387  | 1.0955  |
| Nationalparkzentrum Falkenstein (OUT)                        | 4.7758   | 106.0070  | 10.2023  | 0.6063  | 0.6482  | 1.2331  |

### Understanding the Metrics

Before we analyze the specific results, let's clarify the metrics used:

* **MAE (Mean Absolute Error):** Measures the average absolute difference between predicted and actual values.
* **MSE (Mean Squared Error):** Measures the average squared difference between predicted and actual values.
* **RMSE (Root Mean Squared Error):** The square root of MSE, often preferred for interpretability as it's in the same units as the target variable.
* **R² (R-squared):** Indicates the proportion of variance in the target variable explained by the model. Higher values are better.
* **RMSLE (Root Mean Squared Logarithmic Error):** Useful for predicting values on a logarithmic scale (e.g., when values can range from 0 to infinity).
* **MAPE (Mean Absolute Percentage Error):** Measures the average percentage error between predicted and actual values.

### Interpreting the Results
Based on the provided table, here are some observations:

1. **Overall Visitor Traffic**:

    - The model predicted an average total traffic count (IN + OUT) of **46.12** with a **R² value of 0.8081**, indicating a strong relationship between predicted and actual values. This suggests that the model successfully captures the dynamics of visitor traffic across the entire park.

2. **Regional Performance**:
    - **Lusen-Mauth-Finsterau** showed relatively lower prediction errors, with MAE values of **6.73** (IN) and **8.07** (OUT). These figures suggest a stable visitor count in this region, potentially influenced by its accessibility and visitor amenities.
    - **Nationalparkzentrum Lusen** also performed well, with MAE values of **7.91** (IN) and **7.54** (OUT), indicating a robust model fit for this popular tourist area.
    - In contrast, the **Scheuereck-Schachten-Trinkwassertalsperre** region exhibited the lowest prediction accuracy, with MAE values of **2.44** (IN) and **2.15** (OUT). The low R² value of **0.1636** for IN indicates that this region's visitor traffic may be influenced by more unpredictable factors, warranting further investigation.

3. **Traffic Trends**:
    - The overall IN and OUT values for the park were relatively balanced, with minor variations indicating consistent visitor patterns throughout the year. For example, the predicted IN count of **24.33** for the entire park suggests that visitors are more inclined to enter the park compared to exiting, reflecting a positive visitor experience and prolonged stays.

4. **Regional Disparities**:
    - Some regions, such as **Rachel-Spiegelau** and **Falkenstein-Schwellhäusl**, demonstrated relatively close MAE values (around **5-7** for both IN and OUT),       suggesting similar patterns of visitor behavior. The consistent metrics across these regions may indicate comparable attractions or services that draw visitors in and keep them within these areas.

### Conclusion

The findings illustrate the Extra Trees Regressor's effectiveness in predicting visitor traffic across various regions of the Bavarian Forest National Park. The results indicate strong predictive power, especially for areas with consistent visitor patterns. These insights can aid park management in resource allocation, event planning, and enhancing visitor experience based on anticipated traffic flows.

---

## Visitor Traffic Forecasting for Bavarian Forest National Park (BFNP)

We evaluated the ExtraTree Regressor model on unseen data from **May 1, 2024, to July 22, 2024**. The model was trained on historical data and used to forecast visitor traffic in the Bavarian Forest National Park (BFNP). The results across various regions and target variables are presented below.

### Evaluation on Unseen Data

The graphs presented above show how the model performs on **unseen data** from May to July 2024, which was not included in the model's training or validation phases. Evaluating on unseen data allows us to assess the model's generalizability and real-world applicability for forecasting future visitor counts.

![Plots comparing the predicted vs the actual values of unseen data to the model](asset/Comparison of Predicted and Actual Visitor Counts to Bavarian Forest National Park (BFNP) on Unseen Data - 1.png)
<p style="text-align:center;">Comparison of predicted and actual values on unseen data (1)</p>

![Plots comparing the predicted vs the actual values of unseen data to the model](asset/Comparison of Predicted and Actual Visitor Counts to Bavarian Forest National Park (BFNP) on Unseen Data - 2.png)
<p style="text-align:center;">Comparison of predicted and actual values on unseen data (2)</p>

---

**Traffic Abs**

The total park traffic (`traffic_abs`) is consistently overestimated in both low and high-traffic periods. The predicted values follow the general trend of the actual counts but are consistently higher, particularly during low-traffic periods.

**Sum IN and Sum OUT**

For `sum_IN_abs` and `sum_OUT_abs`, the ExtraTree Regressor successfully captures the cyclic nature of visitor inflow and outflow. However, the magnitude of the predicted values often falls short of actual observations during peak traffic days, indicating that the model could struggle to forecast sharp increases in visitor numbers.

**Region-Specific Analysis**

- **Rachel-Spiegelau IN/OUT**:
  - The model captures the general trend for both inbound and outbound traffic at Rachel-Spiegelau. However, it underestimates the peaks, especially on days of high visitor traffic.

- **Scheuereck-Schachten-Trinkwassertalsperre IN/OUT**:
  - Predictions for `IN` and `OUT` traffic at this region show better alignment with actual counts during low-traffic periods. The model slightly underestimates the peaks during high-traffic days, though it captures the overall flow well.

- **Falkenstein-Schwellhäusl IN/OUT**:
  - For both `IN` and `OUT` traffic, the model demonstrates its strength in recognizing the cyclic nature of the data. However, peak values are significantly underestimated during high-traffic days, indicating a need for additional model tuning.

- **Nationalparkzentrum Lusen IN/OUT**:
  - The model performs similarly in this region, capturing trends but struggling to forecast peak visitor numbers. There is some overestimation during low-traffic days, but the overall cyclic pattern is well-represented.

### Model Performance Insights

**General Performance**

The ExtraTree Regressor does well in capturing general trends and seasonality across all regions. However, the model struggles with accurately predicting the magnitude of peaks and troughs, especially during periods of high traffic. This is a common issue seen in all regions, suggesting the need for advanced feature engineering or hybrid models that can better capture these extremes.

**Predictions for Unseen Data**

Evaluating the model on unseen data has revealed the following key insights:

- **Cyclic Trends**: The model successfully captures recurring patterns in visitor traffic, likely influenced by weekends, holidays, and seasonal effects.

- **Underestimation of Peaks**: Across almost all regions, the model consistently underestimates peak visitor counts, especially during high-traffic periods.

- **Overestimation of Low-Traffic Days**: During low-traffic days, the model tends to overestimate the visitor count slightly, leading to higher errors in some regions.

### Key Takeaways and Future Work

**Improvements to Handle Extremes**

The model's underestimation of peak traffic and overestimation during low-traffic periods suggest that further feature engineering or model enhancements could help. Techniques such as gradient boosting or hybrid models that combine tree-based methods with sequence models like LSTMs may be better suited to handling sharp traffic spikes.

**Region-Specific Analysis**

Different regions exhibit varied model performance, with some regions showing better alignment between actual and predicted values. This variability indicates that additional region-specific features (e.g., local weather patterns, events) may improve the model’s accuracy in certain areas.

**Feature Engineering and Hybrid Approaches**

Future iterations of the model could benefit from advanced feature engineering. For example, incorporating special event indicators, weather anomalies, or holiday-specific flags could help the model better predict extreme values. Additionally, exploring hybrid approaches (e.g., combining ExtraTree with LSTM models) might improve the overall performance and accuracy.

### Feature Importance for Every Target Variable

![Plots showing the feature importance in the models](asset/Feature Importance Plots - Grid.png)
<p style="text-align:center;">Feature Importance among every Target Variable</p>

**Key Findings:**

* **Temporal Factors:**
    * **Hour:** The time of day is the most influential factor, suggesting that traffic patterns exhibit strong diurnal variations.

    * **Hour_cos and Hour_sin:** These features capture cyclical patterns, likely indicating seasonal or weekly trends in visitor traffic.

* **Weather Conditions:**
    * **Temperature (°C):** Warmer temperatures correlate with higher visitor counts, suggesting that favorable weather conditions attract more people.
    * **Relative Humidity (%):** Humidity might also play a role, potentially influencing visitor comfort levels.
* **Special Events:**
    * **Feiertag Bayern:**  Holidays in Bavaria significantly impact traffic, indicating that special events or celebrations draw more visitors.
    * **Schulferien Bayern:** School holidays in Bavaria also influence traffic, likely due to increased leisure time for families.

* **Weather-Related Factors:**
    * **ZScore Daily Max Wind Speed (km/h):** Wind speed might affect visitor comfort and outdoor activities, influencing traffic patterns.

**Interpretation:**

* **Peak Times:** The importance of the "Hour" feature suggests that there are specific peak times during the day when visitor traffic is significantly higher.
* **Seasonal Variations:** The cyclical patterns captured by "Hour_cos" and "Hour_sin" might indicate seasonal variations in traffic, with higher numbers of visitors during certain months or periods of the year.
* **Weather Influence:** Favorable weather conditions (e.g., warm and sunny days) might attract more visitors, as evidenced by the importance of temperature and relative humidity.
* **Special Event Impact:** Holidays and school holidays seem to be significant factors, suggesting that special events or time off from school can drive increased traffic.

---

## Final Product

<div class="doc-outdated" markdown>
The screen recordings (GIFs) on this page show the dashboard as of 2024 (including the weather section and the former Admin Dashboard). They should be replaced with recordings of the current version.
</div>

![A GIF showing the overall dashboard](asset/overall-dashboard.gif)
<p style="text-align:center;">Dashboard Bavarian Forest</p>

### Visitor Dashboard

The visitor dashboard shows the predicted visitor traffic for the next seven days, per day and per hour, for the whole park and for each of the six regions. A real-time map shows the occupancy of the parking lots, the current visitor numbers at the animal enclosures Lusen and Falkenstein, and the visitor counts of three visitor centres. It also links to recreational activities and further visitor information. The dashboard is available in German and English. For details, see [User Frontends](approach/user-frontends.md#visitor-dashboard).

![A GIF showing the visitor dashboard](asset/bf-dashboard-visitor-predictions.gif)
<p style="text-align:center;">Visitor Dashboard</p>

---

### Data Hub

The Data Hub is the password-protected data access point for the park administration. It has two tabs:

- **Upload**: staff upload new visitor counts (Eco-Counter, special counts, or the legacy daily file) as CSV or Excel files. The files are validated, previewed, and saved to Azure. New Eco-Counter data is used the next time the model is trained.
- **Query & Download**: staff select one or more data categories (visitor counts, parking, weather, visitor centre counts) and, optionally, a time range. The data is combined into one table and can be downloaded as a ZIP file with a CSV and a data dictionary.

For details, see [User Frontends](approach/user-frontends.md#data-hub).

![A GIF showing the data access point in the dashboard](asset/upload-download.gif)
<p style="text-align:center;">Dashboard: Data Access Point for Admins</p>

![A GIF showing the data querying section in the data access point in the dashboard](asset/data-query.gif)
<p style="text-align:center;">Dashboard: Data Querying Section in the Data Access Point</p>

---

## Conclusion

The results of our predictive models demonstrated strong performance on unseen data, with MAPE(mean absolute percentage error) indicating reliable accuracy across the various target variables. The feature importance analysis provided valuable insights, highlighting the key factors driving visitor flows and parking occupancy, which are crucial for making data-driven decisions. 

The culmination of these efforts is an interactive dashboard that combines predictive analytics with real-time data. It brings together the visitor forecast, real-time parking availability, and current visitor counts at sensors and visitor centres, supporting both visitors and park administration. The Data Hub makes it possible to keep the underlying data up to date without changing the code.

