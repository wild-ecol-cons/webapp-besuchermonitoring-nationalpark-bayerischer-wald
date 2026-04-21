# API Reference

<!-- Code in General -->

:::src.utils

<!-- Streamlit Dashboard -->

:::Dashboard
:::pages.Admin_🔓
:::pages.Data_Hub_☁️

<!-- Streamlit: Sourcing & Preprocessing & General -->

:::src.streamlit_app.source_data
:::src.streamlit_app.pre_processing.process_forecast_weather_data
:::src.streamlit_app.pre_processing.process_real_time_parking_data
:::src.streamlit_app.pages_in_dashboard.password

<!-- Streamlit: Admin Page -->

:::src.streamlit_app.pages_in_dashboard.admin.parking
:::src.streamlit_app.pages_in_dashboard.admin.visitor_count

<!-- Streamlit: Data Hub Page -->

:::src.streamlit_app.pages_in_dashboard.data_hub.query_and_download_data
:::src.streamlit_app.pages_in_dashboard.data_hub.upload_data

<!-- Streamlit: Visitors Page -->

:::src.streamlit_app.pages_in_dashboard.visitors.language_selection_menu
:::src.streamlit_app.pages_in_dashboard.visitors.other_information
:::src.streamlit_app.pages_in_dashboard.visitors.page_layout_config
:::src.streamlit_app.pages_in_dashboard.visitors.parking
:::src.streamlit_app.pages_in_dashboard.visitors.recreational_activities
:::src.streamlit_app.pages_in_dashboard.visitors.visitor_count
:::src.streamlit_app.pages_in_dashboard.visitors.weather

<!-- Prediction Pipeline -->

<!-- Sourcing Data -->

:::src.prediction_pipeline.sourcing_data.source_historic_parking_data
:::src.prediction_pipeline.sourcing_data.source_historic_visitor_count
:::src.prediction_pipeline.sourcing_data.source_real_time_parking_data
:::src.prediction_pipeline.sourcing_data.source_temporal_features
:::src.prediction_pipeline.sourcing_data.source_weather

<!-- Preprocessing --> 

:::src.prediction_pipeline.pre_processing.features_zscoreweather_distanceholidays
:::src.prediction_pipeline.pre_processing.impute_missing_parking_data
:::src.prediction_pipeline.pre_processing.join_visitorcounts_weather_temporaldata
:::src.prediction_pipeline.pre_processing.preprocess_historic_visitor_count_data
:::src.prediction_pipeline.pre_processing.preprocess_temporal_features
:::src.prediction_pipeline.pre_processing.preprocess_weather_data

<!-- Modeling --> 

:::src.prediction_pipeline.modeling.create_inference_dfs
:::src.prediction_pipeline.modeling.preprocess_inference_features
:::src.prediction_pipeline.modeling.run_inference
:::src.prediction_pipeline.modeling.source_and_feature_selection
:::src.prediction_pipeline.modeling.train_lstm
:::src.prediction_pipeline.modeling.train_regressor