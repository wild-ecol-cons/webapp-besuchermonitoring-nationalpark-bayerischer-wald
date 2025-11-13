import streamlit as st
import awswrangler as wr
import pandas as pd
import plotly.express as px
from src.streamlit_app.pages_in_dashboard.visitors.language_selection_menu import TRANSLATIONS
from src.config import regions


@st.fragment
def visitor_prediction_graph(inference_predictions):
    """
    Get the visitor counts section with the highest occupancy rate.

    Args:
        None
    
    Returns:
        None
    """
    st.markdown(f"## {TRANSLATIONS[st.session_state.selected_language]['visitor_counts_forecasted']}")
    
    # do a dropdown for the all_preds
    regions_to_select = list(regions.keys())
    selected_region = st.selectbox(
        TRANSLATIONS[st.session_state.selected_language]['select_region'],
        regions_to_select,
        width=400
    )

    if selected_region:

        predictions_per_region = regions[selected_region]
        further_columns_to_show = ["Time", "day_date"]

        # Filter the DataFrame based on the selected region
        selected_region_predictions = inference_predictions[predictions_per_region + further_columns_to_show]

        # Get unique values for the day and date list
        days_list = selected_region_predictions['day_date'].unique()

        # Add a note that this is forecasted data
        st.markdown(f":green[*{TRANSLATIONS[st.session_state.selected_language]['forecasted_visitor_data']}*].")

        # Create a layout for the radio button and chart
        col1, _ = st.columns([1, 3])

        with col1:
            # Get radio button for selecting the day
            day_selected = st.radio(
                label=TRANSLATIONS[st.session_state.selected_language]['select_day'], options=days_list, index=0
            )

        # Extract the selected day for filtering (using date)
        day_df = selected_region_predictions[selected_region_predictions['day_date'] == day_selected]

        # Plot an interactive bar chart for relative traffic
        fig1 = px.bar(
            day_df,
            x='Time',  
            y=predictions_per_region,
            barmode='group',
            labels={f'traffic_{selected_region}': '', 'Time': 'Hour of Day'},
            title=f"{TRANSLATIONS[st.session_state.selected_language]['visitor_foot_traffic_for_day']} - {day_selected}",
            color_discrete_map={'red': 'red', 'blue': 'blue', 'green': 'green'}
        )

        # Customize hover text for relative traffic
        fig1.update_traces(
            hovertemplate=(
                'Traffic: %{y}<br>'  # Display the traffic value
                'Hour: %{x|%H:%M}<br>'  # Display the hour in HH:MM format
            )
        )

        weekly_max_value_per_region = selected_region_predictions[predictions_per_region].max().max()

        # Update layout for relative traffic chart
        fig1.update_yaxes(range=[0, weekly_max_value_per_region])  # Set y-axis to range from 0 to the max traffic value of the forecasted week for a region
        fig1.update_xaxes(showticklabels=True)  # Keep the x-axis tick labels visible

        fig1.update_layout(
            xaxis_title=None,  # Hide the x-axis title
            yaxis_title=None,  # Hide the y-axis title
            template='plotly_dark',
            legend_title_text=TRANSLATIONS[st.session_state.selected_language]['visitor_foot_traffic'],
            legend=dict(
                itemsizing='constant',
                traceorder="normal",
                font=dict(size=12),
                orientation="h",
                yanchor="top",
                y=-0.3,  # Position the legend below the chart
                xanchor="center",
                x=0.5  # Center the legend horizontally
            ),
            xaxis=dict(
                tickformat='%H:%M'
            )
        )

        # Display the interactive bar chart for relative traffic below the radio button
        st.plotly_chart(fig1)
