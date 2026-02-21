import streamlit as st
import pandas as pd
import json
import datetime
from snowflake.snowpark.context import get_active_session

# Configure the page layout
st.set_page_config(layout="wide", page_title="Daily Travel Summary")
st.title("🌍 Daily Travel Summary")

session = get_active_session()

# Defaulting to today's date in your current timezone
selected_date = st.date_input(
    "Select Travel Date", 
    value=datetime.date(2026, 2, 19)
)

if st.button("Fetch Summary"):
    date_str = selected_date.strftime("%Y-%m-%d")
    
    with st.spinner(f"Pulling data for {date_str}..."):
        query = f"CALL TRAVEL_DATA.GOLD.SP_GET_DAILY_TRAVEL_SUMMARY('{date_str}')"
        result = session.sql(query).collect()
        
        raw_json = result[0][0]
        data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
            
        # --- 1. TOP LEVEL METRICS ---
        col1, col2, col3 = st.columns(3)
        
        col1.metric("Total Spent", f"${data.get('TOTAL_SPENT', 0):.2f}")
        col2.metric("Total Steps", f"{data.get('TOTAL_STEPS', 0):,}")
        
        sleep_data = data.get('SLEEP_DATA', [])
        col3.metric("Sleep Score", sleep_data[0].get('OVERALL_SCORE', 'N/A') if sleep_data else "No Data")
            
        st.divider()

        # --- 2. LOGS & FLIGHTS ---
        col_logs, col_flights = st.columns(2)
        
        with col_logs:
            st.subheader("📝 Manual Logs")
            if manual_logs := data.get('MANUAL_LOGS', []):
                st.dataframe(pd.DataFrame(manual_logs), use_container_width=True, hide_index=True)
            else:
                st.info("No journal entries for this date.")
                
        with col_flights:
            st.subheader("✈️ Flights")
            if flights := data.get('FLIGHTS', []):
                st.dataframe(pd.DataFrame(flights), use_container_width=True, hide_index=True)
            else:
                st.info("No flights recorded.")

        st.divider()

        # --- 3. SPENDING ANALYSIS ---
        st.subheader("💳 Spending Breakdown")
        spending = data.get('SPENDING_ITEMS', [])
        
        if spending:
            df_spending = pd.DataFrame(spending)
            
            # Ensure AMOUNT is numeric for the chart
            df_spending['AMOUNT'] = pd.to_numeric(df_spending['AMOUNT'], errors='coerce')
            
            col_chart, col_data = st.columns([1, 1])
            
            with col_chart:
                # Group by Category (TYPE) and sum the amounts for the bar chart
                spend_summary = df_spending.groupby('TYPE')['AMOUNT'].sum().reset_index()
                # Set TYPE as index so Streamlit maps it to the X-axis cleanly
                st.bar_chart(spend_summary.set_index('TYPE'))
                
            with col_data:
                # Format for display
                df_display = df_spending.copy()
                df_display['AMOUNT'] = df_display['AMOUNT'].apply(lambda x: f"${x:.2f}")
                st.dataframe(df_display, use_container_width=True, hide_index=True)
        else:
            st.info("No spending recorded.")

        # --- 4. GEOSPATIAL TIMELINE ---
        st.subheader("📍 Movement & Timeline")
        timeline = data.get('TIMELINE_SEGMENTS', [])
        
        if timeline:
            df_timeline = pd.DataFrame(timeline)
            
            # Extract coordinates for the map
            map_points = []
            for seg in timeline:
                # Google separates coordinates depending on if you are stopped or moving
                if seg.get('SEGMENT_TYPE') == 'VISIT' and seg.get('VISIT_LAT'):
                    map_points.append({'lat': seg['VISIT_LAT'], 'lon': seg['VISIT_LON']})
                elif seg.get('SEGMENT_TYPE') == 'ACTIVITY':
                    if seg.get('START_LAT'):
                        map_points.append({'lat': seg['START_LAT'], 'lon': seg['START_LON']})
                    if seg.get('END_LAT'):
                        map_points.append({'lat': seg['END_LAT'], 'lon': seg['END_LON']})
            
            # Display Map if we found valid GPS points
            if map_points:
                st.map(pd.DataFrame(map_points))
            
            # Clean up the timeline table to hide all the raw coordinate columns
            display_cols = ['START_TIME', 'END_TIME', 'SEGMENT_TYPE', 'ACTIVITY_TYPE', 'DISTANCE_METERS']
            df_timeline_display = df_timeline[[c for c in display_cols if c in df_timeline.columns]]
            st.dataframe(df_timeline_display, use_container_width=True, hide_index=True)
        else:
            st.info("No timeline data for this date.")