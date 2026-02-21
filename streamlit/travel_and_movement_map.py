import streamlit as st
from snowflake.snowpark.context import get_active_session
import pydeck as pdk
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz

# Set up the app title and page layout
st.set_page_config(layout="wide")
st.title("My Travel & Movement Map 🗺️✈️")

# Get the current Snowflake session for Snowflake tables
session = get_active_session()

# Set default dates localized to JST (Japan Standard Time)
jst = pytz.timezone('Asia/Tokyo')
today = datetime.now(jst).date()
tomorrow = today + timedelta(days=1)

# Helper function to calculate bearing (angle) for movement arrows
def calculate_bearing(lat1, lon1, lat2, lon2):
    try:
        if pd.isna(lat1) or pd.isna(lon1) or pd.isna(lat2) or pd.isna(lon2) or (lat1 == lat2 and lon1 == lon2):
            return 0.0
        
        # Convert degrees to radians
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        
        dlon = lon2 - lon1
        x = np.sin(dlon) * np.cos(lat2)
        y = np.cos(lat1) * np.sin(lat2) - (np.sin(lat1) * np.cos(lat2) * np.cos(dlon))
        
        # Calculate bearing in degrees
        initial_bearing = np.degrees(np.arctan2(x, y))
        return float((initial_bearing + 360) % 360)
    except:
        return 0.0

# Helper function to assign consistent colors to travel activities
def get_activity_color(activity):
    activity = str(activity).upper()
    if activity == 'WALKING':
        return [50, 205, 50, 200]  # Lime Green
    elif activity in ['IN_PASSENGER_VEHICLE', 'DRIVING', 'MOTORCYCLING']:
        return [255, 140, 0, 200]  # Dark Orange
    elif activity in ['IN_TRAIN', 'IN_SUBWAY', 'IN_TRAM', 'IN_BUS']:
        return [148, 0, 211, 200]  # Purple
    elif activity in ['CYCLING']:
        return [255, 20, 147, 200] # Deep Pink
    else:
        return [30, 144, 255, 200] # Default Dodger Blue

# Sidebar for date range and map filters
with st.sidebar:
    st.header("Dashboard Controls")
    selected_dates = st.date_input(
        "Select Travel Date Range:",
        value=(today, tomorrow)
    )
    
    st.divider()
    st.subheader("Map Filters")
    show_visits = st.checkbox("Show Static Visits (Red Dots)", value=True)
    show_ground = st.checkbox("Show Ground Movement (Lines)", value=True)
    show_arrows = st.checkbox("Show Directional Arrows", value=True)
    show_flights = st.checkbox("Show Flight Arcs", value=True)
    
    st.divider()
    st.markdown("""
    **Color Legend:**
    * 🟢 Walking
    * 🟠 Driving / Car
    * 🟣 Train / Bus / Subway
    * 🔵 Other / Unknown
    """)

# Main logic triggers when a valid date range is selected
if len(selected_dates) == 2:
    start_date, end_date = selected_dates
    num_days = (end_date - start_date).days + 1
    
    # -------------------------------------------------------------------------
    # 1. DATA RETRIEVAL
    # -------------------------------------------------------------------------
    
    visits_df = pd.DataFrame()
    if show_visits:
        visits_query = f"""
            SELECT VISIT_LATITUDE AS LAT, VISIT_LONGITUDE AS LON, START_TIME AS VISIT_TIME, PLACE_ID
            FROM TRAVEL_DATA.SILVER.GOOGLE_TIMELINE
            WHERE SEGMENT_TYPE = 'VISIT' AND TO_DATE(START_TIME) BETWEEN '{start_date}' AND '{end_date}'
              AND VISIT_LATITUDE IS NOT NULL AND VISIT_LONGITUDE IS NOT NULL
        """
        visits_df = session.sql(visits_query).to_pandas()
        if not visits_df.empty:
            visits_df['TOOLTIP_TEXT'] = "<b>Type:</b> Visit <br/><b>Time:</b> " + visits_df['VISIT_TIME'].astype(str)

    ground_df = pd.DataFrame()
    flight_df = pd.DataFrame()
    move_df = pd.DataFrame()
    if show_ground or show_flights:
        move_query = f"""
            SELECT ACTIVITY_START_LATITUDE AS START_LAT, ACTIVITY_START_LONGITUDE AS START_LON, 
                   ACTIVITY_END_LATITUDE AS END_LAT, ACTIVITY_END_LONGITUDE AS END_LON, 
                   ACTIVITY_TYPE, DISTANCE_METERS, START_TIME, END_TIME
            FROM TRAVEL_DATA.SILVER.GOOGLE_TIMELINE
            WHERE SEGMENT_TYPE = 'ACTIVITY' AND TO_DATE(START_TIME) BETWEEN '{start_date}' AND '{end_date}'
              AND ACTIVITY_START_LATITUDE IS NOT NULL AND ACTIVITY_END_LATITUDE IS NOT NULL
        """
        move_df = session.sql(move_query).to_pandas()
        if not move_df.empty:
            move_df['TOOLTIP_TEXT'] = "<b>Type:</b> " + move_df['ACTIVITY_TYPE'].fillna('Unknown') + "<br/><b>Distance:</b> " + move_df['DISTANCE_METERS'].astype(str) + "m"
            move_df['COLOR'] = move_df['ACTIVITY_TYPE'].apply(get_activity_color)
            
            if show_ground:
                ground_df = move_df[move_df['ACTIVITY_TYPE'] != 'FLYING'].copy()
                if not ground_df.empty:
                    ground_df['BEARING'] = ground_df.apply(lambda r: calculate_bearing(r.START_LAT, r.START_LON, r.END_LAT, r.END_LON), axis=1)
            
            if show_flights:
                flight_df = move_df[move_df['ACTIVITY_TYPE'] == 'FLYING']

    logs_df = session.sql(f"SELECT DATE, CITY, COUNTY, DESCRIPTION, COMMENTS, HOTEL FROM TRAVEL_DATA.SILVER.MANUAL_LOGS WHERE TO_DATE(DATE) BETWEEN '{start_date}' AND '{end_date}' ORDER BY DATE ASC").to_pandas()
    tx_df = session.sql(f"SELECT DATE, TYPE, NAME, AMOUNT FROM TRAVEL_DATA.SILVER.ALL_SPENDING WHERE TO_DATE(DATE) BETWEEN '{start_date}' AND '{end_date}' ORDER BY TYPE ASC, AMOUNT DESC").to_pandas()
    flight_logs = session.sql(f'SELECT TO_DATE(DATE) AS DATE, FLIGHT_NUMBER, AIRLINE, "FROM", "TO", AIRCRAFT, DURATION FROM TRAVEL_DATA.SILVER.FLIGHT_LOGS WHERE TO_DATE(DATE) BETWEEN \'{start_date}\' AND \'{end_date}\'').to_pandas()
    sleep_df = session.sql(f"SELECT TO_DATE(LEFT(TIMESTAMP, 10)) AS DATE, OVERALL_SCORE, DEEP_SLEEP_IN_MINUTES, RESTING_HEART_RATE FROM TRAVEL_DATA.SILVER.SLEEP_LOG WHERE TO_DATE(LEFT(TIMESTAMP, 10)) BETWEEN '{start_date}' AND '{end_date}' ORDER BY DATE ASC").to_pandas()
    steps_df = session.sql(f"SELECT DATE, SUM(STEPS) AS TOTAL_STEPS FROM TRAVEL_DATA.SILVER.HOURLY_STEP_COUNT WHERE TO_DATE(DATE) BETWEEN '{start_date}' AND '{end_date}' GROUP BY DATE ORDER BY DATE ASC").to_pandas()

    # -------------------------------------------------------------------------
    # 2. MAP RENDERING
    # -------------------------------------------------------------------------
    
    all_coords = []
    if not visits_df.empty: all_coords.extend(visits_df[['LON', 'LAT']].values.tolist())
    if not ground_df.empty:
        all_coords.extend(ground_df[['START_LON', 'START_LAT']].values.tolist())
        all_coords.extend(ground_df[['END_LON', 'END_LAT']].values.tolist())
            
    if all_coords:
        view_state = pdk.data_utils.compute_view(pd.DataFrame(all_coords, columns=['LON', 'LAT']))
        view_state.pitch = 0
        
        layers = []
        if not visits_df.empty:
            layers.append(pdk.Layer("ScatterplotLayer", data=visits_df, get_position='[LON, LAT]', get_radius=40, get_fill_color='[200, 30, 0, 160]', pickable=True))
                
        if not ground_df.empty:
            layers.append(pdk.Layer("LineLayer", data=ground_df, get_source_position="[START_LON, START_LAT]", get_target_position="[END_LON, END_LAT]", get_color="COLOR", get_width=4, pickable=True))
            if show_arrows:
                layers.append(pdk.Layer("TextLayer", data=ground_df, get_position="[END_LON, END_LAT]", get_text="'▶'", get_angle="-BEARING + 90", get_color="COLOR", get_size=20, size_units="'meters'", size_min_pixels=14, get_alignment_baseline="'center'", pickable=False))
            
        if not flight_df.empty:
            layers.append(pdk.Layer("ArcLayer", data=flight_df, get_source_position="[START_LON, START_LAT]", get_target_position="[END_LON, END_LAT]", get_source_color="[0, 255, 0, 200]", get_target_color="[255, 0, 0, 200]", get_width=3, pickable=True))

        st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=view_state, tooltip={"html": "{TOOLTIP_TEXT}", "style": {"backgroundColor": "steelblue", "color": "white"}}, map_style='light'))
    else:
        st.info("No timeline map data found for these dates.")

    # -------------------------------------------------------------------------
    # 3. ANALYTICS & ITINERARY DASHBOARD
    # -------------------------------------------------------------------------
    st.divider()
    col_logs, col_metrics = st.columns((1, 1))
    
    with col_logs:
        st.subheader("📝 Daily Itinerary")
        if not logs_df.empty:
            for _, row in logs_df.iterrows():
                dt = str(row['DATE'])
                day_flights = flight_logs[flight_logs['DATE'].astype(str) == dt]
                icon = " ✈️" if not day_flights.empty else ""
                with st.expander(f"📍 {dt} - {row['CITY']}, {row['COUNTY']}{icon}"):
                    if pd.notna(row['DESCRIPTION']) and str(row['DESCRIPTION']).lower() != 'none': st.write(f"**Activity:** {row['DESCRIPTION']}")
                    if pd.notna(row['COMMENTS']) and str(row['COMMENTS']).lower() != 'none': st.write(f"**Notes:** {row['COMMENTS']}")
                    if pd.notna(row['HOTEL']) and str(row['HOTEL']).lower() != 'none': st.write(f"**Hotel:** {row['HOTEL']}")
                    
                    if not day_flights.empty:
                        st.divider()
                        st.write("✈️ **Flight Details:**")
                        for _, f in day_flights.iterrows():
                            st.write(f"- **{f['AIRLINE']} ({f['FLIGHT_NUMBER']})**: {f['FROM']} ➡️ {f['TO']} ({f['AIRCRAFT']})")

                    st.divider()
                    st.write("**Transactions:**")
                    day_tx = tx_df[tx_df['DATE'].astype(str) == dt]
                    for t_type in day_tx['TYPE'].dropna().unique():
                        st.markdown(f"**_{t_type}_**")
                        for _, t in day_tx[day_tx['TYPE'] == t_type].iterrows():
                            st.write(f"- {t['NAME']}: **${t['AMOUNT']:,.2f}**")
        else:
            st.write("No manual logs found for this period.")

    with col_metrics:
        t_spend, t_move, t_sleep = st.tabs(["💸 Spending", "🚶‍♂️ Movement", "💤 Sleep & Health"])
        
        with t_spend:
            if not tx_df.empty:
                st.bar_chart(tx_df.groupby(['DATE', 'TYPE'])['AMOUNT'].sum().unstack().fillna(0))
                
                total_spent = tx_df['AMOUNT'].sum()
                avg_daily = total_spent / num_days
                
                s_col1, s_col2 = st.columns(2)
                s_col1.metric("Total Spent", f"${total_spent:,.2f}")
                s_col2.metric("Daily Avg Spent", f"${avg_daily:,.2f}")
                
                st.divider()
                st.write("**Top 5 Most Expensive Transactions**")
                top_5 = tx_df.nlargest(5, 'AMOUNT')[['NAME', 'TYPE', 'AMOUNT']]
                st.dataframe(top_5, hide_index=True, use_container_width=True)
            else:
                st.write("No spending data found.")

        with t_move:
            if not steps_df.empty:
                st.write("**Daily Step Count**")
                st.line_chart(steps_df.set_index('DATE')['TOTAL_STEPS'])
                st.metric("Avg Daily Steps", f"{int(steps_df['TOTAL_STEPS'].mean()):,}")
            if not move_df.empty:
                st.divider()
                st.write("**Distance by Mode (km)**")
                dist = move_df.groupby('ACTIVITY_TYPE')['DISTANCE_METERS'].sum() / 1000
                st.bar_chart(dist)

        with t_sleep:
            if not sleep_df.empty:
                st.subheader("Recovery Trends")
                
                # Combined Chart Logic
                health_chart_data = sleep_df.set_index('DATE')[['OVERALL_SCORE', 'RESTING_HEART_RATE']]
                health_chart_data.columns = ['Sleep Score', 'Resting HR (BPM)']
                
                # Line chart with distinct colors and legend
                st.line_chart(health_chart_data, color=["#1f77b4", "#ff7f0e"]) 
                
                st.markdown("""
                **Legend:**
                * 🔵 **Blue**: Sleep Score (0-100)
                * 🟠 **Orange**: Resting Heart Rate (BPM)
                """)
                
                st.divider()
                s1, s2, s3 = st.columns(3)
                s1.metric("Avg Score", f"{int(sleep_df['OVERALL_SCORE'].mean())}/100")
                s2.metric("Avg Deep Sleep", f"{int(sleep_df['DEEP_SLEEP_IN_MINUTES'].mean())}m")
                s3.metric("Avg RHR", f"{int(sleep_df['RESTING_HEART_RATE'].mean())} bpm")
            else:
                st.write("No sleep data logged for these dates.")

else:
    st.info("Select a start and end date in the sidebar to visualize your travel data.")