import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor

# Page configuration
st.set_page_config(
    page_title="Real-Time Ride-Sharing Dashboard", page_icon="🚗", layout="wide"
)
st.title("🚗 Real-Time Ride-Sharing Dashboard")


def get_connection():
    """Get PostgreSQL connection"""
    return psycopg2.connect(
        dbname="kafka_db",
        user="kafka_user",
        password="kafka_password",
        host="localhost",
        port="5433",
    )


def load_data(status_filter=None, city_filter=None, limit=500):
    """Load trip data from database with filters"""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    base_query = "SELECT * FROM trips"
    conditions = []
    params = []

    if status_filter and status_filter != "All":
        conditions.append("status = %s")
        params.append(status_filter)

    if city_filter and city_filter != "All":
        conditions.append("city = %s")
        params.append(city_filter)

    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)

    base_query += " ORDER BY created_at DESC LIMIT %s"
    params.append(limit)

    try:
        cur.execute(base_query, params)
        data = cur.fetchall()
        df = pd.DataFrame(data)
        cur.close()
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        cur.close()
        conn.close()
        return pd.DataFrame()


# Sidebar controls
st.sidebar.header("⚙️ Dashboard Controls")

status_options = ["All", "Completed", "Cancelled", "In Progress"]
selected_status = st.sidebar.selectbox("Filter by Status", status_options)

city_options = [
    "All",
    "New York",
    "Los Angeles",
    "Chicago",
    "Houston",
    "Phoenix",
    "San Francisco",
    "Boston",
    "Seattle",
]
selected_city = st.sidebar.selectbox("Filter by City", city_options)

update_interval = st.sidebar.slider(
    "Auto-refresh Interval (seconds)", min_value=2, max_value=20, value=5
)

limit_records = st.sidebar.number_input(
    "Number of records to load", min_value=50, max_value=2000, value=500, step=50
)

if st.sidebar.button("🔄 Refresh Now"):
    st.rerun()

# Main dashboard loop
placeholder = st.empty()

while True:
    df_trips = load_data(selected_status, selected_city, limit=int(limit_records))

    with placeholder.container():
        if df_trips.empty:
            st.warning("⏳ No records found. Waiting for data...")
            st.info("Make sure the producer and consumer are running!")
            time.sleep(update_interval)
            continue

        # Convert timestamp
        if "request_time" in df_trips.columns:
            df_trips["request_time"] = pd.to_datetime(df_trips["request_time"])

        # Calculate KPIs
        total_trips = len(df_trips)
        total_revenue = df_trips["total_fare"].sum()
        average_fare = df_trips["total_fare"].mean()
        average_distance = df_trips["distance_km"].mean()

        completed = len(df_trips[df_trips["status"] == "Completed"])
        cancelled = len(df_trips[df_trips["status"] == "Cancelled"])
        in_progress = len(df_trips[df_trips["status"] == "In Progress"])
        completion_rate = (completed / total_trips * 100) if total_trips > 0 else 0.0

        # Count anomalies
        anomalies = (
            df_trips["is_anomaly"].sum() if "is_anomaly" in df_trips.columns else 0
        )

        # Header
        filter_info = f"Filter: {selected_status}"
        if selected_city != "All":
            filter_info += f" | City: {selected_city}"

        st.subheader(f"📊 Displaying {total_trips:,} trips ({filter_info})")

        # KPI Metrics
        col1, col2, col3, col4, col5, col6 = st.columns(6)

        col1.metric("🚕 Total Trips", f"{total_trips:,}")
        col2.metric("💰 Total Revenue", f"${total_revenue:,.2f}")
        col3.metric("💵 Avg Fare", f"${average_fare:.2f}")
        col4.metric("📏 Avg Distance", f"{average_distance:.1f} km")
        col5.metric("✅ Completion Rate", f"{completion_rate:.1f}%")
        col6.metric("⚠️ Anomalies", f"{anomalies}")

        # Status breakdown
        st.markdown("### 📈 Trip Status Breakdown")
        status_col1, status_col2, status_col3 = st.columns(3)
        status_col1.metric("Completed", completed)
        status_col2.metric("In Progress", in_progress)
        status_col3.metric("Cancelled", cancelled)

        # Charts
        st.markdown("### 📊 Analytics")

        chart_col1, chart_col2 = st.columns(2)

        with chart_col1:
            # Revenue by City
            city_revenue = df_trips.groupby("city")["total_fare"].sum().reset_index()
            city_revenue = city_revenue.sort_values("total_fare", ascending=False)

            fig_city_revenue = px.bar(
                city_revenue,
                x="city",
                y="total_fare",
                title="💰 Revenue by City",
                labels={"total_fare": "Total Revenue ($)", "city": "City"},
                color="total_fare",
                color_continuous_scale="Viridis",
            )
            fig_city_revenue.update_layout(showlegend=False)
            st.plotly_chart(fig_city_revenue, use_container_width=True)

        with chart_col2:
            # Trips by City
            city_counts = df_trips["city"].value_counts().reset_index()
            city_counts.columns = ["city", "count"]

            fig_pie = px.pie(
                city_counts,
                values="count",
                names="city",
                title="🚗 Trip Distribution by City",
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        chart_col3, chart_col4 = st.columns(2)

        with chart_col3:
            # Surge pricing distribution
            fig_surge = px.histogram(
                df_trips,
                x="surge_multiplier",
                title="📈 Surge Pricing Distribution",
                labels={
                    "surge_multiplier": "Surge Multiplier",
                    "count": "Number of Trips",
                },
                nbins=20,
            )
            st.plotly_chart(fig_surge, use_container_width=True)

        with chart_col4:
            # Distance vs Fare scatter plot
            completed_trips = df_trips[df_trips["status"] == "Completed"]
            if not completed_trips.empty:
                fig_scatter = px.scatter(
                    completed_trips.head(200),
                    x="distance_km",
                    y="total_fare",
                    color="surge_multiplier",
                    title="📏 Distance vs Fare (Completed Trips)",
                    labels={
                        "distance_km": "Distance (km)",
                        "total_fare": "Total Fare ($)",
                        "surge_multiplier": "Surge",
                    },
                    color_continuous_scale="Reds",
                )
                st.plotly_chart(fig_scatter, use_container_width=True)

        # Time series
        if "request_time" in df_trips.columns and not df_trips.empty:
            st.markdown("### ⏰ Trips Over Time")

            df_trips_sorted = df_trips.sort_values("request_time")
            df_trips_sorted["hour"] = df_trips_sorted["request_time"].dt.floor("10min")

            time_series = (
                df_trips_sorted.groupby("hour").size().reset_index(name="count")
            )

            fig_time = px.line(
                time_series,
                x="hour",
                y="count",
                title="📊 Trip Volume Over Time (10-minute intervals)",
                labels={"hour": "Time", "count": "Number of Trips"},
            )
            fig_time.update_traces(line_color="#1f77b4", line_width=2)
            st.plotly_chart(fig_time, use_container_width=True)

        # Anomaly section
        if anomalies > 0:
            st.markdown("### ⚠️ Anomalous Trips Detected")
            anomaly_trips = df_trips[df_trips["is_anomaly"] == True]
            st.dataframe(
                anomaly_trips[
                    [
                        "trip_id",
                        "city",
                        "distance_km",
                        "duration_minutes",
                        "total_fare",
                        "surge_multiplier",
                        "status",
                    ]
                ].head(10),
                use_container_width=True,
            )

        # Duration Prediction Demo
        st.markdown("### 🔮 Trip Duration Predictor (ML Model)")

        pred_col1, pred_col2, pred_col3 = st.columns(3)

        with pred_col1:
            test_distance = st.number_input(
                "Distance (km)", min_value=0.5, max_value=50.0, value=5.0, step=0.5
            )

        with pred_col2:
            test_surge = st.number_input(
                "Surge Multiplier", min_value=1.0, max_value=3.0, value=1.5, step=0.1
            )

        with pred_col3:
            if st.button("🔮 Predict Duration"):
                try:
                    import joblib

                    model = joblib.load("duration_model.pkl")
                    predicted = model.predict([[test_distance, test_surge]])[0]
                    st.success(f"**Predicted Duration: {predicted:.1f} minutes**")

                    # Calculate expected fare
                    expected_fare = 3.0 + (test_distance * 1.5) + (predicted * 0.3)
                    expected_fare *= test_surge
                    st.info(f"Expected Fare: ${expected_fare:.2f}")
                except:
                    st.error("⚠️ Model not trained yet! Run: python duration_model.py")

        # Raw data table
        st.markdown("### 📋 Recent Trips (Top 15)")
        display_cols = [
            "trip_id",
            "city",
            "status",
            "distance_km",
            "duration_minutes",
            "total_fare",
            "surge_multiplier",
        ]
        if "request_time" in df_trips.columns:
            display_cols.append("request_time")

        st.dataframe(df_trips[display_cols].head(15), use_container_width=True)

        # Footer
        st.markdown("---")
        st.caption(
            f"🕐 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} • "
            f"Auto-refresh: {update_interval}s • Database: kafka_db"
        )

    time.sleep(update_interval)
