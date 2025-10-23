import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import altair as alt

# Load data
@st.cache_data(ttl=300)
def load_data():
    # Load the latest snapshot per station with only the columns used in the app
    session = get_active_session()
    query = """
    SELECT
        STATION_ID,
        STATION_NAME,
        REGION_ID,
        STATION_TYPE,
        CAPACITY,
        IS_RENTING,
        IS_RETURNING,
        NUM_BIKES_AVAILABLE,
        NUM_DOCKS_AVAILABLE,
        NUM_EBIKES_AVAILABLE,
        LATITUDE,
        LONGITUDE,
        GOLD_TIMESTAMP,
        LAST_REPORTED_TS

    FROM GOLD.CITIBIKESNYC.CITIBIKESNYC_LATEST_STATION_DATA
    """
    return session.sql(query).to_pandas()

# App setup
st.title("Citi Bike NYC Stations")

# Load data once per session (cached above)
df = load_data()

# Sidebar filters setup
regions = sorted(df["REGION_ID"].dropna().unique().tolist()) if "REGION_ID" in df.columns else []
types = sorted(df["STATION_TYPE"].dropna().unique().tolist()) if "STATION_TYPE" in df.columns else []

st.sidebar.header("Filters")
sel_regions = st.sidebar.multiselect("Region", regions) if regions else []
sel_type = st.sidebar.selectbox("Station type", ["All"] + types) if types else "All"

# Capacity range slider (handles missing or non-numeric values)
cap_series = pd.to_numeric(df.get("CAPACITY", pd.Series(dtype="float")), errors="coerce")
cap_min = int(cap_series.min()) if not cap_series.dropna().empty else 0
cap_max = int(cap_series.max()) if not cap_series.dropna().empty else 0
cap_rng = st.sidebar.slider("Capacity", min_value=cap_min, max_value=cap_max, value=(cap_min, cap_max)) if cap_max > 0 else (0, 0)

# Renting/returning flags and text search
renting_opt = st.sidebar.selectbox("Is renting", ["All", "Yes", "No"]) if "IS_RENTING" in df.columns else "All"
returning_opt = st.sidebar.selectbox("Is returning", ["All", "Yes", "No"]) if "IS_RETURNING" in df.columns else "All"
search = st.sidebar.text_input("Search station")

# Apply filters
f = df.copy()
if regions and sel_regions:
    f = f[f["REGION_ID"].isin(sel_regions)]
if sel_type != "All" and "STATION_TYPE" in f.columns:
    f = f[f["STATION_TYPE"] == sel_type]
if "CAPACITY" in f.columns and cap_max > 0:
    cap_vals = pd.to_numeric(f["CAPACITY"], errors="coerce")
    f = f[(cap_vals >= cap_rng[0]) & (cap_vals <= cap_rng[1])]
if renting_opt != "All" and "IS_RENTING" in f.columns:
    f = f[f["IS_RENTING"].astype("Int64") == (1 if renting_opt == "Yes" else 0)]
if returning_opt != "All" and "IS_RETURNING" in f.columns:
    f = f[f["IS_RETURNING"].astype("Int64") == (1 if returning_opt == "Yes" else 0)]
if search and "STATION_NAME" in f.columns:
    f = f[f["STATION_NAME"].str.contains(search, case=False, na=False)]

col1, col2, col3, col4 = st.columns(4)
# KPI cards
col1.metric("Stations", int(len(f)))
col2.metric("Bikes available", int(pd.to_numeric(f.get("NUM_BIKES_AVAILABLE", pd.Series(dtype="float")), errors="coerce").fillna(0).sum()))
col3.metric("Docks available", int(pd.to_numeric(f.get("NUM_DOCKS_AVAILABLE", pd.Series(dtype="float")), errors="coerce").fillna(0).sum()))
col4.metric("E-bikes available", int(pd.to_numeric(f.get("NUM_EBIKES_AVAILABLE", pd.Series(dtype="float")), errors="coerce").fillna(0).sum()))

if {"LATITUDE", "LONGITUDE"}.issubset(f.columns):
    # Map of stations (lat/lon)
    m = f[["LATITUDE", "LONGITUDE", "STATION_NAME"]].dropna()
    m = m.rename(columns={"LATITUDE": "lat", "LONGITUDE": "lon"})
    st.subheader("Map")
    st.map(data=m, use_container_width=True, size='CAPACITY')

st.subheader("Top stations by bikes available")
top_n = st.slider("Top N", 5, 50, 15)
if "STATION_NAME" in f.columns and "NUM_BIKES_AVAILABLE" in f.columns:
    # Altair chart to control sort (descending by bikes available) and orientation
    top = (
        f[["STATION_NAME", "NUM_BIKES_AVAILABLE"]]
        .assign(NUM_BIKES_AVAILABLE=pd.to_numeric(f["NUM_BIKES_AVAILABLE"], errors="coerce").fillna(0))
        .sort_values("NUM_BIKES_AVAILABLE", ascending=False)
        .head(top_n)
    )
    chart = (
        alt.Chart(top)
        .mark_bar()
        .encode(
            y=alt.Y("STATION_NAME:N", sort="-x", title="Station"),
            x=alt.X("NUM_BIKES_AVAILABLE:Q", title="Bikes available")
        )
    )
    st.altair_chart(chart, use_container_width=True)

st.subheader("Data")
# Display filtered data
st.dataframe(f, use_container_width=True)

# Last updated caption from available timestamps

last_updated = pd.to_datetime(f["GOLD_TIMESTAMP"], errors="coerce").max()
if pd.notnull(last_updated):
    st.caption(f"Last updated: {last_updated}")
