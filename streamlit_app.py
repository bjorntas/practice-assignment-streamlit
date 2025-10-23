import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import altair as alt

@st.cache_data(ttl=300)
def load_data():
    session = get_active_session()
    query = """
    SELECT *
    FROM (
      SELECT
        t.*,
        ROW_NUMBER() OVER (
          PARTITION BY t.STATION_ID
          ORDER BY COALESCE(t.RAW_TIMESTAMP, t.LAST_REPORTED_TS) DESC
        ) AS RN
      FROM GOLD.CITIBIKESNYC.CITIBIKESNYC_STATION_DATA AS t
    )
    WHERE RN = 1
    """
    return session.sql(query).to_pandas()

st.title("Citi Bike NYC Stations")

df = load_data()

regions = sorted([r for r in df["REGION_ID"].dropna().unique().tolist()]) if "REGION_ID" in df.columns else []
types = sorted([t for t in df["STATION_TYPE"].dropna().unique().tolist()]) if "STATION_TYPE" in df.columns else []

st.sidebar.header("Filters")
sel_regions = st.sidebar.multiselect("Region", regions, default=regions) if regions else []
sel_type = st.sidebar.selectbox("Station type", ["All"] + types) if types else "All"
cap_min = int(pd.to_numeric(df["CAPACITY"], errors="coerce").min()) if "CAPACITY" in df.columns else 0
cap_max = int(pd.to_numeric(df["CAPACITY"], errors="coerce").max()) if "CAPACITY" in df.columns else 0
cap_rng = st.sidebar.slider("Capacity", min_value=cap_min, max_value=cap_max, value=(cap_min, cap_max)) if cap_max > 0 else (0, 0)
renting_opt = st.sidebar.selectbox("Is renting", ["All", "Yes", "No"]) if "IS_RENTING" in df.columns else "All"
returning_opt = st.sidebar.selectbox("Is returning", ["All", "Yes", "No"]) if "IS_RETURNING" in df.columns else "All"
search = st.sidebar.text_input("Search station")

f = df.copy()
if regions and sel_regions:
    f = f[f["REGION_ID"].isin(sel_regions)]
if sel_type != "All" and "STATION_TYPE" in f.columns:
    f = f[f["STATION_TYPE"] == sel_type]
if "CAPACITY" in f.columns and cap_max > 0:
    f = f[(pd.to_numeric(f["CAPACITY"], errors="coerce") >= cap_rng[0]) & (pd.to_numeric(f["CAPACITY"], errors="coerce") <= cap_rng[1])]
if renting_opt != "All" and "IS_RENTING" in f.columns:
    f = f[f["IS_RENTING"].astype("Int64") == (1 if renting_opt == "Yes" else 0)]
if returning_opt != "All" and "IS_RETURNING" in f.columns:
    f = f[f["IS_RETURNING"].astype("Int64") == (1 if returning_opt == "Yes" else 0)]
if search:
    f = f[f["STATION_NAME"].str.contains(search, case=False, na=False)] if "STATION_NAME" in f.columns else f

col1, col2, col3, col4 = st.columns(4)
col1.metric("Stations", int(len(f)))
col2.metric("Bikes available", int(pd.to_numeric(f.get("NUM_BIKES_AVAILABLE", pd.Series([])), errors="coerce").fillna(0).sum()))
col3.metric("Docks available", int(pd.to_numeric(f.get("NUM_DOCKS_AVAILABLE", pd.Series([])), errors="coerce").fillna(0).sum()))
col4.metric("E-bikes available", int(pd.to_numeric(f.get("NUM_EBIKES_AVAILABLE", pd.Series([])), errors="coerce").fillna(0).sum()))

if {"LATITUDE", "LONGITUDE"}.issubset(f.columns):
    m = f[["LATITUDE", "LONGITUDE", "STATION_NAME"]].dropna()
    m = m.rename(columns={"LATITUDE": "lat", "LONGITUDE": "lon"})
    st.subheader("Map")
    st.map(m, use_container_width=True)

st.subheader("Top stations by bikes available")
top_n = st.slider("Top N", 5, 50, 15)
if "STATION_NAME" in f.columns and "NUM_BIKES_AVAILABLE" in f.columns:
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
            x=alt.X("NUM_BIKES_AVAILABLE:Q", title="Bikes available"),
            y=alt.Y("STATION_NAME:N", sort="-x", title="Station")
        )
    )
    st.altair_chart(chart, use_container_width=True)

st.subheader("Data")
st.dataframe(f, use_container_width=True)

ts_cols = []
if "RAW_TIMESTAMP" in f.columns:
    ts_cols.append(pd.to_datetime(f["RAW_TIMESTAMP"], errors="coerce"))
if "LAST_REPORTED_TS" in f.columns:
    ts_cols.append(pd.to_datetime(f["LAST_REPORTED_TS"], errors="coerce"))
if ts_cols:
    last_updated = pd.concat(ts_cols).max()
    if pd.notnull(last_updated):
        st.caption(f"Last updated: {last_updated}")
