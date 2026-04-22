import streamlit as st
import psycopg2
import pandas as pd

conn = psycopg2.connect(
    dbname="weather_db",
    user="bunny",
    password="1102",
    host="localhost",
    port="5432"
)

query = """
SELECT city, temperature, humidity, weather, timestamp
FROM weather_data
ORDER BY timestamp DESC
LIMIT 200;
"""

df = pd.read_sql(query, conn)

st.title("🌦️ Real-Time Weather Pipeline Dashboard")

# 🔍 Filter
city_filter = st.selectbox("Select City", df["city"].unique())
filtered_df = df[df["city"] == city_filter]

st.subheader("Latest Data")
st.dataframe(filtered_df)

st.subheader("Temperature Trend")
st.line_chart(filtered_df.set_index("timestamp")["temperature"])

st.subheader("Average Temperature")
st.bar_chart(filtered_df.groupby("city")["temperature"].mean())