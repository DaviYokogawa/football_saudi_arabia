import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px

DATA_URL = "data/silver/saudi_data.parquet"

st.title("Historical analysis of Saudi Pro League")

data = pd.read_parquet(DATA_URL)

st.dataframe(data)

last_value_data = (
    data[data["date"] == data["date"].max()]
    .groupby("team_name")
    .agg(Actual_Market_Value=("market_value", "sum"))
    .sort_values("Actual_Market_Value", ascending=True)
    .reset_index()
)

plotly_bar = px.bar(
    last_value_data,
    x="Actual_Market_Value",
    y="team_name",
    orientation="h",
    title="Actual Market Value of Teams in Saudi Pro League",
    labels={"Actual_Market_Value": "Actual Market Value(Millions)"},
)
plotly_bar.update_xaxes(title_text="Actual Market Value(Millions)")
plotly_bar.update_yaxes(title_text="Team Name")

st.write(plotly_bar)
