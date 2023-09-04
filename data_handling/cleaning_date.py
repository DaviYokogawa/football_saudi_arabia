import pandas as pd

df = pd.read_parquet("data/bronze/saudi_data.parquet", engine="pyarrow")
df = df[
    [
        "date_of_extraction",
        "team_name",
        "League",
        "market_value_in_date",
        "n_players_in_date",
    ]
]
df = df.rename(
    columns={
        "date_of_extraction": "date",
        "League": "league",
        "market_value_in_date": "market_value",
        "n_players_in_date": "n_players",
    }
)
df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")

df[["market_value_numeric", "market_value_metric", "euro"]] = df[
    "market_value"
].str.split(pat=" ", expand=True)
df = df.drop(columns=["euro"])
df = df[df["market_value_numeric"] != "-"]
df["market_value_numeric"] = (
    df["market_value_numeric"].str.replace("-", "0").str.replace(",", ".").astype(float)
)
df["n_players"] = df["n_players"].astype(int)
df["market_value_numeric"] = df.apply(
    lambda x: x["market_value_numeric"] * 1000000
    if x["market_value_metric"] == "mi."
    else x["market_value_numeric"] * 1000,
    axis=1,
)

df_last_value = df.groupby(["team_name"]).first().reset_index()
df_last_value = df_last_value.rename(
    columns={
        "market_value_numeric": "last_market_value",
        "market_value_metric": "last_market_value_metric",
        "date": "last_date",
        "n_players": "last_n_players",
    }
)
df_last_value = df_last_value[
    [
        "team_name",
        "last_date",
        "last_market_value",
        "last_market_value_metric",
        "last_n_players",
    ]
]

df_final = df.merge(df_last_value, on=["team_name"], how="left")
df_final["difference"] = (
    df_final["last_market_value"] - df_final["market_value_numeric"]
)
df_final["percent_difference"] = 1 - (
    df_final["difference"] / df_final["last_market_value"]
)
df_final["n_players_difference"] = df_final["n_players"] - df_final["last_n_players"]
df_final = df_final.drop(
    columns=["market_value", "market_value_metric", "last_market_value_metric"]
)
df_final = df_final.rename(columns={"market_value_numeric": "market_value"})

df_final.to_parquet("data/silver/saudi_data.parquet", engine="pyarrow")
