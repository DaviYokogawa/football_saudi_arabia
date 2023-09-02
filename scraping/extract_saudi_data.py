import requests
import pyarrow as pa
from bs4 import BeautifulSoup
import pandas as pd

HEADERS = {"User-Agent": "MyAgent"}
URL = "https://www.transfermarkt.com.br/saudi-professional-league/marktwerteverein/wettbewerb/SA1"
DATA_PLACEHOLDER_CLASS = "chzn-select"

req_placeholder = requests.get(URL, headers=HEADERS)
page_bs4_placeholder = BeautifulSoup(req_placeholder.content, "html.parser")

data_placeholder = page_bs4_placeholder.find_all(
    "select", class_=DATA_PLACEHOLDER_CLASS
)
data_placeholder_list = [
    data.get("value") for data in data_placeholder[0].find_all("option")
]

df_final = []

for date in data_placeholder_list:
    url_to_scrape = f"{URL}/plus/1?stichtag={date}"
    print(f"Starting to scrape {date}")

    req = requests.get(url_to_scrape, headers=HEADERS)
    page_bs4 = BeautifulSoup(req.content, "html.parser")

    table = page_bs4.find_all("table", class_="items")
    table = table[0]

    df = []
    for col in table.find_all("tr")[1:]:
        row_data = col.find_all("td")
        row = [data.text for data in row_data]
        df.append(row)
    df = df[1:]
    df = pd.DataFrame(
        df,
        columns=[
            "row",
            "empty",
            "team_name",
            "League",
            "market_value_in_date",
            "n_players_in_date",
            "actual_market_value",
            "actual_n_players",
            "abs_difference",
            "difference_percent",
        ],
    ).assign(date_of_extraction=date)
    df_final.append(df)
    print(f"Finished {date}")

df_final = pd.concat(df_final)

df_final.to_parquet("data/saudi_data.parquet", engine="pyarrow")
