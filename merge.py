#%%
import argparse
import datetime as dt
import numpy as np
import pandas as pd
from tqdm import tqdm

parser = argparse.ArgumentParser()
parser.add_argument('--stock_id', type=str, default="2330")
parser.add_argument('--start', type=str, default="2018-01-01")
parser.add_argument('--end', type=str, default="2022-07-01")
parser_args = parser.parse_args()
stock_id = parser_args.stock_id
start = parser_args.start
end = parser_args.end
news_ym_start = f"{dt.datetime.strptime(start, '%Y-%m-%d').year}{str(dt.datetime.strptime(start, '%Y-%m-%d').month).zfill(2)}"
news_ym_end = f"{dt.datetime.strptime(end, '%Y-%m-%d').year}{str(dt.datetime.strptime(end, '%Y-%m-%d').month).zfill(2)}"
news_filepath = f"/home/jovyan/graph-stock-pred/Astock/data/raw/news/GOODINFO_NEWS/{stock_id}-{news_ym_start}-{news_ym_end}.csv"
stock1d_filepath = f"/home/jovyan/graph-stock-pred/Astock/data/raw/talib/{stock_id}-technique_indicators.csv"
fin1d_filepath = f"/home/jovyan/graph-stock-pred/Astock/data/raw/finlab/{stock_id}-daily_financial_factors.csv"
# fin1q_filepath = f"/home/jovyan/graph-stock-pred/Astock/data/raw/finlab/{stock_id}-quarterly_financial_factors.csv"

#%%
stock1d = pd.read_csv(stock1d_filepath)
stock1d = stock1d.drop("volume", axis=1)
stock1d = stock1d.rename(columns={"Date": "date"})
stock1d["date"] = pd.to_datetime(stock1d["date"])
fin1d = pd.read_csv(fin1d_filepath)
# fin1q = pd.read_csv(fin1q_filepath)
# fin1q = fin1q.rename(columns={"date": "quarter"})
fin1d["date"] = pd.to_datetime(fin1d["date"])
# fin1d["quarter"] = fin1d["date"].dt.to_period("Q").dt.strftime("%Y-Q%q")
news = pd.read_csv(news_filepath)
news = news[["title","datetime","affected_date"]]

# %%
# fin = pd.merge(fin1d, fin1q, on="quarter", how="left")
stock = pd.merge(stock1d, fin1d, on="date", how="left")

#%%
news = news.groupby("affected_date").agg({"title": ["count", "\n".join]}, axis=1).reset_index()
news.columns = ["affected_date", "news_count", "titles"]
news["affected_date"] = pd.to_datetime(news["affected_date"])

#%% 
merged = pd.merge(
    stock, news, 
    left_on="date", right_on="affected_date", how="left"
)
merged = merged[(merged["date"]>=start) & (merged["date"]<end)].reset_index(drop=True)

# %%
merged.to_csv(f"/home/jovyan/graph-stock-pred/Astock/data/raw/{stock_id}-merged.csv", index=False)
print(f"stock {stock_id} data merged")