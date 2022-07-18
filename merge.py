import argparse
import datetime as dt
import numpy as np
import os
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
output_filepath = f"/home/jovyan/graph-stock-pred/Astock/data/pre/merged/{stock_id}-merged.csv"


def affect_day(x, stock_date_list):
    """compute affect date (~14:30 -> today / 14:30~ -> next day)
    """

    def find_nearest_open_date(stock_date_list, ts, is_closed):
        if ts in stock_date_list:
            if is_closed: 
                if stock_date_list.index(ts) + 1 < len(stock_date_list):
                    return stock_date_list[stock_date_list.index(ts) + 1]
                else:
                    return None
            else:
                return ts
        else:
            if ts > stock_date_list[-1]:
                return None
            # either market closed or not, find the nearest market open date
            # count = 0
            while ts not in stock_date_list:
                ts = ts + dt.timedelta(days=1)
                # count += 1
                # if count >= 30:
                #     return "error"
            return ts
    
    if x.hour == 14:
        if x.minute >= 30:
            return find_nearest_open_date(
                stock_date_list, pd.Timestamp(x.to_pydatetime().date()), is_closed=True)
        else:
            return find_nearest_open_date(
                stock_date_list, pd.Timestamp(x.to_pydatetime().date()), is_closed=False)
    elif x.hour < 14:
        return find_nearest_open_date(
            stock_date_list, pd.Timestamp(x.to_pydatetime().date()), is_closed=False)
    else:
        return find_nearest_open_date(
            stock_date_list, pd.Timestamp(x.to_pydatetime().date()), is_closed=True)


def find_prev_open_date(ts, stock_date_list):
    if pd.isna(ts): 
        return None
    elif ts <= stock_date_list[0]:
        return None
    else:
        return stock_date_list[stock_date_list.index(ts) - 1]


if __name__ == "__main__":

    # check if data has been downloaded before
    if os.path.exists(output_filepath):
        print(f"Data already merged at {output_filepath}")
    
    else:
        # load datasets
        stock1d = pd.read_csv(stock1d_filepath)
        stock1d = stock1d.drop("volume", axis=1)
        stock1d = stock1d.rename(columns={"Date": "date"})
        stock1d["date"] = pd.to_datetime(stock1d["date"])
        fin1d = pd.read_csv(fin1d_filepath)
        fin1d["date"] = pd.to_datetime(fin1d["date"])
        news = pd.read_csv(news_filepath)
        news = news[["title","datetime"]]

        # compute trade_date and affected_date of the news
        print("computing trade_date & affected_date of news data...")

        news["datetime"] = pd.to_datetime(news["datetime"])
        news["affected_date"] = news["datetime"].apply(
            lambda x: affect_day(x, stock_date_list=stock1d.date.tolist()))
        news["trade_date"] = news["affected_date"].apply(
            lambda x: find_prev_open_date(x, stock_date_list=stock1d.date.tolist()))
        news = news[news["trade_date"].notnull()].reset_index(drop=True)

        # aggregate title text by trade_date
        news = news.groupby("trade_date").agg({"title": ["count", "\n".join]}, axis=1).reset_index()
        news.columns = ["trade_date", "news_count", "titles"]
        news["trade_date"] = pd.to_datetime(news["trade_date"])

        # merging
        stock = pd.merge(stock1d, fin1d, on="date", how="left")
        merged = pd.merge(
            stock, news, 
            left_on="date", right_on="trade_date", how="left"
        )
        merged = merged[(merged["date"]>=start) & (merged["date"]<end)].reset_index(drop=True)

        merged.to_csv(output_filepath, index=False)
        print(f"Stock {stock_id} data merged")