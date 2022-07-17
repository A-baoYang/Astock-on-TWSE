# Process files from Octoparse
import argparse
import datetime as dt
from glob import glob
import os
import pandas as pd
import re

parser = argparse.ArgumentParser()
parser.add_argument('--stock_id', type=str, default="2330")
parser.add_argument('--start', type=str, default="201801")
parser.add_argument('--end', type=str, default="202207")
parser_args = parser.parse_args()
stock_id = parser_args.stock_id
start = parser_args.start
end = parser_args.end
news_dir = "/home/jovyan/graph-stock-pred/Astock/data/raw/news/GOODINFO_NEWS"
likely_paths = glob(os.path.join(news_dir, f"{stock_id}*.csv"))
output_filepath = os.path.join(news_dir, f"{stock_id}-{start}-{end}.csv")


def affect_day(x):
    """compute affect date (~14:30 -> today / 14:30~ -> next day)
    """
    if x.hour == 14:
        if x.minute >= 30:
            return (x + dt.timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            return x.strftime("%Y-%m-%d")
    elif x.hour < 14:
        return x.strftime("%Y-%m-%d")
    else:
        return (x + dt.timedelta(days=1)).strftime("%Y-%m-%d")


def clean_text(x):
    special_marks = ["\u3000","\xa0"]
    return re.sub("|".join(special_marks), " ", x)


if __name__ == "__main__":

    # check if data has been downloaded before
    if os.path.exists(output_filepath):
        print(f"News data already processed at {output_filepath}")
    else:

        news = pd.DataFrame()
        for path in likely_paths:
            filepath = os.path.join(news_dir, path)
            news = pd.concat([news, pd.read_csv(filepath)])
            
        news = news.drop_duplicates()
        news.columns = ["url","media","title","datetime"]

        # get publish time
        news["datetime"] = news["datetime"].apply(lambda x: x.split("\xa0")[-1][:-1])

        # compute affect date
        news["datetime"] = pd.to_datetime(news["datetime"])
        news["affected_date"] = news["datetime"].apply(lambda x: affect_day(x))
        news["affected_date"] = pd.to_datetime(news["affected_date"])
        news["title"] = news["title"].apply(lambda x: clean_text(x))

        news.to_csv(output_filepath, index=False)
        print(f"Stock {stock_id} news data finish processing")
