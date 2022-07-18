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


def clean_text(x):
    special_marks = ["\u3000","\xa0"]
    return re.sub("|".join(special_marks), " ", x)


if __name__ == "__main__":

    if not likely_paths:
        print(f"{stock_id} need to Octoparse news first")
    
    else:
        # collect news which hasn't been processed
        processed = 0
        news = pd.DataFrame()
        for path in likely_paths:
            filepath = os.path.join(news_dir, path)
            _df = pd.read_csv(filepath)
            
            if "url" not in _df.columns:
                news = pd.concat([news, _df])
            else:
                processed += 1
        
        if news.empty:
            print("No news data need to be processed")
        
        elif len(likely_paths) == processed:
            print("News data have all been processed")

        else:
            news.columns = ["url","media","title","datetime"]
            news = news[news["datetime"].notnull()].reset_index(drop=True)
            news = news.drop_duplicates()

            # get publish time
            news["datetime"] = news["datetime"].apply(lambda x: x.split("\xa0")[-1][:-1])
            
            # cleansing title text
            news["title"] = news["title"].apply(lambda x: clean_text(x))

            news.to_csv(output_filepath, index=False)
            print(f"Stock {stock_id} news data finish processing")
