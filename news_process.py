# %%
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
input_dir = "/home/jovyan/graph-stock-pred/Astock/data/raw/news/GOODINFO_NEWS"
input_filepath_likely = os.path.join(input_dir, f"{stock_id}*.csv")
likely_paths = glob(input_filepath_likely)

# %%
news = pd.DataFrame()
for path in likely_paths:
    filepath = os.path.join(input_dir, path)
    news = pd.concat([news, pd.read_csv(filepath)])
    
# %%
news = news.drop_duplicates()
news.columns = ["url","media","title","datetime"]

# get publish time
news["datetime"] = news["datetime"].apply(lambda x: x.split("\xa0")[-1][:-1])

# compute affect date (~14:30 -> today / 14:30~ -> next day)
def affect_day(x):
    if x.hour == 14:
        if x.minute >= 30:
            return (x + dt.timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            return x.strftime("%Y-%m-%d")
    elif x.hour < 14:
        return x.strftime("%Y-%m-%d")
    else:
        return (x + dt.timedelta(days=1)).strftime("%Y-%m-%d")

news["datetime"] = pd.to_datetime(news["datetime"])
news["affected_date"] = news["datetime"].apply(lambda x: affect_day(x))
news["affected_date"] = pd.to_datetime(news["affected_date"])

# %%
def clean_text(x):
    special_marks = ["\u3000","\xa0"]
    return re.sub("|".join(special_marks), " ", x)

news["title"] = news["title"].apply(lambda x: clean_text(x))

#%%
output_filepath = os.path.join(input_dir, f"{stock_id}-{start}-{end}.csv")
news.to_csv(output_filepath, index=False)
