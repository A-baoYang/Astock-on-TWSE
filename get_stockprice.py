import argparse
import numpy as np
import os
import pandas as pd
import yfinance as yf


parser = argparse.ArgumentParser()
parser.add_argument('--symbol', type=str, default="2330.TW")
parser.add_argument('--start', type=str, default="2017-12-29")
parser.add_argument('--end', type=str, default="2022-07-01")
parser_args = parser.parse_args()
symbol = parser_args.symbol
start = parser_args.start
end = parser_args.end
output_dir = "/home/jovyan/graph-stock-pred/Astock/data/raw/yf_daily"
output_filepath = os.path.join(output_dir, f"{symbol}.csv")


if __name__ == "__main__":
    
    # check if data has been downloaded before
    if os.path.exists(output_filepath):
        print(f"Data already fetched at {output_filepath}")
    else:
        # fetch data
        data = yf.download(tickers=symbol, start=start, end=end, auto_adjust=True, interval="1d")
        data.columns = [col.lower() for col in data.columns]

        # Add columns: `pre_close`, `change`, `pct_chg`
        data["pre_close"] = data["close"].shift()
        data["change"] = data["close"] - data["pre_close"]
        data["pct_chg"] = (data["change"] / data["pre_close"]).round(6)

        # store
        data.to_csv(output_filepath)
