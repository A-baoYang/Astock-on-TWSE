import argparse
import configparser
import json
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
stock_filepath = f"/home/jovyan/graph-stock-pred/Astock/data/raw/yf_daily/{stock_id}.TW.csv"
finlab_dir = "/home/jovyan/graph-stock-pred/Astock/data/raw/finlab"
# 上市櫃市場成交資訊 及 基本面-個股日本益比、殖利率及股價淨值比
finlab_dataset_names = ["price:成交股數","price:成交筆數","price:成交金額","price_earning_ratio:殖利率(%)","price_earning_ratio:本益比","price_earning_ratio:股價淨值比"]
basic_aspect_filepath = os.path.join(finlab_dir, f"{stock_id}-daily_financial_factors.csv")
technical_aspect_filepath = os.path.join("/home/jovyan/graph-stock-pred/Astock/data/raw/talib", f"{stock_id}-technique_indicators.csv")


def finlab_login():

    import finlab
    from finlab import data

    config = configparser.ConfigParser()
    config.read_file(open("secret.cfg"))
    api_token = config.get("FINLAB", "API_TOKEN")
    finlab.login(api_token=api_token)
    return data



if __name__ == "__main__":

    # check if data has been downloaded before
    if os.path.exists(basic_aspect_filepath):
        print(f"Data already fetched at {basic_aspect_filepath}")
    
    else:
        
        df = pd.DataFrame()
        for dataname in tqdm(finlab_dataset_names):
            filepath = os.path.join(finlab_dir, dataname.replace(":","_") + ".csv")
            # check if finlab data up-to-date
            if os.path.exists(filepath):
                dataset = pd.read_csv(filepath)
                dataset = dataset.set_index("date")
                if start >= dataset.index.min() and end <= dataset.index.max():
                    pass
                else:
                    data = finlab_login()
                    dataset = data.get(dataname)
                    dataset.to_csv(filepath)
            else:
                data = finlab_login()
                dataset = data.get(dataname)
                dataset.to_csv(filepath)

            dataset = dataset[(dataset.index>=start) & (dataset.index<end)][[stock_id]]
            dataset.columns = [dataname.split(":")[-1]]
            df = pd.concat([df, dataset], axis=1)
            
        df.to_csv(basic_aspect_filepath)
        print(f"Stock {stock_id} basic aspect factors fetched")



    # 技術面
    # 關注指標類型: Momentum, Volatility, Volume, Statistic, Overlap
    # Momentum: ADX, MACD, RSI
    # Volatility: ATR
    # Volume: OBV
    # Statistic: BETA
    # Overlap: BBANDS

    # check if data has been downloaded before
    if os.path.exists(technical_aspect_filepath):
        print(f"Data already fetched at {technical_aspect_filepath}")
    
    else:
        
        if os.path.exists(stock_filepath):

            import talib
            indicator_groups = talib.get_function_groups()
            from talib.abstract import *

            stock1d = pd.read_csv(stock_filepath)
            stock1d["ADX"] = ADX(stock1d)
            stock1d["MACD"], stock1d["MACDsignal"], stock1d["MACDhist"] = MACD(stock1d["close"])
            stock1d["RSI"] = RSI(stock1d)
            stock1d["ATR"] = ATR(stock1d)
            stock1d["OBV"] = OBV(stock1d)
            stock1d["BETA"] = BETA(stock1d)
            stock1d[["upperBAND", "middleBAND", "lowerBAND"]] = BBANDS(stock1d)

            stock1d.to_csv(technical_aspect_filepath, index=False)
            print(f"Stock {stock_id} technical aspect factors fetched")

        else:
            print(f"Need to fetch {stock_id} daily price data first")