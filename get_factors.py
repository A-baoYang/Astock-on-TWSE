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
basic_aspect_filepath = os.path.join("/home/jovyan/graph-stock-pred/Astock/data/raw/finlab", f"{stock_id}-daily_financial_factors.csv")
technical_aspect_filepath = os.path.join("/home/jovyan/graph-stock-pred/Astock/data/raw/talib", f"{stock_id}-technique_indicators.csv")


if __name__ == "__main__":

    # check if data has been downloaded before
    if os.path.exists(basic_aspect_filepath):
        print(f"Data already fetched at {basic_aspect_filepath}")
    else:

        import finlab
        from finlab import data

        # login
        config = configparser.ConfigParser()
        config.read_file(open("secret.cfg"))
        api_token = config.get("FINLAB", "API_TOKEN")
        finlab.login(api_token=api_token)

        # 上市櫃市場成交資訊
        df = pd.DataFrame()
        for col in tqdm(["成交股數","成交筆數","成交金額"]):
            dataset = data.get(f"price:{col}")
            dataset = dataset[(dataset.index>=start) & (dataset.index<end)][[stock_id]]
            dataset.columns = [col]
            df = pd.concat([df, dataset], axis=1)

        # 基本面
        # 個股日本益比、殖利率及股價淨值比
        for col in tqdm(["殖利率(%)","本益比","股價淨值比"]):
            dataset = data.get(f"price_earning_ratio:{col}")
            dataset = dataset[(dataset.index>=start) & (dataset.index<end)][[stock_id]]
            dataset.columns = [col]
            df = pd.concat([df, dataset], axis=1)

        df.to_csv(daily_factor_filepath)
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