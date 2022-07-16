# %%
import configparser
import finlab
from finlab import data
import json
import os
import pandas as pd
from tqdm import tqdm

stock_filepath = "/home/jovyan/graph-stock-pred/Astock/data/raw/yf_daily/2330.TW.csv"
output_dir = "/home/jovyan/graph-stock-pred/Astock/data/raw/finlab"
stock_id = "2330"
#%%
# login
config = configparser.ConfigParser()
config.read_file(open("secret.cfg"))
api_token = config.get("FINLAB", "API_TOKEN")
finlab.login(api_token=api_token)

#%%
finlab_datakeys = {
    "還原股價": {
        "adj": ["open","high","close","low"]
    },
    "市場成交資訊": {
        "price": ["成交股數","成交筆數","成交金額"]
    },
    "股價評估指標": {
        "price_earning_ratio": ["殖利率(%)","本益比","股價淨值比"]
    },
    "財務指標": {
        "fundamental_features": ["營業利益", "營運現金流", "歸屬母公司淨利", "折舊", "流動資產", "流動負債", "取得不動產廠房及設備", "經常稅後淨利", "ROA稅後息前", "ROA綜合損益", "ROE稅後", "ROE綜合損益", "稅前息前折舊前淨利率", "營業毛利率", "營業利益率", "稅前淨利率", "稅後淨利率", "業外收支營收率", "貝里比率", "研究發展費用率", "現金流量比率", "稅率", "每股營業額", "每股營業利益", "每股現金流量", "每股稅前淨利", "每股綜合損益", "每股稅後淨利", "總負債除總淨值", "負債比率", "淨值除資產", "營收成長率", "營業毛利成長率", "營業利益成長率", "稅前淨利成長率", "稅後淨利成長率", "經常利益成長率", "資產總額成長率", "淨值成長率", "流動比率", "速動比率", "利息支出率", "營運資金", "總資產週轉次數", "存貨週轉率", "固定資產週轉次數", "淨值週轉率次數", "自由現金流量"]
    },
    "三大法人買賣超": {
        "institutional_investors_trading_summary": ["外陸資買進股數(不含外資自營商)", "外陸資賣出股數(不含外資自營商)", "外陸資買賣超股數(不含外資自營商)", "外資自營商買進股數", "外資自營商賣出股數", "外資自營商買賣超股數", "投信買進股數", "投信賣出股數", "投信買賣超股數", "自營商買進股數(自行買賣)", "自營商賣出股數(自行買賣)", "自營商買賣超股數(自行買賣)", "自營商買進股數(避險)", "自營商賣出股數(避險)", "自營商買賣超股數(避險)"]
    },
    "內部人持股變化": {
        "internal_equity_changes": ["發行股數", "董監增加股數", "董監減少股數", "董監持有股數", "董監持有股數占比", "經理人持有股數", "百分之十以上大股東持有股數", "市場別"]
    },
    "指數價格": {
        "stock_index_price": ["收盤指數", "漲跌百分比(%)"]
    },
    "指數成交量": {
        "stock_index_vol": ["成交股數","成交筆數","成交金額"]
    },
    "台灣加權指數": {
        "taiex_total_index": ["開盤指數", "最高指數", "最低指數", "收盤指數"]
    },
    "世界指數": {
        "world_index": ["open","high","low","close","adj_close","vol"]
    }
}
#%%
dict_filepath = os.path.join(output_dir, "dataset_namedict.json")
with open(dict_filepath, "w", encoding="utf-8") as f:
    json.dump(finlab_datakeys, f, ensure_ascii=False, indent=4)
    
#%%
# fetch databases 
# 還原股價
for col in tqdm(["open","high","low","close"]):
    dataset = data.get(f"etl:adj_{col}")
    output_filepath = os.path.join(output_dir, f"adj_{col}.csv")
    dataset.to_csv(output_filepath)

# %%
# 上市櫃市場成交資訊
df = pd.DataFrame()
for col in tqdm(["成交股數","成交筆數","成交金額"]):
    dataset = data.get(f"price:{col}")
    dataset = dataset[dataset.index>="2018-01-01"][[stock_id]]
    dataset.columns = [col]
    df = pd.concat([df, dataset], axis=1)
    # output_filepath = os.path.join(output_dir, f"price_{col}.csv")
    # dataset.to_csv(output_filepath)

# %%
# 基本面
# 個股日本益比、殖利率及股價淨值比
for col in tqdm(["殖利率(%)","本益比","股價淨值比"]):
    dataset = data.get(f"price_earning_ratio:{col}")
    dataset = dataset[dataset.index>="2018-01-01"][[stock_id]]
    dataset.columns = [col]
    df = pd.concat([df, dataset], axis=1)
    # output_filepath = os.path.join(output_dir, f"price_earning_ratio_{col}.csv")
    # dataset.to_csv(output_filepath)

#%%
output_filepath = os.path.join(output_dir, f"{stock_id}-daily_financial_factors.csv")
df.to_csv(output_filepath)

# %%
# 財務指標
cols = [
    "營業利益", 
    "營運現金流", 
    "歸屬母公司淨利", 
    "折舊", 
    "流動資產", 
    "流動負債", 
    "取得不動產廠房及設備", 
    "經常稅後淨利", 
    "ROA稅後息前", 
    "ROA綜合損益", 
    "ROE稅後", 
    "ROE綜合損益", 
    "稅前息前折舊前淨利率", 
    "營業毛利率", 
    "營業利益率", 
    "稅前淨利率", 
    "稅後淨利率", 
    "業外收支營收率", 
    "貝里比率", 
    "研究發展費用率", 
    "現金流量比率", 
    "稅率", 
    "每股營業額", 
    "每股營業利益", 
    "每股現金流量", 
    "每股稅前淨利", 
    "每股綜合損益", 
    "每股稅後淨利", 
    "總負債除總淨值", 
    "負債比率", 
    "淨值除資產", 
    "營收成長率", 
    "營業毛利成長率", 
    "營業利益成長率", 
    "稅前淨利成長率", 
    "稅後淨利成長率", 
    "經常利益成長率", 
    "資產總額成長率", 
    "淨值成長率", 
    "流動比率", 
    "速動比率", 
    "利息支出率", 
    "營運資金", 
    "總資產週轉次數", 
    "存貨週轉率", 
    "固定資產週轉次數", 
    "淨值週轉率次數", 
    "自由現金流量", 
]

df = pd.DataFrame()
for col in tqdm(cols):
    try:
        dataset = data.get(f"fundamental_features:{col}")
        dataset = dataset[dataset.index>="2018-Q1"][[stock_id]]
        dataset.columns = [col]
        df = pd.concat([df, dataset], axis=1)
        # output_filepath = os.path.join(output_dir, f"fundamental_features_{col}.csv")
        # dataset.to_csv(output_filepath)
    except Exception as e:
        print(f"error at {col}. reason: {e}")
        pass

#%%
output_filepath = os.path.join(output_dir, f"{stock_id}-quarterly_financial_factors.csv")
df.to_csv(output_filepath)

# %%
# 三大法人買賣超
cols = [
    "外陸資買進股數(不含外資自營商)", 
    "外陸資賣出股數(不含外資自營商)", 
    "外陸資買賣超股數(不含外資自營商)", 
    "外資自營商買進股數", 
    "外資自營商賣出股數", 
    "外資自營商買賣超股數", 
    "投信買進股數", 
    "投信賣出股數", 
    "投信買賣超股數", 
    "自營商買進股數(自行買賣)", 
    "自營商賣出股數(自行買賣)", 
    "自營商買賣超股數(自行買賣)", 
    "自營商買進股數(避險)", 
    "自營商賣出股數(避險)", 
    "自營商買賣超股數(避險)", 
]

for col in tqdm(cols):
    try:
        dataset = data.get(f"institutional_investors_trading_summary:{col}")
        output_filepath = os.path.join(output_dir, f"institutional_investors_trading_summary_{col}.csv")
        dataset.to_csv(output_filepath)
    except Exception as e:
        print(f"error at {col}. reason: {e}")
        pass

# %%
# 內部人持股變化
cols = [
    "發行股數", 
    "董監增加股數", 
    "董監減少股數", 
    "董監持有股數", 
    "董監持有股數占比", 
    "經理人持有股數", 
    "百分之十以上大股東持有股數", 
    "市場別", 
]

for col in tqdm(cols):
    try:
        dataset = data.get(f"internal_equity_changes:{col}")
        output_filepath = os.path.join(output_dir, f"internal_equity_changes_{col}.csv")
        dataset.to_csv(output_filepath)
    except Exception as e:
        print(f"error at {col}. reason: {e}")
        pass

# %%
# 指數價格
cols = [
    "收盤指數", 
    "漲跌百分比(%)", 
]

for col in tqdm(cols):
    try:
        dataset = data.get(f"stock_index_price:{col}")
        output_filepath = os.path.join(output_dir, f"stock_index_price_{col}.csv")
        dataset.to_csv(output_filepath)
    except Exception as e:
        print(f"error at {col}. reason: {e}")
        pass


# %%
# 指數成交量
cols = ["成交股數","成交筆數","成交金額"]

for col in tqdm(cols):
    try:
        dataset = data.get(f"stock_index_vol:{col}")
        dataset = dataset[dataset.index>="2018-01-01"][[stock_id]]
        dataset.columns = [col]
        df = pd.concat([df, dataset], axis=1)
        # output_filepath = os.path.join(output_dir, f"stock_index_price_{col}.csv")
        # dataset.to_csv(output_filepath)
    except Exception as e:
        print(f"error at {col}. reason: {e}")
        pass

# %%
# 台灣加權指數
cols = [
    "開盤指數", 
    "最高指數", 
    "最低指數", 
    "收盤指數", 
]

for col in tqdm(cols):
    try:
        dataset = data.get(f"taiex_total_index:{col}")
        output_filepath = os.path.join(output_dir, f"taiex_total_index_{col}.csv")
        dataset.to_csv(output_filepath)
    except Exception as e:
        print(f"error at {col}. reason: {e}")
        pass

#%%
# 世界指數
cols = [
    "open", 
    "high", 
    "low", 
    "close", 
    "adj_close", 
    "vol", 
]

for col in tqdm(cols):
    try:
        dataset = data.get(f"world_index:{col}")
        output_filepath = os.path.join(output_dir, f"world_index_{col}.csv")
        dataset.to_csv(output_filepath)
    except Exception as e:
        print(f"error at {col}. reason: {e}")
        pass

#%%
# 技術面
import talib
indicator_groups = talib.get_function_groups()
from talib.abstract import *
# %%
# 關注指標類型: Momentum, Volatility, Volume, Statistic, Overlap
# Momentum: ADX, MACD, RSI
# Volatility: ATR
# Volume: OBV
# Statistic: BETA
# Overlap: BBANDS
stock1d = pd.read_csv(stock_filepath)
stock1d["ADX"] = ADX(stock1d)
stock1d["MACD"], stock1d["MACDsignal"], stock1d["MACDhist"] = MACD(stock1d["close"])
stock1d["RSI"] = RSI(stock1d)
stock1d["ATR"] = ATR(stock1d)
stock1d["OBV"] = OBV(stock1d)
stock1d["BETA"] = BETA(stock1d)
stock1d[["upperBAND", "middleBAND", "lowerBAND"]] = BBANDS(stock1d)
# %%
output_dir = "/home/jovyan/graph-stock-pred/Astock/data/raw/talib"
output_filepath = os.path.join(output_dir, "2330-technique_indicators.csv")
stock1d.to_csv(output_filepath, index=False)
# %%
