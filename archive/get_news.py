#%%
# not use, since ip are easily been blocked, using octoparse
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import requests
from fake_useragent import UserAgent
from time import sleep
import random
from tqdm import tqdm
ua = UserAgent()
proxy_list = pd.read_csv("/home/jovyan/smartproxies.csv").ip.values.tolist()
# %%
news_url = "https://goodinfo.tw/tw/StockAnnounceList.asp"
url_params = {
    "PAGE": 1,
    "START_DT": "2022/6/1",
    "END_DT": "2022/7/1",
    "STOCK_ID": 2330
}
headers = {
    "User-Agent": ua.random
}
ip_proxy = {
    "http": random.choice(proxy_list),
    "https": random.choice(proxy_list),
}
r = requests.get(news_url, params=url_params, headers=headers, proxies=ip_proxy).content
soup = BeautifulSoup(r, "html.parser")
table = soup.find("table", {"class": "p6_4"})
pages = int(table.find("nobr").text.strip().split("共")[-1][:-2])

df = pd.DataFrame()
for i in tqdm(range(1, pages + 1)):
    sleep(3)
    url_params["PAGE"] = i
    r = requests.get(news_url, params=url_params, headers=headers, proxies=ip_proxy).content
    soup = BeautifulSoup(r, "html.parser")
    table = soup.find("table", {"class": "p4_4 row_bg_2n row_mouse_over"})
    table = pd.read_html(str(table))[0]
    table.columns = ["media", "title"]
    table["stock_id"] = url_params["STOCK_ID"]
    df = pd.concat([df, table]).reset_index(drop=True)
    df.to_csv("/home/jovyan/graph-stock-pred/Astock/data/raw/news_2330.csv", index=False)
    
