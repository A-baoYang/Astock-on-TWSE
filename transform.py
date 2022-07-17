import argparse
from ltp import LTP
from opencc import OpenCC
tw2sp = OpenCC("tw2sp")
s2tw = OpenCC("s2tw")
import os
import numpy as np
import pandas as pd
import re
from tqdm import tqdm
from transformers import BertTokenizer

parser = argparse.ArgumentParser()
parser.add_argument('--stock_id', type=str, default="2330")
parser_args = parser.parse_args()
stock_id = parser_args.stock_id

ltp_model = LTP("base2")
PRE_TRAINED_MODEL_NAME = "hfl/chinese-roberta-wwm-ext"
tokenizer = BertTokenizer.from_pretrained(PRE_TRAINED_MODEL_NAME, do_lower_case=True)

input_dir = "/home/jovyan/graph-stock-pred/Astock/data/raw"
input_filepath = os.path.join(input_dir, f"{stock_id}-merged.csv")
output_dir = "/home/jovyan/graph-stock-pred/Astock/data/pre"
output_filepath = os.path.join(output_dir, f"{stock_id}-transformed.csv")


def do_srl(text):
    # 分句產生 srl 語義標註: 因為各自句子的語義不同，只是因為同一天被接在一起
    text_list = text.replace("\n","。\n").split("\n")
    # 第一次先透過 ltp 分詞將空格和無效標點移除，中文不受影響
    ws_list, hidden = ltp_model.seg(text_list)
    text_list = ["".join(w) for w in ws_list]
    # 再分一次為正式分詞結果
    ws_list, hidden = ltp_model.seg(text_list)
    token_text_list = [tokenizer.tokenize(sent) for sent in text_list]
    all_token_text = []
    for l in token_text_list:
        all_token_text += l
    sent_lens = [len(sent) for sent in token_text_list]
    text_ws = ["".join(_ws) for _ws in ws_list]
    text = "".join(text_ws)
    token_ws = [[tokenizer.tokenize(chr) for chr in _ws] for _ws in ws_list]
    srl_list = ltp_model.srl(hidden, keep_empty=False)
    srl_dict = []
    srl_text = []
    for _id in range(len(srl_list)):
        srl, ws = srl_list[_id], ws_list[_id]
        _srl_dict = []
        for s in srl:
            _A_dict = {"verb": s[0]}
            for A in s[1]:
                if A[0] in _A_dict:
                    _A_dict[A[0]].append((A[1], A[2]))
                else:
                    _A_dict.update({A[0]: [(A[1], A[2])]})
            _srl_dict.append(_A_dict)
        srl_dict.append(_srl_dict)

        _srl_text = []
        for d in _srl_dict:
            _dict = {}
            for key in d:
                if key == "verb":
                    _dict[key] = [ws[d[key]]]
                else:
                    _val = []
                    for v in d[key]:
                        _val.append("".join(ws[v[0]:v[1]+1]))
                    _dict[key] = _val
            _srl_text.append(_dict)
        srl_text.append(_srl_text)
    return text_list, token_text_list, all_token_text, sent_lens, ws_list, text_ws, token_ws, srl_list, srl_dict, srl_text, hidden, text


# 重寫 position 轉換函式
def token_pos_trans(token_ws, tup):
    """利用個別 tokenize 的 ltp 分詞結果 (token_ws) 
       推得語義角色 tokenize 後的位置
       
    """
    start, end = tup
    # 累加前面有的 token 數推得目標位置
    start_pos = sum(len(t) for t in token_ws[:start])
    length = 0
    for i in range(start, end+1):
        length += len(token_ws[i])
    return (start_pos, length)
    

def count_tokenize_position(srl_dict, token_ws, token_text_list):
    """調用 token_pos_trans 函式
       將 LTP SRL 的位置轉為詞彙在 tokenize 結果中的位置

    """
    token_srl_dict = []
    token_srl_text = []

    for _id in range(len(srl_dict)):
        _srl_dict = srl_dict[_id]
        _list, _text_list = [], []
        for _srl in _srl_dict:
            _dict, _text_dict = {"verb": [], "A0": [], "A1": []}, {"verb": [], "A0": [], "A1": []}
            for k, v in _srl.items():
                if k == "verb":
                    _tup_list = [(v, v)]
                else:
                    _tup_list = v
                if k in _dict:
                    for _tup in _tup_list:
                        token_tup = token_pos_trans(token_ws=token_ws[_id], tup=_tup)
                        _dict[k].append(token_tup)
                        start, length = token_tup
                        _text_dict[k].append("".join(token_text_list[_id][start:start+length]))
            if _dict["A0"] and _dict["A1"]:
                _list.append(_dict)
                _text_list.append(_text_dict)
        token_srl_dict.append(_list)
        token_srl_text.append(_text_list)
    return token_srl_dict, token_srl_text


def arrange_verbA0A1_positions(token_srl_dict, sent_lens):
    """整理動詞(verb)、主詞(A0)、受詞(A1)在 tokenized text 中的位置
       輸出成模型輸入資料所需的格式樣式
       如 : [(14, 2), [(9, 3), (12, 2)], [(16, 3), (19, 1)]]

    """
    all_token_srl_dict = []
    sent_lens_cumsum = np.cumsum(sent_lens).tolist()
    for _id in range(len(token_srl_dict)):
        l1 = token_srl_dict[_id]
        sent_len = sent_lens_cumsum[_id-1] if _id else 0
        if l1:
            for l2 in l1:
                l2_copy = l2.copy()
                for k, v in l2_copy.items():
                    _tups = []
                    for _v in v:
                        _tups.append((_v[0]+sent_len, _v[1]))
                    l2_copy[k] = _tups
                all_token_srl_dict.append(l2_copy)
    all_token_verbA0A1 = [list(item.values()) for item in all_token_srl_dict]
    all_token_verb, all_token_A0, all_token_A1 = [], [], []
    for item in all_token_srl_dict:
        all_token_verb += item["verb"]
        all_token_A0 += item["A0"]
        all_token_A1 += item["A1"]
    return all_token_srl_dict, all_token_verbA0A1, all_token_verb, all_token_A0, all_token_A1


def srl_tranform(text):
    """進行語義角色標註並轉換為在 tokenized text 中的位置的主程式
    
    """
    text_list, token_text_list, all_token_text, sent_lens, ws_list, text_ws, token_ws, srl_list, srl_dict, srl_text, hidden, text = do_srl(tw2sp.convert(text))
    token_srl_dict, token_srl_text = count_tokenize_position(srl_dict, token_ws, token_text_list)
    all_token_srl_dict, all_token_verbA0A1, all_token_verb, all_token_A0, all_token_A1 = arrange_verbA0A1_positions(token_srl_dict, sent_lens)
    return text, all_token_verbA0A1, all_token_verb, all_token_A0, all_token_A1


if __name__ == "__main__":

    # check if data has been downloaded before
    if os.path.exists(output_filepath):
        print(f"Data already transformed at {output_filepath}")
    else:

        data = pd.read_csv(input_filepath)
        data["titles"] = data["titles"].fillna("")
        data["news_count"] = data["news_count"].fillna(0.0)
        data["affected_date"] = data["affected_date"].fillna(data["date"])

        for col in ["verbA0A1","verb","A0","A1"]:
            data[col] = ""
        for i in tqdm(range(data.shape[0])):
            text = data.loc[i, "titles"]
            if text:
                a,b,c,d,e = srl_tranform(text)
                data.loc[i, "titles"], data.loc[i, "verbA0A1"], data.loc[i, "verb"], data.loc[i, "A0"], data.loc[i, "A1"] = str(a), str(b), str(c), str(d), str(e)
            else:
                data.loc[i, "titles"], data.loc[i, "verbA0A1"], data.loc[i, "verb"], data.loc[i, "A0"], data.loc[i, "A1"] = "",None,None,None,None

        data["stock_id"] = stock_id
        data.to_csv(output_filepath, index=False)
        print(f"Stock {stock_id} training data transformed")
