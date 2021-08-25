import re
import os
import sys
import time
import codecs
import random
import shutil
import smtplib
import requests
import datetime
import openpyxl
import configparser
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup as BS
from pathlib import Path
from chardet import detect
from functools import reduce
# from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

def _load_config():
    config_path = "./config.ini"
    with open(config_path, "rb") as ef:
        config_encoding = detect(ef.read())["encoding"]
    config = configparser.ConfigParser()
    config.read_file(codecs.open(config_path, "r", config_encoding))
    return config

def _requests_session():
    session = requests.session()
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36"
    }
    session.headers.update(headers)
    return session

def multiple_replace(sub_dict, text):
     # Create a regular expression  from the dictionary keys
    regex = re.compile("(%s)" % "|".join(map(re.escape, sub_dict.keys())))

     # For each match, look-up corresponding value in dictionary
    return regex.sub(lambda mo: sub_dict[mo.string[mo.start():mo.end()]], text)

def mkdirs(path):
    if not os.path.exists(path):
        os.makedirs(path)

def df_m_pick(df_path):
    df = pd.read_csv(df_path, encoding="utf-8-sig")
    df[df["資料年月"][0]] = df["月營收"]
    df_m = df[["代號", "名稱", df["資料年月"][0]]]
    return df_m

def save_rev(market, date_Y, data_m):
    url = "https://mops.twse.com.tw/nas/t21/{}/t21sc03_{}_{}.csv".format(market, str(date_Y-1911), str(data_m))
    rs = _requests_session()
    res = rs.get(url)
    res.encoding = "utf-8-sig"
    mkdirs("./csv/{}".format(market))

#     date_save = date.today().strftime("%Y-%m-%d")
    # date_save = yesterday.strftime("%Y-%m-%d")
    date_save = datetime.datetime.now().strftime("%Y-%m-%d-%H%M")

    save_path = "./csv/_market/{}/m-rev-{}-{}-{}-{}.csv".format(market, str(date_Y), str(data_m).zfill(2), market, date_save)
    with open(save_path, "w", encoding="utf-8-sig") as save:
        save.write(res.text.replace("\r\n", "\n"))

#     past_path = "./csv/past/m-rev-{}-{}-{}.csv".format(str(date_Y), str(data_m).zfill(2), market)
#     shutil.copy(save_path, past_path)

    return save_path

def data_date(x):
    match = re.search("(\d+)/(\d+)", x)
    data_Y = int(match[1]) + 1911
    data_m = int(match[2])
    return datetime.datetime(data_Y, data_m, 1) + relativedelta(months=1) - datetime.timedelta(days=1)

def df_to_xlsx(df, path, sht_name):
    df.to_excel(path, sheet_name=sht_name, index=False, float_format="%.2f", engine="openpyxl", encoding="utf-8-sig", freeze_panes=(1, 3))

def merge_previous_yoy(df, df_yoy):
    previous_yoy = df_yoy[["代號", yoy_m]]
    previous_yoy.columns = ["代號", "上月YoY"]
    df_merge_yoy = df.merge(previous_yoy, on="代號", how="left", indicator=False)
    yoy_cols = df_merge_yoy.columns.tolist()
    yoy_cols.insert(-4, yoy_cols[-1])
    return df_merge_yoy[yoy_cols[:-1]]

def pickup_filter(list_f, pick_num):
    if not list_f:
        return None
    if len(list_f) <= pick_num*2:
        pick = int(round(len(list_f)/3))
        return ", ".join(list_f[:pick])
    else:
        return ", ".join(list_f[:pick_num])

def mail(attach_file=None):
    to_list = config["SMTP"]["to"].replace(" ", "").split(",")
    ccto_list = config["SMTP"]["ccto"].replace(" ", "").split(",")
    bccto_list = config["SMTP"]["bccto"].replace(" ", "").split(",")

    # Import the email modules
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders

    # Define email addresses to use
    addr_to = ",".join(to_list)    # 注意，不是分號
    addr_cc = ",".join(ccto_list)
    # addr_bcc = ",".join(bccto_list)
    addr_from = config["SMTP"]["from"]

    receive = to_list
    receive.extend(ccto_list)
    receive.extend(bccto_list)

    # Define SMTP email server details
    smtp_server = config["SMTP"]["smtp_server"]
    smtp_user   = config["SMTP"]["smtp_user"]
    smtp_pass   = config["SMTP"]["smtp_pass"]

    # Construct email
    msg = MIMEMultipart("alternative")

    msg["To"] = addr_to
    msg["CC"] = addr_cc
    msg["From"] = addr_from
    msg["Subject"] = "{}月營收-{}".format(attach_file.stem[11:13], re.search("\d{4}-\d{2}-\d{2}", attach_file.stem)[0])

    msg.attach(MIMEText(mail_text, "plain"))

    part = MIMEBase("application", "octet-stream")
    part.set_payload(open(attach_file, "rb").read())
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", "attachment; filename={}".format(attach_file.name))
    msg.attach(part)

    # Attach parts into message container.
    # According to RFC 2046, the last part of a MIMEMultipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(part)

    # Send the message via an SMTP server
    s = smtplib.SMTP_SSL(smtp_server, 465)
    s.ehlo()
    s.login(smtp_user,smtp_pass)
    s.sendmail(addr_from, receive, msg.as_string())
    s.quit()
    print("Email sent!\n")

# path
# 過去raw csv
past_dir = "./csv/past"
# 過去上市興櫃合併
past_all_dir = "./csv/past_all"
# 全部併成data
data_dir = "./csv/data"
data_rev_dir = "./csv/data/rev"
data_yoy_dir = "./csv/data/yoy"
data_cum_rev_dir = "./csv/data/cum-rev"
data_cum_yoy_dir = "./csv/data/cum-yoy"
# 單天csv
market_dir = "./csv/_market"
sii_dir = "./csv/_market/sii"
otc_dir = "./csv/_market/otc"
rotc_dir = "./csv/_market/rotc"
# 單天合併
concat_dir = "./csv/all"
# 當天新增
add_dir = "./csv/add"
# 月營收xlsx
xlsx_dir = "./xlsx"

date_Y = datetime.date.today().year

if datetime.date.today().day < 15:
    data_m = datetime.date.today().month - 1
else:
    data_m = datetime.date.today().month
data_m

# market = ["sii", "otc", "rotc"]
market = ["sii", "otc"]

df_path = [save_rev(i, date_Y, data_m) for i in market]

# concat a single day
dfs = [pd.read_csv(path, encoding="utf-8-sig") for path in df_path]
df_c = pd.concat(dfs, axis=0, join="outer", ignore_index=True, copy=True)
df_cols = ["出表日期", "資料年月", "代號", "名稱", "產業", "月營收", "上月營收", "去年當月營收", "MoM", "YoY", "累計營收", "去年累計", "累計YoY", "備註"]
df_c.columns = df_cols
drop_cols = ["出表日期", "上月營收", "去年當月營收", "去年累計"]
df_c = df_c.drop(drop_cols, axis=1, inplace=False).drop_duplicates()
df_c["資料年月"] = df_c["資料年月"].apply(data_date)
df_c = df_c.sort_values(["YoY", "MoM"], ascending=False, na_position="last")

mkdirs("./csv/all")
concat_path = "./csv/all/m-rev-{}-{}-all-{}.csv".format(str(date_Y), str(data_m).zfill(2), datetime.datetime.now().strftime("%Y-%m-%d-%H%M"))
df_c.to_csv(concat_path, index=False, encoding="utf-8-sig")

# get add part
concat_list = list(Path(concat_dir).iterdir())
concat_list.sort()
last_path = concat_list[-1]
previous_path = concat_list[-2]

df_0 = pd.read_csv(last_path, encoding="utf-8-sig")

mkdirs(add_dir)
add_path = "{}/{}".format(add_dir, concat_list[-1].name.replace("all", "add"))

if last_path.stem[:13] != previous_path.stem[:13]:
    df_0.to_csv(add_path, index=False, encoding="utf-8-sig")
else:
    df_1 = pd.read_csv(previous_path, encoding="utf-8-sig")
    df_0_1 = df_0.merge(df_1, on="代號", how="left", indicator=True)
    df_0_not_1 = df_0_1[df_0_1["_merge"] == "left_only"].drop(columns=["_merge"])
    df_0_not_1.columns = df_0_not_1.columns.str.replace("_x", "")
    df_0_not_1.drop(list(df_0_not_1.filter(regex = "_")), axis = 1, inplace = True)
    df_0_not_1.fillna("", inplace=True)
    df_0_not_1 = df_0_not_1.sort_values(["YoY", "MoM"], ascending=False, na_position="last")
    df_0_not_1.to_csv(add_path, index=False, encoding="utf-8-sig")

# export to xlsx
add_list = list(Path(add_dir).iterdir())
concat_list = list(Path(concat_dir).iterdir())
df_add = pd.read_csv(add_list[-1], encoding="utf-8-sig")
df_con = pd.read_csv(concat_list[-1], encoding="utf-8-sig")
df_add = df_add.sort_values(["YoY", "MoM"], ascending=False, na_position="last")
df_con = df_con.sort_values(["YoY", "MoM"], ascending=False, na_position="last")
rev_m = re.search("\d+-\d+", add_list[-1].stem)[0]
rev_m_dtime = datetime.datetime.strptime(rev_m, "%Y-%m")
previous_m = datetime.datetime.strftime(rev_m_dtime - relativedelta(months=1), "%Y-%m")

data_yoy_list = list(Path(data_yoy_dir).iterdir())
df_data_yoy = pd.read_csv(data_yoy_list[-1], encoding="utf-8-sig")
yoy_m = [i for i in df_data_yoy.columns if i.startswith(previous_m)][0]

df_add_yoy = merge_previous_yoy(df_add, df_data_yoy)
df_con_yoy = merge_previous_yoy(df_con, df_data_yoy)

mkdirs(xlsx_dir)
xlsx_path = "{}/{}.xlsx".format(xlsx_dir, add_list[-1].stem)

df_to_xlsx(df_add_yoy, xlsx_path, "新增")
wb = openpyxl.load_workbook(filename=xlsx_path)
if rev_m in wb.sheetnames:
    del wb[rev_m]
wb.save(xlsx_path)
wb.close()
with pd.ExcelWriter(xlsx_path, engine="openpyxl", mode="a") as writer:
    df_to_xlsx(df_con_yoy, writer, rev_m)

# filter
df_add_yoy_c = df_add_yoy.copy()
df_add_yoy_c["yoy_chg"] = df_add_yoy_c["YoY"] - df_add_yoy_c["上月YoY"]
df_add_yoy_c["名稱代號"] = df_add_yoy_c["名稱"] + " " + df_add_yoy_c["代號"].astype(str) + \
    " YoY " + df_add_yoy_c["YoY"].fillna(0).round(0).astype(int).astype(str) + "%"
df_add_yoy_c["名稱代號"] = df_add_yoy_c["名稱代號"].str.replace(" -", "-", regex=False)

yoy_rank = df_add_yoy_c[df_add_yoy_c["YoY"] < 1000].sort_values(by=["YoY"], \
    ascending=False, na_position="last")["名稱代號"].tolist()
yoy_chg_p_rank = df_add_yoy_c[df_add_yoy_c["yoy_chg"] > 0].sort_values(by=["yoy_chg"], \
    ascending=False, na_position="last")["名稱代號"].tolist()
yoy_chg_n_rank = df_add_yoy_c[df_add_yoy_c["yoy_chg"] < 0].sort_values(by=["yoy_chg"], \
    ascending=True, na_position="last")["名稱代號"].tolist()
if not df_add_yoy_c[(df_add_yoy_c["上月YoY"] < 0) & (df_add_yoy_c["YoY"] > 0)].empty:
    df_yoy_n_to_p = df_add_yoy_c[(df_add_yoy_c["上月YoY"] < 0) & (df_add_yoy_c["YoY"] > 0)]
    yoy_n_to_p = df_yoy_n_to_p.sort_values(by=["yoy_chg"], ascending=False, na_position="last")["名稱代號"].tolist()
if not df_add_yoy_c[(df_add_yoy_c["上月YoY"] > 0) & (df_add_yoy_c["YoY"] < 0)].empty:
    df_yoy_p_to_n = df_add_yoy_c[(df_add_yoy_c["上月YoY"] > 0) & (df_add_yoy_c["YoY"] < 0)]
    yoy_p_to_n = df_yoy_p_to_n.sort_values(by=["yoy_chg"], ascending=True, na_position="last")["名稱代號"].tolist()

rank_list = [yoy_rank, list(reversed(yoy_rank)), yoy_chg_p_rank, yoy_chg_n_rank, yoy_n_to_p, yoy_p_to_n]
yoy_rank_str, yoy_rank_r_str, yoy_chg_p_rank_str, yoy_chg_n_rank_str, yoy_n_to_p_str, yoy_p_to_n_str = \
    [pickup_filter(x, 7) for x in rank_list]

# email with xlsx attachment
config = _load_config()

update_time = datetime.datetime.strptime(add_list[-1].stem[-15:], "%Y-%m-%d-%H%M")
update_text = datetime.datetime.strftime(update_time, "%Y-%m-%d %H:%M")
if add_list[-1].stem[:13] != add_list[-2].stem[:13]:
    previous_text = "無"
else:
    previous_time = datetime.datetime.strptime(add_list[-2].stem[-15:], "%Y-%m-%d-%H%M")
    previous_text = datetime.datetime.strftime(update_time, "%Y-%m-%d %H:%M")

mail_text ="""
### 測試中

月營收: {}

更新時間: {}
上次更新時間: {}

YoY優: {}
YoY差: {}

YoY跳升: {}
YoY下降: {}

YoY轉正: {}
YoY轉負: {}
""".format(rev_m, update_text, previous_text, yoy_rank_str, yoy_rank_r_str, \
    yoy_chg_p_rank_str, yoy_chg_n_rank_str, yoy_n_to_p_str, yoy_p_to_n_str)

xlsx_list = list(Path(xlsx_dir).iterdir())
# mail(xlsx_list[-1])