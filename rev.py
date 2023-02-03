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
    # headers = {
    #     "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36"
    # }
    headers = {
        "user-agent": config["Requests_header"]["user-agent"]
    }
    session.headers.update(headers)
    return session

def multiple_replace(sub_dict, text):
     # Create a regular expression  from the dictionary keys
    regex = re.compile("(%s)" % "|".join(map(re.escape, sub_dict.keys())))

     # For each match, look-up corresponding value in dictionary
    return regex.sub(lambda mo: sub_dict[mo.string[mo.start():mo.end()]], text)

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
    os.makedirs("./csv/{}".format(market), exist_ok=True)

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
    # df.to_excel(path, sheet_name=sht_name, index=False, float_format="%.2f", engine="openpyxl", encoding="utf-8-sig", freeze_panes=(1, 3))
    df.to_excel(path, sheet_name=sht_name, index=False, float_format="%.2f", engine="openpyxl", freeze_panes=(1, 3))

def merge_previous_mom(df, df_mom):
    previous_mom = df_mom[["代號", mom_m]]
    previous_mom.columns = ["代號", "上月MoM"]
    df_merge_mom = df.merge(previous_mom, on="代號", how="left", indicator=False)
    mom_cols = df_merge_mom.columns.tolist()
    mom_cols.insert(-5, mom_cols[-1])
    return df_merge_mom[mom_cols[:-1]]

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

def html_tag(element, atr, value, text):
    return '<{} {}="{}">{}</{}>'.format(element, atr, value, text, element)

def html_colorize(x):
    if x < 0:
        return html_tag("span", "style", "color:crimson;", "{}%".format(round(x)))
    elif x > 0:
        return html_tag("span", "style", "color:dodgerblue;", "{}%".format(round(x)))
    elif x == 0:
        return "{}%".format(round(x))
    else:
        return ""

def html_colorize_M(x):
    if x < 0:
        return html_tag("span", "style", "color:#C24641;", "{}%".format(round(x)))
    elif x > 0:
        return html_tag("span", "style", "color:cornflowerblue;", "{}%".format(round(x)))
    elif x == 0:
        return "{}%".format(round(x))
    else:
        return ""

def html_italic(text):
    return '<i>{}</i>'.format(text)

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

    msg.attach(MIMEText(mail_content, "html"))

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

config = _load_config()

os.makedirs("./sii_otc", exist_ok=True)
os.chdir("./sii_otc")

# path
# 過去raw csv
past_dir = "./csv/past"
# 過去上市櫃合併
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
# rotc_dir = "./csv/_market/rotc"
# 單天合併
concat_dir = "./csv/all"
# 當天新增
add_dir = "./csv/add"
# 月營收xlsx
xlsx_dir = "./xlsx"

data_rs = datetime.date.today() - relativedelta(months=1)
date_Y = data_rs.year
data_m = data_rs.month

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

os.makedirs("./csv/all", exist_ok=True)
concat_path = "./csv/all/m-rev-{}-{}-all-{}.csv".format(str(date_Y), str(data_m).zfill(2), datetime.datetime.now().strftime("%Y-%m-%d-%H%M"))
df_c.to_csv(concat_path, index=False, encoding="utf-8-sig")

# get add part
concat_list = list(Path(concat_dir).iterdir())
concat_list.sort()
last_path = concat_list[-1]
previous_path = concat_list[-2]

df_0 = pd.read_csv(last_path, encoding="utf-8-sig")

os.makedirs(add_dir, exist_ok=True)
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

# Calculate the MoM data
data_rev_list = list(Path(data_rev_dir).iterdir())
df_data_rev = pd.read_csv(data_rev_list[-1], encoding="utf-8-sig", index_col="代號").drop(["名稱"], axis=1)
df_data_mom = df_data_rev.pct_change(axis="columns", periods=1).apply(lambda x: x*100).round(decimals=2).reset_index(level=0)
mom_m = [i for i in df_data_mom.columns if i.startswith(previous_m)][0]

# df_add_yoy = merge_previous_yoy(df_add, df_data_yoy)
# df_con_yoy = merge_previous_yoy(df_con, df_data_yoy)

df_add_mom = merge_previous_mom(df_add, df_data_mom)
df_con_mom = merge_previous_mom(df_con, df_data_mom)
df_add_yoy = merge_previous_yoy(df_add_mom, df_data_yoy)
df_con_yoy = merge_previous_yoy(df_con_mom, df_data_yoy)

os.makedirs(xlsx_dir, exist_ok=True)
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
df_add_yoy_c["mom_chg"] = df_add_yoy_c["MoM"] - df_add_yoy_c["上月MoM"]

df_add_yoy_c["YoY_html"] = df_add_yoy_c["YoY"].apply(html_colorize)

df_add_yoy_c["MoM_color"] = " M " + df_add_yoy_c["MoM"].apply(html_colorize_M)
df_add_yoy_c["MoM_html"] = df_add_yoy_c["MoM_color"].apply(html_italic)

df_add_yoy_c["名稱代號"] = df_add_yoy_c["名稱"] + " " + df_add_yoy_c["代號"].astype(str) + \
    df_add_yoy_c["MoM_html"] + " Y " + df_add_yoy_c["YoY_html"]
df_add_yoy_c["名稱代號"] = df_add_yoy_c["名稱代號"].str.replace(" -", "-", regex=False)

# ranking by YoY
yoy_rank = df_add_yoy_c[df_add_yoy_c["YoY"] < 1000].sort_values(by=["YoY"], \
    ascending=False, na_position="last")["名稱代號"].tolist()
yoy_chg_p_rank = df_add_yoy_c[df_add_yoy_c["yoy_chg"] > 0].sort_values(by=["yoy_chg"], \
    ascending=False, na_position="last")["名稱代號"].tolist()
yoy_chg_n_rank = df_add_yoy_c[df_add_yoy_c["yoy_chg"] < 0].sort_values(by=["yoy_chg"], \
    ascending=True, na_position="last")["名稱代號"].tolist()

if not df_add_yoy_c[(df_add_yoy_c["上月YoY"] < 0) & (df_add_yoy_c["YoY"] > 0)].empty:
    df_yoy_n_to_p = df_add_yoy_c[(df_add_yoy_c["上月YoY"] < 0) & (df_add_yoy_c["YoY"] > 0)]
    yoy_n_to_p = df_yoy_n_to_p.sort_values(by=["yoy_chg"], ascending=False, na_position="last")["名稱代號"].tolist()
else:
    yoy_n_to_p = []

if not df_add_yoy_c[(df_add_yoy_c["上月YoY"] > 0) & (df_add_yoy_c["YoY"] < 0)].empty:
    df_yoy_p_to_n = df_add_yoy_c[(df_add_yoy_c["上月YoY"] > 0) & (df_add_yoy_c["YoY"] < 0)]
    yoy_p_to_n = df_yoy_p_to_n.sort_values(by=["yoy_chg"], ascending=True, na_position="last")["名稱代號"].tolist()
else:
    yoy_p_to_n = []

yoy_rank_list = [yoy_rank, list(reversed(yoy_rank)), yoy_chg_p_rank, yoy_chg_n_rank, yoy_n_to_p, yoy_p_to_n]
yoy_rank_str, yoy_rank_r_str, yoy_chg_p_rank_str, yoy_chg_n_rank_str, yoy_n_to_p_str, yoy_p_to_n_str = \
    [pickup_filter(x, 6) for x in yoy_rank_list]

# ranking by MoM
mom_rank_100_1000 = df_add_yoy_c[(df_add_yoy_c["MoM"] >= 100) & (df_add_yoy_c["MoM"] < 1000)].sort_values(by=["MoM"], \
    ascending=False, na_position="last")["名稱代號"].tolist()
mom_rank_50_100 = df_add_yoy_c[(df_add_yoy_c["MoM"] >= 50) & (df_add_yoy_c["MoM"] < 100)].sort_values(by=["MoM"], \
    ascending=False, na_position="last")["名稱代號"].tolist()
mom_rank_25_50= df_add_yoy_c[(df_add_yoy_c["MoM"] >= 25) & (df_add_yoy_c["MoM"] < 50)].sort_values(by=["MoM"], \
    ascending=False, na_position="last")["名稱代號"].tolist()
mom_rank_10_25= df_add_yoy_c[(df_add_yoy_c["MoM"] >= 10) & (df_add_yoy_c["MoM"] < 25)].sort_values(by=["MoM"], \
    ascending=False, na_position="last")["名稱代號"].tolist()

mom_rank_n_60 = df_add_yoy_c[df_add_yoy_c["MoM"] <= -60].sort_values(by=["MoM"], \
    ascending=True, na_position="last")["名稱代號"].tolist()
mom_rank_n_40_60 = df_add_yoy_c[(df_add_yoy_c["MoM"] <= -40) & (df_add_yoy_c["MoM"] > -60)].sort_values(by=["MoM"], \
    ascending=True, na_position="last")["名稱代號"].tolist()
mom_rank_n_20_40 = df_add_yoy_c[(df_add_yoy_c["MoM"] <= -20) & (df_add_yoy_c["MoM"] > -40)].sort_values(by=["MoM"], \
    ascending=True, na_position="last")["名稱代號"].tolist()

# mom_chg_p_rank = df_add_yoy_c[(df_add_yoy_c["mom_chg"] > 0) & (df_add_yoy_c["mom_chg"] < 1000)].sort_values(by=["mom_chg"], \
#     ascending=False, na_position="last")["名稱代號"].tolist()
# mom_chg_n_rank = df_add_yoy_c[(df_add_yoy_c["mom_chg"] < 0) & (df_add_yoy_c["mom_chg"] > -100)].sort_values(by=["mom_chg"], \
#     ascending=True, na_position="last")["名稱代號"].tolist()

mom_rank_list = [mom_rank_100_1000, mom_rank_50_100, mom_rank_25_50, mom_rank_10_25, \
    mom_rank_n_60, mom_rank_n_40_60, mom_rank_n_20_40]
mom_rank_100_1000_str, mom_rank_50_100_str, mom_rank_25_50_str, mom_rank_10_25_str, \
    mom_rank_n_60_str, mom_rank_n_40_60_str, mom_rank_n_20_40_str = [pickup_filter(x, 6) for x in mom_rank_list]

# ranking by MoM and YoY
m_y_rank_ss = df_add_yoy_c[(df_add_yoy_c["MoM"] >= 20) & \
    (df_add_yoy_c["YoY"] >= 1000)].sort_values(by=["MoM", "YoY"], \
    ascending=[False, False], na_position="last")["名稱代號"].tolist()
m_y_rank_s = df_add_yoy_c[(df_add_yoy_c["MoM"] >= 20) & \
    (df_add_yoy_c["YoY"] >= 50) & (df_add_yoy_c["YoY"] < 1000)].sort_values(by=["MoM", "YoY"], \
    ascending=[False, False], na_position="last")["名稱代號"].tolist()
m_y_rank_good = df_add_yoy_c[(df_add_yoy_c["MoM"] >= 10) & (df_add_yoy_c["MoM"] < 20) & \
    (df_add_yoy_c["YoY"] >= 30) & (df_add_yoy_c["YoY"] < 1000)].sort_values(by=["MoM", "YoY"], \
    ascending=[False, False], na_position="last")["名稱代號"].tolist()
m_y_rank_poor = df_add_yoy_c[(df_add_yoy_c["MoM"] <= -10) & (df_add_yoy_c["MoM"] > -20) & \
    (df_add_yoy_c["YoY"] <= -30)].sort_values(by=["MoM", "YoY"], \
    ascending=[True, True], na_position="last")["名稱代號"].tolist()
m_y_rank_hell = df_add_yoy_c[(df_add_yoy_c["MoM"] <= -20) & (df_add_yoy_c["MoM"] > -100) & \
    (df_add_yoy_c["YoY"] <= -50)].sort_values(by=["MoM", "YoY"], \
    ascending=[True, True], na_position="last")["名稱代號"].tolist()

m_y_rank_list = [m_y_rank_ss, m_y_rank_s, m_y_rank_good, m_y_rank_poor, m_y_rank_hell]
m_y_rank_ss_str, m_y_rank_s_str, m_y_rank_good_str, m_y_rank_poor_str, m_y_rank_hell_str = \
    [pickup_filter(x, 6) for x in m_y_rank_list]

# email with xlsx attachment
# config = _load_config()

update_time = datetime.datetime.strptime(add_list[-1].stem[-15:], "%Y-%m-%d-%H%M")
update_text = datetime.datetime.strftime(update_time, "%Y-%m-%d %H:%M")
if add_list[-1].stem[:13] != add_list[-2].stem[:13]:
    previous_text = "無"
else:
    previous_time = datetime.datetime.strptime(add_list[-2].stem[-15:], "%Y-%m-%d-%H%M")
    previous_text = datetime.datetime.strftime(previous_time, "%Y-%m-%d %H:%M")

mail_html_head ="""
<html>\
### 測試中<br/>\
閱讀性改善中，歡迎提供建議<br/>\
未來可能加上排除名單，避免沒用的上榜<br/>\
<br/>\
月營收: {}<br/>\
Y即YoY，M即MoM<br/>\
<br/>\
更新時間: {}<br/>\
上次更新時間: {}<br/>\
<br/>\
""".format(rev_m, update_text, previous_text)

mail_html_m_y="""
<span style="font-weight:bold;color:lightseagreen;">月年炸裂</span>: {}<br/><br/>\
<span style="font-weight:bold;color:lightseagreen;">月年雙噴</span>: {}<br/><br/>\
<span style="font-weight:bold;color:lightseagreen;">月年優</span>: {}<br/><br/>\
<span style="font-weight:bold;color:lightseagreen;">月年降</span>: {}<br/><br/>\
<span style="font-weight:bold;color:lightseagreen;">月年雙廢</span>: {}<br/><br/>\
<br/><br/>\
""".format(m_y_rank_ss_str, m_y_rank_s_str, m_y_rank_good_str, m_y_rank_poor_str, m_y_rank_hell_str)

mail_html_mom ="""
<span style="font-weight:bold;color:lightseagreen;">MoM破百</span>: {}<br/><br/>\
<span style="font-weight:bold;color:lightseagreen;">MoM 50%+</span>: {}<br/><br/>\
<span style="font-weight:bold;color:lightseagreen;">MoM 25%+</span>: {}<br/><br/>\
<span style="font-weight:bold;color:lightseagreen;">MoM 10%+</span>: {}<br/><br/>\
<span style="font-weight:bold;color:lightseagreen;">MoM -20%</span>: {}<br/><br/>\
<span style="font-weight:bold;color:lightseagreen;">MoM -40%</span>: {}<br/><br/>\
<span style="font-weight:bold;color:lightseagreen;">MoM廢</span>: {}<br/><br/>\
<br/><br/>\
""".format(mom_rank_100_1000_str, mom_rank_50_100_str, mom_rank_25_50_str, mom_rank_10_25_str, \
    mom_rank_n_20_40_str, mom_rank_n_40_60_str, mom_rank_n_60_str)

mail_html_yoy ="""
<span style="font-weight:bold;color:lightseagreen;">YoY優</span>: {}<br/><br/>\
<span style="font-weight:bold;color:lightseagreen;">YoY差</span>: {}<br/><br/>\
<br/><br/>\
<span style="font-weight:bold;color:lightseagreen;">YoY跳升</span>: {}<br/><br/>\
<span style="font-weight:bold;color:lightseagreen;">YoY下降</span>: {}<br/><br/>\
<br/><br/>\
<span style="font-weight:bold;color:lightseagreen;">YoY轉正</span>: {}<br/><br/>\
<span style="font-weight:bold;color:lightseagreen;">YoY轉負</span>: {}<br/><br/>\
<br/><br/>\
""".format(yoy_rank_str, yoy_rank_r_str, \
    yoy_chg_p_rank_str, yoy_chg_n_rank_str, yoy_n_to_p_str, yoy_p_to_n_str)

mail_html_end ="""
<br/>\
附件excel有兩個sheet，一個是新增，一個是累計公告<br/>\
"""

mail_html = mail_html_head + mail_html_m_y + mail_html_mom + mail_html_yoy + mail_html_end
soup_mail = BS(mail_html, "lxml")
mail_content = soup_mail.prettify()

xlsx_list = list(Path(xlsx_dir).iterdir())
mail(xlsx_list[-1])