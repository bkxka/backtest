# -*- coding: utf-8 -*-
"""
Created on Thu Sep 16 09:36:44 2021

@author: 好鱼
"""

import requests
import json
import random
import datetime as dt
import pandas as pd
import time

# from fake_useragent import UserAgent
# ua = UserAgent(verify_ssl=False)

from tools.tools_config import *
__warning__ = '如果导入超过1天的历史数据，则该交易日前的所有分钟的开盘价都为0，所以尽量做到每天更新' # warning！！！


# 获取实时的陆股通统计数据
def get_shszhk_realtime():
    ''' 获取实时的陆股通统计数据 '''
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    para_cb      = 'jQuery112307187800974662852_1613986338803'  # checkbox，HTML中的复选框
    para_fields1 = 'f1%2Cf2%2Cf3%2Cf4'                          # 查询字段1
    para_fields2 = 'f51%2Cf52%2Cf53%2Cf54%2Cf63'                # 查询字段2
    para_ut      = 'b2884a393a59ad64002292a3e90d46a5'           # 237310241054850368224806260804486820628
    para__       = '1613970338804'                              # 查询时间戳(秒)，不影响查询结果
    base_url = "http://push2.eastmoney.com/api/qt/kamt/get?"
    URL = base_url + "cb=" + para_cb + "&fields1=" + para_fields1 + "&fields2=" + para_fields2 + "&ut=" + para_ut + "&_=" + para__
    r = requests.get(URL, headers = headers)
    tmp_str = r.content.decode('utf-8')
    tmp_dic = tmp_str.split("(")[1].split(")")[0]
    result = json.loads(tmp_dic)['data']

    return result    
    

# 获取实时的北上资金净买入数据/亿元
def get_shszhk_netbuy():
    ''' 获取实时的北上资金净买入数据/亿元 '''
    
    rlt_dict_netbuy = get_shszhk_realtime()
    rlt_flt_netbuy  = rlt_dict_netbuy['hk2sh']['netBuyAmt']/10000 + rlt_dict_netbuy['hk2sz']['netBuyAmt']/10000
    return rlt_flt_netbuy
    

# 获取证券交易所编码
def get_exchange_code(str_ticker):
    ''' 获取证券交易所编码 '''
    tmp_exchange = 1
    if str_ticker.split('.')[1] == "SH":
        tmp_exchange = 1
    elif str_ticker.split('.')[1] == "SZ":
        tmp_exchange = 0
    elif str_ticker.split('.')[1] == "CFE":
        tmp_exchange = 8
    elif str_ticker.split('.')[1] == "HK":
        tmp_exchange = 116
    elif str_ticker.split('.')[1] == "US":
        tmp_exchange = 105
    elif str_ticker.split('.')[1] == "OF":
        if str_ticker[:2] == '51':
            tmp_exchange = 1
        elif str_ticker[:2] == '15':
            tmp_exchange = 0

    return tmp_exchange


# 获取最近一日的股票成交一分钟数据，支持股指、ETF基金查询
def get_price_realtime_1m(str_ticker, int_days=1):
    ''' 获取最近一日的股票成交一分钟数据,开,收,高,低,总手,总金额,当日累计成交均价 '''
    headers = {'User-Agent': random.choice(USER_AGENTS)}
 
    tmp_exchange = get_exchange_code(str_ticker)
    para_ticker  = str(tmp_exchange) + "." + str_ticker.split('.')[0]
    para_cb      = 'jQuery112406734237976539736_1615335275005'  # checkbox，HTML中的复选框
    para_fields1 = 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13' # 查询字段1
    para_fields2 = 'f51,f52,f53,f54,f55,f56,f57,f58'            # 查询字段2
    para_ut      = 'fa5fd1943c7b386f172d6893dbfba10b'           # 237310241054850368224806260804486820628
    para__       = '1613970338804'                              # 查询时间戳(秒)，不影响查询结果
    base_url = "http://push2his.eastmoney.com/api/qt/stock/trends2/get?"
    URL = base_url + "fields1=" + para_fields1 + "&fields2=" + para_fields2 + "&ut=" + para_ut + "&ndays=" + str(int_days)\
        + "&iscr=0" + "&secid=" + para_ticker + "&cb=" + para_cb + "&_=" + para__

    r = requests.get(URL, headers = headers)
    tmp_str = r.content.decode('utf-8')
    tmp_dic = tmp_str.split("(")[1].split(")")[0]
    result = json.loads(tmp_dic)['data']

    return result    


# 获取最近一日的股票成交一分钟数据，支持股指、ETF基金查询
def get_price_realtime_1m_df(str_ticker, int_days=1):
    ''' 获取最近一日的股票成交一分钟数据,开,收,高,低,总手,总金额,当日累计成交均价 '''
    
    # 注意最后几分钟可能是集合竞价，但是仍按照1分钟划分K线
    try:
        tmp_list  = get_price_realtime_1m(str_ticker, int_days)['trends']
        tmp_df    = pd.DataFrame(tmp_list)[0].str.split(',', expand=True)
        tmp_df[0] = tmp_df[0].apply(lambda x:dt.datetime.strptime(x, "%Y-%m-%d %H:%M"))
        tmp_df    = tmp_df.iloc[:,1:].rename(index=tmp_df[0])
        tmp_df    = tmp_df.applymap(lambda x:float(x))
        tmp_df.columns = ['open', 'close', 'high', 'low', 'lots', 'amount', 'vwap_addup']
        return tmp_df
    
    except:
        return None
    
    

# 获取最新的成交数据(最近一分钟的收盘价或最近交易日的收盘价)，支持股指查询
def get_price_realtime(str_ticker):
    ''' 获取最新的成交数据(最近一分钟的收盘价或最近交易日的收盘价)，支持股指查询 '''
    
    try:
        tmp_df_data = get_price_realtime_1m_df(str_ticker)
        return tmp_df_data['close'].iloc[-1]
    except:
        return None
    
    
    
# 获取最新的交易日期
def get_tradingDay_recent():
    ''' 获取最新的交易日期 '''
    
    try:
        tmp_data = get_price_realtime_1m('000300.SH')
        tmp_str_date = tmp_data['trends'][-1].split(",")[0]
        return dt.datetime.strptime(tmp_str_date, "%Y-%m-%d %H:%M")
    except:
        return None


# 获取公司公告列表/标题/发布时间等信息
def get_list_announce(str_ticker, page_index, page_size=100):
    ''' 获取公司公告列表/标题/发布时间等信息 '''
    
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    tmp_exchange = get_exchange_code(str_ticker)
    
    base_url  = 'http://np-anotice-stock.eastmoney.com/api/security/ann?'
    para_page = 'page_size=' + str(page_size) + '&page_index=' + str(page_index)
    para_cb  = '&cb=jQuery183026174794613872066_1636699031768&'
    para_ticker = 'market_stock_list=' + str(tmp_exchange) + "." + str_ticker.split('.')[0]
    para_type   = '&CodeType=1&_=1636699032164'
    
    URL = base_url + para_page + para_cb + para_ticker + para_type
    r = requests.get(URL, headers = headers)
    tmp_str = r.content.decode('utf-8')
    tmp_dic = tmp_str[len(tmp_str.split('(')[0])+1:-1]
    result = json.loads(tmp_dic)['data']

    return result


if False:
    for q in list_agent_add:
        headers = {'User-Agent': q}
        para_cb      = 'jQuery112307187800974662852_1613986338803'  # checkbox，HTML中的复选框
        para_fields1 = 'f1%2Cf2%2Cf3%2Cf4'                          # 查询字段1
        para_fields2 = 'f51%2Cf52%2Cf53%2Cf54%2Cf63'                # 查询字段2
        para_ut      = 'b2884a393a59ad64002292a3e90d46a5'           # 237310241054850368224806260804486820628
        para__       = '1613970338804'                              # 查询时间戳(秒)，不影响查询结果
        base_url = "http://push2.eastmoney.com/api/qt/kamt/get?"
        URL = base_url + "cb=" + para_cb + "&fields1=" + para_fields1 + "&fields2=" + para_fields2 + "&ut=" + para_ut + "&_=" + para__
        r = requests.get(URL, headers = headers)
        tmp_str = r.content.decode('utf-8')
        
        print("\n\n", q, '>>>====>>>',tmp_str)
        
    

potential_url = {
"多日分钟数据":"http://push2his.eastmoney.com/api/qt/stock/trends2/get?cb=jQuery1124028339551188973_1634094015427&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6%2Cf7%2Cf8%2Cf9%2Cf10%2Cf11%2Cf12%2Cf13&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58&ut=fa5fd1943c7b386f172d6893dbfba10b&ndays=5&iscr=0&iscca=0&secid=1.600519&_=1634094015554",
"港币/人民币汇率": "https://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/var%20_fx_shkdcny2021_3_11=/NewForexService.getDayKLine?symbol=fx_shkdcny&_=2021_3_11",
"日K线": "http://push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery112403024568536948198_1616898424824&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&ut=7eea3edcaed734bea9cbfc24409ed989&klt=101&fqt=1&secid=1.600066&beg=0&end=20500000&_=1616898424882",
"日K线": "http://push2.eastmoney.com/api/qt/stock/cqcx/get?id=SH600066&ut=e1e6871893c6386c5ff6967026016627&cb=jsonp1616899911324",
"日K线": "http://90.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery1124049848821488444317_1617171967804&secid=1.600066&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=101&fqt=0&end=20500101&lmt=120&_=1617171967868",
"公司新闻": "http://cmsdataapi.eastmoney.com/api/infomine?code=600066&marketType=1&types=1%2C2&startTime=2020-12-25&endTime=2021-03-26&format=yyyy-MM-dd&cb=jsonp1616900033139",
"股指期货": "http://push2.eastmoney.com/api/qt/stock/trends2/get?cb=jQuery1124042443897962606636_1617020740825&secid=8.041104&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6%2Cf7%2Cf8%2Cf9%2Cf10%2Cf11%2Cf12%2Cf13&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58&iscr=0&ndays=1&_=1617020740861",
"公告列表": "http://np-anotice-stock.eastmoney.com/api/security/ann?page_size=3&page_index=1&cb=jQuery183026174794613872066_1636699031768&market_stock_list=0.000002&CodeType=1&_=1636699032164",
}



if __name__ == "__main__":  # 当程序执行时
    
    pass


