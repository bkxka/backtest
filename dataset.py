# -*- coding: utf-8 -*-
"""
Created on Tue Nov 17 09:01:30 2020

@author: 王笃
"""

import pandas as pd
import numpy as np
from datetime import *
import os

from tools.tools_func import *

''' 短程序/处理函数 '''
if True:
    # 主板Main，中小板SME，创业板GME，科创板STAR
    get_board = lambda x:'STAR' if x>=688000 else('Main' if x>=600000 else('GEM' if x>=300000 else('SME' if x>=2000 else 'Main')))
                                                                        
                                                                    
''' 设置参数 '''
if True:
    par_str_start = '2005-01-04'                                        # 数据起始时间
    par_dt_start = str_to_time(par_str_start)
    # par_dt_today = datetime.now()-timedelta(days=1)                   # 更新到昨日
    # par_str_today = str(time_to_int(par_dt_today))
    par_str_path = "C:/InvestmentResearch/database"                     # 数据表的地址
    par_str_path_cb     = par_str_path + '/cb/'
    par_str_path_etf    = par_str_path + '/etf/'
    par_str_path_index  = par_str_path + '/index/'
    par_str_path_ticker = par_str_path + '/ticker/'
    par_str_path_price  = par_str_path + '/price/'
    par_str_path_report = par_str_path + '/report/'
    par_str_path_market = par_str_path + '/market/'
    # par_str_path_bulletin = par_str_path + '/bulletin/'
    par_str_path_moneyflow = par_str_path_price + 'moneyflow/'


''' 文件夹路径与文件名 '''
path = 'C:/InvestmentResearch/chiyeguang/'
path_moneyflow = path + 'moneyflow/moneyflow_'


''' 读取本地原始数据 '''
# 根据指标读取最新的数据文件，并对时间索引做标准化处理
def read_file(str_metric):
    ''' 根据指标读取最新的数据文件，并对时间索引做标准化处理 '''
    if str_metric[:3]=='000' or str_metric in ['index_close', 'CFE_close', 'CFE_vwap']:
        intm_list_files = os.listdir(par_str_path_index)
        intm_int_last_date = max([int(v.split("_")[0]) for v in intm_list_files if v[8:]=='_'+str_metric+'.csv'])
        intm_df_file = pd.read_csv(par_str_path_index+str(intm_int_last_date)+'_'+str_metric+'.csv', index_col=0)
        return df_index_time(intm_df_file)
    elif 'ticker' in str_metric:
        intm_list_files = os.listdir(par_str_path_ticker)
        intm_str_target_file = max([v for v in intm_list_files if str_metric in v])
        intm_df_file = pd.read_csv(par_str_path_ticker+intm_str_target_file, index_col=0)
        if 'cb' in str_metric:
            intm_df_file[['InterestDateBegin', 'InterestDateEnd', 'DateListing', 'DateRedeemNotice']] = \
            intm_df_file[['InterestDateBegin', 'InterestDateEnd', 'DateListing', 'DateRedeemNotice']].applymap(lambda x:str_to_time(x[:10]) if type(x)==str else x)
        if 'CFE' in str_metric:
            intm_df_file[['contract_issue_date', 'last_trade_date', 'last_delivery_month']] = \
            intm_df_file[['contract_issue_date', 'last_trade_date', 'last_delivery_month']].applymap(lambda x:str_to_time(x[:10]) if type(x)==str else x)
        if 'stock' in str_metric:
            intm_df_file['DateIPO']    = intm_df_file['DateIPO'].apply(lambda x:str_to_time(x[:10]) if type(x)==str else x)
            intm_df_file['DateDelist'] = intm_df_file['DateDelist'].apply(lambda x:str_to_time(x) if x!='0' else x)
            intm_df_file['DateDelist'][intm_df_file['DateDelist']=='0'] = np.nan
        return intm_df_file
    elif str_metric in ['open', 'high', 'low', 'close', 'hclose', 'vwap', 'adjfactor', 'mktcap', 'dayReturn', 'st', 'dayLimit', 'shszhkHold', 'amount', 'floatAmktcap', 'beta300', 'beta905']:
        intm_list_files = os.listdir(par_str_path_price)
        intm_int_last_date = max([int(v.split("_")[0]) for v in intm_list_files if v[8:]=='_price_'+str_metric+'.csv'])
        intm_df_file = pd.read_csv(par_str_path_price+str(intm_int_last_date)+'_price_'+str_metric+'.csv', index_col=0).fillna(0).rename(index=lambda x:str_to_time(x))
        return df_index_time(intm_df_file)
    elif 'cb_' in str_metric:
        intm_list_files = os.listdir(par_str_path_cb)
        intm_int_last_date = max([int(v.split("_")[0]) for v in intm_list_files if v[8:]=='_'+str_metric+'.csv'])
        intm_df_file = pd.read_csv(par_str_path_cb+str(intm_int_last_date)+'_'+str_metric+'.csv', index_col=0).fillna(0).rename(index=lambda x:str_to_time(x))
        return df_index_time(intm_df_file)
    elif str_metric in ['announceDate', 'netQProfit', 'netEquity', 'title']:
        intm_list_files = os.listdir(par_str_path_report)
        intm_int_last_date = max([int(v.split("_")[0]) for v in intm_list_files if v[8:]=='_report_'+str_metric+'.csv'])
        intm_df_file = pd.read_csv(par_str_path_report+str(intm_int_last_date)+'_report_'+str_metric+'.csv', index_col=0, low_memory=False, encoding='utf_8_sig')
        if str_metric == 'title':
            intm_df_file['notice_date'] = intm_df_file['notice_date'].apply(lambda x:str_to_time(x[:10]) if type(x)==str else x)
            return intm_df_file
        elif str_metric == 'announceDate':
            intm_df_file = intm_df_file.applymap(lambda x:str_to_time(x[:10]) if type(x)==str else x)
        return df_index_time(intm_df_file)
    elif str_metric in ['industry_sw', 'industry_wind']:
        intm_list_files = os.listdir(par_str_path_report)
        intm_int_last_date = max([int(v.split("_")[0]) for v in intm_list_files if v[8:]=='_'+str_metric+'.csv'])
        intm_df_file = pd.read_csv(par_str_path_report+str(intm_int_last_date)+'_'+str_metric+'.csv', index_col=0)
        return df_index_time(intm_df_file)
    elif str_metric in ['buyAmount_exlarge', 'sellAmount_exlarge', 'buyAmount_large', 'sellAmount_large', 
                        'buyAmount_middle',  'sellAmount_middle',  'buyAmount_small', 'sellAmount_small']:
        intm_list_files = os.listdir(par_str_path_moneyflow)
        intm_int_last_date = max([int(v.split("_")[0]) for v in intm_list_files if v[8:]=='_moneyflow_'+str_metric+'.csv'])
        intm_df_file = pd.read_csv(par_str_path_moneyflow+str(intm_int_last_date)+'_moneyflow_'+str_metric+'.csv', index_col=0).fillna(0).rename(index=lambda x:str_to_time(x))
        return df_index_time(intm_df_file)
    elif str_metric in ['etf_close', 'etf_netvalue', 'etf_list', 'etf_amount']:
        intm_list_files = os.listdir(par_str_path_etf)
        intm_int_last_date = max([int(v.split("_")[0]) for v in intm_list_files if v[8:]=='_'+str_metric+'.csv'])
        intm_df_file = pd.read_csv(par_str_path_etf+str(intm_int_last_date)+'_'+str_metric+'.csv', index_col=0).fillna(0)
        if str_metric == 'etf_list':
            return intm_df_file
        else:
            return df_index_time(intm_df_file)
    elif str_metric in ['shszhkFlow', 'insiderTrade', 'bonus', 'currency']:
        intm_list_files = os.listdir(par_str_path_market)
        intm_int_last_date = max([int(v.split("_")[0]) for v in intm_list_files if v[8:]=='_market_'+str_metric+'.csv'])
        intm_df_file = pd.read_csv(par_str_path_market+str(intm_int_last_date)+'_market_'+str_metric+'.csv', index_col=0, low_memory=False)
        if str_metric in ['shszhkFlow', 'currency']:
            return df_index_time(intm_df_file)
        if str_metric == 'insiderTrade':
            intm_df_file[['AnnounceDate', 'StartDate', 'EndDate']] = intm_df_file[['AnnounceDate', 'StartDate', 'EndDate']].applymap(lambda x:str_to_time(x[:10]) if type(x)==str else x)
            return intm_df_file
        if str_metric == 'bonus':
            list_column_dates = ['reporting_date', 'share_benchmark_date', 'dividends_announce_date', 'shareregister_date', 'exrights_exdividend_date', 'dividend_payment_date']
            intm_df_file[list_column_dates] = intm_df_file[list_column_dates].applymap(lambda x:str_to_time(x[:10]) if type(x)==str else x)
            return intm_df_file

    else:
        print(">>> 未找到所请求的文件 %s ..."%str_metric)
        return 0


# 导入指数的行情数据
def load_index(str_metric):
    ''' 导入指数的行情数据 '''
    return read_file(str_metric)


# 导入历史的交易日数据
def load_tradingDays():
    ''' 导入历史的交易日数据 '''
    tmp_df_index_close = load_index()
    return list(tmp_df_index_close.index)
        

# 导入股票代码列表
def load_tickers(str_metric='stock'):
    ''' 导入股票代码列表 '''
    return read_file('ticker_'+str_metric)


# 导入股票行情数据
def load_price(str_metric):
    ''' 导入股票行情数据 '''
    return read_file(str_metric)


# 获取最新的日期列表
def get_newest_date_list():
    ''' 读取最新的日期列表，基于现有的指数行情数据 '''
    intm_df_index_close = read_file('index_close')
    data_list_date = list(df_index_time(intm_df_index_close).index)
    
    return data_list_date


# 获取最新的股票代码和上市/退市日期
def get_newest_ticker_date():
    ''' 读取最新的股票列表，以及股票列表中的IPO/Delist日期信息 '''
    intm_df_ticker_list = ds.read_file('tickers')
    return intm_df_ticker_list


# 根据持仓变动计算净买入数据
def get_netBuy(data_df_hold, data_df_close, data_df_dayReturn):
    ''' 根据持仓变动计算净买入数据 '''
    data_df_hold_mktcap = data_df_hold * data_df_close
    data_df_hold_mktcap_old = data_df_hold_mktcap.shift(1) * (1 + data_df_dayReturn/100)
    data_df_netBuy = data_df_hold_mktcap - data_df_hold_mktcap_old
    
    return data_df_netBuy.fillna(0)


# 导入每只股票季度数据
def load_QResults(str_metric):
    ''' 导入每只股票季度数据 '''
    return read_file(str_metric)


# 导入每只股票的行业数据
def load_industry(str_metric, str_split):
    ''' 导入每只股票的行业数据 '''
    tmp_1 = read_file(str_metric).applymap(lambda x:x.split(str_split[0])[0] if x[0]!=str_split else x)
    tmp_2 = read_file(str_metric).applymap(lambda x:str_split.join(x.split(str_split)[:2]) if x[0]!=str_split else x)
    tmp_3 = read_file(str_metric).applymap(lambda x:str_split.join(x.split(str_split)[:3]) if x[0]!=str_split else x)
    tmp_4 = read_file(str_metric).applymap(lambda x:str_split.join(x.split(str_split)) if x[0]!=str_split else x)
    return [tmp_1, tmp_2, tmp_3, tmp_4] 


# # 将少量截面数据转变成全周期时序数据
# def cross_to_sequence(list_tradingDays, data_df_industry_raw):
#     ''' 将少量截面数据转变成全周期时序数据 '''
    
#     intm_df_industry = pd.DataFrame(0, index=list_tradingDays, columns=data_df_industry_raw.columns)
#     for u in data_df_industry_raw.index:
#         for q in data_df_industry_raw.columns:
#             intm_df_industry.loc[intm_df_industry.index>u, q] = data_df_industry_raw.loc[u, q]
#     intm_df_industry = intm_df_industry.loc[intm_df_industry.index>=data_df_industry_raw.index[0]]
        
#     return intm_df_industry


# 读取分钟数据
def read_minutes_price(str_path, str_ticker):
    ''' 读取分钟数据 '''
    tmp_list_files = os.listdir(str_path)
    tmp_str_file = [v for v in tmp_list_files if str_ticker in v]
    if len(tmp_str_file)==1:
        tmp_df_price = pd.read_csv(str_path+tmp_str_file[0], index_col=0, encoding='utf_8_sig')\
                         .rename(index=lambda x:datetime.strptime(x, "%Y-%m-%d")) 
        if len(tmp_df_price.columns)==237:
            return tmp_df_price
        else:
            print(">>> "+str_ticker+" 数据分时异常")
            return None
    else:
        print(">>> "+str_ticker+" 数据不存在")
        return None





if __name__=='__main__':
    
    pass


