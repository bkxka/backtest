# -*- coding: utf-8 -*-
"""
Created on Tue Nov 17 09:01:30 2020

@author: 王笃
"""

import pandas as pd
import numpy as np
from datetime import *
from openpyxl import Workbook, load_workbook
import os
import warnings

# import sys
# sys.path.append("C:\\InvestmentResearch")
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
    par_str_path_minute = par_str_path + '/minute/temp/'
    # par_str_path_bulletin = par_str_path + '/bulletin/'
    par_str_path_option = par_str_path + '/option/'
    par_str_path_moneyflow = par_str_path_price + 'moneyflow/'



''' 读取本地原始数据 '''
# 根据指标读取最新的数据文件，并对时间索引做标准化处理
def read_file(str_metric):
    ''' 根据指标读取最新的数据文件，并对时间索引做标准化处理 '''
    # 第一类：时间索引，不需处理数据字符
    # 第二类：时间索引，需要处理数据字符
    # 第三类：非时间索引，需要处理数据字符
    
    # 检索数据库所有文件，并提取出可能的对象（对于字段重复的，有特殊情况处理）
    list_files      = get_list_files("C:\\InvestmentResearch\\database")[0]
    list_files_cand = [v for v in list_files if str_metric in v]
    
    # 修正查询字段
    if str_metric in ['st', 'low', 'vwap', 'close', 'mktcap', 'amount', 'dayReturn']:
        list_files_cand = [v for v in list_files_cand if 'price_'+str_metric in v]
    if 'minu' not in str_metric:
        list_files_cand = [v for v in list_files_cand if 'minu' not in v]
    
    # 自检逻辑，如果读取到的文件名称相仿则为真，否则为假
    self_check, ii = True, 0
    while ii<len(list_files_cand)-1:
        self_check = self_check and list_files_cand[ii].split('_')[1:] == list_files_cand[ii+1].split('_')[1:]
        ii += 1
    
    if self_check:

        if str_metric[:10] in ['000300.SH_', '000905.SH_', '000852.SH_', '000832.CSI'] \
        or str_metric in ['index_close', 'CFE_close', 'etf_close', 'etf_netvalue', 'etf_amount',
                          'netQProfit', 'netQCashflowOper', 'netEquity', 'announceDate', 
                          'industry_sw', 'industry_wind',
                          'currency', 'shszhkFlow',
                          'open', 'high', 'low', 'close', 'hclose', 'vwap', 'adjfactor', 'mktcap', 'dayReturn', 'st', 
                          'dayLimit', 'shszhkHold', 'amount', 'floatAmktcap', 'beta300', 'beta905',
                          'buyAmount_exlarge', 'sellAmount_exlarge', 'buyAmount_large', 'sellAmount_large', 
                          'buyAmount_middle',  'sellAmount_middle',  'buyAmount_small', 'sellAmount_small']:
            
            intm_df_file = pd.read_csv(max(list_files_cand), index_col=0, encoding='utf_8_sig')
            if str_metric == 'announceDate':
                intm_df_file = intm_df_file.applymap(lambda x:str_to_time(x) if type(x)==str else x)
            intm_df_file = df_index_time(intm_df_file)
            
        elif str_metric in ['analyst_forecast', 'bonus', 'insiderTrade']\
        or   'ticker' in str_metric:

            intm_df_file = pd.read_csv(max(list_files_cand), index_col=0, encoding='utf_8_sig', low_memory=False)
            list_column_dates = []
            if str_metric == 'analyst_forecast':
                list_column_dates = ['last_rating_date', 'eps_date']
            if str_metric == 'bonus':
                list_column_dates = ['reporting_date', 'share_benchmark_date', 'dividends_announce_date', 'shareregister_date', 'exrights_exdividend_date', 'dividend_payment_date']
            if str_metric == 'insiderTrade':
                list_column_dates = ['AnnounceDate', 'StartDate', 'EndDate']
            if 'cb' in str_metric:
                list_column_dates = ['InterestDateBegin', 'InterestDateEnd', 'DateListing', 'DateRedeemNotice']
            if 'CFE' in str_metric:
                list_column_dates = ['contract_issue_date', 'last_trade_date', 'last_delivery_month']
            if 'option' in str_metric:
                list_column_dates = ['first_tradedate', 'last_tradedate']
            if 'stock' in str_metric:
                list_column_dates = ['DateIPO']
                intm_df_file['DateDelist'] = intm_df_file['DateDelist'].apply(lambda x:str_to_time(x) if x!='0' else x)
                intm_df_file['DateDelist'][intm_df_file['DateDelist']=='0'] = np.nan

            intm_df_file[list_column_dates] = intm_df_file[list_column_dates].applymap(lambda x:str_to_time(x) if type(x)==str else x)

        elif 'minu' in str_metric:

            intm_df_file = pd.read_csv(max(list_files_cand), index_col=0, encoding='utf_8_sig')
            intm_df_file = intm_df_file.rename(index=lambda x:dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
            
        # elif 'cb' in str_metric:
        else:
            
            intm_df_file = pd.read_csv(max(list_files_cand), index_col=0, encoding='utf_8_sig')
            intm_df_file = df_index_time(intm_df_file)
            

        return intm_df_file
    
    else:
        print(">>> 数据读取错误，请检查指标名称 %s 是否正确..."%str_metric)
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
    
    return data_df_netBuy


# 导入每只股票季度数据
def load_QResults(str_metric):
    ''' 导入每只股票季度数据 '''
    return read_file(str_metric)


# 导入每只股票的行业数据
def load_industry(str_metric, str_split='-'):
    ''' 导入每只股票的行业数据 '''
    tmp_file = read_file(str_metric)
    tmp_file = tmp_file.applymap(lambda x:x.split(str_split) if type(x)==str else np.nan)
    
    tmp1 = tmp_file.applymap(lambda x:x[0] if (type(x)==list and len(x)>=1) else np.nan)
    tmp2 = tmp_file.applymap(lambda x:str_split.join(x[:2]) if (type(x)==list and len(x)>=2) else np.nan)
    tmp3 = tmp_file.applymap(lambda x:str_split.join(x[:3]) if (type(x)==list and len(x)>=3) else np.nan)
    tmp4 = tmp_file.applymap(lambda x:str_split.join(x[:4]) if (type(x)==list and len(x)>=4) else np.nan)
    
    return [tmp1, tmp2, tmp3, tmp4] 


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


# 临时函数
def tmpfunc_inner_clean(df_data, list_timesep):
    ''' 临时函数 '''
    df_new = df_data.copy(deep=True)
    df_data['timesep'] = df_data.index
    df_data['timesep'] = df_data['timesep'].apply(lambda x:x.hour*100+x.minute)
    df_data = df_data[df_data['timesep'].isin(list_timesep)]
    del df_data['timesep']

    return df_data


# 读取分钟数据
def read_minute_data(str_path, list_ticker, list_date, list_timesep):
    '''
    读取分钟数据
    '''
    print(">>> reading data files...", str_hours(2))
    data_df_stock_minute = pd.DataFrame()
    for u in list_date:
        print(">>> processing", u, str_hours(2))
        tmp_df_minute = pd.read_csv(str_path+str(time_to_int(u))+'.csv', encoding='utf_8_sig').iloc[:,1:]
        tmp_df_minute = tmp_df_minute[tmp_df_minute['ticker'].isin(list_ticker)]
        # data_df_stock_minute = data_df_stock_minute.append(tmp_df_minute)
        data_df_stock_minute = pd.concat([data_df_stock_minute, tmp_df_minute], axis=0)

    data_df_stock_minute['timestamp'] = data_df_stock_minute['timestamp'].apply(lambda x:dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
    data_df_stock_minute['timesep']   = data_df_stock_minute['timestamp'].apply(lambda x:x.hour*100+x.minute)

    print(">>> reconstructing data...", str_hours(2))
    data_df_cb_close_minu    = pd.DataFrame()
    data_df_cb_avgprice_minu = pd.DataFrame()
    data_df_cb_amount_minu   = pd.DataFrame()
    
    for u in list_ticker:
        try:
            # print(">>> processing", u, str_hours(2))
            # 读取原始的分钟级数据，并将空缺的分钟线填补上收盘价数据
            # 需要注意，此处 tmp_df_cb 是股票/转债的分钟数据，源数据混杂了wind/taobao/eastmoney数据源
            tmp_df_cb = data_df_stock_minute[data_df_stock_minute.ticker==u].set_index('timestamp').sort_index(ascending=True)
            tmp_df_fill = tmp_df_cb['close'].fillna(method='pad')
            for str_metric in ['open', 'close', 'high', 'low']:
                tmp_df_cb[str_metric][tmp_df_cb['amount']<=0] = tmp_df_fill
                
            # 找出准确的时间分割点
            tmp_df_cb['flag'] = tmp_df_cb['timesep'].apply(lambda x:True if x in list_timesep else False)
            tmp_list_cut = list(sorted(tmp_df_cb[tmp_df_cb['flag']].index))

            # 按照标定的时间节点切分
            tmp_df_cb_sum              = df_cut_sum(tmp_df_cb, tmp_list_cut)
            tmp_df_cb_sum['avg_price'] = tmp_df_cb_sum['amount'] / tmp_df_cb_sum['volume']
            tmp_df_cb_sum              = tmp_df_cb_sum[['amount', 'avg_price']]
            tmp_df_cb_sum['close']     = tmp_df_cb['close'].loc[tmp_df_cb_sum.index]
            # tmp_df_cb_sum              = tmp_df_cb_sum.iloc[:-1]
            
            data_df_cb_close_minu    = pd.concat([data_df_cb_close_minu,    tmp_df_cb_sum[['close']].rename(    columns={'close':    u})], axis=1)
            data_df_cb_avgprice_minu = pd.concat([data_df_cb_avgprice_minu, tmp_df_cb_sum[['avg_price']].rename(columns={'avg_price':u})], axis=1)
            data_df_cb_amount_minu   = pd.concat([data_df_cb_amount_minu,   tmp_df_cb_sum[['amount']].rename(   columns={'amount':   u})], axis=1)
        except:
            data_df_cb_close_minu    = pd.concat([data_df_cb_close_minu,    pd.DataFrame(columns=[u])], axis=1)
            data_df_cb_avgprice_minu = pd.concat([data_df_cb_avgprice_minu, pd.DataFrame(columns=[u])], axis=1)
            data_df_cb_amount_minu   = pd.concat([data_df_cb_amount_minu,   pd.DataFrame(columns=[u])], axis=1)

    data_df_cb_close_minu    = tmpfunc_inner_clean(data_df_cb_close_minu,    list_timesep)
    data_df_cb_avgprice_minu = tmpfunc_inner_clean(data_df_cb_avgprice_minu, list_timesep)
    data_df_cb_amount_minu   = tmpfunc_inner_clean(data_df_cb_amount_minu,   list_timesep)
        
    print(">>> outputing data...", str_hours(2))
    return data_df_cb_close_minu, data_df_cb_avgprice_minu, data_df_cb_amount_minu


# 读取财务报表的某一指标,转换成ttm数据
def read_statement_ttm(metric, sheet, file_path, date_type='reportDate'):
    ''' 
    读取财务报表的某一指标,转换成ttm数据
    method(数据处理方式): stock存量，flow流量
    当出现多个指标符合输入参数时，选择最短的一个
    '''
    
    ticker = file_path.split('_')[1][:9]
    wb = load_workbook(file_path, data_only=True)
    df = pd.DataFrame(wb[sheet].values)
    l_targ = [v for v in list(df.iloc[:,0]) if metric in v]
    m_date =  [v for v in list(df.iloc[:,0]) if '公告日期' in v][0]
    
    if ticker!=df.iloc[0,1]:
        print("财报数据错误...")
        return None, df

    if len(l_targ)<1:
        print("指标读取错误...", metric, ticker)
        return None, df
    else:
        m_targ = l_targ[0]
        for u in l_targ:
            m_targ = u if len(u)<len(m_targ) else m_targ
        
    # slice = pd.concat([df.iloc[4:6,:].T, df[df[0]==m_targ].T, df[df[0]==m_date].T], axis=1).set_index(4)
    slice = pd.concat([df.iloc[4:6,:], df[df[0]==m_targ], df[df[0]==m_date]], axis=0)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        slice = slice.T.set_index(4)
    slice = slice.iloc[1:,:].rename(columns=lambda x:slice[x].iloc[0]).fillna(0)
    
    if sheet in ['利润表', '现金流量表']:
        
        slice['m_ttm'] = slice[slice.iloc[:,0]=='年报'][m_targ]
        # ttm流量计算方法：当季总量+去年总量-去年同期总量
        tmp_m_lastyear = slice['m_ttm'].fillna(method='backfill').shift(-1)
        tmp_m_lastquar = pd.Series(slice.index, index=slice.index)\
                           .apply(lambda x:x.replace(year=x.year-1))\
                           .apply(lambda x:slice.loc[x, m_targ] if x in slice.index else np.nan)
        tmp_m_ttm      = slice[m_targ] + tmp_m_lastyear - tmp_m_lastquar
        
        # 年报采用年报数据，非年报采用上述计算数据
        slice['m_ttm'] = slice['m_ttm'].fillna(0) + ((slice['m_ttm'] * 0).fillna(1) * tmp_m_ttm).fillna(0)
        slice['m_ttm'][slice['m_ttm']==0] = np.nan
        slice['m_ttm'] = slice['m_ttm'].fillna(method='backfill')
    
    elif sheet=='资产负债表':
        slice['m_ttm'] = slice[m_targ]
    
    # 不要使用annouceDate，因为wind导出的报表中的披露日期信息有误
    return slice, df
    # if date_type == 'annouceDate':
    #     return slice.set_index(m_date), df
    # elif date_type == 'reportDate':
    #     return slice, df
    # else:
    #     return None, None
    
    
    
    
    
    
    
    
if __name__=='__main__':
    
    pass


