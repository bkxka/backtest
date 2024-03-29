# -*- coding: utf-8 -*-
"""
Created on Thu Jul  8 11:49:50 2021

@author: 好鱼
"""
import datetime
import math
import numpy as np
import pandas as pd
from scipy.stats import linregress

from tools.tools_func import *

import dataset as ds
import option as op
import data_access as da

from data_access import eastmoney
from data_access.wind import *
from data_access import tonghuashun




''' 设置参数 '''
if True:
    par_str_start = '2005-01-04'                                        # 数据起始时间
    par_dt_start = str_to_time(par_str_start)
    # par_dt_today = datetime.now()                                       # 更新到今日（收盘后）
    par_dt_today = datetime.datetime.now()-datetime.timedelta(days=1)                     # 更新到昨日
    par_str_today = str(time_to_int(par_dt_today))
    par_str_path = "C:/InvestmentResearch/database"               # 本脚本的文件夹地址
    par_str_path_log       = par_str_path + '/log/'
    par_str_path_cb        = par_str_path + '/cb/'
    par_str_path_index     = par_str_path + '/index/'
    par_str_path_ticker    = par_str_path + '/ticker/'
    par_str_path_price     = par_str_path + '/price/'
    par_str_path_moneyflow = par_str_path_price + 'moneyflow/'
    par_str_path_report    = par_str_path + '/report/'
    par_str_path_etf       = par_str_path + '/etf/'
    par_str_path_market    = par_str_path + '/market/'
    par_dct_order_type = {'exlarge':'1', 'large':'2', 'middle':'3', 'small':'4'}
    par_dct_industry_type = {'industry_sw':'1', 'industry_wind':'2'}
    par_dct_shhk = {'sh_buy_amount':'SHHK_BUY_AMT', 'sh_sell_amount':'SHHK_SELL_AMT', 'hk_buy_amount':'HKSH_BUY_AMT', 'hk_sell_amount':'HKSH_SELL_AMT'}
    par_dct_szhk = {'sz_buy_amount':'SZHK_BUY_AMT', 'sz_sell_amount':'SZHK_SELL_AMT', 'hk_buy_amount':'HKSZ_BUY_AMT', 'hk_sell_amount':'HKSZ_SELL_AMT'}
    par_dct_cb   = {'valueStock':'convvalue', 'valueBond':'strbvalue', 'convPrice':'convprice', 'convDilution':'ldiluterate', 'impliedVol':'impliedvol'}
    par_list_nan = [np.nan, np.inf, -np.inf]
    par_int_vol_period = 30


# 保存更新日志到文件中
def log_msg(txt, print_option=True):
    
    print(str_hours(0)+' > '+txt) if print_option else None
    log_flag = par_str_path_log+str(time_to_int(datetime.datetime.now()))+".log"
    f = open(log_flag, "a", encoding='utf_8_sig')
    f.write(str_hours(0)+' > '+txt+'\n')
    f.close()
    return 0


# 重构 dataframe 数据
def get_df_dict(df_map, df_data):
    ''' 重构 dataframe 数据 '''
    df_result = df_data[df_map.iloc[:,0]]
    df_result.columns = df_map.index
    
    return df_result


# 获取更新的日期列表
def get_list_date_update(df_data):
    ''' 获取更新的日期列表 '''
    
    data_list_date = ds.get_newest_date_list()
    tmp_dt_max = max(df_data.index)
    intm_list_date_update = list(sorted([v for v in data_list_date if v>tmp_dt_max]))
    
    return intm_list_date_update


# 获取更新的股票代码列表
def get_list_stock_update(df_data):
    ''' 获取更新的股票代码列表 '''
    data_list_tickers = list(ds.load_tickers().index)
    return [v for v in data_list_tickers if v not in df_data.columns]


# 读取指定证券列表的某一日的行情数据    
def get_price_ticker_list(str_metric, str_date, list_tickers, int_piece=100):
    ''' 读取指定证券列表的某一日的行情数据 '''    
    
    # 第一次读取数据
    intm_df_price = pd.DataFrame()
    list_tickers_piece = get_ticker_pieces(list_tickers, int_piece)
    for u in list_tickers_piece:
        tmp_raw_price = wind_func_wss(u, str_metric, str_date)
        tmp_df_price = pd.DataFrame(tmp_raw_price.Data[0],columns=[str_date],index=tmp_raw_price.Codes)
        intm_df_price = pd.concat([intm_df_price, tmp_df_price], axis=0)
        
    # 判断读数是否正常, 若有异常则再次读数：
    while (len(intm_df_price.index) > len(list_tickers)):
        intm_df_error = intm_df_price.loc[~intm_df_price.index.isin(list_tickers),:]
        log_msg('日行情读取异常，异常原因：%s'%str(intm_df_error.iloc[0,0]))
        intm_df_price = intm_df_price.loc[list_tickers,:]
        if intm_df_price.isnull().any():
            intm_df_price_normal = intm_df_price.dropna()
            intm_df_price_abnormal = intm_df_price.loc[~intm_df_price.index.isin(intm_df_price_normal.index)]
            intm_df_price_abnormal = get_price_ticker_list(str_metric, str_date, list(intm_df_price_abnormal.index), int_piece)
            intm_df_price = pd.concat([intm_df_price_normal, intm_df_price_abnormal], axis=0)
        else:
            pass
        
    return intm_df_price


# 更新全市场证券的行情信息
def update_price(str_metric, str_path):
    ''' 更新全市场证券的行情信息 '''
    
    # 读取现有的股票数据
    log_msg('开始处理 %s 行情数据...'%str_metric)
    intm_df_price = ds.read_file(str_metric)
    if 'cb' in str_metric:
        intm_list_tickers = list(ds.read_file('ticker_cb').index)
    # 固定证券代码列表，无需更新
    elif (str_metric in ['hclose', 'CFE_close', 'fund_dayReturn']) or ('etf' in str_metric):
        intm_list_tickers = list(intm_df_price.columns)
    else:
        intm_list_tickers = list(ds.load_tickers().index)
    
    # 根据新增日期更新价格数据【现存数据已经是当下最全的数据，所有新增证券都不需要更早的历史数据】
    intm_list_date_update = get_list_date_update(intm_df_price)
    intm_list_date_update = intm_list_date_update[:-3] if str_metric=='fund_dayReturn' else intm_list_date_update
    if len(intm_list_date_update) == 0:
        log_msg('%s 数据已是最新的'%str_metric)
    else:
        data_df_price_update = pd.DataFrame()
        for u in intm_list_date_update:
            str_time = str(time_to_int(u))
            log_msg('正在提取 %s 的 %s 数据'%(str_time, str_metric))
            tmp_df_price_update = get_price_ticker_list(str_metric, str_time, intm_list_tickers, 50).T
            tmp_df_price_update.index = [u]
            data_df_price_update = pd.concat([data_df_price_update, tmp_df_price_update], axis=0)
        # 读取成功的数据拼贴到原有数据上
        intm_df_price = df_rows_dedu(pd.concat([intm_df_price,data_df_price_update], axis=0))

    # 数据清洗整理
    intm_df_price = intm_df_price.fillna(0)
    if ('cb' in str_metric) or ('etf' in str_metric) or (str_metric in ['hclose', 'CFE_close', 'fund_dayReturn']):
        pass                                                # 港股/ETF/cb：不需要做特殊处理
    else:
        # 剔除不符合要求的列名，并将nan替换为0; st数据较为特殊，nan保存为-99(st数据不在本函数内处理)
        intm_df_price = intm_df_price[[v for v in intm_df_price.columns if v[0] in ['0', '3', '6']]]
        # 特殊处理：沪港通、深港通数据因两地交易日不一致所导致的数据缺失
        if str_metric=='shszhkHold':
            for ii in range(len(intm_df_price)-250, len(intm_df_price)):
                if intm_df_price.iloc[ii].sum()<=0:
                    log_msg('填补 %s 缺失的持仓数据...'%str(time_to_int(intm_df_price.index[ii])))
                    intm_df_price.iloc[ii] = intm_df_price.iloc[ii-1]
                
    # 保存数据:注意资金流数据与普通日行情数据有这不同的存储路径
    if ('buyAmount' in str_metric or 'sellAmount' in str_metric):
        tmp_str_fileName = str(time_to_int(max(intm_df_price.index)))+'_moneyflow_'+str_metric+'.csv'
    elif ('cb_' in str_metric) or ('etf_' in str_metric) or (str_metric == 'CFE_close') or (str_metric == 'fund_dayReturn'):
        tmp_str_fileName = str(time_to_int(max(intm_df_price.index)))+'_'+str_metric+'.csv'
    else:
        tmp_str_fileName = str(time_to_int(max(intm_df_price.index)))+'_price_'+str_metric+'.csv'

    intm_df_price.to_csv(str_path+tmp_str_fileName, encoding='utf_8_sig')
    log_msg('成功保存 %s 行情数据...'%str_metric)
        
    return 0


# 获取最新的指数收盘价
def update_index_close(str_today):
    ''' 更新指数点位数据 '''
    # 读取文件列表
    log_msg("开始更新指数收盘价格数据...")
    intm_df_index_close = ds.read_file('index_close')
                                    
    # 读取现有的指数收盘价数据和日期信息
    # intm_df_index_close = time_index_df(intm_df_index_close)
    intm_int_end_date = time_to_int(max(intm_df_index_close.index[:-1]))
    intm_str_end_date = int_to_str(intm_int_end_date)
    intm_list_index = list(intm_df_index_close.columns)
    
    # 判断是否需要更新
    if str_today<=intm_str_end_date:
        log_msg("指数收盘价已是最新数据...")
        return 0
    else:
        # 调用wind接口，更新指数收盘价数据
        tmp_list_index = ','.join(intm_list_index)
        data_raw_index_close = w.wsd(tmp_list_index, "close", intm_str_end_date, str_today, "")
        if data_raw_index_close.ErrorCode!=0:
            log_msg("指数收盘价数据无更新，请检查代码...")
            log_msg(data_raw_index_close.Data[0])
        else:
            # 注意，只有一日和有多日所返回的数据结构不完全一致；为了函数结构简单，我们多调用一天的数据（重复调用已有数据的最后一日）
            data_df_index_close_new = pd.DataFrame(data_raw_index_close.Data, columns=data_raw_index_close.Times, index=data_raw_index_close.Codes).T
            data_df_index_close_new = df_index_time(data_df_index_close_new)
        data_df_index_close = df_rows_dedu(pd.concat([intm_df_index_close, data_df_index_close_new], axis=0))
        data_df_index_close.to_csv(par_str_path_index+str(time_to_int(max(data_df_index_close.index)))+'_index_close.csv', encoding='utf_8_sig')
            
        log_msg("成功更新指数收盘价格数据...")
        return 0


# 更新每日的陆港通双向资金流入信息
def update_market_shszhkFlow(str_path):
    ''' 更新每日的陆港通双向资金流入信息 '''
    # 读取现有的行情数据
    str_metric = 'shszhkFlow'
    log_msg("开始处理 %s 数据..."%str_metric)
    intm_df_price = ds.read_file(str_metric)
    intm_list_tickers = list(intm_df_price.columns)
    
    # 根据新增加的日期更新价格数据
    intm_list_date_update = get_list_date_update(intm_df_price)
    
    if len(intm_list_date_update) == 0:
        log_msg("%s 数据已是最新的..."%str_metric)
    else:
        str_start_date = time_to_str(min(intm_list_date_update))
        str_end_date   = time_to_str(max(intm_list_date_update))
        tmp_raw_shhk = wind_func_wset('shhkFlow', str_start_date, str_end_date)
        tmp_raw_szhk = wind_func_wset('szhkFlow', str_start_date, str_end_date)
        if tmp_raw_shhk.Data!=[] or tmp_raw_szhk.Data!=[]:
            tmp_df_shhk = pd.DataFrame(tmp_raw_shhk.Data[1:], columns=tmp_raw_shhk.Data[0], index=tmp_raw_shhk.Fields[1:])\
                            .T.sort_index(ascending=True).fillna(0)[['sh_buy_amount', 'sh_sell_amount', 'hk_buy_amount', 'hk_sell_amount']]\
                            .rename(columns = par_dct_shhk)
            tmp_df_szhk = pd.DataFrame(tmp_raw_szhk.Data[1:], columns=tmp_raw_szhk.Data[0], index=tmp_raw_szhk.Fields[1:])\
                            .T.sort_index(ascending=True).fillna(0)[['sz_buy_amount', 'sz_sell_amount', 'hk_buy_amount', 'hk_sell_amount']]\
                            .rename(columns = par_dct_szhk)
            tmp_df_shszhk = df_index_time(pd.concat([tmp_df_shhk, tmp_df_szhk], axis=1))
        
            # 读取成功的数据拼贴到原有数据上
            intm_df_price = df_rows_dedu(pd.concat([intm_df_price, tmp_df_shszhk], axis=0))

    # 剔除不符合要求的列名，并将nan替换为0
    intm_df_price = intm_df_price.fillna(0)
                
    # 保存数据:注意资金流数据与普通日行情数据有这不同的存储路径
    log_msg("成功保存 %s 数据..."%str_metric)
    intm_df_price.to_csv(str_path+str(time_to_int(max(intm_df_price.index)))+'_market_'+str_metric+'.csv', encoding='utf_8_sig')
        
    return 0


# 更新delta数据
def update_delta(str_period, str_path):
    ''' 更新delta数据 '''

    # 读取本地的历史数据
    str_metric = 'cb_delta' + str_period
    log_msg("开始处理 %s 行情数据..."%str_metric)
    intm_df_delta = ds.read_file(str_metric)
    np.seterr(invalid='ignore')

    # 读取日期和cb代码数据
    intm_df_blank        = ds.read_file('cb_close').applymap(lambda x:0)
    intm_df_cb_ticker    = ds.read_file('ticker_cb')
    intm_list_ticker_update = [v for v in intm_df_cb_ticker.index if v not in intm_df_delta.columns]
    intm_list_date_update   = get_list_date_update(intm_df_delta)

    # 读取计算delta所需的参数
    intm_df_strike_price = ds.read_file('cb_convPrice')
    intm_df_riskfree     = intm_df_blank.copy(deep=True).applymap(lambda x:0.01)
    data_df_tickers_cb   = ds.read_file("ticker_cb")
    intm_df_close_stock  = get_df_dict(data_df_tickers_cb[['StockTicker']], ds.load_price('close'))

    # 计算波动率参数【设定为最近30个交易日】
    if str_period == 'Gar':
        intm_df_vola_stock  = ds.read_file('cb_garchVol') * math.sqrt(250)
    else:
        intm_df_dailyReturn = get_df_dict(data_df_tickers_cb[['StockTicker']], ds.load_price('dayReturn')) / 100
        intm_df_flag_stock  = get_df_dict(data_df_tickers_cb[['StockTicker']], ds.read_file('amount')).applymap(lambda x:1 if x>0 else 0)
        intm_df_vola_stock = pd.DataFrame()
        for u in data_df_tickers_cb.index:
            tmp_df_returns = pd.concat([intm_df_flag_stock[u], intm_df_dailyReturn[u]], axis=1)
            tmp_df_returns = tmp_df_returns[tmp_df_returns.iloc[:,0]>0]
            tmp_df_vola_stock = tmp_df_returns.iloc[:,1].rolling(window=int(str_period)).std()
            intm_df_vola_stock = pd.concat([intm_df_vola_stock, tmp_df_vola_stock], axis=1)
        # 向后填充空缺值
        intm_df_vola_stock = intm_df_vola_stock.sort_index(ascending=True).fillna(method='pad').fillna(0).loc[intm_df_blank.index] * math.sqrt(250)
    
    
    
    # 计算maturity参数
    intm_df_maturity = intm_df_blank.copy(deep=True).loc[intm_list_date_update]
    for u in intm_df_maturity.index:
        intm_df_maturity.loc[u] = (intm_df_cb_ticker[['InterestDateEnd']].T.iloc[0] - u).apply(lambda x:x.days/365)
    intm_df_maturity[intm_df_maturity<=0] = 0
    
    # 计算更新日期的delta数据
    if len(intm_list_date_update) == 0:
        log_msg(" %s 数据已是最新的..."%str_metric)
    else:
        data_df_price_update = pd.DataFrame(0, index=intm_list_date_update, columns=intm_df_cb_ticker.index)
        for u in intm_list_date_update:
            str_time = str(time_to_int(u))
            log_msg("正在提取 %s 的 %s 数据..."%(str_time, str_metric))
            for q in data_df_price_update.columns:
                if intm_df_close_stock.loc[u, q] * intm_df_strike_price.loc[u, q] * intm_df_maturity.loc[u, q] * intm_df_riskfree.loc[u, q] * intm_df_vola_stock.loc[u, q]<=0:
                       data_df_price_update.loc[u, q] = 0
                else:
                    data_df_price_update.loc[u, q] = op.greek.delta('c', 
                        intm_df_close_stock.loc[u, q], intm_df_strike_price.loc[u, q], intm_df_maturity.loc[u, q], 
                        intm_df_riskfree.loc[u, q],    intm_df_vola_stock.loc[u, q])

        intm_df_delta = pd.concat([intm_df_delta, data_df_price_update], axis=0).sort_index().fillna(0)

    tmp_str_fileName = str(time_to_int(max(intm_df_delta.index)))+'_'+str_metric+'.csv'
    intm_df_delta.to_csv(str_path+tmp_str_fileName, encoding='utf_8_sig')
    log_msg("成功保存 %s 行情数据..."%str_metric)

    return 0


# 更新garch模型预测的波动率
def update_garch_vol(str_path):
    ''' 更新garch模型预测的波动率 '''
    
    str_metric = 'cb_garchVol'
    np.seterr(invalid='ignore')
    log_msg("开始处理 %s 行情数据..."%str_metric)
    
    # 读取基础数据
    data_df_tickers_cb  = ds.read_file("ticker_cb")
    data_df_dailyReturn = get_df_dict(data_df_tickers_cb[['StockTicker']], ds.load_price('dayReturn'))/100
    data_df_tradeFlag   = get_df_dict(data_df_tickers_cb[['StockTicker']], ds.load_price('amount')).applymap(lambda x:1 if x>1 else 0)
    # 对应的两只股票均为借壳上市，剔除掉借壳上市之前及之后一段时间的数据
    data_df_tradeFlag.loc[data_df_tradeFlag.index<datetime.datetime(2008,12,31), '127005.SZ'] = 0
    data_df_tradeFlag.loc[data_df_tradeFlag.index<datetime.datetime(2010,12,31), '110042.SH'] = 0
    data_df_close_cb    = ds.read_file("cb_close")
    data_df_garchVol    = ds.read_file(str_metric)
    
    # 新增日期和新增转债
    intm_list_date_update   = get_list_date_update(data_df_garchVol)

    # 折算的剩余到期日
    data_df_maturity   = pd.DataFrame(0, index=intm_list_date_update, columns=data_df_tickers_cb.index).sort_index(ascending=True)
    for u in data_df_maturity.index:
        data_df_maturity.loc[u] = (data_df_tickers_cb[['InterestDateEnd']].T.iloc[0] - u).apply(lambda x:x.days/365)
    data_df_maturity    = data_df_maturity.applymap(lambda x:int(x*243) if x>0 else 0)
    for u in data_df_tickers_cb.index:
        data_df_maturity[u].loc[data_df_maturity.index<data_df_tickers_cb.loc[u, 'DateListing']] = 0

    # 计算更新日期的delta数据
    if len(intm_list_date_update) == 0:
        log_msg(" %s 数据已是最新的..."%str_metric)
    else:
        intm_df_garchVol = data_df_maturity.copy(deep=True).applymap(lambda x:0)
        # 更新波动率数据
        for u in intm_df_garchVol.index:
            str_time = str(time_to_int(u))
            log_msg("正在提取 %s 的 %s 数据..."%(str_time, 'garchVol'))
            tmp_df_return    = data_df_dailyReturn.loc[data_df_dailyReturn.index<u]
            tmp_df_tradeFlag = data_df_tradeFlag.loc[:u]
            
            for q in intm_df_garchVol.columns:
                tmp_df_return_prev = tmp_df_return[q][tmp_df_tradeFlag[q]>0]
                try:
                    tmp_df_return_prev = tmp_df_return_prev.iloc[20:]
                except:
                    tmp_df_return_prev = None
                    
                tmp_int_maturity = data_df_maturity.loc[u, q]
                if tmp_int_maturity>0:
                    intm_df_garchVol.loc[u, q] = op.garch_forecast_vol(tmp_df_return_prev, tmp_int_maturity)
                
        data_df_garchVol = pd.concat([data_df_garchVol, intm_df_garchVol], axis=0)
    
    tmp_str_fileName = str(time_to_int(max(data_df_garchVol.index)))+'_'+str_metric+'.csv'
    data_df_garchVol.to_csv(str_path+tmp_str_fileName, encoding='utf_8_sig')
    log_msg("成功保存 %s 行情数据..."%str_metric)
    
    return 0


# 更新beta数据
def update_beta(str_metric, period, str_path):
    ''' 更新beta数据 '''
    
    data_df_beta = ds.read_file(str_metric)
    log_msg("开始处理 %s 行情数据..."%str_metric)
    
    # 读取基础数据
    data_df_tickers        = ds.load_tickers()
    data_df_dailyReturn    = ds.read_file('dayReturn') / 100
    data_df_tradeFlag      = ds.read_file('amount').applymap(lambda x:1 if x>0 else 0)
    data_df_index_close    = ds.load_index("index_close")
    data_df_dailyReturn_index = ((data_df_index_close / data_df_index_close.shift(1)) - 1)
    data_df_dailyReturn_index = data_df_dailyReturn_index[['000905.SH']] if str_metric=='beta905' else\
                                data_df_dailyReturn_index[['000300.SH']] if str_metric=='beta300' else None
    
    # 新增日期和新增转债
    intm_list_date_update = get_list_date_update(data_df_beta)
    
    # 计算更新日期的beta数据
    if len(intm_list_date_update) == 0:
        log_msg(" %s 数据已是最新的..."%str_metric)
    else:
        # 更新beta数据
        intm_df_beta = pd.DataFrame(index=intm_list_date_update, columns=data_df_tickers.index)
        for u in intm_list_date_update:
            str_time = str(time_to_int(u))
            log_msg("正在提取 %s 的 %s 数据..."%(str_time, str_metric))
            for q in intm_df_beta.columns:
                tmp_df_input = pd.concat([data_df_dailyReturn_index, data_df_dailyReturn[q], data_df_tradeFlag[q]], axis=1)
                tmp_df_input = tmp_df_input[(tmp_df_input.iloc[:,2]>0) & (tmp_df_input.index<=u)].dropna()
                if len(tmp_df_input)>120:
                    res = linregress(tmp_df_input.iloc[-period:, 0], tmp_df_input.iloc[-period:, 1])
                    intm_df_beta.loc[u, q] = int(100*res.slope)
                else:
                    intm_df_beta.loc[u, q] = 0
        data_df_beta = pd.concat([data_df_beta, intm_df_beta], axis=0).fillna(0)

    tmp_str_fileName = str(time_to_int(max(data_df_beta.index)))+'_price_'+str_metric+'.csv'
    data_df_beta.to_csv(str_path+tmp_str_fileName, encoding='utf_8_sig')
    log_msg("成功保存 %s 行情数据..."%str_metric)
    return 0


# 更新红利信息
def update_market_bonus(str_path):
    ''' 更新红利信息 '''
    str_metric = 'bonus'
    log_msg("更新%s数据..."%str_metric)

    # 提取应更新的日期信息:最近三个季度加上下一个季度
    tmp_df_data = ds.read_file(str_metric)
    tmp_list_quarters = list(sorted(set(tmp_df_data['reporting_date'])))
    tmp_list_quarters = tmp_list_quarters[-3:] + [next_quarter(max(tmp_list_quarters))]

    for u in tmp_list_quarters:
        log_msg("正在提取 %s 的 %s 数据..."%(time_to_str(u), str_metric))
        tmp_df_data = tmp_df_data[tmp_df_data['reporting_date']!=u]
        tmp_raw_data_update = wind_func_wset(str_metric, time_to_str(u), time_to_str(u))
        tmp_df_data_update = pd.DataFrame(tmp_raw_data_update.Data, index=tmp_raw_data_update.Fields).T
        tmp_df_data = pd.concat([tmp_df_data, tmp_df_data_update], axis=0)
        
    tmp_df_data = tmp_df_data.sort_values(by='reporting_date')
    tmp_df_data.index = range(len(tmp_df_data))
    tmp_str_file_name = str(time_to_int(max(tmp_df_data['reporting_date'])))+'_report_bonus.csv'
    tmp_df_data.to_csv(str_path+tmp_str_file_name, encoding='utf_8_sig')
    log_msg("成功保存 %s 行情数据..."%str_metric)
    return 0


# 更新汇率数据
def update_market_currency(str_path):
    ''' 更新汇率数据 '''
    str_metric = 'currency'
    log_msg("更新%s数据..."%str_metric)
    
    intm_df_data = ds.read_file(str_metric)
    tmp_newest_dates = max(ds.get_newest_date_list())
    str_list_tickers = get_ticker_pieces(list(intm_df_data.columns))[0]
    tmp_list_dates = [max(intm_df_data.index)+datetime.timedelta(days=ii+1) for ii in range((tmp_newest_dates - max(intm_df_data.index)).days)]
   
    if len(tmp_list_dates) == 0:
        pass
    else:
        for u in tmp_list_dates:
            tmp_raw_currency = wind_func_wss(str_list_tickers, 'close', str(time_to_int(u)))
            tmp_df_currency = pd.DataFrame(tmp_raw_currency.Data[0], index=tmp_raw_currency.Codes, columns=[u])
            intm_df_data = pd.concat([intm_df_data, tmp_df_currency.T], axis=0)
    
    # 保存数据:注意资金流数据与普通日行情数据有这不同的存储路径
    tmp_str_file_name = str(time_to_int(max(intm_df_data.index)))+'_market_currency.csv'
    intm_df_data.to_csv(str_path+tmp_str_file_name, encoding='utf_8_sig')
    log_msg("成功保存 %s 行情数据..."%str_metric)
    return 0


# 更新每日的内部交易者信息
def update_market_insiderTrade(str_path):
    ''' 更新每日的内部交易者信息 '''
    
    # 读取现有的行情数据
    str_metric = 'insiderTrade'
    log_msg("开始更新 %s 数据..."%str_metric)
    intm_df_price = ds.read_file(str_metric)
    
    # 根据新增加的日期更新价格数据
    tmp_date_begin = intm_df_price['AnnounceDate'].max() + datetime.timedelta(days=1)
    tmp_date_end = max(ds.get_newest_date_list())

    if tmp_date_end < tmp_date_begin:
        log_msg("%s 数据已是最新的..."%str_metric)
    else:
        str_start_date = time_to_str(tmp_date_begin)
        str_end_date   = time_to_str(tmp_date_end)
        tmp_raw_data = wind_func_wset('insiderTrade', str_start_date, str_end_date)
        tmp_df_data = pd.DataFrame(tmp_raw_data.Data, index=tmp_raw_data.Fields).T[[
            'wind_code', 'announcement_date', 'change_start_date', 'change_end_date', 'shareholder_type', 'relationship', 
            'direction', 'change_number', 'change_number_big', 'change_proportion_floating', 'change_proportin_total', 
            'value_change', 'is_sell_restricted']]
        tmp_df_data.columns = intm_df_price.columns
        
        # 将缺失起始日期的空格填充为终止日期
        tmp_df_data_lost = tmp_df_data[(tmp_df_data['StartDate'] > datetime.datetime(1999,1,1))==False]
        tmp_df_data.loc[tmp_df_data_lost.index, 'StartDate'] = tmp_df_data_lost['EndDate'].copy()
        # 读取成功的数据拼贴到原有数据上
        intm_df_price = pd.concat([intm_df_price, tmp_df_data], axis=0).sort_values(by='AnnounceDate', ascending=True)
        intm_df_price.index = range(len(intm_df_price))
                
    # 保存数据:注意资金流数据与普通日行情数据有这不同的存储路径
    intm_df_price.to_csv(str_path+str(time_to_int(max(intm_df_price['AnnounceDate'])))+'_report_'+str_metric+'.csv', encoding='utf_8_sig')
    log_msg("成功保存 %s 行情数据..."%str_metric)
    return 0


# 更新某期的全部公告数据
def update_reports_quarter(str_metric, df_reports_input, dt_quarter):
    ''' 更新某期的全部公告数据 '''

    df_reports = df_reports_input.copy(deep=True)
    data_df_report_undisclose = df_reports.loc[dt_quarter].T
    data_df_report_undisclose = data_df_report_undisclose[data_df_report_undisclose.isnull()]
    data_list_undisclose = get_ticker_pieces(list(data_df_report_undisclose.index), 100)
    
    data_df_report_date = pd.DataFrame()
    for u in data_list_undisclose:
        tmp_raw_report_date_wanke = wind_func_wss(u, str_metric, str(time_to_int(dt_quarter)))
        tmp_df_report_date_wanke = pd.DataFrame(tmp_raw_report_date_wanke.Data[0], index=tmp_raw_report_date_wanke.Codes, columns=[dt_quarter])
        data_df_report_date = pd.concat([data_df_report_date, tmp_df_report_date_wanke], axis=0)
    
    df_reports.loc[dt_quarter, data_df_report_date.index] = (data_df_report_date.T).loc[dt_quarter, data_df_report_date.index]

    return df_reports


# 更新分析师预测
def update_analyst_forecast(str_path):
    ''' 更新分析师预测 '''
    
    log_msg("开始处理 %s 数据..."%"forecast")
    intm_df_data = ds.read_file("analyst_forecast")
    intm_list_new_days = [max(intm_df_data.last_rating_date)+dt.timedelta(days=ii) for ii in range(1, 
                          (max(ds.get_newest_date_list()) - max(intm_df_data.last_rating_date)).days+1)]
    
    for u in intm_list_new_days:
        
        log_msg("更新 %s 数据..."%time_to_str(u))
        intm_raw_forecast = wind_func_wset("analyst_forecast", time_to_str(u), str(u.year))
        intm_df_forecast = pd.DataFrame(intm_raw_forecast.Data,index=intm_raw_forecast.Fields).T
        intm_df_forecast['rating_year'] = u.year
        intm_df_forecast['counts'] = intm_df_forecast.count(axis=1)
    
        # 若处于密集报告发布期，则选两个预测期对照，找到预测期对齐的分析师预测
        if u.month<=4:
            intm_raw_forecast_lastyear = wind_func_wset("analyst_forecast", time_to_str(u), str(u.year-1))
            intm_df_forecast_lastyear = pd.DataFrame(intm_raw_forecast_lastyear.Data,index=intm_raw_forecast_lastyear.Fields).T
            intm_df_forecast_lastyear['rating_year'] = u.year - 1
            intm_df_forecast_lastyear['counts'] = intm_df_forecast_lastyear.count(axis=1)
            
            # 按照股票代码、评级机构、评级年份降序排列（同股、同机构的评级会靠在一起，且靠后年份排前面）
            intm_df_forecast = pd.concat([intm_df_forecast, intm_df_forecast_lastyear], axis=0).sort_values(by=['wind_code', 'organization', 'rating_year'], ascending=False)
            # 再按照股票代码、评级机构、预测指标个数降序排列重新排一次（指标个数相同的会有靠后年份排在前面，保留了前面的排序痕迹）
            # 删除掉预测指标个数更少的记录
            intm_df_forecast = intm_df_forecast.sort_values(by=['wind_code', 'organization', 'counts'], ascending=False)\
                                               .drop_duplicates(subset=['wind_code', 'organization'], keep='first')
    
        del intm_df_forecast['close'], intm_df_forecast['counts']
        intm_df_forecast.index = intm_df_forecast['last_rating_date'].apply(lambda x:str(time_to_int(x))) + '_'\
                               + intm_df_forecast['wind_code'] + '_' + intm_df_forecast['organization']
        intm_df_forecast = intm_df_forecast.sort_index()
        intm_df_data = pd.concat([intm_df_data, intm_df_forecast], axis=0)

    intm_df_data.to_csv(str_path+str(time_to_int(max(intm_df_data['last_rating_date'])))+'_analyst_forecast.csv', encoding='utf_8_sig')
    log_msg("成功保存 %s 数据..."%"forecast")
    return 0
    
    
# 更新定期公告/披露日期/行业分类
def update_reports(str_metric, str_path):
    ''' 更新定期公告/披露日期/行业分类 '''
    
    log_msg("开始更新 %s 行情数据..."%str_metric)
    intm_df_report_date = ds.read_file(str_metric)
    
    # 加上新增股票
    intm_list_new_stocks = get_list_stock_update(intm_df_report_date)
    intm_df_report_date = pd.concat([intm_df_report_date, pd.DataFrame(columns=intm_list_new_stocks)], axis=1)

    # 加上新增季度/日期: 若是财报数据，按财报季更新；若是行业数据，于每月第一个交易日更新
    if 'industry' in str_metric:
        try:
            intm_newdays = [min([v for v in ds.get_newest_date_list() if v>intm_df_report_date.index[-1] and v.month!=intm_df_report_date.index[-1].month])]
        except:
            intm_newdays = []
    else:
        intm_newdays, tmp_dt_new_quarters = [], next_quarter(max(intm_df_report_date.index))
        while tmp_dt_new_quarters<max(ds.get_newest_date_list()):
            intm_newdays.append(tmp_dt_new_quarters)
            tmp_dt_new_quarters = next_quarter(tmp_dt_new_quarters)
    intm_df_report_date = pd.concat([intm_df_report_date, pd.DataFrame(index=intm_newdays)], axis=0).sort_index(ascending=True)

    # 检测当季报告是否全部披露完毕
    intm_df_report_date = update_reports_quarter(str_metric, intm_df_report_date, intm_df_report_date.index[-1])
    intm_df_report_date = update_reports_quarter(str_metric, intm_df_report_date, intm_df_report_date.index[-2])
    intm_df_report_date = intm_df_report_date.fillna(pd.NaT)
    
    # 将数据空白处填补上相应的空值符号
    if str_metric=='announceDate':
        intm_df_report_date = intm_df_report_date.stack()
        intm_df_report_date[intm_df_report_date<=par_dt_start] = pd.NaT
        intm_df_report_date = intm_df_report_date.unstack()

    if 'industry' in str_metric:
        intm_df_report_date[intm_df_report_date.isin(['--', '---', '----'])] = np.nan
        intm_df_report_date = intm_df_report_date.applymap(lambda x:x.replace('--', '-') if type(x)==str else x)
        tmp_str_file_name = str(time_to_int(max(intm_df_report_date.index)))+'_'+str_metric+'.csv'
    else:
        tmp_str_file_name = str(time_to_int(max(intm_df_report_date.index)))+'_report_'+str_metric+'.csv'
    intm_df_report_date.to_csv(str_path+tmp_str_file_name, encoding='utf_8_sig')
    log_msg("成功保存 %s 行情数据..."%str_metric)
    
    return 0


# 更新股指期货合约列表
def update_ticker_cfe():
    ''' 更新股指期货合约列表 '''
    
    str_metric = "ticker_CFE"
    log_msg("开始更新 %s 行情数据..."%str_metric)
    intm_df_data = ds.read_file(str_metric)
    
    str_start_date = time_to_str(par_dt_today - datetime.timedelta(days=90))
    str_end_date   = time_to_str(par_dt_today)
    for u in ['IC.CFE', 'IF.CFE', 'IH.CFE', 'IM.CFE', 'T.CFE', 'TF.CFE', 'TS.CFE']:
        intm_raw_new_cfe = wind_func_wset(u, str_start_date, str_end_date)
        intm_df_new_cfe  = pd.DataFrame(intm_raw_new_cfe.Data, index=intm_raw_new_cfe.Fields, columns=intm_raw_new_cfe.Data[2]).T
        intm_df_new_cfe = intm_df_new_cfe[intm_df_data.columns]
        intm_df_data = pd.concat([intm_df_data, intm_df_new_cfe], axis=0)
        
    intm_df_data = df_rows_dedu(intm_df_data).sort_index(ascending=True)
    log_msg("将数据保存到本地...")
    tmp_str_file_name = str(time_to_int(par_dt_today))+'_ticker_CFE.csv'
    intm_df_data.to_csv(par_str_path_ticker+tmp_str_file_name, encoding='utf_8_sig')
    log_msg("成功保存 %s 行情数据..."%str_metric)
    return 0
    

# 更新可转债数据
def update_ticker_cb():
    ''' 更新可转债数据 '''
    str_metric = 'ticker_cb'
    
    log_msg("开始更新 %s 行情数据..."%str_metric)
    # 读取现存的可转债列表, 并查询最近90日新上市交易的可转债
    tmp_df_tickers = ds.read_file('ticker_cb')
    tmp_newest_dates = max(ds.get_newest_date_list())
    tmp_raw_tickers = wind_func_wset('ticker_cb', time_to_str(tmp_newest_dates-datetime.timedelta(days=30)), time_to_str(tmp_newest_dates))
    tmp_df_tickers_add = pd.DataFrame(tmp_raw_tickers.Data[1], index=tmp_raw_tickers.Data[0]).rename(columns={0:'BondName'}).iloc[:-1]
    
    if len(tmp_df_tickers_add) == 0:
        pass
    else:
        tmp_list_columns = ['StockTicker', 'Maturity', 'Issuance', 'InterestDateBegin', 'InterestDateEnd', 'DateListing']
        tmp_list_new_tickers = [v for v in tmp_df_tickers_add.index if (v not in tmp_df_tickers.index)] # 过滤掉已经存在的可转债
        tmp_list_new_tickers = [v for v in tmp_list_new_tickers if (('SH' in v) or ('SZ' in v))]        # 过滤掉上交所/深交所之外的企业
        if len(tmp_list_new_tickers) == 0:
            pass
        else:
            tmp_df_tickers_add = tmp_df_tickers_add.loc[tmp_list_new_tickers]
            tmp_str_tickers = get_ticker_pieces(tmp_list_new_tickers, 300)[0]
            
            tmp_raw_Ashare_issue  = wind_func_wss(tmp_str_tickers, 'asharewindcode,term,issueamount,carrydate,carryenddate,ipo_date')
            tmp_df_Ashare_issue   = pd.DataFrame(tmp_raw_Ashare_issue.Data, columns=tmp_raw_Ashare_issue.Codes, index=tmp_list_columns).T
            tmp_df_Ashare_issue['Issuance']  = tmp_df_Ashare_issue['Issuance'] / 100000000
            
            for u in tmp_df_Ashare_issue.index:
                tmp_raw_Ashare_secName  = wind_func_wss(tmp_df_Ashare_issue.loc[u, 'StockTicker'], 'sec_name')
                tmp_df_Ashare_issue.loc[u, 'StockName'] = tmp_raw_Ashare_secName.Data[0][0]
            
            tmp_df_tickers_add = pd.concat([tmp_df_tickers_add, tmp_df_Ashare_issue], axis=1).dropna()
            tmp_df_tickers = pd.concat([tmp_df_tickers, tmp_df_tickers_add], axis=0)

    # 赎回公告日
    tmp_str_tickers = get_ticker_pieces(list(tmp_df_tickers.index), 1000)[0]
    tmp_raw_redeem_date = wind_func_wss(tmp_str_tickers, 'cb_redeemNoticeDate')
    tmp_df_redeem_date = pd.DataFrame(tmp_raw_redeem_date.Data[0], index=tmp_raw_redeem_date.Codes, columns=['DateRedeemNotice'])
    tmp_df_redeem_date[tmp_df_redeem_date==datetime.datetime(1899, 12, 30)] = datetime.datetime(par_dt_today.year+1, 12,31)
    tmp_df_redeem_date = pd.concat([tmp_df_tickers[['InterestDateEnd']]-dt.timedelta(days=30), tmp_df_redeem_date], axis=1).min(axis=1)
    
    tmp_df_tickers['DateRedeemNotice'] = tmp_df_redeem_date

    # 清除掉定向转债（转债名称包含“定转”，“定0”字样）
    tmp_df_tickers.BondName = tmp_df_tickers.BondName.apply(lambda x:'定向转债' if ('定转'in x or '定0' in x) else x)
    tmp_df_tickers = tmp_df_tickers[tmp_df_tickers.BondName != '定向转债']
    
    # 数据保存到本地
    tmp_df_tickers.to_csv(par_str_path_ticker+str(time_to_int(par_dt_today))+'_ticker_cb.csv', encoding='utf_8_sig')
    log_msg("成功保存 %s 行情数据..."%str_metric)
    return 0


# 更新全市场股票列表信息，最新数据
def update_ticker_list():
    ''' 更新全市场股票列表信息，最新数据 '''
    str_metric = 'ticker_stock'
    
    log_msg("开始更新 %s 行情数据..."%str_metric)
    intm_df_ticker_list = ds.read_file('ticker_stock')
    
    # 检索全市场现存股票
    data_raw_ticker_list = wind_func_wset('listed_tickers', 0, 0)
    data_df_ticker_list = pd.DataFrame(np.array([data_raw_ticker_list.Data[1], data_raw_ticker_list.Data[2], data_raw_ticker_list.Data[4]]).T,
                                       index=data_raw_ticker_list.Data[0],columns=['CompanyName', 'DateIPO', 'Board'])

    # 过滤掉北交所的证券
    list_ticker_bj = [v for v in data_df_ticker_list.index if v.split('.')[1]=='BJ']
    data_df_ticker_list = data_df_ticker_list.loc[data_df_ticker_list.index.isin(list_ticker_bj)==False]

    # 与历史记录的股票合并
    intm_df_ticker_list = df_rows_dedu(pd.concat([intm_df_ticker_list, data_df_ticker_list], axis=0)).fillna(0)
    
    # 检索退市股票：
    tmp_raw_ticker_delist = wind_func_wset('delisted_tickers', time_to_str(par_dt_today), 0)
    tmp_str_ticker_delist = get_ticker_pieces([v for v in tmp_raw_ticker_delist.Data[1] if v[0] in ['0', '3', '6']], 10000)
    tmp_raw_delist_date = w.wss(tmp_str_ticker_delist[0], "delist_date","ShowBlank=0")
    tmp_df_delist_date = pd.DataFrame(tmp_raw_delist_date.Data[0],index=tmp_raw_delist_date.Codes,columns=['DateDelist'])
    intm_df_ticker_list.loc[tmp_df_delist_date.index, 'DateDelist'] = tmp_df_delist_date
    
    # 保存到本地
    intm_df_ticker_list.to_csv(par_str_path_ticker+str(time_to_int(par_dt_today))+'_ticker_stock.csv', encoding='utf_8_sig')
    log_msg("成功保存 %s 行情数据..."%str_metric)
    return 0


# 更新期权列表
def update_ticker_option():
    ''' 更新全市场期权列表信息，最新数据 '''
    
    str_metric = 'ticker_option'
    log_msg("开始更新 %s 行情数据..."%str_metric)
    intm_df_ticker_list = ds.read_file('ticker_option')

    tmp_raw_510050 = wind_func_wset('ticker_option_510050.OF', time_to_str(par_dt_today), 0)
    tmp_raw_510300 = wind_func_wset('ticker_option_510300.OF', time_to_str(par_dt_today), 0)
    tmp_raw_159919 = wind_func_wset('ticker_option_159919.OF', time_to_str(par_dt_today), 0)
    tmp_raw_000852 = wind_func_wset('ticker_option_000852.SH', time_to_str(par_dt_today), 0)
    
    tmp_df_tickers = pd.concat([pd.DataFrame(tmp_raw_510050.Data, index=tmp_raw_510050.Fields, columns=tmp_raw_510050.Codes).T, 
                                pd.DataFrame(tmp_raw_510300.Data, index=tmp_raw_510300.Fields, columns=tmp_raw_510300.Codes).T, 
                                pd.DataFrame(tmp_raw_159919.Data, index=tmp_raw_159919.Fields, columns=tmp_raw_159919.Codes).T, 
                                pd.DataFrame(tmp_raw_159919.Data, index=tmp_raw_159919.Fields, columns=tmp_raw_159919.Codes).T], axis=0)
    
    intm_df_ticker_list = pd.concat([intm_df_ticker_list, tmp_df_tickers], axis=0).sort_values('option_name', ascending=True)
    intm_df_ticker_list['strike_price'] = intm_df_ticker_list['strike_price'].apply(lambda x:round(x,3))
    intm_df_ticker_list = intm_df_ticker_list.drop(columns=['exe_type', 'expiredate', 'settle_method']).drop_duplicates(keep='first')
    intm_df_ticker_list.index = range(len(intm_df_ticker_list))
    
    intm_df_ticker_list.to_csv(par_str_path_ticker+str(time_to_int(par_dt_today))+'_ticker_option.csv', encoding='utf_8_sig')
    log_msg("成功保存 %s 行情数据..."%str_metric)
    return 0


# 更新股票涨跌停板信息
def update_trade_states_dayLimit():
    ''' 更新涨跌停板信息 '''
    # 科创板：上市后的前5个交易日不设涨跌幅，正常交易日涨跌幅不超过20%
    # 主板，中小板：上市首日涨幅不超过44%，跌幅不超过36%; 正常交易日涨跌幅不超过10%
    # 创业板：6月15日之前上市首日涨幅不超过44%，普通交易日涨跌幅不超过10%
    # 创业板：6月15日之后同科创板
    # st股：涨跌幅不超过5%
    # 注意，涨跌停板的数据是依据st数据推算而来的，更新日期和股票范围均不超过st数据的范围，在更新顺序上也靠后
    str_metric = 'dayLimit'
    log_msg("开始处理 %s 数据..."%str_metric)
    intm_df_price     = ds.read_file('dayLimit')
    intm_df_ticker_st = ds.read_file('st')
    
    intm_df_ticker_date = ds.read_file('ticker_stock')
    intm_list_ticker_GEM_STAR = list(intm_df_ticker_date[intm_df_ticker_date['Board'].isin(['科创板', '创业板'])].index)
    
    # 根据新增加的股票计算涨跌停板
    intm_list_tickers_update = [v for v in intm_df_ticker_date.index if v not in intm_df_price.columns]
    data_df_ticker_update = pd.DataFrame(10,index=intm_df_price.index,columns=intm_list_tickers_update)
    for u in intm_list_tickers_update:
        if intm_df_ticker_date.loc[u, 'Board'] in ['科创板', '创业板']:
            data_df_ticker_update[u] = 20
    intm_df_price = pd.concat([intm_df_price, data_df_ticker_update], axis=1)

    # 根据新增加的交易日期计算涨跌停板
    tmp_dt_max = max(intm_df_price.index)
    intm_list_date_update = list(sorted([v for v in intm_df_ticker_st.index if v>tmp_dt_max]))
    if len(intm_list_date_update) == 0:
        log_msg("%s 数据已是最新的..."%str_metric)
    else:
        for u in intm_list_date_update:
            log_msg("正在处理 %s 的 %s 行情数据..."%(str(time_to_int(u)), 'dayLimit'))
            tmp_df_price = pd.DataFrame(10,index=[u],columns=intm_df_price.columns)
            # 科创板/科创板
            for p in intm_list_ticker_GEM_STAR:
                tmp_df_price.loc[u,p] = 10000 if (intm_df_ticker_date.loc[p,'DateIPO']<=u and \
                                                  len(intm_df_ticker_st.loc[intm_df_ticker_date.loc[p,'DateIPO']:u,:])<=5) else 20
            intm_df_price = pd.concat([intm_df_price, tmp_df_price], axis=0)

    # ST 股票涨跌停板设为5
    intm_df_price[intm_df_ticker_st==1] = 5
    
    # 保存数据
    intm_df_price.to_csv(par_str_path_price+str(time_to_int(max(intm_df_price.index)))+'_price_'+str_metric+'.csv', encoding='utf_8_sig')
    log_msg("成功保存 %s 行情数据..."%str_metric)
    return 0


# 更新ST信息
def update_trade_states_st():
    ''' 更新交易状态，如ST警示等;注意ST股会返回B股代码 '''
    # 读取现有的交易状态数据
    str_metric = 'st'
    log_msg("开始处理 %s 数据..."%str_metric)
    intm_df_price = ds.read_file('st')
    intm_list_tickers = list(intm_df_price.columns)

    # 读取最新的股票上市日期数据
    intm_df_ticker_list = ds.read_file('ticker_stock')
    
    # 根据新增加的日期更新价格数据
    intm_list_date_update = get_list_date_update(intm_df_price)
    if len(intm_list_date_update) == 0:
        log_msg(" %s 数据已是最新的..."%str_metric)
    else:
        data_df_price_update = pd.DataFrame(0, index=intm_list_date_update, columns=list(intm_df_ticker_list.index))
        for u in intm_list_date_update:
            str_time = str(time_to_int(u))
            log_msg("正在提取 %s 的 %s 数据..."%(str_time, str_metric))
            # 注意有三类不同的股票：st类，未上市/已退市，正常交易
            tmp_raw_data = wind_func_wset(str_metric, str_time, 0)
            tmp_list_st_ticker = [v for v in tmp_raw_data.Data[1] if v[0] in ['0', '3', '6']]
            tmp_list_before_IPO = list(intm_df_ticker_list[intm_df_ticker_list['DateIPO']>u].index)
            tmp_list_after_delist = intm_df_ticker_list.loc[intm_df_ticker_list['DateDelist'].dropna().index, :]
            tmp_list_after_delist = list(tmp_list_after_delist[tmp_list_after_delist['DateDelist']<=u].index)
            
            data_df_price_update.loc[u,tmp_list_st_ticker] = 1
            data_df_price_update.loc[u,tmp_list_before_IPO+tmp_list_after_delist] = -99
            
        # 读取成功的数据拼贴到原有数据上; 注意新增股票的历史部分是空缺的，需要补上 -99 标记
        intm_df_price = df_rows_dedu(pd.concat([intm_df_price, data_df_price_update], axis=0)).fillna(-99)
        
    # 保存数据
    intm_df_price.to_csv(par_str_path_price+str(time_to_int(max(intm_df_price.index)))+'_price_'+str_metric+'.csv', encoding='utf_8_sig')
    log_msg("成功保存 %s 行情数据..."%str_metric)
    return 0


# 获取最新的指数成分：代码，权重，行业
def update_index_constituent(str_index, str_path):
    ''' 更新股票指数的权重信息 '''
    
    log_msg("开始更新 %s 成分数据..."%str_index)
    # 读取现有的指数成分数据，注意读取出来的表头是字符不是整形数值
    intm_df_stocks   = ds.read_file(str_index+"_stocks").rename(columns=lambda x:int(x))
    intm_df_weight   = ds.read_file(str_index+"_weight").rename(columns=lambda x:int(x))
    intm_df_industry = ds.read_file(str_index+"_industry").rename(columns=lambda x:int(x))
    
    # 判断是否有月份更迭的两种逻辑：
    # 000300：每个月的最新一天；其他：每个月的最后一天
    intm_list_date_update = get_list_date_update(intm_df_industry)
    if str_index == "000300.SH":
        try:
            tmp_dt_update = min([v for v in intm_list_date_update if v.month!=min(intm_list_date_update).month])
        except:
            log_msg("当月指数成分数据已是最新的:" + str_index)
            return 0
    else:
        tmp_dt_update = max([v for v in intm_list_date_update if v.month==min(intm_list_date_update).month])
    
    tmp_dt_update = str(time_to_int(tmp_dt_update))
    tmp_raw_index_constituent = wind_func_wset(str_index, tmp_dt_update, tmp_dt_update)

    if len(tmp_raw_index_constituent.Data)>0:
        
        log_msg("更新 %s 的 %s 指数成分数据..."%(tmp_dt_update, str_index))
        tmp_dt_date = tmp_raw_index_constituent.Data[0][0]
        tmp_df_stocks   = pd.DataFrame(tmp_raw_index_constituent.Data[1], columns=[tmp_dt_date], index=range(len(tmp_raw_index_constituent.Data[1]))).T
        tmp_df_weight   = pd.DataFrame(tmp_raw_index_constituent.Data[3], columns=[tmp_dt_date], index=range(len(tmp_raw_index_constituent.Data[3]))).T
        tmp_df_industry = pd.DataFrame(tmp_raw_index_constituent.Data[4], columns=[tmp_dt_date], index=range(len(tmp_raw_index_constituent.Data[4]))).T

        intm_df_stocks   = df_rows_dedu(pd.concat([intm_df_stocks,   tmp_df_stocks],   axis=0))
        intm_df_weight   = df_rows_dedu(pd.concat([intm_df_weight,   tmp_df_weight],   axis=0))
        intm_df_industry = df_rows_dedu(pd.concat([intm_df_industry, tmp_df_industry], axis=0))

        # 去除掉重复的行 并 保存最新的数据文件
        intm_df_stocks.to_csv(  str_path+str(time_to_int(tmp_dt_date)) +'_'+str_index+'_stocks.csv',   encoding='utf_8_sig')
        intm_df_weight.to_csv(  str_path+str(time_to_int(tmp_dt_date)) +'_'+str_index+'_weight.csv',   encoding='utf_8_sig')
        intm_df_industry.to_csv(str_path+str(time_to_int(tmp_dt_date)) +'_'+str_index+'_industry.csv', encoding='utf_8_sig')
        
        return 0
    else:
        log_msg("当月指数成分数据为空:"+ tmp_dt_update)
        return 0
    log_msg("成功保存 %s 成分数据..."%str_metric)


# 更新金融期货的分钟数据
def update_minute_cfe(path_cfe):
    '''
    更新期货的分钟数据   
    '''
    dt_date = max(ds.get_newest_date_list())
    
    log_msg("开始处理期货分钟数据...")
    tmp_df_tickers_cfe = ds.load_tickers('CFE')[ds.load_tickers('CFE').last_trade_date>=dt_date-datetime.timedelta(days=10)]
    for ii in range(len(tmp_df_tickers_cfe)):
        str_date_end   = time_to_str(min(tmp_df_tickers_cfe['last_trade_date'].iloc[ii], dt_date))
        str_date_start = time_to_str(dt_date - datetime.timedelta(days=365))
        str_ticker     = tmp_df_tickers_cfe.index[ii]
        print(' -> '+str(ii), end="\r")
        log_msg(str(ii)+'_'+str_ticker, print_option=False)
        
        tmp_df_minute = wind_func_wsi(str_ticker, str_date_start, str_date_end)
        if len(tmp_df_minute)>10:
            tmp_df_minute['ticker'] = str_ticker
            tmp_str_day = str(time_to_int(tmp_df_minute.index.max()))
            tmp_df_minute.to_csv(path_cfe+tmp_str_day+'_'+str_ticker+'.csv', encoding='utf_8_sig')

    log_msg("成功保存期货分钟数据...")
    return 0




# 更新期权的分钟数据
def update_minute_option(path_option, path_etf):
    '''
    更新期权的分钟数据   
    '''
    dt_date = max(ds.get_newest_date_list())
    
    log_msg("开始处理期权分钟数据...")
    tmp_df_tickers_option = ds.load_tickers('option')[ds.load_tickers('option').last_tradedate>=dt_date-datetime.timedelta(days=10)]
    tmp_df_tickers_option = tmp_df_tickers_option[['option_code', 'last_tradedate']].drop_duplicates(keep='first')
    for ii in range(len(tmp_df_tickers_option)):
        str_date_end   = time_to_str(min(tmp_df_tickers_option['last_tradedate'].iloc[ii], dt_date))
        str_date_start = time_to_str(dt_date - datetime.timedelta(days=365))
        str_ticker     = tmp_df_tickers_option.option_code.iloc[ii]
        print(' -> '+str(ii), end="\r")
        log_msg(str(ii)+'_'+str_ticker, print_option=False)
        
        tmp_df_minute = wind_func_wsi(str_ticker, str_date_start, str_date_end)
        if len(tmp_df_minute)>10:
            tmp_df_minute['ticker'] = str_ticker
            tmp_str_day = str(time_to_int(tmp_df_minute.index.max()))
            tmp_df_minute.to_csv(path_option+tmp_str_day+'_'+str_ticker+'.csv', encoding='utf_8_sig')
            
    log_msg("成功保存期权分钟数据...")
    log_msg("开始处理标的ETF的分钟数据...")
    tmp_df_price   = ds.read_file("minu_etf")
    str_date_start = time_to_str(tmp_df_price.index.max() + datetime.timedelta(days=1))
    str_date_end   = time_to_str(dt_date)
    
    tmp_df_510050 = wind_func_wsi("510050.SH", str_date_start, str_date_end)
    tmp_df_510050['ticker'] = "510050.OF"
    
    tmp_df_510300 = wind_func_wsi("510300.SH", str_date_start, str_date_end)
    tmp_df_510300['ticker'] = "510300.OF"

    tmp_df_159919 = wind_func_wsi("159919.SZ", str_date_start, str_date_end)
    tmp_df_159919['ticker'] = "159919.OF"
    
    tmp_df_price = pd.concat([tmp_df_price, tmp_df_510050, tmp_df_510300, tmp_df_159919], axis=0)
    tmp_df_price.to_csv(path_etf+str(time_to_int(dt_date))+'_minu_etf.csv', encoding='utf_8_sig')
    log_msg("成功保存ETF分钟数据...")
    return 0


# 更新分钟数据
def update_minute_day(str_security, str_path_in, str_path_out, max_try=200):
    ''' 更新分钟数据 '''

    list_files = os.listdir(str_path_out)
    if   str_security == 'cb':
        data_df_amount_stock = ds.read_file("cb_amount")
        tmp_int_vol_rate     = 10
    elif str_security == 'stock':
        data_df_amount_stock = ds.read_file("amount")
        tmp_int_vol_rate     = 100

    ii = len(data_df_amount_stock) - 1                         # 回溯最近的20个交易日，检验是否需要重补数据
    while (ii>len(data_df_amount_stock)-20) and (max_try>0):

        tmp_dt_date   = data_df_amount_stock.index[ii]
        tmp_str_date  =     time_to_str(tmp_dt_date)
        tmp_str_date2 = str(time_to_int(tmp_dt_date))
        if str_security + '_' + tmp_str_date2 + '.csv' in list_files:
            break                                               # 如果数据文件已经存在，则退出循环

        tmp_df_amount = data_df_amount_stock.iloc[ii]
        tmp_df_amount = tmp_df_amount[tmp_df_amount>0]
        tmp_df_raw_data = pd.read_csv(str_path_in+str_security +'_'+tmp_str_date2+'.csv', low_memory=True, encoding='utf_8_sig').set_index('Unnamed: 0')
        tmp_df_raw_data['volume'] = tmp_df_raw_data['lots'] * tmp_int_vol_rate
    
        tmp_df_sign     = tmp_df_raw_data.groupby('ticker').sum()['amount'] - tmp_df_amount
        tmp_df_sign_new = pd.concat([pd.Series(dtype=np.float64), 
                                     tmp_df_sign[tmp_df_sign.isnull().values==True], 
                                     tmp_df_sign[tmp_df_sign> 1e4], 
                                     tmp_df_sign[tmp_df_sign<-1e4]], axis=0)
        if len(tmp_df_sign_new)>100:
            log_msg("数据空缺过多，暂不处理" + str_security + tmp_str_date2)
            break                # 数据空缺过多，暂不补充
        else:
            log_msg("开始处理 %s %s 分钟行情数据..."%(str_security, tmp_str_date))
            for u in tmp_df_sign_new.index:
                if (u in data_df_amount_stock.columns) and (data_df_amount_stock.loc[tmp_dt_date, u]>0):
                    tmp_df_new_value = wind_func_wsi(u, tmp_str_date, tmp_str_date)     
                    tmp_df_new_value['ticker']    = u
                    tmp_df_new_value['timestamp'] = tmp_df_new_value.index
                    # wind 输出的volume就是准确的volume，无需缩放
                    # 以上说法错误：wind输出的上交所可转债volume，是实际volume的1/10
                    tmp_df_new_value['volume'] = tmp_df_new_value['volume'] * (10 if (str_security=='cb' and u.split('.')[1]=='SH') else 1)
                    
                    max_try = max_try - 1
                    tmp_df_raw_data = pd.concat([tmp_df_raw_data[tmp_df_raw_data.ticker!=u], tmp_df_new_value], axis=0)
                    log_msg("   > 填充"+u)
    
            tmp_df_final_data = tmp_df_raw_data[['open', 'close', 'high', 'low', 'volume', 'amount', 'timestamp', 'ticker']]
            tmp_df_final_data = tmp_df_final_data.sort_values(by=['ticker', 'timestamp'], ascending=True)
            tmp_df_final_data.index = range(len(tmp_df_final_data))
            tmp_df_final_data.to_csv(str_path_out+str_security+'_'+tmp_str_date2+'.csv', encoding='utf_8_sig')
            ii = ii - 1
            log_msg("将数据保存到本地...")

    return 0


# 更新30分钟数据
def update_30minute(str_security, str_path_in, str_path_out):
    ''' 更新30分钟数据 '''
    
    log_msg("开始处理 %s 数据..."%str_security)
    set_list_timesep = [945, 1015, 1045, 1115, 1315, 1345, 1415, 1445]
    data_df_tickers_cb    = ds.read_file("ticker_cb")

    # 读取已存的数据
    intm_df_minu_cb_vwap   = ds.read_file("30minu_"+str_security+"_vwap")
    intm_df_minu_cb_close  = ds.read_file("30minu_"+str_security+"_close")
    intm_df_minu_cb_amount = ds.read_file("30minu_"+str_security+"_amount")

    # 需要更新的日期
    intm_dt_date     = time_to_date(max(intm_df_minu_cb_vwap.index))
    intm_list_update = [int_to_time(int(v.split('.')[0].split('_')[1])) for v in os.listdir(str_path_in+str_security+'/')]
    intm_list_update = [v for v in intm_list_update if v>=intm_dt_date]
    list_tickers     = list(data_df_tickers_cb.index) if (str_security=='cb') else list(set(data_df_tickers_cb.StockTicker))

    # 读取更新日的分钟线数据并重新裁剪
    tmp_df_minu_cb_close, tmp_df_minu_cb_vwap, tmp_df_minu_cb_amount\
        = ds.read_minute_data(str_path_in+str_security+'/'+str_security+'_', list_tickers, intm_list_update, set_list_timesep)

    # 股票数据需要映射成可转债代码
    if str_security=='stock':
        data_df_mapping       = data_df_tickers_cb[['StockTicker']]
        tmp_df_minu_cb_vwap   = df_mapper_clip(data_df_mapping, tmp_df_minu_cb_vwap)
        tmp_df_minu_cb_close  = df_mapper_clip(data_df_mapping, tmp_df_minu_cb_close)
        tmp_df_minu_cb_amount = df_mapper_clip(data_df_mapping, tmp_df_minu_cb_amount)
    
    # 拼接保存（去掉重复的部分）
    intm_df_minu_cb_vwap   = pd.concat([intm_df_minu_cb_vwap,   tmp_df_minu_cb_vwap],   axis=0).reset_index().drop_duplicates(subset='index', keep='first').set_index('index').sort_index(ascending=True)
    intm_df_minu_cb_close  = pd.concat([intm_df_minu_cb_close,  tmp_df_minu_cb_close],  axis=0).reset_index().drop_duplicates(subset='index', keep='first').set_index('index').sort_index(ascending=True)
    intm_df_minu_cb_amount = pd.concat([intm_df_minu_cb_amount, tmp_df_minu_cb_amount], axis=0).reset_index().drop_duplicates(subset='index', keep='first').set_index('index').sort_index(ascending=True)

    log_msg("将数据保存到本地...")
    intm_str_date_new = str(time_to_int(time_to_date(max(intm_df_minu_cb_close.index))))
    intm_df_minu_cb_vwap.to_csv(  str_path_out+intm_str_date_new+"_30minu_"+str_security+"_vwap.csv",   encoding='utf_8_sig')
    intm_df_minu_cb_close.to_csv( str_path_out+intm_str_date_new+"_30minu_"+str_security+"_close.csv",  encoding='utf_8_sig')
    intm_df_minu_cb_amount.to_csv(str_path_out+intm_str_date_new+"_30minu_"+str_security+"_amount.csv", encoding='utf_8_sig')

    log_msg("成功保存 %s 数据..."%str_security)
    return 0
    
    



if __name__=='__main__':
    
    pass
    
    # # # # # # # # # # # # # # # # # # # # # # # # 
    
    # 分钟线数据说明：
    
    #                              淘宝数据                        东财数据                         wind数据
    # 时间戳标记                  1分钟的结束                     1分钟的结束                     1分钟的开始
    # 无量K线价格                 上一收盘价                      上一收盘价                         空缺nan
    # 上午开盘集合竞价       含在第一根1分钟K线中      有一根单独的K线，开高低收价格相等      含在第一根1分钟K线中
    # 上午收盘                                                                              上午收盘有一根单独的K线
    # 下午开盘
    # 下午收盘集合竞价          有一根单独的K线                 有一根单独的K线                 有一根单独的K线
    # 收盘集合竞价3分钟    1根0 K线，竞价在最后一根        1根0 K线，竞价在最后一根         2根0 K线，竞价在最后一根
    # 上午K线数量                     120                             121                             121
    # 下午K线数量                     120                             120                             121
    # 全天K线数量                     240                             241                             242






