# -*- coding: utf-8 -*-
"""
Created on Mon Nov 29 13:12:28 2021

@author: 好鱼
"""


import math
import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
import sys
# path_pkg = '\\'.join(sys.argv[0].split('\\')[:-1])
path_pkg = "C:\\InvestmentResearch"
if path_pkg not in sys.path:
    sys.path.append(path_pkg)
    
import backtest
import dataset as ds
import dataprocess as dp
import trade as td
import profitNloss as pnl
import tracking as tk
from tools.tools_func import *

# 可选值为 trade / backtest
# 默认为回测模式，可在外部调用和更改
mode = 'trade'
path = 'C:/InvestmentResearch/database/minute/'

def print_mode():
    print(">>> mode:", mode)
    return 0


# 给成交量空缺的时间部分填充收盘价数据
def fill_price_nan(df_input):
    '''
    给成交量空缺的时间部分填充收盘价数据
    '''
    df_output   = df_input.copy(deep=True).sort_index(ascending=True)
    tmp_df_fill = df_output['close'].fillna(method='pad')
    for str_metric in ['open', 'close', 'high', 'low']:
        df_output[str_metric][df_output['amount']<=0] = tmp_df_fill
    
    
    return df_output

    
# 根据目标持仓，计算每次调仓后的实际持仓，并记录下调仓过程(股票交易清单，成交金额，成本费用)
# 修订内容的核心是，实际成交的量会远小于计划成交量，需要处理实际持仓与计划持仓之间不一致的矛盾
# 函数主要用于日内的交易模拟，因此使用均价、成交量信息对模拟做修正尤其重要
# 注意现金是一个重要因素，日内交易不可能做到满仓换股，必须要对现金做加减；输入数据必须包含现金项
def trade_model_intraday(stgy_df_target_position, df_close, df_amount, df_avgprice, fund_size=1e7, close_rate=1):
    ''' 
    根据目标持仓，计算每次调仓后的实际持仓，并记录下调仓过程(股票交易清单，成交金额，成本费用)
    修订内容的核心是，实际成交的量会远小于计划成交量，需要处理实际持仓与计划持仓之间不一致的矛盾
    函数主要用于日内的交易模拟，因此使用均价、成交量信息对模拟做修正尤其重要
    注意现金是一个重要因素，日内交易不可能做到满仓换股，必须要对现金做加减；输入数据必须包含现金项
    '''
    intm_df_trade_log  = pd.DataFrame(0, index=stgy_df_target_position.index, columns=['cash', 'slide', 'amount'])
    intm_df_price_diff = df_avgprice / (df_close.fillna(0)+1e-5) - 1
    
    # 交易成功后的实际持仓(百分比)，将第一天的持仓初始化
    stgy_df_actual_position         = stgy_df_target_position.copy(deep=True).applymap(lambda x:0)
    stgy_df_actual_position.iloc[0] = stgy_df_target_position.iloc[0]
    intm_df_trade_log['cash'].iloc[0] = 1 - stgy_df_actual_position.iloc[0].sum()
    
    # 循环处理每个调仓日的实际持仓和交易情况
    # 基本原则为，将实际持仓调整至目标持仓的初始状态
    for ii in range(1,len(stgy_df_target_position)):
        
        tmp_dt_lastday = stgy_df_target_position.index[ii-1]
        tmp_dt_today   = stgy_df_target_position.index[ii]
        
        # 模拟持仓的实际变化
        # 根据股价变化对实际持仓进行校准
        # 程序有一个隐藏bug，买卖不平衡的时候，实际持仓会偏离 1（高于1说明头寸高于净资产，现金为负，买入容易卖出难）持续累积会导致账户不平衡
        tmp_df_position_old = (stgy_df_actual_position.loc[tmp_dt_lastday] * df_close.loc[tmp_dt_today] / df_close.loc[tmp_dt_lastday]).fillna(0)
        tmp_df_position_old = tmp_df_position_old / (tmp_df_position_old.sum() + intm_df_trade_log.loc[tmp_dt_lastday, 'cash'])

        # 根据新一期的实际持仓、目标持仓模拟成交情况
        # 实际流动性约束：测试成交不应超过真实的历史成交量
        intm_df_position_trade       = stgy_df_target_position.loc[tmp_dt_today] - tmp_df_position_old
        intm_df_position_change_plan = pd.concat([fund_size*intm_df_position_trade.abs(), close_rate*df_amount.loc[tmp_dt_today]], axis=1).min(axis=1) / fund_size
        intm_df_position_change_real = intm_df_position_change_plan * intm_df_position_trade.apply(lambda x:int_sign(x))
        
        # 增加一个现金约束条件：如果当前现金余额为负，则只卖不买
        # 这个条件是为了防止负的现金持续累积，账户不平衡
        if intm_df_trade_log.loc[tmp_dt_lastday, 'cash']<0:
            intm_df_position_change_real[intm_df_position_change_real>0] = 0
        
        # 变化后的实际持仓，以及交易过程中产生的偏差
        stgy_df_actual_position.loc[tmp_dt_today]     = tmp_df_position_old + intm_df_position_change_real
        intm_df_trade_log.loc[tmp_dt_today, 'cash']   = 1 - stgy_df_actual_position.loc[tmp_dt_today].sum()
        intm_df_trade_log.loc[tmp_dt_today, 'slide']  = (intm_df_position_change_real * intm_df_price_diff.loc[tmp_dt_today]).sum()
        intm_df_trade_log.loc[tmp_dt_today, 'amount'] = intm_df_position_change_plan.sum()
            
    return stgy_df_actual_position, intm_df_trade_log
    
    
# 读取分钟数据
def read_minute_data(str_path, list_ticker, list_date, list_timesep):
    '''
    读取分钟数据

    Parameters
    ----------
    str_path : str
        数据文件路径.
    list_ticker : list_str
        筛选所需的代码.
    list_date : list_datetime
        筛选所需要的日期.

    Returns
    -------
    data_df_cb_close_minu : dataframe
        收盘价.
    data_df_cb_avgprice_minu : dataframe
        成交均价.
    data_df_cb_amount_minu : dataframe
        成交额.

    '''
    print(">>> reading data files...", str_hours(2))
    data_df_stock_minute = pd.DataFrame()
    for u in list_date:
        # print(">>> processing", u)
        tmp_df_minute = pd.read_csv(str_path+str(time_to_int(u))+'.csv', encoding='utf_8_sig').iloc[:,1:]
        tmp_df_minute = tmp_df_minute[tmp_df_minute['ticker'].isin(list_ticker)]
        data_df_stock_minute = data_df_stock_minute.append(tmp_df_minute)

    data_df_stock_minute['timestamp'] = data_df_stock_minute['timestamp'].apply(lambda x:dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
    data_df_stock_minute['timesep']   = data_df_stock_minute['timestamp'].apply(lambda x:x.hour*100+x.minute)

    print(">>> reconstructing data...", str_hours(2))
    data_df_cb_close_minu    = pd.DataFrame()
    data_df_cb_avgprice_minu = pd.DataFrame()
    data_df_cb_amount_minu   = pd.DataFrame()
    
    for u in list_ticker:
        try:
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

    print(">>> outputing data...", str_hours(2))
    return data_df_cb_close_minu, data_df_cb_avgprice_minu, data_df_cb_amount_minu



if __name__=='__main__':
    
    pass
    