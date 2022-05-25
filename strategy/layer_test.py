# -*- coding: utf-8 -*-
"""
Created on Thu Oct 28 11:33:02 2021

@author: 好鱼
"""
import math
import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
    
import dataset as ds
import dataprocess as dp
from tools.tools_func import *

import cx_factor.NorthBoundMoneyFlow as cx_nbmf 
import cx_factor.ahPriceGap as ahpg

# 分层测试
def layer_test(data_df_stockPool, data_df_score, data_df_dayReturn, set_dt_begin_date, set_int_reposition_period, int_layers):
    ''' 分层测试 '''
    
    data_list_tradingDays = list(ds.load_index("index_close").index)
    data_list_tradingDays_repo_signal = list_interval([v for v in data_list_tradingDays if v>= set_dt_begin_date], set_int_reposition_period, 0)

    data_dict_slices = dp.get_df_slices(data_df_score, data_list_tradingDays_repo_signal, data_df_stockPool, int_layers, int_signal=1)
    data_dict_return = {v:((data_dict_slices[v]*data_df_dayReturn).fillna(0).sum(axis=1)\
                           / data_dict_slices[v].sum(axis=1) / 100).cumsum().loc[set_dt_begin_date:].fillna(0) for v in data_dict_slices}
    data_df_return_base = ((data_df_stockPool*data_df_dayReturn).fillna(0).sum(axis=1) / data_df_stockPool.sum(axis=1) / 100).cumsum()
    data_dict_return_excess = {v:data_dict_return[v]-data_df_return_base for v in data_dict_return}
    
    data_df_return_excess = pd.DataFrame()
    for u in data_dict_return_excess:
        data_df_return_excess = pd.concat([data_df_return_excess, data_dict_return_excess[u].to_frame().rename(columns={0:u})], axis=1)
        
    data_df_return_excess = data_df_return_excess.loc[set_dt_begin_date:]
    data_df_return_excess = data_df_return_excess - data_df_return_excess.iloc[0]
    
    return data_df_return_excess
    
    
    
    
    
    
    
if __name__=='__main__':
    
    if True:
        pass
    
    if False:
        ''' 回测参数设置 '''
        set_int_reposition_period = 5
        set_str_index = '000300.SH'
        set_list_fmkc_range = [0, 400]
        set_str_index = '000905.SH'
        set_list_fmkc_range = [300, 1000]
        set_dt_begin_date = dt.datetime(2017,1,1)         # 回测起始日期
        set_dt_begin_date = dt.datetime(2021,1,1)         # 回测起始日期
        set_int_avail_afterIPO = 180
        set_str_title = 'excess_return'
        
        int_layers = 10
    
        
        ''' 导入数据 '''
        # 必须的数据
        data_df_tickers      = ds.load_tickers()
        data_df_dayReturn    = ds.load_price('dayReturn')
        data_df_st           = ds.load_price('st')
        data_df_floatAmktcap = ds.load_price('floatAmktcap')
        
        
            
        ''' 处理数据，生成可选股票池 '''
        # 另有提供选股池函数自由组合
        # 标准选股操作
        data_df_stockPool = dp.get_stocks_pool_standard(data_df_st, data_df_st, data_df_floatAmktcap, data_df_tickers, set_str_index, 
                                                        set_int_avail_afterIPO, set_list_fmkc_range[0], set_list_fmkc_range[1])
        data_df_factor    = cx_nbmf.get_df_nbmf(90)
        data_df_score     = dp.rescale_score('raw', data_df_stockPool, data_df_factor)
    
        data_df_return_excess = layer_test(data_df_stockPool, data_df_score, data_df_dayReturn, set_dt_begin_date, set_int_reposition_period, int_layers)
        data_df_return_excess.plot(title=set_str_title, figsize=(12,6))




