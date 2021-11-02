# -*- coding: utf-8 -*-
"""
Created on Thu Oct 28 12:15:47 2021

@author: 好鱼
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Jan 22 12:08:40 2021

@author: 好鱼
"""

# 策略描述
# 对标指数 set_str_index
# 权重配置 equalWeight/fmcWeight


import math
import pandas as pd
import numpy as np
from datetime import *
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


''' 回测参数设置 '''
if True:
    set_int_holding_redund = 20
    set_int_reposition_period = 5
    #set_int_lookback_window_shszhk = 5
    #set_int_lookback_window = 30
    #set_dt_lookback_window = timedelta(days=set_int_lookback_window)
    set_int_lookback_window_risk = 180
    set_int_holding_target_num = 20
    set_flt_holding_target_rate = 0.005
    set_flt_fee = 0.002
    set_flt_impact_cost = 0.002
    set_flt_riskfree_rate = 3.0
    set_str_index = '000905.SH'
    set_str_industry = 'sw'
    set_list_floatmktcap_range = [300, 1000]
    set_dt_begin_date = datetime(2016,6,30)         # 回测起始日期
    set_list_null = [np.inf, -np.inf, np.nan]
    set_int_avail_afterIPO = 180
    set_flt_hedge_rate = 0.8
    stgy_str_weight_method = 'equalWeight'


''' 导入数据 '''
# 必须的数据
if True:
    data_df_index_close   = ds.load_index("index_close")
    data_df_tickers       = ds.load_tickers()
    data_list_tradingDays = list(data_df_index_close.index)
    
    data_list_metrics       = ['open', 'high', 'low', 'close', 'vwap', 'adjfactor', 'mktcap', 
                               'dayReturn', 'st', 'dayLimit', 'shszhkHold', 'amount', 'floatAmktcap']
    data_df_close        = ds.load_price('close')
    data_df_vwap         = ds.load_price('vwap')
    data_df_adjfactor    = ds.load_price('adjfactor')
    data_df_close_adj    = data_df_adjfactor * data_df_close
    data_df_mktcap       = ds.load_price('mktcap')
    data_df_dayReturn    = ds.load_price('dayReturn')
    data_df_st           = ds.load_price('st')
    data_df_dayLimit     = ds.load_price('dayLimit')
    data_df_amount       = ds.load_price('amount')
    data_df_floatAmktcap = ds.load_price('floatAmktcap')
    data_df_shszhkHold   = ds.load_price('shszhkHold')
    data_df_shszhkBuy    = ds.get_netBuy(data_df_shszhkHold, data_df_close, data_df_dayReturn)
    
    data_df_index_stocks = ds.load_index(set_str_index+"_stocks")
    data_df_index_weight = ds.load_index(set_str_index+"_weight")
    
    data_list_industry = ds.load_industry("industry_"+set_str_industry, '-')
    data_df_industry   = dp.cross_to_sequence(data_list_tradingDays, data_list_industry[0])


    
''' 处理数据，生成可选股票池 '''
# 另有提供选股池函数自由组合
# 标准选股操作
data_df_stockPool = dp.get_stocks_pool_standard(data_df_close, data_df_st, data_df_floatAmktcap, data_df_tickers, set_str_index, 
                                                set_int_avail_afterIPO, set_list_floatmktcap_range[0], set_list_floatmktcap_range[1])
# # 跟随指数选股操作
# data_df_stockPool = dp.select_stocks_pool_index(data_df_close, set_str_index)

''' 处理数据，生成股票策略分值表 '''
# 打分策略一：北向资金净买入
stgy_int_lbw_shszhk = 90
tmp_df_score_raw = (data_df_shszhkBuy / data_df_floatAmktcap).replace(set_list_null, 0)
tmp_df_score     = tmp_df_score_raw.rolling(window=stgy_int_lbw_shszhk, min_periods=1).sum().fillna(0)
stgy_df_scores_A = dp.rescale_score('raw', data_df_stockPool, tmp_df_score)
stgy_df_scores   = dp.weight_scores([(stgy_df_scores_A, -1)])


''' 计算行业权重，生成行业权重控制表 '''
data_df_index_industry_control = dp.get_index_industry_weight(data_df_index_stocks, data_df_index_weight, data_df_industry)
data_df_index_industry_control = dp.cross_to_sequence(data_list_tradingDays, data_df_index_industry_control)
# 可以再增加对行业权重控制的处理，使得行业权重有控制地偏离


# 测试三：测试选择全部股票后的交易回测
if True:
    ''' 计算目标持仓，控制行业暴露 '''
    data_list_tradingDays_repo_signal = list_interval([v for v in data_list_tradingDays if v>= set_dt_begin_date], set_int_reposition_period, 1)
    data_list_tradingDays_reposition  = [element_shift(data_list_tradingDays,v,1) for v in data_list_tradingDays_repo_signal if v<data_list_tradingDays[-1]]


    # 控制行业权重的持仓
    stgy_df_target_position \
    = dp.get_target_position(data_list_tradingDays_repo_signal[:-1], None, 
                             data_df_stockPool, data_df_industry, stgy_df_scores, data_df_floatAmktcap, set_int_holding_target_num, 'equalWeight')
    stgy_df_target_position.index = data_list_tradingDays_reposition[:len(data_list_tradingDays_repo_signal)-1]
    
    ''' 计算调仓交易细节和股票组合的净值曲线 '''
    intm_df_actual_position, intm_df_trade_record \
    = td.trade_model(stgy_df_target_position, data_df_close_adj, data_df_dayReturn, data_df_dayLimit, 
                     data_df_st, data_df_amount, data_df_floatAmktcap)
    
    intm_list_tradingDays = [v for v in data_list_tradingDays if v>=set_dt_begin_date]
    intm_df_netvalue = td.trade_netvalue(intm_df_actual_position, intm_df_trade_record, intm_list_tradingDays, 
                                         data_df_close_adj, set_flt_fee, set_flt_impact_cost)
    
    stgy_dt_last_repo_signal = data_list_tradingDays_repo_signal[-1]
    stgy_dt_last_reposition  = intm_list_tradingDays[-1]
    
if __name__=='__main__':
    
    # 生成交易清单
    set_str_path_orders = "C:/Investment/TradeOrders/"
    stgy_str_today = datetime.now().strftime("%Y%m%d")
    stgy_df_order_new = dp.get_target_position_day(data_list_tradingDays_repo_signal[-1], None, data_df_stockPool, None, 
                                                    stgy_df_scores, data_df_floatAmktcap, set_int_holding_target_num, stgy_str_weight_method)
    stgy_df_order_new = stgy_df_order_new[stgy_df_order_new>0].to_frame().rename(columns={0:'position_new'})
    
    stgy_df_order_last = tk.read_prev_orders(set_str_path_orders, "NorthBoundFollow")
    if stgy_df_order_last is not None:
        stgy_df_order_last = stgy_df_order_last[['position_new', 'hold_new', 'surplus']]
        stgy_df_order_last = stgy_df_order_last[stgy_df_order_last['hold_new']>0].dropna()#.rename(columns={"position_new":"position_old", "hold_new":"hold_old", "surplus":"surplus"})

    stgy_df_order = tk.get_trade_order(stgy_df_order_last, stgy_df_order_new, None, set_str_path_orders, "NorthBoundFollow"+stgy_str_today+'.csv')
    # stgy_df_order_last = None
    # stgy_df_order = tk.get_trade_order(stgy_df_order_last, stgy_df_order_new, 600000, set_str_path_orders, "NorthBoundFollow"+stgy_str_today+'.csv')

    print(">>> 最新调仓信号日期:", stgy_dt_last_repo_signal)
    # print(">>> 最新调仓执行日期:", stgy_dt_last_reposition)
    print(">>> 点击回车键以结束程序 ...")
    set_exit = input()
