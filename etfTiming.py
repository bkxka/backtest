# -*- coding: utf-8 -*-
"""
Created on Tue Mar  9 15:16:18 2021

@author: 好鱼
"""

from datetime import *
import pandas as pd
import numpy as np
import math


''' 选择交易标的 '''
def get_target_close(set_str_target, set_str_index, data_df_etf_close, data_df_index_close, set_dt_begin_date):
    ''' 根据跟踪指数和交易标的，获取交易标的的收盘价数据 '''

    # intm_list_etf = list(data_df_etf_list[data_df_etf_list['跟踪指数']==set_str_index].index)
    if set_str_target==set_str_index:
        intm_df_target_close = data_df_index_close[set_str_index]
    else:
        intm_df_target_close = data_df_etf_close[set_str_target]
        intm_df_target_close = intm_df_target_close[intm_df_target_close>0]
    intm_df_target_close = intm_df_target_close.loc[intm_df_target_close.index>set_dt_begin_date]
    
    return intm_df_target_close


''' 计算买入金额 '''
def get_netbuy_signal(data_df_shszhk, str_data_select):
    ''' 计算净买入金额 '''
    
    tmp_df_netbuy_shhk = data_df_shszhk['SHHK_BUY_AMT'] - data_df_shszhk['SHHK_SELL_AMT'] 
    tmp_df_netbuy_szhk = data_df_shszhk['SZHK_BUY_AMT'] - data_df_shszhk['SZHK_SELL_AMT']
    if str_data_select in ["shsz", "szsh"]:
        tmp_df_netbuy = tmp_df_netbuy_szhk + tmp_df_netbuy_shhk
    elif str_data_select == "sh":
        tmp_df_netbuy = tmp_df_netbuy_shhk
    elif str_data_select == "sz":
        tmp_df_netbuy = tmp_df_netbuy_szhk

    return tmp_df_netbuy


''' 生成择时策略信号 '''
def get_target_position(tmp_df_netbuy, set_int_lbw_percentile, func_adjust, set_bool_fusing=True):
    ''' 根据策略生成交易信号和目标持仓 '''

    intm_df_shszhk_netbuy = pd.DataFrame(columns=list(range(set_int_lbw_percentile)))
    for u in range(set_int_lbw_percentile):
        intm_df_shszhk_netbuy[u] = tmp_df_netbuy.shift(u)
    intm_df_shszhk_netbuy = intm_df_shszhk_netbuy.dropna()
    intm_df_shszhk_rank = intm_df_shszhk_netbuy.rank(axis=1)
    
    stgy_df_shszhk_francktile = intm_df_shszhk_rank[0] / set_int_lbw_percentile
    # 注意调整函数的次序：先过滤，后微调
    if set_bool_fusing:
        stgy_df_shszhk_francktile[tmp_df_netbuy.loc[stgy_df_shszhk_francktile.index]<=0] = 0
    stgy_df_shszhk_francktile = stgy_df_shszhk_francktile.apply(lambda x:func_adjust(x))

    return stgy_df_shszhk_francktile


''' 模拟交易 '''
def get_trade_simulate(stgy_df_shszhk_francktile, intm_df_target_close, set_flt_riskfree_rate, set_flt_fee):
    ''' 模拟生成交易记录 '''
    
    # 生成交易状态表
    intm_list_record = ['close', 'dayReturn', 'etfPosition', 'etfValue', 'cash', 'netvalueRaw', 'tradeAmount', 'tradeCost', 'costAdjFactor', 'netvalueCosted']
    data_list_tradingDays_reposition = [v for v in stgy_df_shszhk_francktile.index if (v in intm_df_target_close.index)]
    intm_df_trade_record = pd.DataFrame(0, index=data_list_tradingDays_reposition, columns=intm_list_record)
    
    # 策略仓位
    intm_df_trade_record['close']       = intm_df_target_close.loc[data_list_tradingDays_reposition]
    intm_df_trade_record['dayReturn']   = (intm_df_trade_record['close'] / intm_df_trade_record['close'].shift(1) - 1).fillna(0) * 100
    intm_df_trade_record['etfPosition'] = stgy_df_shszhk_francktile.loc[data_list_tradingDays_reposition]
    
    # 交易模拟
    intm_df_trade_record['cash'].iloc[0], intm_df_trade_record['netvalueRaw'].iloc[0] = 1, 1
    for ii in range(1, len(intm_df_trade_record)):
        intm_df_trade_record['netvalueRaw'].iloc[ii] = intm_df_trade_record['etfValue'].iloc[ii-1] * (1 + intm_df_trade_record['dayReturn'].iloc[ii]/100)\
                                                     + intm_df_trade_record['cash'].iloc[ii-1]     * (1 + set_flt_riskfree_rate/100/243)
        intm_df_trade_record['etfValue'].iloc[ii] = intm_df_trade_record['netvalueRaw'].iloc[ii] *      intm_df_trade_record['etfPosition'].iloc[ii]
        intm_df_trade_record['cash'].iloc[ii]     = intm_df_trade_record['netvalueRaw'].iloc[ii] * (1 - intm_df_trade_record['etfPosition'].iloc[ii])
        
    # 计算交易成本
    intm_df_trade_record['tradeAmount'] = intm_df_trade_record['etfValue'] - intm_df_trade_record['etfValue'].shift(1) * (1 + intm_df_trade_record['dayReturn']/100)
    intm_df_trade_record['tradeAmount'] = intm_df_trade_record['tradeAmount'].fillna(0).abs() / intm_df_trade_record['netvalueRaw']
    intm_df_trade_record['tradeCost']   = intm_df_trade_record['tradeAmount'] * set_flt_fee
    
    
    # 计算扣除成本后的净值曲线
    intm_df_trade_record['costAdjFactor'] = (1 - intm_df_trade_record['tradeCost']).cumprod()
    intm_df_trade_record['netvalueCosted'] = intm_df_trade_record['netvalueRaw'] * intm_df_trade_record['costAdjFactor']

    return intm_df_trade_record





if __name__=='__main__':
    
    pass

