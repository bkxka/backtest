# -*- coding: utf-8 -*-
"""
Created on Thu Oct 28 11:37:52 2021

@author: 好鱼
"""

import math
import pandas as pd
import numpy as np
import datetime as dt

import dataset as ds
import option as op
from tools.tools_func import *


# 生成套利因子的默认参数
set_dict_trade = {
    'null': [np.inf, -np.inf, np.nan],
    'loanRate':   0.1/250, 
    'volscoreEnter': -0.1,
    'volscoreExit':     0,
    'deltaUpper':    0.95,
    'deltaFloor':     0.6,
    'minuteFloat':      0,
    'premiumEnter':  0.06,
    'premiumExit':    0.2, 
    'feeCB':        0.003, 
    'feeStock':     0.002, 
    'fix':         'cb',
    'singleLeg': 'None',
    'deltaHedge': False,
    }


# 生成套利因子的默认规则
set_dict_rules = {
    "buy":[
        "(df_trade_cycle.loc[p, q] > 0)",                                           # 处于交易周期中
        "(df_iv.loc[p, q] > 0)",                                                    # 隐含波动率是个有效数值
        "(df_score.loc[p, q] <= dict_trade['volscoreEnter'])",                      # 波动率分值低于阈值
        "(df_delta.loc[p, q] >= dict_trade['deltaFloor'])",                         # delta处于阈值区间
        "(df_delta.loc[p, q] <  dict_trade['deltaUpper'])",
        "(df_conv_premium.loc[p, q] < dict_trade['premiumEnter'])",                 # 转股溢价低于阈值
        "(df_gross_value.loc[p, q] >= 1E8)",                                        # 转债存量高于1亿元
        "(p - df_ticker_cb.loc[q, 'InterestDateBegin']>=dt.timedelta(days=180))",   # 发行日(起息日)6个月后开始转股
        ],
    "sell":[
        "(df_trade_cycle.loc[p, q] <= 0)",                                          # 处于交易周期之外
        "(df_score.loc[p, q] >= dict_trade['volscoreExit']) ",                      # 波动率分值高于阈值
        "(df_conv_premium.loc[p, q] > dict_trade['premiumExit'])",                  # 转股溢价高于阈值
        ],
    }


    
# 计算可转债的隐含波动率
def get_df_implied_volatility(df_option_price, df_close_stock, df_conv_price, df_maturity, df_riskfree):
    ''' 
    计算可转债的隐含波动率
    注意所有指标的列名都是转债代码，包括 df_close_stock
    '''
    
    df_iv_new = pd.DataFrame()

    for u in df_option_price.columns:
        tmp_df_iv = op.find_vol_series(df_option_price[u], df_close_stock[u], df_conv_price[u], df_maturity[u], df_riskfree[u])\
                      .to_frame().rename(columns={0:u})
        df_iv_new = pd.concat([df_iv_new, tmp_df_iv], axis=1)
        
    return df_iv_new


# 计算可转债的到期时间
def get_df_maturity(df_cb, df_tickers_cb):
    ''' 计算可转债的到期时间 '''
    
    data_df_maturity     = df_initialize(df_cb, 0)
    for u in data_df_maturity.index:
        data_df_maturity.loc[u] = (df_tickers_cb[['InterestDateEnd']].T.iloc[0] - u).apply(lambda x:x.days/365)
    data_df_maturity[data_df_maturity<0] = 0
    
    return data_df_maturity


# 计算套利因子
def get_df_arbitrage(dict_trade, dict_rules, df_trade_cycle, df_iv, df_hv, df_delta, df_conv_premium, df_gross_value, df_ticker_cb):
    ''' 
    计算套利因子
    1, 昨日状态复制至当日
    2, 当日状态为空仓，且满足入场条件的，设置为入场
    3, 当日状态为非空仓，且满足离场条件的，设置为离场
    '''
    
    df_score = df_iv - df_hv
    tmp_df_arb = df_trade_cycle.iloc[0,:].to_frame().T.applymap(lambda x:0)
    for p in df_trade_cycle.index[1:]:
        
        tmp_se_arb_new   = tmp_df_arb.iloc[-1].copy(deep=True).rename(p)
        tmp_se_arb_new_0 = tmp_se_arb_new[tmp_se_arb_new<=0]
        tmp_se_arb_new_1 = tmp_se_arb_new[tmp_se_arb_new>0]
        
        # 是否满足新的入场条件
        for q in tmp_se_arb_new_0.index:
            
            tmp_bool_buy = True
            for u in dict_rules['buy']:
                tmp_bool_buy = tmp_bool_buy and eval(u)
            tmp_se_arb_new.loc[q] = 1 if tmp_bool_buy else 0
        
        # 是否满足新的离场条件
        for q in tmp_se_arb_new_1.index:
            tmp_bool_sell = False
            for u in dict_rules['sell']:
                tmp_bool_sell = tmp_bool_sell or eval(u)
            tmp_se_arb_new.loc[q] = 0 if tmp_bool_sell else 1
                
        tmp_df_arb = tmp_df_arb.append(tmp_se_arb_new.to_frame().T)
        
    return tmp_df_arb
        
    





if __name__=='__main__':
    
    pass

