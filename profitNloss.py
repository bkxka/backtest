# -*- coding: utf-8 -*-
"""
Created on Sat Feb  6 10:18:40 2021

@author: 好鱼
"""

from datetime import *
import pandas as pd
import numpy as np
import math


list_periods = ['year', 'month', 'week', 'rebalanceCycle']


# 股票组合的收益率(连续复合收益率)，只能用于有完整净值记录的净值曲线
def profit(df_netvalue, int_rebalanceCycle):
    ''' 股票组合的收益率(连续复合收益率)，只能用于有完整净值记录的净值曲线 '''
    tmp_days = len(df_netvalue)
    tmp_netvalue = df_netvalue.iloc[-1] / df_netvalue.iloc[0]
    df_result = pd.DataFrame(0, index=list_periods+['fullCycle'], columns=['profit(%)'])
    df_result.loc['rebalanceCycle', 'profit(%)'] = math.pow(tmp_netvalue, int_rebalanceCycle/tmp_days) - 1
    df_result.loc['week',           'profit(%)'] = math.pow(tmp_netvalue, 5/tmp_days)   - 1
    df_result.loc['month',          'profit(%)'] = math.pow(tmp_netvalue, 20/tmp_days)  - 1
    df_result.loc['year',           'profit(%)'] = math.pow(tmp_netvalue, 243/tmp_days) - 1
    df_result.loc['fullCycle',      'profit(%)'] = tmp_netvalue - 1
    
    return df_result * 100


# 股票组合的回撤率，只能用于有完整净值记录的净值曲线
def drawdown(df_netvalue, int_rebalanceCycle):
    ''' 股票组合的回撤率，只能用于有完整净值记录的净值曲线 '''
    df_result = pd.DataFrame(0, index=list_periods+['fullCycle'], columns=['drawdown(%)'])
    df_result.loc['rebalanceCycle', 'drawdown(%)'] = (df_netvalue / df_netvalue.rolling(window=int_rebalanceCycle, min_periods=1).max() - 1).min()
    df_result.loc['week',           'drawdown(%)'] = (df_netvalue / df_netvalue.rolling(window=5, min_periods=1).max()   - 1).min()
    df_result.loc['month',          'drawdown(%)'] = (df_netvalue / df_netvalue.rolling(window=20, min_periods=1).max()  - 1).min()
    df_result.loc['year',           'drawdown(%)'] = (df_netvalue / df_netvalue.rolling(window=243, min_periods=1).max() - 1).min()
    df_result.loc['fullCycle',      'drawdown(%)'] = (df_netvalue / df_netvalue.cummax() - 1).min()
    
    return df_result * 100


# 股票组合的波动率(标准差)，只能用于有完整净值记录的净值曲线
def volatility(df_netvalue, int_rebalanceCycle):
    ''' 股票组合的波动率(标准差)，只能用于有完整净值记录的净值曲线 '''
    df_result = pd.DataFrame(0, index=list_periods, columns=['volatility(%)'])
    df_result.loc['rebalanceCycle', 'volatility(%)'] = (df_netvalue / df_netvalue.shift(int_rebalanceCycle) - 1).std()
    df_result.loc['week',           'volatility(%)'] = (df_netvalue / df_netvalue.shift(5)   - 1).std()
    df_result.loc['month',          'volatility(%)'] = (df_netvalue / df_netvalue.shift(20)  - 1).std()
    df_result.loc['year',           'volatility(%)'] = (df_netvalue / df_netvalue.shift(243) - 1).std()
    
    return df_result * 100


# 计算夏普率，只能用于有完整净值记录的净值曲线
def sharpe_ratio(df_netvalue, flt_riskfree):
    ''' 计算夏普率，只能用于有完整净值记录的净值曲线 '''
    tmp_return = math.pow(df_netvalue.iloc[-1] / df_netvalue.iloc[0], 243/len(df_netvalue)) - 1
    tmp_volatility = (df_netvalue / df_netvalue.shift(1) - 1).std() * math.sqrt(243)
    return (tmp_return - flt_riskfree/100) / tmp_volatility
    

# 收益回撤比，只能用于有完整净值记录的净值曲线
def return_to_drawdown(df_netvalue):
    ''' 收益回撤比，只能用于有完整净值记录的净值曲线 '''
    tmp_return = math.pow(df_netvalue.iloc[-1] / df_netvalue.iloc[0], 243/len(df_netvalue)) - 1
    tmp_drawdown = (df_netvalue / df_netvalue.cummax()   - 1).min()
    return - tmp_return / tmp_drawdown
    

# 股票组合的换手率
def turnover(df_trade_amount):
    ''' 股票组合的换手率 '''
    tmp_days = (max(df_trade_amount.index) - min(df_trade_amount.index)).days
    df_result = pd.DataFrame(0, index=list_periods, columns=['turnover(%)'])
    df_result.loc['rebalanceCycle', 'turnover(%)'] = df_trade_amount.mean()
    df_result.loc['week',           'turnover(%)'] = df_trade_amount.sum() * 7   / tmp_days
    df_result.loc['month',          'turnover(%)'] = df_trade_amount.sum() * 30  / tmp_days
    df_result.loc['year',           'turnover(%)'] = df_trade_amount.sum() * 365 / tmp_days
    df_result.loc['fullCycle',           'turnover(%)'] = df_trade_amount.sum()
    df_result = df_result * 100
    return df_result


# 对冲组合的收益曲线，只能用于有完整净值记录的净值曲线
def excess_return(df_netvalue, df_market, method, flt_hddgeRate):
    ''' 对冲组合的收益率，只能用于有完整净值记录的净值曲线 '''
    list_dates = [v for v in df_netvalue.index if v in df_market.index]
    if method=='MarketNeutral':
        df_result = df_netvalue.loc[list_dates] / df_market.loc[list_dates]
        return df_result / df_result.iloc[0]
    elif method=='DailyHedge':
        df_netvalue_dailyReturn = (df_netvalue / df_netvalue.shift(1)).loc[list_dates]
        df_market_dailyReturn   = (df_market / df_market.shift(1)).loc[list_dates]
        df_excess_dailyReturn = (df_netvalue_dailyReturn - df_market_dailyReturn) * flt_hddgeRate
        df_excess_Return = df_excess_dailyReturn.fillna(0) + 1
        return df_excess_Return.cumprod()
    else:
        return 0
    
# 根据证券的相对仓位和日收益率计算合并收益率(不计交易成本)
def combine_netvalue_position(intm_df_signal_final, intm_df_return_daily):
    ''' 据证券的相对仓位和日收益率计算合并收益率(不计交易成本) '''
    
    intm_df_return_sum = (intm_df_return_daily * intm_df_signal_final.shift(1)).replace([np.nan, np.inf, -np.inf], 0).sum(axis=1)
    intm_df_return_sum = intm_df_return_sum / intm_df_signal_final.shift(1).sum(axis=1)
    intm_df_return_accum = (intm_df_return_sum.fillna(0) + 1).cumprod()
    
    return intm_df_return_accum



    
if __name__=='__main__':
    
    pass

