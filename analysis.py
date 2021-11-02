# -*- coding: utf-8 -*-
"""
Created on Wed Nov 18 10:22:39 2020

@author: 王笃
"""

import math
import pandas as pd
import numpy as np
import datetime as dt
from scipy.stats import linregress

# import sys
# if 'C:\InvestmentResearch\chiyeguang\\backtest' not in sys.path:
#     sys.path.append('C:\InvestmentResearch\chiyeguang\\backtest')
import dataprocess as dp
import dataset as ds

from tools.tools_func import *
# list_compare = lambda x,y:True if ((len([v for v in x if v not in y]) == 0) and (len([v for v in y if v not in x]) == 0)) else False







''' 生成选股策略 '''
def get_slice_stocks(stgy_df_scores, data_list_tradingDay_reposition, data_df_stocks_pool, set_int_slices, set_int_holding_redund=10):
    ''' 新的打分函数 '''
    stgy_int_stocks_uplimit = math.floor(len(data_df_stocks_pool.columns) / set_int_slices) + set_int_holding_redund
    stgy_list_stockSlice = [pd.DataFrame(index=data_list_tradingDay_reposition, columns=range(stgy_int_stocks_uplimit)) for ii in range(set_int_slices)]
    
    for w in data_list_tradingDay_reposition:
        tmp_df_stocks = pd.concat([stgy_df_scores.loc[w].to_frame().rename(columns={w:'score'}),
                                   data_df_stocks_pool.loc[w].to_frame().rename(columns={w:'filter'})], axis=1)
        tmp_df_stocks = tmp_df_stocks[tmp_df_stocks['filter']>0].sort_values(by='score', ascending=True)
        tmp_list_tickers_ascend = list(tmp_df_stocks.index)
        
        tmp_begin = 0
        for ii in range(set_int_slices):
            tmp_end = math.floor(len(tmp_df_stocks)*(ii+1)/set_int_slices)
            tmp_list_selected = tmp_list_tickers_ascend[tmp_begin:tmp_end]
            stgy_list_stockSlice[ii].loc[w].iloc[:len(tmp_list_selected)] = tmp_list_selected
            tmp_begin = tmp_end
        
    return stgy_list_stockSlice, stgy_int_stocks_uplimit


''' 测试版的交易策略 ---- 去除掉涨跌停和停牌限制 '''
def get_intm_process_test(stgy_df_topStocks, data_df_trade, data_df_close_adj, data_list_tradingDay_new, data_list_tradingDay_reposition):
    
    intm_holding_stocks = pd.DataFrame(index=data_list_tradingDay_new, columns=range(len(stgy_df_topStocks.columns)))
    intm_holding_position = pd.DataFrame(0,index=data_list_tradingDay_new, columns=range(len(stgy_df_topStocks.columns)))
    intm_holding_cash = pd.DataFrame(0,index=data_list_tradingDay_new, columns=['cash'])
    intm_trading_amount = pd.DataFrame(0,index=data_list_tradingDay_new , columns=['buy', 'sell', 'amount'])
    
    # 初始化第一天的持仓
    tmp_int_holding_num = len(stgy_df_topStocks.iloc[0,:].dropna())
    intm_holding_stocks.iloc[0] = stgy_df_topStocks.iloc[0,:]
    intm_holding_position.iloc[0,:tmp_int_holding_num] = 1 / tmp_int_holding_num
    #tmp_dict_holdings = {v:(1/tmp_int_holding_num) for v in stgy_df_topStocks.iloc[0,:].dropna()}
    tmp_df_holdings = pd.DataFrame([{v:(1/tmp_int_holding_num) for v in stgy_df_topStocks.iloc[0,:].dropna()}])
    
    # 循环初始化
    ii, jj = 1, 1
    tmp_dt_today, tmp_dt_lastday, tmp_dt_repositionDay = \
        data_list_tradingDay_new[ii], data_list_tradingDay_new[ii-1], data_list_tradingDay_reposition[jj]
    
    # 循环结束条件，进行到最后一个交易日或调仓日
    while (ii<len(data_list_tradingDay_new)):
    
        # 每天循环开始，更新当天的交易日日期
        tmp_dt_today = data_list_tradingDay_new[ii]
        if (tmp_dt_today.year > tmp_dt_lastday.year):
            print('> 回测年度: date =', str(tmp_dt_today)[:4])
            
        # 每日正常的股价波动，按照复权后价格计算
        tmp_df_daily_growth = data_df_close_adj[tmp_df_holdings.columns].loc[tmp_dt_today] \
                            / data_df_close_adj[tmp_df_holdings.columns].loc[tmp_dt_lastday]
        tmp_df_holdings = tmp_df_holdings * tmp_df_daily_growth.to_frame().T
        intm_holding_stocks.iloc[ii,:tmp_int_holding_num] = list(tmp_df_holdings.columns)
        intm_holding_position.iloc[ii,:tmp_int_holding_num] = list(tmp_df_holdings.iloc[0])
        
        # 调仓日，按规则调仓
        if (tmp_dt_today >= tmp_dt_repositionDay and jj<len(stgy_df_topStocks)):
            
            # 现有持仓和目标持仓
            tmp_list_holding_today = list(intm_holding_stocks.iloc[ii, :tmp_int_holding_num])
            tmp_list_holding_target = list(stgy_df_topStocks.iloc[jj,:].dropna()) if jj<len(stgy_df_topStocks) else tmp_list_holding_target
            # 若现有持仓与目标持仓不一致，则进行调仓
            if list_compare(tmp_list_holding_today, tmp_list_holding_target)==False:
                
                # 在目标清单上和不能交易的股票予以保留
                tmp_list_keep = [v for v in tmp_list_holding_today if v in tmp_list_holding_target]
                # 不在目标清单上且可以交易的股票予以卖出
                tmp_list_sell = [v for v in tmp_list_holding_today if v not in tmp_list_holding_target]
                # 不在现有持仓中且可以交易的股票予以买入
                tmp_list_buy = [v for v in tmp_list_holding_target if v not in tmp_list_holding_today]
                
                # 可卖/可买数量为0，则不进行交易
                if (len(tmp_list_sell)==0 or len(tmp_list_buy)==0):
                    print('> 可卖/可买股票数为0, 不进行调仓 date =',str(tmp_dt_today)[:10])
                else:
                    tmp_flt_amount = tmp_df_holdings[tmp_list_sell].sum(axis=1).iloc[0]
                    tmp_df_keep = tmp_df_holdings[tmp_list_keep]
                    tmp_df_buy = pd.DataFrame(tmp_flt_amount/len(tmp_list_buy),index=[0],columns=tmp_list_buy)
                    tmp_df_holdings = pd.concat([tmp_df_keep, tmp_df_buy], axis=1)
                    tmp_int_holding_num = len(tmp_df_holdings.columns)
                    
                    # 先初始化，再赋值
                    intm_holding_stocks.iloc[ii,:] = np.nan
                    intm_holding_position.iloc[ii,:] = 0
                    intm_holding_stocks.iloc[ii,:len(tmp_df_holdings.columns)] = list(tmp_df_holdings.columns)
                    intm_holding_position.iloc[ii,:len(tmp_df_holdings.columns)] = list(tmp_df_holdings.iloc[0])
                    
                    # 记录交易的过程
                    intm_trading_amount.loc[tmp_dt_today, 'buy'] = len(tmp_list_buy)
                    intm_trading_amount.loc[tmp_dt_today, 'sell'] = len(tmp_list_sell)
                    intm_trading_amount.loc[tmp_dt_today, 'amount'] = tmp_flt_amount
                
            # 若现有持仓与目标持仓一致，则不作任何处理
            else:
                pass
            
            # 循环递增到下一个目标调仓日
            jj = jj + 1
            tmp_dt_repositionDay = data_list_tradingDay_reposition[jj] \
                                    if jj<len(data_list_tradingDay_reposition) else tmp_dt_repositionDay
                                    
        # 非调仓日，不作任何额外处理
        else:
            pass
        
        # 交易日循环递增，进行到下一个交易日，更新日期标记
        ii = ii + 1
        tmp_dt_lastday = tmp_dt_today
    
    return intm_holding_stocks, intm_holding_position, intm_holding_cash, intm_trading_amount



def get_slices_return(set_int_slices, stgy_df_scores, data_df_stocks_pool, data_df_trade, data_df_close_adj, data_df_index, set_int_reposition_period, 
                      data_list_tradingDay_new, data_list_tradingDay_reposition, set_str_index, set_flt_fee, set_flt_impact_cost,
                      set_flt_riskfree_rate, data_df_crowdedTrade, set_bool_tradeLimit, set_bool_progressReport):
    
    print('>>> 开始生成选股策略, time =', str(dt.datetime.now())[11:21])
#    set_int_slices = 20
    stgy_list_stockSlice, stgy_int_stocks_uplimit = get_slice_stocks(stgy_df_scores, data_list_tradingDay_reposition, data_df_stocks_pool, set_int_slices)
    
#    intm_list_stocks, intm_list_position, intm_list_amount, result_df_netvalue, result_df_pnl = [], [], [], pd.DataFrame(), pd.DataFrame()
    result_df_netvalue, result_df_pnl = pd.DataFrame(), pd.DataFrame()
    for ii in range(set_int_slices):
        
        tmp_df_stocks = stgy_list_stockSlice[ii]
        print('\n>>> testing slices #',ii,' time=',str(dt.datetime.now())[11:21])
        # 交易限制的开关，True选择放开交易限制，False选择
#        tmp_a, tmp_b, tmp_c, tmp_d = get_intm_process_test(tmp_df_stocks, data_df_trade, data_df_close_adj, data_list_tradingDay_new, data_list_tradingDay_reposition)
        tmp_a, tmp_b, tmp_c, tmp_d = dp.get_intm_process(tmp_df_stocks, data_df_trade, data_df_close_adj, data_df_crowdedTrade, 
                                                         data_list_tradingDay_new, data_list_tradingDay_reposition, set_bool_tradeLimit, set_bool_progressReport)
        tmp_e = dp.get_netvalue_curve(data_df_index, data_list_tradingDay_new, tmp_b, tmp_d, set_str_index, set_flt_fee, set_flt_impact_cost)
        tmp_f = dp.get_return_metrics(tmp_e, data_list_tradingDay_reposition, set_int_reposition_period, set_flt_riskfree_rate)
#        intm_list_stocks.append(tmp_a)
#        intm_list_position.append(tmp_b)
#        intm_list_amount.append(tmp_c)
        xx = tmp_e['marketNeutralReturn'].to_frame().rename(columns={'marketNeutralReturn':ii})
        result_df_netvalue = pd.concat([result_df_netvalue, xx], axis=1)
        result_df_pnl = result_df_pnl.append(tmp_f[0].rename(index={tmp_f[0].index[0]:ii}))
        
    return result_df_netvalue, result_df_pnl
    

# 计算beta
def get_beta(data_df_index_return, data_df_daily_return, data_df_tradeFlag, period=0):
    ''' 计算beta，参数分别为 指数收益率、日收益率、成交过滤、周期 '''
    tmp_df_data = pd.concat([data_df_index_return, data_df_daily_return, data_df_tradeFlag], axis=1)
    tmp_df_data.columns = ['index', 'stock', 'flag']
    tmp_df_data['beta'] = 0
    tmp_df_data = tmp_df_data[tmp_df_data['flag']>0].dropna()
    if len(tmp_df_data)>120:
        tmp_df_beta = tmp_df_data.iloc[120:]
        tmp_df_data = tmp_df_data.iloc[60:]
        if period<=0:
            for u in tmp_df_beta.index:
                res = linregress(tmp_df_data.loc[:u, 'index'], tmp_df_data.loc[:u, 'stock'])
                tmp_df_beta.loc[u, 'beta'] = res.slope
            return tmp_df_beta[['beta']]
        else:
            for u in tmp_df_beta.index:
                res = linregress(tmp_df_data.loc[:u, 'index'].iloc[-period:], tmp_df_data.loc[:u, 'stock'].iloc[-period:],)
                tmp_df_beta.loc[u, 'beta'] = res.slope
            return tmp_df_beta[['beta']]
    else:
        return None


# 计算股指期货的日收益率（含贴水），周三临近收盘时调仓切换合约
def get_cfe_return():
    ''' 计算股指期货的日收益率（含贴水），周三临近收盘时调仓切换合约 '''
    
    tmp_df_return      = ds.read_file("CFE_close")
    tmp_df_cfe_tickers = ds.read_file('ticker_CFE')
    
    tmp_list_delivery_days = list(sorted(set(tmp_df_cfe_tickers['last_trade_date'])))
    tmp_list_delivery_days = [v for v in tmp_list_delivery_days if v in tmp_df_return.index]
    
    tmp_df_return_daily = tmp_df_return / tmp_df_return.shift(1) - 1 # 小心这里合约切换后第一天的收益率计算不正确
    tmp_df_return_daily['last_day_0']  = tmp_df_return_daily.index
    tmp_df_return_daily['last_day_1']  = tmp_df_return_daily['last_day_0'].shift(1)
    tmp_df_return_daily['last_day_1_'] = tmp_df_return_daily['last_day_0'].shift(-1)
    tmp_list_switch_days_backward = list(set(tmp_df_return_daily.loc[tmp_list_delivery_days, 'last_day_0']))\
                                  + list(set(tmp_df_return_daily.loc[tmp_list_delivery_days, 'last_day_1']))
    tmp_list_switch_days_forward  = list(set(tmp_df_return_daily.loc[tmp_list_delivery_days, 'last_day_1_']))

    tmp_df_daily_return_complex = pd.DataFrame()
    for u in ['IF', 'IC', 'IH']:
        tmp_df_daily_return_complex[u] = tmp_df_return_daily[u+'00.CFE']
        tmp_df_daily_return_complex.loc[tmp_list_switch_days_backward, u] =  tmp_df_return_daily[u+'01.CFE'].loc[tmp_list_switch_days_backward]
        tmp_df_daily_return_complex.loc[tmp_list_switch_days_forward,  u] = (tmp_df_return[u+'00.CFE'] / tmp_df_return[u+'01.CFE'].shift(1) - 1).loc[tmp_list_switch_days_forward]
    
    return tmp_df_daily_return_complex.fillna(0)



if __name__=='__main__':
    
    pass
