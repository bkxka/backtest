# -*- coding: utf-8 -*-
"""
Created on Tue Feb  2 22:45:24 2021

@author: 好鱼

交易模块分成两部分：
第一部分是调仓日的交易模拟
第二部分是非调仓日的净值波动模拟
"""

import datetime as dt
import pandas as pd
import numpy as np
import math
from sklearn import linear_model
import dataset as ds
from tools.tools_func import *



# 根据指定的股票代码列表和指定的日期提取股票的交易状态，包括日涨跌停板、成交额、st等
def get_stock_state(tmp_dt_today, list_tickers, df_dayReturn, df_dayLimit, df_st, df_amount, df_fmc):
    ''' 根据指定的股票代码列表和指定的日期提取股票的交易状态，包括日涨跌停板、成交额、st等 '''
    data_df_result = pd.DataFrame(index=list_tickers)
    data_df_result['dayReturn']   = df_dayReturn.loc[tmp_dt_today, list_tickers]
    data_df_result['dayLimit']    = df_dayLimit.loc[tmp_dt_today, list_tickers]
    data_df_result['st']          = df_st.loc[tmp_dt_today, list_tickers]
    data_df_result['amount']      = df_amount.loc[tmp_dt_today, list_tickers]
    data_df_result['floatmktcap'] = df_fmc.loc[tmp_dt_today, list_tickers]
    return data_df_result
    
    
# 根据股票当日交易情况，判断是否为可买入/卖出标的
# 具体标准：当日成交额超过1kw，日收益率/涨跌停板<0.9, st可卖不可买
def bool_liquidity(df_state, str_ticker, str_direction):
    ''' 
    根据股票当日交易情况，判断该股票是否为可买入/卖出标的
    具体标准：当日成交额超过1kw，日收益率/涨跌停板<0.9, st可卖不可买
    '''
    bool_a = df_state.loc[str_ticker, 'amount']>10000000
    # if df_state.loc[str_ticker, 'dayLimit']<=0:
    #     print(df_state, '|', str_ticker)
    bool_b = abs(df_state.loc[str_ticker, 'dayReturn'])/df_state.loc[str_ticker, 'dayLimit'] < 0.9
    bool_c = df_state.loc[str_ticker, 'st'] == 0
    if bool_a and bool_b:
        if bool_c:
            return True
        else:
            if str_direction == 'buy':
                return False
            elif str_direction == 'sell':
                return True
    else:
        return False


# 根据目标持仓，计算每次调仓后的实际持仓，并记录下调仓过程(股票交易清单，成交金额，成本费用)
def trade_model(stgy_df_target_position, df_close, df_dayReturn, df_dayLimit, df_st, df_amount, df_fmc, bool_limit=True):
    ''' 根据目标持仓，计算每次调仓后的实际持仓，并记录下调仓过程(股票交易清单，成交金额，成本费用) '''
    
    # 交易成功后的实际持仓(百分比)
    stgy_df_actual_position = stgy_df_target_position.copy(deep=True).applymap(lambda x:0)
    stgy_df_actual_position.iloc[0] = stgy_df_target_position.iloc[0]
    
    # 记录交易状态的中间变量
    intm_list_metrics_trade = ['holdingNumBefore', 'holdingNumAfter', 'tradeAmountOneSide', 'sellList', 'keepList', 'buyList']
    intm_df_trading_record = pd.DataFrame(0, index=stgy_df_target_position.index, columns=intm_list_metrics_trade)
    tmp_se_initial = list(stgy_df_target_position.iloc[0][stgy_df_target_position.iloc[0]>0].index)
    intm_df_trading_record['holdingNumAfter'].iloc[0] = len(tmp_se_initial)
    
    # 循环处理每个调仓日的实际持仓和交易情况
    # 基本原则为，将实际持仓调整至目标持仓的初始状态
    for ii in range(1,len(stgy_df_target_position)):
        
        tmp_dt_lastday = stgy_df_target_position.index[ii-1]
        tmp_dt_today   = stgy_df_target_position.index[ii]
        
        # 检索每只股票的状态--调仓前
        intm_df_position_old = stgy_df_actual_position.iloc[ii-1] * df_close.loc[tmp_dt_today] / df_close.loc[tmp_dt_lastday]
        intm_df_position_old = intm_df_position_old[intm_df_position_old>0].to_frame().rename(columns={0:'position'})
        intm_df_position_old = intm_df_position_old / intm_df_position_old.sum()                # 注意要重新归一化，计量的是每只股票的相对持仓
        
        # 计算目标仓位
        intm_df_position_new = stgy_df_target_position.iloc[ii]
        intm_df_position_new = intm_df_position_new[intm_df_position_new>0].to_frame().rename(columns={tmp_dt_today:'position'})

        # 采用成交限制条件：流动性/涨跌停/ST
        if bool_limit==True:
            tmp_df_stock_state_before = pd.concat([get_stock_state(tmp_dt_today, list(intm_df_position_old.index), df_dayReturn, 
                                                                   df_dayLimit, df_st, df_amount, df_fmc), intm_df_position_old], axis=1)
            # 检索每只股票的状态--调仓目标
            tmp_df_stock_state_target = pd.concat([get_stock_state(tmp_dt_today, list(intm_df_position_new.index), df_dayReturn, 
                                                                   df_dayLimit, df_st, df_amount, df_fmc), intm_df_position_new], axis=1)
            
            # 根据可交易状态，计算可以实际交易的股票列表
            tmp_list_trade_block = [v for v in tmp_df_stock_state_before.index if not bool_liquidity(tmp_df_stock_state_before, v, 'sell')] # 不可卖出的股票
            tmp_list_trade_allow = [v for v in tmp_df_stock_state_target.index if     bool_liquidity(tmp_df_stock_state_target, v, 'buy')]  # 可以买入的股票
            tmp_list_trade_allow = [v for v in tmp_list_trade_allow if v not in tmp_list_trade_block]                                       # 过滤掉重复股票
            
            # 综合可得实际持仓
            tmp_df_stock_state_keep  = tmp_df_stock_state_before.loc[tmp_list_trade_block]
            tmp_df_stock_state_append = tmp_df_stock_state_target.loc[tmp_list_trade_allow]
            tmp_df_stock_state_append['position'] = tmp_df_stock_state_append['position'] * (1-tmp_df_stock_state_keep['position'].sum()) / tmp_df_stock_state_append['position'].sum()
            # 根据实际情况调整后的仓位
            # intm_df_position_new = tmp_df_stock_state_keep.append(tmp_df_stock_state_append).loc[:,'position'].to_frame()
            intm_df_position_new = pd.concat([tmp_df_stock_state_keep, tmp_df_stock_state_append], axis=0).loc[:,'position'].to_frame()
        # 无成交限制条件
        else:
            pass
        
        # 根据实际持仓变动，计算持仓变动中间表
        intm_df_trading_record.loc[tmp_dt_today, 'holdingNumBefore'] = len(intm_df_position_old)
        intm_df_trading_record.loc[tmp_dt_today, 'holdingNumAfter']  = len(intm_df_position_new)
        tmp_list_buy  = [v for v in intm_df_position_new.index if v not in intm_df_position_old.index]
        tmp_list_keep = [v for v in intm_df_position_new.index if v     in intm_df_position_old.index]
        tmp_list_sell = [v for v in intm_df_position_old.index if v not in intm_df_position_new.index]
        intm_df_trading_record.loc[tmp_dt_today, 'buyList']   = ','.join(tmp_list_buy)
        intm_df_trading_record.loc[tmp_dt_today, 'keepList']  = ','.join(tmp_list_keep)
        intm_df_trading_record.loc[tmp_dt_today, 'sellList']  = ','.join(tmp_list_sell)
        
        # 计算调仓成交金额和成本费用
        intm_df_position_change = (intm_df_position_new - intm_df_position_old).applymap(lambda x:0)
        intm_df_position_change.loc[intm_df_position_new.index] = intm_df_position_change.loc[intm_df_position_new.index] + intm_df_position_new
        intm_df_position_change.loc[intm_df_position_old.index] = intm_df_position_change.loc[intm_df_position_old.index] - intm_df_position_old
        intm_df_trading_record.loc[tmp_dt_today, 'tradeAmountOneSide'] = intm_df_position_change.abs().sum().iloc[0] / 2
    
        # 将实际持仓记录下来
        stgy_df_actual_position.loc[tmp_dt_today, list(intm_df_position_new.index)] = intm_df_position_new['position']
    
    return stgy_df_actual_position, intm_df_trading_record


# 根据实际持仓，将持仓转变为股票组合的净值
# 新增了默认参数实际成交价 df_price_trade，默认值等同于收盘价；根据实际成交价对策略净值进行修正
# 修改了计算交易量/交易成本的方法，实际上废弃了 df_trade_record 参数
# amountOneSide 实际指的是总的交易金额（含买入和卖出金额之和）
# 函数支持对非连续、日间与日内混杂的净值曲线计算
# 修订函数，使之支持复利与单利两种计算模式
def trade_netvalue(df_actual_position, df_trade_record, list_tradingDays, df_close, flt_fee, flt_impact_cost, df_price_trade=None, interest='compound'):
    ''' 根据实际持仓，将持仓转变为股票组合的净值 '''

    df_netvalue = pd.DataFrame(0, index=list_tradingDays, columns=['netvalueRaw', 'amountOneSide', 'tradeCost', 'costFactor', 'tradeFactor', 'netvalueCosted'])

    # 实际成交价的测算
    # 默认情况是按照收盘价成交，成交对净值计算无影响
    # 计入实际成交价，则把盘中买入的证券收盘时收益计入净值；正的成交(买入)+正的bias(成交价高于收盘价) 带来负的收益
    tmp_df_trade_bias          = df_close.applymap(lambda x:0) if (df_price_trade is None) else (df_price_trade / df_close - 1)
    tmp_df_trade_bias['cash']  = 0
    tmp_df_trade_amount        = df_actual_position - df_index_norm(df_actual_position.shift(1) * df_close / df_close.shift(1))
    df_netvalue.loc[df_actual_position.index, 'amountOneSide'] = tmp_df_trade_amount.abs().sum(axis=1)
    df_netvalue['tradeCost']   = df_netvalue['amountOneSide'] * (flt_fee + flt_impact_cost)
    
    # 交易成本的测算：计入交易量
    if interest == 'compound':
        df_netvalue['tradeFactor'] = (1 - (tmp_df_trade_amount * tmp_df_trade_bias).fillna(0).sum(axis=1)).cumprod()
        df_netvalue['costFactor']  = (1 - df_netvalue['tradeCost']).cumprod()
        df_netvalue['netvalueRaw'] = 1
    elif interest == 'simple':
        df_netvalue['tradeFactor'] = (0 - (tmp_df_trade_amount * tmp_df_trade_bias).fillna(0).sum(axis=1)).cumsum()
        df_netvalue['costFactor']  = - df_netvalue['tradeCost'].cumsum()
        df_netvalue['netvalueRaw'] = 0
    
    ii, jj = 1, 0
    tmp_dt_base_date = df_actual_position.index[jj]
    while ii<len(df_netvalue):
        
        if df_netvalue.index[ii]>df_actual_position.index[jj]:
            tmp_dt_base_date = df_actual_position.index[jj]   # 标定的基准日
            jj = min(jj+1, len(df_actual_position)-1)
            
        tmp_df_dayReturn = df_actual_position.loc[tmp_dt_base_date] * df_close.loc[list_tradingDays[ii]] / df_close.loc[tmp_dt_base_date] 
        if interest == 'compound':
            df_netvalue['netvalueRaw'].iloc[ii] = df_netvalue['netvalueRaw'].loc[tmp_dt_base_date] * tmp_df_dayReturn.dropna().sum()
        elif interest == 'simple':
            df_netvalue['netvalueRaw'].iloc[ii:] = df_netvalue['netvalueRaw'].loc[tmp_dt_base_date] + tmp_df_dayReturn.dropna().sum() - 1
        
        ii = ii + 1
    
    # 计算交易成本修正后的股票组合净值
    if interest == 'compound':
        df_netvalue['netvalueCosted'] = df_netvalue['netvalueRaw'] * df_netvalue['costFactor'] * df_netvalue['tradeFactor']
    elif interest == 'simple':
        df_netvalue['netvalueCosted'] = df_netvalue['netvalueRaw'] + df_netvalue['costFactor'] + df_netvalue['tradeFactor']
    
    return df_netvalue
    
    
    
# 模拟交易股票和转债
def trade_strategy_simulation(stgy_str_cb, data_df_tickers_cb, stgy_df_cb_full, dict_trade, dict_rules, df_minute=None):
    ''' 
    模拟交易股票和转债 
    fix_position = 'fix_cb', 'fix_stock'
    single_leg = None, 'stock', 'cb'
    '''
    
    # 参数翻译
    set_list_null        = dict_trade['null']
    set_flt_rate_loan    = dict_trade['loanRate']
    set_flt_fee_cb       = dict_trade['feeCB']
    set_flt_fee_stock    = dict_trade['feeStock']
    # fix_position         = dict_trade['fix']
    # single_leg           = dict_trade['singleLeg']
    
    stgy_df_trade_signal = pd.DataFrame(0, index=stgy_df_cb_full.index, columns=['trade_cycle', 'mdDays'])
    tmp_df_data = (stgy_df_cb_full['close_stock'] / stgy_df_cb_full['strike_price']).replace(set_list_null, 0)
    tmp_df_data[tmp_df_data< 1.3] = 0
    tmp_df_data[tmp_df_data>=1.3] = 1
    stgy_df_trade_signal['mdDays'] = tmp_df_data.rolling(window=30, min_periods=0).sum()

    # 修正交易信号约束条件：正股停牌时不影响交易状态；只需要将正股与转债 上市前/退市后 的日期剔除即可; 可转债已发布赎回公告的不予交易
    tmp_df_amount_stock = stgy_df_cb_full['amount_stock'][stgy_df_cb_full['amount_stock']>0]
    tmp_df_amount_cb    = stgy_df_cb_full['amount_cb'][stgy_df_cb_full['amount_cb']>0]
    if len(tmp_df_amount_stock) <=0 or len(tmp_df_amount_cb) <= 0:
        pass
    else:
        tmp_date_start      = max(min(tmp_df_amount_stock.index), min(tmp_df_amount_cb.index))
        tmp_date_end        = min(max(tmp_df_amount_stock.index), max(tmp_df_amount_cb.index), 
                                  data_df_tickers_cb.loc[stgy_str_cb, ['DateRedeemNotice', 'InterestDateEnd']].min())
        stgy_df_trade_signal.loc[tmp_date_start:tmp_date_end, 'trade_cycle'] = 1

    # 计算交易信号和买入仓位
    if True:
        
        # 买入条件：卖出条件：
        stgy_df_trade_signal['signal'] = False
        for u in stgy_df_trade_signal.index:
            tmp_bool_buy, tmp_bool_sell = True, False
            for p in dict_rules['buy']:
                tmp_bool_buy  = tmp_bool_buy  and eval(p)
            for q in dict_rules['sell']:
                tmp_bool_sell = tmp_bool_sell or  eval(q)
            if tmp_bool_buy:
                stgy_df_trade_signal.loc[u:, 'signal'] = True
            if tmp_bool_sell:
                stgy_df_trade_signal.loc[u:, 'signal'] = False
        
    
        # 反过来根据交易周期对交易信号再次进行矫正，把不属于交易周期内的买入信号剔除掉
        stgy_df_trade_signal['signal'][stgy_df_trade_signal['trade_cycle']==0] = False
        
        # 持有转债头寸：注意股票头寸是负数
        stgy_df_trade_signal['position_cb']    =   stgy_df_trade_signal['signal'].apply(lambda x:1 if x else 0)
        stgy_df_trade_signal['position_stock'] = -(stgy_df_trade_signal['position_cb'] * stgy_df_cb_full['conv_rate'] * stgy_df_cb_full['delta']).fillna(0)
    
        # 修正停牌期间的头寸数据
        tmp_df_suspend = pd.concat([stgy_df_cb_full[['amount_cb', 'amount_stock']], stgy_df_trade_signal[['trade_cycle']]], axis=1)
        tmp_df_suspend['date'] = tmp_df_suspend.index
        tmp_df_suspend['date_prev'] = tmp_df_suspend['date'].shift(1)
        tmp_df_suspend = tmp_df_suspend[(tmp_df_suspend['trade_cycle']>0) & (tmp_df_suspend['amount_stock']<=0)].sort_index(ascending=True)
        for ii in range(len(tmp_df_suspend)):
            stgy_df_trade_signal.loc[tmp_df_suspend['date'].iloc[ii]] = stgy_df_trade_signal.loc[tmp_df_suspend['date_prev'].iloc[ii]]
    
    # 调整交易方式
    if True:
        
        # delta-hedge 开关：
        if dict_trade['deltaHedge']:
            pass
        else:
            tmp_df_position_stock_nodelta = stgy_df_trade_signal['position_stock'].copy(deep=True)
            # 寻找买入/卖出交易的时点，将这个状态覆盖后续所有日期
            tmp_df_signal_adjust = stgy_df_trade_signal['signal'] ^ stgy_df_trade_signal['signal'].shift(1)
            tmp_df_signal_adjust = tmp_df_signal_adjust[tmp_df_signal_adjust==True].sort_index()
            for u in tmp_df_signal_adjust.index:
                # stgy_df_trade_signal.loc[u:, 'position_stock'] = tmp_df_position_stock_nodelta.loc[u]
                # 修复一个错误：固定对冲比例，股票分红送转后对冲头寸未能相应变化
                stgy_df_trade_signal.loc[u:, 'position_stock'] = tmp_df_position_stock_nodelta.loc[u]\
                                                               * stgy_df_cb_full.loc[u:, 'conv_rate'] / stgy_df_cb_full.loc[u, 'conv_rate']
        
        # 根据交易方式调整仓位：
        if dict_trade['fix'] == 'cb':
            pass
        elif dict_trade['fix'] == 'stock':
            stgy_df_trade_signal['position_cb']    = (stgy_df_trade_signal['position_cb'] / stgy_df_trade_signal['position_stock']).abs().replace(set_list_null, 0)
            stgy_df_trade_signal['position_stock'] =  stgy_df_trade_signal['position_stock'].apply(lambda x:-1 if x<0 else 0)
        else:
            print(">>> 缺失交易模式参数")
            return None
        
        # 单腿测试
        if dict_trade['singleLeg'] == 'stock':
            stgy_df_trade_signal['position_cb'] = 0
        elif dict_trade['singleLeg'] == 'cb':
            stgy_df_trade_signal['position_stock'] = 0
    
    # 计算盈亏
    if True:
        
        # 转债收益：当日收盘价减去昨日收盘价，再乘以昨日仓位
        # 股票收益：当日折算收盘价减去昨日收盘价，再乘以昨日仓位
        stgy_df_trade_signal['pnl_cb']     = stgy_df_trade_signal['position_cb'].shift(1) * (stgy_df_cb_full['close_cb'] - stgy_df_cb_full['close_cb'].shift(1))
        tmp_df_stock_adj = (stgy_df_cb_full['adjfactor'] / stgy_df_cb_full['adjfactor'].shift(1)).replace(set_list_null, 1)
        stgy_df_trade_signal['pnl_stock']  = stgy_df_trade_signal['position_stock'].shift(1)\
                                           * (stgy_df_cb_full['close_stock'] * tmp_df_stock_adj - stgy_df_cb_full['close_stock'].shift(1))
        
        # 交易成本;注意
        stgy_df_trade_signal['cost_loan']  = set_flt_rate_loan *  stgy_df_trade_signal['position_stock'].shift(1).abs() * stgy_df_cb_full['close_stock']
        stgy_df_trade_signal['cost_cb']    = set_flt_fee_cb    * (stgy_df_trade_signal['position_cb']    - stgy_df_trade_signal['position_cb'].shift(1)).abs()
        stgy_df_trade_signal['cost_stock'] = set_flt_fee_stock * (stgy_df_trade_signal['position_stock'] - stgy_df_trade_signal['position_stock'].shift(1)).abs()
    
        # 分钟线交易
        if (df_minute is None) or (set_flt_minute_trade<=0):
            stgy_df_trade_signal['pnl_stock_minute'] = 0
        else:
            stgy_df_trade_signal['pnl_stock_minute'] = trade_minute_simulation(stgy_df_trade_signal, stgy_df_cb_full, df_minute, set_flt_minute_trade)
                
        # 综合收益：注意，股票仓位是负数，所以是加pnl_stock
        stgy_df_trade_signal['pnl_entire']    = stgy_df_trade_signal['pnl_cb']  + stgy_df_trade_signal['pnl_stock']  + stgy_df_trade_signal['pnl_stock_minute']\
                                              - stgy_df_trade_signal['cost_cb'] - stgy_df_trade_signal['cost_stock'] - stgy_df_trade_signal['cost_loan']
                                              
        # 计算收益率
        stgy_df_trade_signal['return_entire'] = stgy_df_trade_signal['pnl_entire'] / (stgy_df_cb_full['close_cb']    * stgy_df_trade_signal['position_cb']
                                                                                   +  stgy_df_cb_full['close_stock'] * stgy_df_trade_signal['position_stock'].abs()).shift(1)
            
        stgy_df_trade_signal = stgy_df_trade_signal.replace(set_list_null, 0)
        stgy_df_trade_signal['return_accum']  = (stgy_df_trade_signal['return_entire']+1).cumprod()
    
    return stgy_df_trade_signal


# 测算日内交易带来的增益
def trade_minute_simulation(stgy_df_trade_signal, stgy_df_cb_full, df_minute, set_flt_minute_trade):
    ''' 测算日内交易带来的增益 '''
    
    tmp_df_minute_pnl = pd.DataFrame(0, index=stgy_df_trade_signal.index, columns=['pnl_stock_minute'])
    tmp_df_signal = stgy_df_trade_signal.copy(deep=True)
    tmp_df_signal['lastDay'] = tmp_df_signal.index

    tmp_df_signal[['signal', 'lastDay']] = tmp_df_signal[['signal', 'lastDay']].shift(1).fillna(False)
    tmp_df_signal = tmp_df_signal[(tmp_df_signal['signal']) & (tmp_df_signal.index.isin(df_minute.index))]

    for tmp_dt_b in tmp_df_signal.index:
        # tmp_dt_b是日内交易当天；tmp_dt_a是日内交易的前一天（当天已经有底仓）
        tmp_dt_a = tmp_df_signal.loc[tmp_dt_b, 'lastDay']
        
        # 计算复权因子，并计算基准价格（对基准价格进行调整）；注意复权因子的使用方法与传统不同
        tmp_flt_adjfactor = stgy_df_cb_full.loc[tmp_dt_b, 'adjfactor']   / stgy_df_cb_full.loc[tmp_dt_a, 'adjfactor']
        tmp_flt_price     = stgy_df_cb_full.loc[tmp_dt_a, 'close_stock'] / tmp_flt_adjfactor
        
        # 抽取价格数据进行处理
        intm_df_minute = df_minute.T[[tmp_dt_b]].sort_index(ascending=True).fillna(method='ffill').rename(columns={tmp_dt_b:'price'})
        intm_df_minute['position']   = stgy_df_trade_signal.loc[tmp_dt_a, 'position_stock']
        
        tmp_se_delta = stgy_df_cb_full.loc[tmp_dt_b, ['close_stock', 'strike_price', 'maturity', 'riskfree', 'hv']]
        tmp_se_delta.index = ['S', 'K', 'T', 'r', 'sigma']
        tmp_flt_stock_position_factor = -stgy_df_trade_signal.loc[tmp_dt_a, 'position_cb'] * stgy_df_cb_full.loc[tmp_dt_a, 'conv_rate']
        
        # 日内波段操作：上涨卖出，下跌买入
        for jj in range(0,len(intm_df_minute)):
            # 由于底仓是负的，所以上涨卖出仓位绝对值变大
            if  intm_df_minute['price'].iloc[jj]       > tmp_flt_price * (1+set_flt_minute_trade):
                tmp_flt_price                          = tmp_flt_price * (1+set_flt_minute_trade)
                tmp_se_delta.loc['S']                  = tmp_flt_price
                intm_df_minute['position'].iloc[jj:]   = op.find_delta_row(tmp_se_delta) * tmp_flt_stock_position_factor
            # 由于底仓是负的，下跌买入仓位绝对值变小
            elif intm_df_minute['price'].iloc[jj]      < tmp_flt_price * (1-set_flt_minute_trade):
                tmp_flt_price                          = tmp_flt_price * (1-set_flt_minute_trade)
                tmp_se_delta.loc['S']                  = tmp_flt_price
                intm_df_minute['position'].iloc[jj:]   = op.find_delta_row(tmp_se_delta) * tmp_flt_stock_position_factor
        
        # 计算仓位的变动，和对应产生的额外交易成本/额外收益
        intm_df_minute['position_change'] = (intm_df_minute['position'] - intm_df_minute['position'].shift(1)).fillna(0)
        tmp_df_cost_extra = (intm_df_minute['position_change'].abs() * intm_df_minute['price']).sum() * 0.001
        tmp_df_pnl_extra  = (intm_df_minute['position_change']       * (stgy_df_cb_full.loc[tmp_dt_b, 'close_stock'] - intm_df_minute['price'])).sum()
        
        # 计算日内交易带来的额外收益：
        tmp_df_minute_pnl.loc[tmp_dt_b, 'pnl_stock_minute'] = tmp_df_pnl_extra - tmp_df_cost_extra

    return tmp_df_minute_pnl




    
if __name__=='__main__':
    
    pass

