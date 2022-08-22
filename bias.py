# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 10:07:29 2021

@author: 好鱼
"""

import pandas as pd
import numpy as np
import math
from datetime import *
import matplotlib.pyplot as plt

from tools.tools_func import *

import os
import sys



# 根据交易指令和交割单计算交易成本/误差
def analysis_bias_etf(data_df_delivery_etf, data_df_orders_etf):
    ''' 根据交易指令和交割单计算交易成本/误差 '''
    if len(set(data_df_delivery_etf['成交日期'])) != len(data_df_orders_etf):
        print(">>> ETF Timing 策略交易记录无法对应，请检查程序或交易记录！")
        return 0
    else:
        aly_df_bias_trade_etf = data_df_orders_etf.copy(deep=True)[['price', 'netbuy', 'asset', 'surplus']]
        aly_df_bias_trade_etf['totalAsset'] = aly_df_bias_trade_etf['asset'] + aly_df_bias_trade_etf['surplus']
        aly_df_bias_trade_etf['priceReal'] = 0
        aly_df_bias_trade_etf['fee'] = 0
        
        for ii in range(len(data_df_orders_etf)):
            tmp_dt = data_df_orders_etf.index[ii]
            tmp_df = data_df_delivery_etf[data_df_delivery_etf['成交日期']==tmp_dt]
            aly_df_bias_trade_etf.loc[tmp_dt, 'priceReal'] = abs(tmp_df['成交金额'].sum() / tmp_df['成交数量'].sum())
            aly_df_bias_trade_etf.loc[tmp_dt, 'fee'] = tmp_df['佣金'].sum() + tmp_df['印花税'].sum() \
                                                     + tmp_df['其他费'].sum() + tmp_df['过户费'].sum()

        aly_df_bias_trade_etf['priceBias'] = aly_df_bias_trade_etf['priceReal'] - aly_df_bias_trade_etf['price']
        aly_df_bias_trade_etf['tradeCost'] = aly_df_bias_trade_etf['fee']\
                                           + aly_df_bias_trade_etf['priceBias'] * aly_df_bias_trade_etf['netbuy']

        return aly_df_bias_trade_etf


# 比较回测与实盘交易的净值曲线
def analysis_netvalue_etf(df_netvalue_backtest, df_price_close, str_etf_ticker, aly_df_bias_trade_etf, data_df_orders_etf, df_index):
    ''' 比较回测与实盘交易的净值曲线 '''
    
    # aly_df_netvalue = stgETF.result_df_netvalue.copy(deep=True).loc[min(data_df_orders_etf.index):].rename(columns={str_etf_ticker:'backtest_netvalue'})
    # aly_df_netvalue['price'] = ds.read_file('etf_close').loc[aly_df_netvalue.index, str_etf_ticker]
    aly_df_netvalue = df_netvalue_backtest.loc[min(data_df_orders_etf.index):].rename(columns={str_etf_ticker:'backtest_netvalue'})
    aly_df_netvalue['price'] = df_price_close.loc[aly_df_netvalue.index, str_etf_ticker]
    aly_df_netvalue['index_close'] = df_index.loc[min(data_df_orders_etf.index):]
    aly_df_netvalue['holding'] = 0
    aly_df_netvalue['surplus'] = 0
    aly_df_netvalue['cost'] = 0
    for ii in range(len(aly_df_bias_trade_etf)):
        tmp_dt = aly_df_bias_trade_etf.index[ii]
        aly_df_netvalue.loc[tmp_dt:, 'cost']    = aly_df_netvalue.loc[tmp_dt:, 'cost'] + aly_df_bias_trade_etf.loc[tmp_dt, 'tradeCost']
        aly_df_netvalue.loc[tmp_dt:, 'surplus'] = aly_df_bias_trade_etf.loc[tmp_dt, 'surplus']
        aly_df_netvalue.loc[tmp_dt:, 'holding'] = aly_df_netvalue.loc[tmp_dt:, 'holding'] + aly_df_bias_trade_etf.loc[tmp_dt, 'netbuy']
    aly_df_netvalue['trade_netvalue'] = aly_df_netvalue['holding'] * aly_df_netvalue['price'] \
                                      + aly_df_netvalue['surplus'] - aly_df_netvalue['cost']
                                      
    aly_df_netvalue['netvalue_backtest'] = aly_df_netvalue['backtest_netvalue'] / aly_df_netvalue['backtest_netvalue'].iloc[0]
    aly_df_netvalue['netvalue_trade']    = aly_df_netvalue['trade_netvalue']    / aly_df_netvalue['trade_netvalue'].iloc[0]
    aly_df_netvalue['close_index']       = aly_df_netvalue['index_close']       / aly_df_netvalue['index_close'].iloc[0]
    # aly_df_netvalue['excess_return']     = aly_df_netvalue['netvalue_trade']    - aly_df_netvalue['close_index']

    return aly_df_netvalue


# 读取股票交易指令
def get_orders_cx(str_path_order, str_cx_ah):
    ''' 读取交易指令 '''
    
    list_orders = os.listdir(str_path_order)
    data_df_orders_cx_ah = pd.DataFrame()
    for u in list_orders:
        if str_cx_ah in u:
            # data_df_orders_cx_ah = data_df_orders_cx_ah.append(pd.read_csv(str_path_order+u, encoding='utf_8_sig'))
            data_df_orders_cx_ah = pd.concat([data_df_orders_cx_ah, pd.read_csv(str_path_order+u, encoding='utf_8_sig')], axis=0)
    data_df_orders_cx_ah['date'] = data_df_orders_cx_ah['orderTime'].apply(lambda x:str_to_time(x))
    # data_df_orders_cx_ah['date'] = data_df_orders_cx_ah['orderTime'].apply(lambda x:str_to_time(x.split(' ')[0]))
    data_df_orders_cx_ah.rename(columns={'Unnamed: 0':'ticker'}, inplace=True)
    data_df_orders_cx_ah['ticker'] = data_df_orders_cx_ah['ticker'].apply(lambda x:int(x.split('.')[0]))
    data_df_orders_cx_ah = data_df_orders_cx_ah[data_df_orders_cx_ah['netbuy']!=0]
    
    return data_df_orders_cx_ah


# 从交割单中提取相关的交易记录,分红利和成交记录两部分
def get_delivery_cx(data_df_delivery, data_df_orders_cx_ah, list_ignore=[]):
    ''' 从交割单中提取相关的交易记录,分红利和成交记录两部分 '''
    
    data_list_tickers = list(set(data_df_orders_cx_ah['ticker']))
    data_df_delivery_cx_ah = data_df_delivery[(data_df_delivery['证券代码'].isin(data_list_tickers)) \
                                            & (data_df_delivery['操作'].isin(['股息入账', '股息红利税补', '兑息扣税']))]
    for ii in range(len(data_df_orders_cx_ah)):
        tmp_dict = data_df_orders_cx_ah.iloc[ii].to_dict()
        tmp_df = data_df_delivery[(data_df_delivery['成交日期']==tmp_dict['date']) \
                                & (data_df_delivery['证券代码']==tmp_dict['ticker'])]
        if tmp_df['成交数量'].sum()==tmp_dict['netbuy']:
            # data_df_delivery_cx_ah = data_df_delivery_cx_ah.append(tmp_df)
            data_df_delivery_cx_ah = pd.concat([data_df_delivery_cx_ah, tmp_df], axis=0)
        else:
            if tmp_dict['date'] in list_ignore:
                pass
            else:
                print(">>> 找不到对应的交割记录，请检查相关的交易指令和交割单 ...")
                print("Date:", tmp_dict['date'], "Ticker:", tmp_dict['ticker'])
            
    return data_df_delivery_cx_ah


# 根据交易指令和交割单计算交易成本/误差
def analysis_bias_cx(data_df_orders_cx_ah, data_df_delivery_cx_ah):
    ''' 根据交易指令和交割单计算交易成本/误差 '''
    
    data_list_dates = list(set(data_df_orders_cx_ah['date']))
    list_bias_columns = ['buy_order', 'buy_delivery', 'sell_order', 'sell_delivery', 'netTrade', 'tradeSlide', 'fee', 'tradeCost']
    aly_df_bias_trade_cx = pd.DataFrame(0,index=data_list_dates,columns=list_bias_columns)
        
    for u in data_list_dates:
        
        tmp_df_order = data_df_orders_cx_ah[data_df_orders_cx_ah['date']==u]
        tmp_df_order_buy = tmp_df_order[tmp_df_order['netbuy']>0]
        tmp_df_order_sell = tmp_df_order[tmp_df_order['netbuy']<0]
    
        aly_df_bias_trade_cx.loc[u, 'buy_order'] = (tmp_df_order_buy['price_new'] * tmp_df_order_buy['netbuy']).sum()
        aly_df_bias_trade_cx.loc[u, 'sell_order'] = (tmp_df_order_sell['price_new'] * tmp_df_order_sell['netbuy']).sum()
        aly_df_bias_trade_cx.loc[u, 'netTrade'] = aly_df_bias_trade_cx.loc[u, 'buy_order'] - aly_df_bias_trade_cx.loc[u, 'sell_order']
        
        tmp_df_delivery = data_df_delivery_cx_ah[data_df_delivery_cx_ah['成交日期']==u]
        tmp_df_delivery_buy = tmp_df_delivery[tmp_df_delivery['操作']=='证券买入']
        tmp_df_delivery_sell = tmp_df_delivery[tmp_df_delivery['操作']=='证券卖出']
        
        aly_df_bias_trade_cx.loc[u, 'buy_delivery'] = tmp_df_delivery_buy['成交金额'].sum()
        aly_df_bias_trade_cx.loc[u, 'sell_delivery'] = - tmp_df_delivery_sell['成交金额'].sum()
        aly_df_bias_trade_cx.loc[u, 'fee'] = tmp_df_delivery['佣金'].sum() + tmp_df_delivery['印花税'].sum() \
                                           + tmp_df_delivery['其他费'].sum() + tmp_df_delivery['过户费'].sum()

    aly_df_bias_trade_cx['netTrade']   = aly_df_bias_trade_cx['buy_delivery'] - aly_df_bias_trade_cx['sell_delivery']
    # 注意，卖出交割金额（负数）大于卖出指令金额（负数），即金额绝对值偏少，冲击成本为正
    aly_df_bias_trade_cx['tradeSlide'] = aly_df_bias_trade_cx['buy_delivery']  - aly_df_bias_trade_cx['buy_order']\
                                       + aly_df_bias_trade_cx['sell_delivery'] - aly_df_bias_trade_cx['sell_order']
    # aly_df_bias_trade_cx['tradeSlide'] = aly_df_bias_trade_cx['buy_delivery'] - aly_df_bias_trade_cx['buy_order']\
    #                                    + aly_df_bias_trade_cx['sell_order']   - aly_df_bias_trade_cx['sell_delivery']  
    aly_df_bias_trade_cx['tradeCost']  = aly_df_bias_trade_cx['tradeSlide'] + aly_df_bias_trade_cx['fee']

    return aly_df_bias_trade_cx


# 比较回测与实盘交易的净值曲线
def analysis_netvalue_cx(df_netvalue_backtest, data_df_price, aly_df_bias_trade_cx, data_df_orders_cx, data_df_delivery_cx, df_index, 
                         range_period=(datetime(2001,1,1), datetime(2050,1,1))):
    ''' 
    比较回测与实盘交易的净值曲线
    新增一个默认参数range_period，限定比较的时间区间；默认参数区间远大于实际操作区间【注意这会剪掉时间区间外的所有交易记录】
    '''
    tmp_df_orders_cx  = data_df_orders_cx[(data_df_orders_cx['date']>=range_period[0]) & (data_df_orders_cx['date']<=range_period[1])]
    data_list_dates   = list(sorted(set(tmp_df_orders_cx['date'])))
    dt_start          = min(data_list_dates)                # 回测的起点，通常这一天包含了初次建仓的全部指令
    dt_end            = range_period[1] if range_period[1]<datetime(2050,1,1) else max(data_list_dates)
    data_list_tickers = list(set(tmp_df_orders_cx['ticker']))

    df_price = data_df_price.rename(columns=lambda x:ticker_str_to_int(x)).loc[dt_start:, data_list_tickers]
    aly_df_netvalue_cx = df_netvalue_backtest.loc[dt_start:dt_end,'netvalueCosted'].to_frame().rename(columns={'netvalueCosted':'backtest_netvalue'})
    aly_df_netvalue_cx['index_close'] = df_index.loc[dt_start:]
    
    # 计算股票头寸和相应的收盘总市值，注意这里没有考虑送股和转股的影响
    # 注意这里有一个bug，如果一个测试之前做过，但是结束后没有做清零处理，导致之前残存的仓位留到后面，影响了总净值
    tmp_df_holding = pd.DataFrame(0, index=df_price.index, columns=data_list_tickers)
    for ii in range(len(tmp_df_orders_cx)):
        tmp_dict = tmp_df_orders_cx.iloc[ii].to_dict()
        tmp_df_holding.loc[tmp_dict['date']:, tmp_dict['ticker']] = tmp_dict['hold_new']
    aly_df_netvalue_cx['stocksAsset'] = (tmp_df_holding * df_price).sum(axis=1)
    
    # 计算浮金和交易成本的影响
    aly_df_netvalue_cx['cost'] = 0
    aly_df_netvalue_cx['surplus'] = 0
    aly_df_netvalue_cx['dividends'] = 0
    for u in data_list_dates:
        aly_df_netvalue_cx.loc[u:, 'surplus'] = tmp_df_orders_cx[tmp_df_orders_cx['date']==u]['surplus'].mean()
    for u in aly_df_bias_trade_cx.index:
        aly_df_netvalue_cx.loc[u:, 'cost'] = aly_df_netvalue_cx.loc[u:, 'cost'] + aly_df_bias_trade_cx.loc[u, 'tradeCost']
    tmp_df_dividends = data_df_delivery_cx[data_df_delivery_cx['操作'].isin(['股息入账', '股息红利税补', '兑息扣税'])]
    for ii in range(len(tmp_df_dividends)):
        tmp_dict = tmp_df_dividends.iloc[ii].to_dict()
        aly_df_netvalue_cx.loc[tmp_dict['成交日期']:, 'dividends'] = aly_df_netvalue_cx.loc[tmp_dict['成交日期']:, 'dividends'] + tmp_dict['发生金额']

    aly_df_netvalue_cx['trade_netvalue'] = aly_df_netvalue_cx['stocksAsset'] + aly_df_netvalue_cx['dividends'] \
                                         + aly_df_netvalue_cx['surplus']     - aly_df_netvalue_cx['cost']

    # 计算归一化的净值曲线
    aly_df_netvalue_cx['netvalue_backtest'] = aly_df_netvalue_cx['backtest_netvalue'] / aly_df_netvalue_cx['backtest_netvalue'].iloc[0]
    aly_df_netvalue_cx['netvalue_trade']    = aly_df_netvalue_cx['trade_netvalue']    / aly_df_netvalue_cx['trade_netvalue'].iloc[0]
    aly_df_netvalue_cx['close_index']       = aly_df_netvalue_cx['index_close']       / aly_df_netvalue_cx['index_close'].iloc[0]
    # aly_df_netvalue_cx['excess_return']     = aly_df_netvalue_cx['netvalue_trade']    - aly_df_netvalue_cx['close_index']

    return aly_df_netvalue_cx


def analysis_cut(aly_df_netvalue_cx, list_date_cut):
    '''有过仓位调整的记录<新增>'''

    for u in list_date_cut:
        try:
            tmp_t = max([v for v in aly_df_netvalue_cx.index if v < u])
            tmp_adj = (aly_df_netvalue_cx.loc[u, 'netvalue_backtest'] / aly_df_netvalue_cx.loc[tmp_t, 'netvalue_backtest'])\
                    / (aly_df_netvalue_cx.loc[u, 'netvalue_trade']    / aly_df_netvalue_cx.loc[tmp_t, 'netvalue_trade'])
            aly_df_netvalue_cx.loc[u:, 'netvalue_trade'] = aly_df_netvalue_cx.loc[u:, 'netvalue_trade'] * tmp_adj
        except:
            pass

    return aly_df_netvalue_cx


def plot_stack_line(df_data, list_lines, list_stacks, str_path, str_title):
    ''' 绘制面积图和折线图 '''
    # df_data = aly_df_netvalue.iloc[:,-4:]
    # list_lines = ['netvalue_backtest', 'netvalue_trade', 'close_index']
    # list_stacks = ['excess_return']
    
    list_lightcolor = ['lightskyblue', 'lightsalmon', 'lightsage', 'lightgray']
    list_darkcolor  = ['limegreen', 'indianred', 'violet', 'yellowgreen', 'dimgray', 'deepskyblue', 'magenta']
    
    # 设置图像格式，尺寸，主次坐标轴等
    plt.style.use("ggplot")
    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax2 = ax1.twinx()
    list_index = [str(v)[:10] for v in df_data.index]
    xticks = np.array(list_index)

    # 绘制面积图
    ax1.stackplot(xticks, [df_data[v] for v in list_stacks], labels=list_stacks, colors=list_lightcolor[:len(list_stacks)])
    # 绘制折线图
    for jj in range(len(list_lines)):
        tmp_label = list_lines[jj]
        ax2.plot(xticks,      df_data[tmp_label], label=tmp_label, color=list_darkcolor[jj])
        
    # 重设坐标轴，只显示12个坐标
    try:
        list_index = [list_index[ii] for ii in range(len(list_index)) if ii%(len(list_index)//12)==0]
    except:
        pass
            
    ax1.set_xticks(np.array(list_index))
    ax1.set_xticklabels(list_index, fontsize=12, rotation=30)
    
    ax1.legend(fontsize=12, loc='upper right')
    ax2.legend(fontsize=12, loc='upper left')    
    plt.title(str_title, fontsize=15)
    
    plt.savefig(str_path+str_title+'.png')
    
    return 0
    

    
if __name__=='__main__':
    
    pass
