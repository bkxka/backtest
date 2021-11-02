# -*- coding: utf-8 -*-
"""
Created on Sat Feb  6 10:19:10 2021

@author: 好鱼

20210701: 修改了读取下单记录的函数，将一些处理过程从读取函数转移到指令函数，以使函数适应更普遍的场景

"""

from datetime import *
import pandas as pd
import numpy as np
import math
import os

import data_access.eastmoney as em

# 获取交易指令清单，支持从模型历史回测中提取交易指令清单
def get_order(method, dt_date, flt_totalAsset, df_target_position, df_close):
    ''' 获取最新的交易清单，目前只支持从模型历史回测中提取交易指令清单 '''
    if method == 'history':
        tmp_df_record = df_target_position[df_target_position>0]
        
        data_df_orderList = pd.DataFrame(0, index=tmp_df_record.index, columns=['date', 'weight', 'position', 'close', 'volume', 'volume_round'])
        data_df_orderList['date']     = dt_date
        data_df_orderList['weight']   = tmp_df_record
        data_df_orderList['position'] = data_df_orderList['weight'] * flt_totalAsset * 10000
        data_df_orderList['close']    = df_close.loc[dt_date, tmp_df_record.index]
        data_df_orderList['volume']   = (data_df_orderList['position'] / data_df_orderList['close']).apply(lambda x:int(x))
        data_df_orderList['volume_round'] = data_df_orderList['volume'].apply(lambda x:round(x/100)*100)
        
        return data_df_orderList
    elif method == 'newlyCreated':
        return 0
    else:
        return 0
        
        
# 读取历史的下单记录
def read_prev_orders(str_path, str_file):
    ''' 读取历史的下单记录 '''
    list_files_qualified = [v for v in os.listdir(str_path) if str_file in v]
    if list_files_qualified == []:
        return None
    else:
        str_file_latest = max(list_files_qualified)
        tmp_file = pd.read_csv(str_path+str_file_latest, index_col=0, encoding='utf_8_sig')
        # 20210701
        # tmp_file = tmp_file[['position_new', 'hold_new', 'surplus']]
        # tmp_file = tmp_file[tmp_file['hold_new']>0].dropna().rename(columns={"position_new":"position_old", "hold_new":"hold_old", "surplus":"surplus"})
        return tmp_file
        

# 根据最新价格生成最新的下单指令
def get_trade_order(stgy_df_order_last, stgy_df_order_new, flt_totalAsset, path, str_filename, int_lot=100):
    ''' 根据最新价格生成最新的下单指令 '''

    if stgy_df_order_last is None:
        df_result = stgy_df_order_new.copy(deep=True)
    else:
        # 20210701
        # df_result = pd.concat([stgy_df_order_last, stgy_df_order_new], axis=1).fillna(0)
        tmp_file = stgy_df_order_last[['position_new', 'hold_new', 'surplus']]
        tmp_file = tmp_file[tmp_file['hold_new']>0].dropna().rename(columns={"position_new":"position_old", "hold_new":"hold_old", "surplus":"surplus"})
        df_result = pd.concat([tmp_file, stgy_df_order_new], axis=1).fillna(0)
    
    df_result['price_new'] = [em.get_price_realtime(x) for x in df_result.index]
    if flt_totalAsset is not None:
        df_result['totalAsset'] = flt_totalAsset
    else:
        df_result['totalAsset'] = (df_result['price_new'] * df_result['hold_old']).sum() + df_result['surplus'].iloc[0]

    # df_result['price_new'] = [em.get_price_realtime(x) for x in df_result.index]
    df_result['value_new'] = df_result['totalAsset'] * df_result['position_new']
    df_result['hold_new'] = (df_result['value_new'] / df_result['price_new']).apply(lambda x:round(x/int_lot)*int_lot)

    df_result['netbuy'] = df_result['hold_new'] if (stgy_df_order_last is None) else df_result['hold_new'] - df_result['hold_old']
    df_result['surplus'] = df_result['totalAsset'] - (df_result['hold_new'] * df_result['price_new']).sum() 
    df_result['orderTime'] = str(datetime.now())[:19]
    df_result.to_csv(path+str_filename, encoding='utf_8_sig')

    return 0



    
if __name__=='__main__':
    
    pass

    