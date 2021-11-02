# -*- coding: utf-8 -*-
"""
Created on Thu Oct 28 11:37:52 2021

@author: 好鱼
"""


import math
import pandas as pd
import numpy as np
from datetime import *
    
import dataset as ds
from tools.tools_func import *


''' 导入数据 '''
def get_df_ahPriceGap():
    
    set_list_null      = [np.inf, -np.inf, np.nan]
    data_df_tickers_ah = ds.read_file("ticker_ah")
    data_df_close      = ds.load_price('close')[list(data_df_tickers_ah['Ashare'])]
    
    # 读取港股数据，并将列名转化成A股代码便于比较
    tmp_dict_ah_tickers  = data_df_tickers_ah.to_dict()
    tmp_dict_ah_tickers  = {tmp_dict_ah_tickers['Hshare'][v]:tmp_dict_ah_tickers['Ashare'][v] for v in tmp_dict_ah_tickers['Hshare']}
    data_df_close_hshare = ds.load_price('hclose').rename(columns=tmp_dict_ah_tickers)
    
    ''' 处理数据，生成股票策略分值表 '''
    # 打分策略：两地价差
    factor_df_ahPriceGap = (data_df_close_hshare / data_df_close).replace(set_list_null, 0)

    return factor_df_ahPriceGap
    



if __name__=='__main__':
    
    factor_df_ahPriceGap = get_df_ahPriceGap()
    pass

