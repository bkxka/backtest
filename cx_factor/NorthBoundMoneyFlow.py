# -*- coding: utf-8 -*-
"""
Created on Thu Oct 28 11:47:18 2021

@author: 好鱼
"""


import math
import pandas as pd
import numpy as np
from datetime import *
import dataset as ds
from tools.tools_func import *


''' 处理数据，生成股票策略分值表 '''
def get_df_nbmf(int_lbw=90):
    
    # 回测参数设置
    set_list_null = [np.inf, -np.inf, np.nan]
    
   # 导入数据
    data_df_close        = ds.load_price('close')
    data_df_dayReturn    = ds.load_price('dayReturn')
    data_df_floatAmktcap = ds.load_price('floatAmktcap')
    data_df_shszhkHold   = ds.load_price('shszhkHold')
    data_df_shszhkBuy    = ds.get_netBuy(data_df_shszhkHold, data_df_close, data_df_dayReturn)
        
    tmp_df_score_raw = (data_df_shszhkBuy / data_df_floatAmktcap).replace(set_list_null, 0)
    factor_df_nbmf   = tmp_df_score_raw.rolling(window=int_lbw, min_periods=1).sum().fillna(0)
    
    return factor_df_nbmf




if __name__=='__main__':
    
    factor_df_nbmf = get_df_nbmf(90)
    
    
    
    