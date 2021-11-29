# -*- coding: utf-8 -*-
"""
Created on Mon Nov 29 13:12:28 2021

@author: 好鱼
"""


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

# 可选值为 trade / backtest
# 默认为回测模式，可在外部调用和更改
mode = 'trade'
path = 'C:/InvestmentResearch/database/minute/'

def print_mode():
    print(">>> mode:", mode)
    return 0


# 读取分钟数据
def read_file_minute(dt_date, str_file):
    ''' 读取分钟数据 '''    
    tmp_int_date = str(time_to_int(dt_date))
    data_df = pd.read_csv(path+str_file+'/'+str_file+'_'+tmp_int_date+'.csv', encoding='utf_8_sig').iloc[:,1:]
    data_df['timestamp'] = data_df['timestamp'].apply(lambda x:dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
    
    return data_df
    
    
    
    
    