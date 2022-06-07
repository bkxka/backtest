# -*- coding: utf-8 -*-
"""
Created on Thu Sep 16 11:53:45 2021

@author: 好鱼
"""

import pandas as pd
import numpy as np
import datetime as dt
import os



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# 股票代码格式转换函数：数值<-->同花顺代码

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

ticker_str_to_int = lambda x:int(x.split('.')[0])
ticker_int_to_str = lambda x:str(x)+'.SH' if x>=600000 else '0'*(6-len(str(x)))+str(x)+'.SZ'



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# 时间处理函数:int<-->datetime(timestamp)<-->str
# 输出str格式：2019-10-01

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

time_to_int = lambda x:x.year*10000+x.month*100+x.day
int_to_time = lambda x:dt.datetime.strptime(str(x), "%Y%m%d") if len(str(x))==8 else np.nan

time_to_str = lambda x:dt.datetime.strftime(x, "%Y-%m-%d")
str_to_time = lambda x:dt.datetime.strptime(x.split(' ')[0], "%Y-%m-%d") if '-' in x else\
                       dt.datetime.strptime(x.split(' ')[0], "%Y/%m/%d") if '/' in x else\
                       dt.datetime.strptime(x.split(' ')[0], "%Y%m%d")
                          
int_to_str  = lambda x:str(x)[:4] + '-' + str(x)[4:6] + '-' + str(x)[6:]
str_to_int  = lambda x:int(x.split('-')[0])*10000 + int(x.split('-')[1])*100 + int(x.split('-')[2].split(' ')[0]) if '-' in x else\
                       int(x.split('/')[0])*10000 + int(x.split('/')[1])*100 + int(x.split('/')[2].split(' ')[0]) if '/' in x else\
                       int(x)

time_to_date = lambda x:dt.datetime(x.year, x.month, x.day)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# dataframe 处理函数

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

df_index_time  = lambda x:x.rename(index = lambda y:pd.to_datetime(str_to_time(y) if type(y)==str else y))   # 把时间索引处理成Timestamp格式
df_rows_dedu   = lambda x:x.loc[~x.index.duplicated(keep='first')]                           # 去除掉索引重复的行
df_initialize  = lambda df_data, y:df_data.copy(deep=True).applymap(lambda z:y)
df_index_swap  = lambda x:pd.DataFrame(x.index, index=x.iloc[:,0])                           # 索引与单列数据互换
df_mapper_clip = lambda df_map, df_data:df_data[df_map.iloc[:,0]].set_axis(list(df_map.index), axis='columns', inplace=False)

time_index_df  = lambda x:x.rename(index = lambda y:str_to_time(y) if type(y)==str else y)   # 把时间索引处理成Timestamp格式
dedu_df_rows   = lambda x:x.loc[~x.index.duplicated(keep='first')]                           # 去除掉索引重复的行
df_diff        = lambda x:x.abs().sum(axis=1).sum()                                          # 分析两个dataframe的差异
df_index_norm  = lambda x:(x.T / x.sum(axis=1).T).T                                          # 纵向归一化（同行的数据之和为1）


# dataframe数值过滤
def df_filter(df_data, str_rule, flt_value):
    ''' dataframe数值过滤 '''
    df_result = df_data.copy(deep=True)
    df_result[eval('df_result' + str_rule)] = flt_value
    
    return df_result
    

# 分值变换
def df_rescale_score(df_pool, df_scores, method):
    ''' 
    分值变换:
    df_pool 为True/False
    df_scores 为数值
    method: raw/zscore/percentile
    '''
    
    if method == 'raw':
        tmp_df_score = df_pool.applymap(lambda x:1 if x==True else np.nan) * df_scores
    elif method == 'zscore':
        tmp_df_score = df_scores[df_pool].T
        tmp_df_score = ((tmp_df_score - tmp_df_score.mean(axis=0)) / tmp_df_score.std(axis=0)).T
    elif method == 'percentile':
        tmp_df_score = df_scores[df_pool].rank(axis=1, method='average', ascending=False, na_option='keep', pct=True)

    return tmp_df_score


# 两步赋分函数
def df_twofold_score(df_scores, limit_in, limit_out):
    '''
    两步赋分函数
    ----------
    df_scores : 原始分
    limit_in : 阈值a，高于此阈值赋1分
    limit_out : 阈值b，低于此阈值赋0分，否则延续前一期的分数
    -------
    df_result : 赋分后的返回结果
    '''
    tmp_df_in   = df_scores > limit_in
    tmp_df_keep = df_scores > limit_out
    df_result   = pd.DataFrame(0, index=df_scores.index, columns=df_scores.columns)
    df_result[tmp_df_in] = 1

    for ii in range(1, len(df_result)):
        
        tmp_df_keep_ii = tmp_df_keep.iloc[ii][tmp_df_keep.iloc[ii]].index
        df_result[tmp_df_keep_ii].iloc[ii] = df_result[tmp_df_keep_ii].iloc[ii-1]
        df_result.iloc[ii] += tmp_df_in.iloc[ii]

    df_result[df_result>0] = 1
    return df_result


# 数据聚合切分
def df_cut_sum(df_data, list_sep):
    '''
    Parameters
    ----------
    df_data : dataframe
        待分割的dataframe，已按照index次序排列
    list_sep : list
        分割的节点，应为index中的元素，已按照次序排列

    Returns
    -------
    df_result : dataframe
        分割聚合后的结果
    '''
    
    tmp_df_data = df_data.copy(deep=True)
    df_result = pd.DataFrame(columns=df_data.columns)
    for u in list_sep:
        tmp_df      = tmp_df_data.loc[:u]
        df_result   = df_result.append(pd.DataFrame(tmp_df.sum(axis=0).rename(u, inplace=True)).T)
        try:
            tmp_df_data = tmp_df_data.loc[u:].iloc[1:]
        except:
            break
        
    if len(tmp_df_data)>0:
        df_result   = df_result.append(pd.DataFrame(tmp_df_data.sum(axis=0).rename(tmp_df_data.index[-1], inplace=True)).T)
    
    return df_result






# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# list 处理函数

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

list_compare  = lambda x,y:True if (([v for v in x if v not in y] == []) and ([v for v in y if v not in x] == [])) else False
element_shift = lambda list_elements,x,n:list_elements[list_elements.index(x)+n]            # 注意可能存在数据范围越界的问题
element_next  = lambda list_elements,x:  element_shift(list_elements, x, 1)                 # 下一个元素
next_element  = lambda x,y:x[x.index(y)+1]                           
list_interval = lambda list_days, int_period, int_cut:[list_days[ii] for ii in range(len(list_days)) if ii%int_period==int_cut]

# 提取dataframe中所有非重复元素
def df_to_list(df_data):
    ''' 提取dataframe中所有非重复元素 '''
    list_data = []
    for u in df_data.columns:
        list_data = list(set(list_data + df_data[u].to_list()))
    return list_data



# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# 时间/日期处理函数

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

time_to_day = lambda x:dt.datetime(x.year, x.month, x.day)              # 时间转日期
next_quarter = lambda x:dt.datetime(x.year,6,30)  if x.month==3 else\
                        dt.datetime(x.year,9,30)  if x.month==6 else\
                        dt.datetime(x.year,12,31) if x.month==9 else\
                        dt.datetime(x.year+1,3,31)                      # 下一个(财报)季

# 打印当下时间/日期
def str_hours(x=0):
    ''' 打印当下时间/日期 '''
    if x==0:
        return str(dt.datetime.now())[:19]
    if x==1:
        return str(dt.datetime.now())[:10]
    if x==2:
        return str(dt.datetime.now())[11:19]

# 根据系统时间输出判断值(注意这个时间是含日期的)
def time_bool(dt_a=None, dt_b=None):
    ''' 根据系统时间输出判断值(注意这个时间是含日期的) '''
    dt_now = dt.datetime.now()
    if dt_a is None:
        if dt_b is None:
            return True
        else:
            return True if dt_now<=dt_b else False
    else:
        if dt_b is None:
            return True if dt_now>=dt_a else False
        else:
            return True if (dt_now>=dt_a and dt_now<=dt_b) else False
    


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# 其他函数

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

int_sign = lambda x:1 if x>0 else (-1 if x<0 else 0)                                   # 计算数值的正负符号


# 拼接股票代码
def get_ticker_pieces(list_tickers, int_piece=100):
    ''' 将股票代码拼接成连续的字符串形式 '''
    
    list_tickers_piece = []
    for ii in range(len(list_tickers)//int_piece + 1):
        tmp_head, tmp_tail = ii*int_piece, min((ii+1)*int_piece, len(list_tickers))
        tmp_str_ticker_piece = ','.join(list_tickers[tmp_head:tmp_tail])
        list_tickers_piece.append(tmp_str_ticker_piece)
            
    while '' in list_tickers_piece:
        list_tickers_piece.remove('')
            
    return list_tickers_piece


# 文件夹操作
# 删除旧的数据文件
def remove_file(path, int_split=8, keep='new'):
    ''' 根据指标删除旧的数据文件 '''
    # 注意操作规范，在更新程序的最后一步删除掉旧的数据文件
    # 基础逻辑：搜索文件夹内全部日期不同的同名文件，保留日期较新的，删除日期较早的
    print("\n>>> %s| 清空旧的历史数据 %s"%(str_hours(0),path))
    intm_list_files = os.listdir(path)
    intm_list_files_essential = list(set([v[int_split:] for v in intm_list_files]))
    for p in intm_list_files_essential:
        intm_list_all = [v for v in intm_list_files if v[int_split:]==p]
        if keep == 'new':
            intm_int_keep = max([v[:int_split] for v in intm_list_all])
        elif keep == 'old':
            intm_int_keep = min([v[:int_split] for v in intm_list_all])
        intm_list_drop = [v for v in intm_list_all if v!=intm_int_keep+p]
        for q in intm_list_drop:
            print(">>> %s| 正在删除旧的历史数据 %s ..."%(str_hours(0), q))
            os.remove(path+q)
            
    return 0






if __name__=='__main__':
    
    pass
