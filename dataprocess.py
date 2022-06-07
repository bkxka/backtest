# -*- coding: utf-8 -*-
"""
Created on Tue Nov 17 17:13:25 2020

@author: 王笃
"""

from datetime import *
import pandas as pd
import numpy as np
import math
from sklearn import linear_model
import dataset as ds

from tools.tools_func import *

pd.set_option('mode.chained_assignment',None) 
# 屏蔽了警告信息，可能会导致不预知的问题
# If you would like pandas to be more or less trusting about assignment to a chained indexing expression, 
# you can set the option mode.chained_assignment to one of these values:
#     'warn', the default, means a SettingWithCopyWarning is printed.
#     'raise' means pandas will raise a SettingWithCopyException you have to deal with.
#     None will suppress the warnings entirely.
# please refer to https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy

# test

''' 短程序/处理函数 '''
filter_weight = lambda x, y:x[x>=y].append(pd.Series(x[x<y].sum(), index=['其他'])) if type(x) == pd.core.series.Series else 0


# 操作选股池/并集or交集
def operate_stocks_pool(filter_method, df_stocksPool_A, df_stocksPool_B):
    ''' 操作选股池/并集or交集 '''
    data_df_stocksPool = df_stocksPool_A.copy(deep=True).applymap(lambda x:0)
    if filter_method=='intersection':
        tmp_bool = (df_stocksPool_A>0) & (df_stocksPool_B>0)
    elif filter_method=='union':
        tmp_bool = (df_stocksPool_A>0) | (df_stocksPool_B>0)
    data_df_stocksPool[tmp_bool] = 1
    return data_df_stocksPool


# 选股操作之二：指数成分股
def select_stocks_pool_index(df_close, str_index):
    ''' 选股操作之二：指数成分股 '''
    
    tmp_df_index_stocks = ds.load_index(str_index+"_stocks")
    data_df_stocksPool = pd.DataFrame(0, index=df_close.index, columns=df_to_list(tmp_df_index_stocks))
    for u in tmp_df_index_stocks.index:
        data_df_stocksPool.loc[u:]                                   = 0
        data_df_stocksPool.loc[u:, list(tmp_df_index_stocks.loc[u])] = 1
    
    if 'T00018.SH' in data_df_stocksPool.columns:
        del data_df_stocksPool['T00018.SH']
    
    return data_df_stocksPool


# 选股操作之三：上市超过一定时间的股票
def select_stocks_pool_listed(int_days, df_dateIPO, df_close):
    ''' 选股操作之三：上市超过一定时间的股票 '''
    
    data_df_stocksPool = df_close.applymap(lambda x:0)
    df_tciekr_date_coolingoff = df_dateIPO['DateIPO'].apply(lambda x:x+timedelta(days=int_days+1))
    for u in df_tciekr_date_coolingoff.index:
        data_df_stocksPool.loc[df_tciekr_date_coolingoff.loc[u]:,u] = 1
        
    return data_df_stocksPool


# 标准化股票池操作
def get_stocks_pool_standard(df_close, df_st, df_floatmktcap, df_dateIPO, str_index, int_days, int_head, int_tail):
    ''' 
    标准化股票池操作:
    1, 添加流通市值排序处于目标区间的股票和指数成分股
    2, 过滤掉上市未满 %d 天的股票
    3, 过滤ST股及未上市/已退市股票
    '''
    tmp_stockPool_st          = df_st.applymap(lambda x:1 if x==0 else 0)           # 选股操作之一：挑选处于可交易状态的非ST股
    tmp_stockPool_index       = select_stocks_pool_index(df_close, str_index)
    tmp_stockPool_index       = (tmp_stockPool_index * df_close.applymap(lambda x:1)).fillna(0)
    tmp_stockPool_listed      = select_stocks_pool_listed(int_days, df_dateIPO, df_close)
    tmp_stockPool_floatmktcap = (df_floatmktcap * tmp_stockPool_st).rank(axis=1, method='average', ascending=False, na_option='bottom')\
                                                                   .applymap(lambda x:1 if (x>int_head and x<=int_tail) else 0)
    
    df_stock_pool = tmp_stockPool_floatmktcap * tmp_stockPool_listed * tmp_stockPool_st
    df_stock_pool = (df_stock_pool + tmp_stockPool_index).applymap(lambda x:1 if x>0 else 0)
    
    return df_stock_pool


# 标准化可转债池操作
# 修订该函数，使之支持与日线不符的时间格式
def get_cbs_pool_standard(df_amount_stock, df_amount_cb, df_ticker_cb):
    '''
    可转债池标准：
    1, 认定标准为，转债与正股同为可交易状态；转债从起始日至截止日，可交易状态为一条连续的线，中间停牌不影响可交易状态的认定
    2, 起始日为 正股/转债 成交量>0 的起始日中最大的那个（股债均进入交易状态）
    3, 终止日为 正股/转债 成交量>0 的最后一日中最小的那个，且不晚于强赎预告发布日（股债中至少有一个退出交易状态）
    4, 所有指标的列名都是可转债代码
    '''
    
    tmp_df_cb_pool = pd.DataFrame(index=df_amount_cb.index)
    for u in df_amount_cb.columns:
        tmp_df_amount_stock = df_amount_stock[u][df_amount_stock[u]>0]
        tmp_df_amount_cb    = df_amount_cb[u][df_amount_cb[u]>0]
        if len(tmp_df_amount_stock) <=0 or len(tmp_df_amount_cb) <= 0:
            tmp_df_trade_cycle = pd.DataFrame(columns=[u])
        else:
            tmp_date_start  = max(min(tmp_df_amount_stock.index), min(tmp_df_amount_cb.index))
            tmp_date_end    = min(max(tmp_df_amount_stock.index), max(tmp_df_amount_cb.index), 
                                  df_ticker_cb.loc[u, ['DateRedeemNotice', 'InterestDateEnd']].min())
            # tmp_df_trade_cycle = df_amount_cb[[u]].loc[tmp_date_start:tmp_date_end].applymap(lambda x:1)
            tmp_list_index = [v for v in df_amount_cb.index if (v>=tmp_date_start and v<=tmp_date_end)]
            tmp_df_trade_cycle = df_amount_cb[[u]].loc[tmp_list_index].applymap(lambda x:1)
        tmp_df_cb_pool = pd.concat([tmp_df_cb_pool, tmp_df_trade_cycle], axis=1).fillna(0)
    
    return tmp_df_cb_pool


# 分数转化：将股票池中所选股票的分值按照特定方法处理
def rescale_score(method, df_stocksPool, df_scores):
    ''' 分数转化：将股票池中所选股票的分值按照特定方法处理 '''
    df_pool      = df_stocksPool.applymap(lambda x:True if x==1 else False)
    tmp_df_score = df_rescale_score(df_pool, df_scores, method)
    
    return tmp_df_score


# 策略综合打分，按照给定权重比例加权求和；输入数据类型：((策略A，权重A), (策略B，权重B), ...)
def weight_scores(list_scores):
    ''' 
    策略综合打分，按照给定权重比例加权求和；
    输入数据类型：((策略A，权重A), (策略B，权重B), ...) 
    '''
    stgy_df_scores = list_scores[0][0].applymap(lambda x:0)
    for u in list_scores:
        stgy_df_scores = stgy_df_scores + u[0].fillna(0) * u[1]
    
    return stgy_df_scores


# 指数成分股的行业权重汇总
def get_index_industry_weight(df_index_stocks, df_index_weight, df_industry):
    ''' 指数成分股的行业权重汇总 '''
    data_df_index_industry = pd.DataFrame()
    for u in df_index_weight.index:
        
        tmp_df_weight    = df_index_weight.loc[u].rename(index=df_index_stocks.loc[u].to_dict()).to_frame()
        tmp_df_weight    = tmp_df_weight.loc[tmp_df_weight.index.isna()==False]
        tmp_df_industry  = df_industry.loc[u].to_frame().rename(columns={u:'industry'})
        tmp_df_aggregate = pd.concat([tmp_df_weight, tmp_df_industry], axis=1).fillna(0).groupby('industry').sum()
        data_df_index_industry = data_df_index_industry.append(tmp_df_aggregate.T)

    # 权重数据重新整理并归一化
    # data_df_index_industry = data_df_index_industry[[v for v in data_df_index_industry.columns if v[0] not in ['0', '-']]]
    # data_df_index_industry = (data_df_index_industry.T / data_df_index_industry.sum(axis=1)).T
    data_df_index_industry = data_df_index_industry[[v for v in data_df_index_industry.columns if (v!=0 and v[0] not in ['0', '-'])]]
    data_df_index_industry['其他'] = data_df_index_industry.sum(axis=1).apply(lambda x:max(0, 100-x))
    data_df_index_industry = df_index_norm(data_df_index_industry)
    
    return data_df_index_industry.fillna(0)
    

# 将少量截面数据转变成全周期时序数据
def cross_to_sequence(list_tradingDays, data_df_industry_raw):
    ''' 将少量截面数据转变成全周期时序数据 '''
    
    # intm_df_industry = pd.DataFrame(0, index=list_tradingDays, columns=data_df_industry_raw.columns)
    # for u in data_df_industry_raw.index:
    #     for q in data_df_industry_raw.columns:
    #         intm_df_industry.loc[intm_df_industry.index>u, q] = data_df_industry_raw.loc[u, q]
    # intm_df_industry = intm_df_industry.loc[intm_df_industry.index>=data_df_industry_raw.index[0]]
    
    intm_df_industry = pd.DataFrame()
    tmp_df_date = pd.DataFrame(data_df_industry_raw.index)
    tmp_df_date = pd.concat([tmp_df_date, tmp_df_date.shift(-1).rename(columns={0:1})], axis=1)
    tmp_df_date.iloc[-1, 1] = max(list_tradingDays)
    
    for u in tmp_df_date.index:
        dt_head, dt_tail = tmp_df_date.loc[u,0], tmp_df_date.loc[u,1]
        tmp_list_date = [v for v in list_tradingDays if v>dt_head and v<=dt_tail]
        tmp_df_data   = pd.DataFrame().append([data_df_industry_raw.loc[dt_head].to_frame().T]*len(tmp_list_date))
        tmp_df_data.index = tmp_list_date
        intm_df_industry = intm_df_industry.append(tmp_df_data)
        
    return intm_df_industry


# 将结构化的股票成分-权重数据转化成股票权重二维表
def get_index_constitution(data_df_index_stocks, data_df_index_weight, data_df_close_cb):
    ''' 将结构化的股票成分-权重数据转化成股票权重二维表 '''
    
    if (list(data_df_index_stocks.index) != list(data_df_index_weight.index))\
        or (list(data_df_index_stocks.columns) != list(data_df_index_weight.columns)):
        print(">>> wrong data imput ...")
        return None

    data_df_index_constitution = pd.DataFrame()
    for u in data_df_index_weight.index:
        tmp_df_cons = pd.concat([data_df_index_stocks.loc[u].to_frame().rename(columns={u:0}), data_df_index_weight.loc[u]], axis=1).dropna().set_index(0)
        data_df_index_constitution = data_df_index_constitution.append(tmp_df_cons.T).fillna(0)
        
    tmp_df_index_constitution = cross_to_sequence(list(data_df_close_cb.index), data_df_index_constitution)

    # 将权重股重新组织成二维表的标准形式
    data_df_index_constitution = pd.DataFrame()
    for u in data_df_close_cb.columns:
        data_df_index_constitution = pd.concat([data_df_index_constitution, 
                                                tmp_df_index_constitution[[u]] if u in tmp_df_index_constitution.columns else pd.DataFrame(columns=[u])], axis=1)
    # 根据转债价格变动校正指数权重
    tmp_u = data_df_index_constitution.index[0]
    for u in data_df_index_constitution.index:
        if u in data_df_index_stocks.index:
            tmp_u = u
        else:
            data_df_index_constitution.loc[u] = data_df_index_constitution.loc[u] * (data_df_close_cb.loc[u] / data_df_close_cb.loc[tmp_u])
    data_df_index_constitution = df_index_norm(data_df_index_constitution).replace([np.inf, -np.inf, np.nan], 0)

    return data_df_index_constitution


# 获取调仓日列表
def select_tradingDays(list_days, dt_start, int_period, int_cut):
    ''' 获取调仓日列表 '''
    # list_newdays = [v for v in list_days if v>=dt_start]
    # list_newdays = [list_newdays[ii] for ii in range(len(list_newdays)) if ii%int_period==int_cut]
    return list_interval([v for v in list_days if v>=dt_start], int_period, int_cut)


# 根据行业权重控制和股票分值，获取目标持仓
# 注意，是依据策略打分升序排列；暂时只支持同行业内股票等权重选股
# 权重较低的行业全部被统一划拨为“其他”，参与排序和选股
# 隐藏功能：当int_holdNum>=10000时，选择选股池中全部股票（按照总行业权重分类）
# 新增功能：可选按照流通市值的比例调整股票权重
# 修改函数结构和功能
def get_target_position(list_days, df_target_industry, df_stockPool, df_industry, df_scores, df_fmc, int_holdNum, method='equalWeight'):
    ''' 
    权重配置：equalWeight/fmcWeight
    根据行业权重控制和股票分值，获取目标持仓
    注意，是依据策略打分升序排列；暂时只支持同行业内股票等权重选股
    权重较低的行业全部被统一划拨为“其他”，参与排序和选股
    隐藏功能：当int_holdNum>=10000时，选择选股池中全部股票（按照总行业权重分类）
    新增功能：可选按照流通市值的比例调整股票权重
    新版函数，对日期逻辑有一定调整，注意区别;参数的顺序也有变动
    '''
    data_df_target_position = pd.DataFrame(0, index=list_days, columns=df_stockPool.columns)
    for u in list_days:
        data_df_target_position.loc[u] = get_target_position_day(u, df_target_industry, df_stockPool, df_industry, df_scores, df_fmc, int_holdNum, method=method)

    return data_df_target_position


# 根据选定日期计算目标仓位
def get_target_position_day(u, df_target_industry, df_stockPool, df_industry, df_scores, df_fmc, int_holdNum, method='equalWeight'):
    ''' 
    权重配置：equalWeight/fmcWeight
    根据行业权重控制和股票分值，获取目标持仓
    注意，是依据策略打分升序排列；支持同行业内股票等权重选股和按照流通市值加权选股
    权重较低的行业全部被统一划拨为“其他”，参与排序和选股
    隐藏功能：当int_holdNum>=10000时，选择选股池中全部股票（按照总行业权重分类）
    隐藏功能：当int_holdNum<1时，采用按股票池可选股票比例选股
    新增功能：可选按照流通市值的比例调整股票权重
    '''
    if int_holdNum>=1:
        stgy_df_target_position = func_target_position_day_branch_1(u, df_target_industry, df_stockPool, df_industry, df_scores, df_fmc, int_holdNum, method)
    elif int_holdNum>0 and int_holdNum<1:
        stgy_df_target_position = func_target_position_day_branch_2(u, df_target_industry, df_stockPool, df_industry, df_scores, df_fmc, int_holdNum, method)
    else:
        stgy_df_target_position = 0
        
    return stgy_df_target_position
    

# 策略功能分支，处理int_holdNum>=1的情况
def func_target_position_day_branch_1(u, df_target_industry, df_stockPool, df_industry, df_scores, df_fmc, int_holdNum, method):
    ''' 策略功能分支，详情见下位函数 '''
    
    flt_floor = 1/int_holdNum/2 # 计算单一股票持仓下限
    stgy_df_target_position = pd.Series(0, index=df_stockPool.columns)
        
    if df_target_industry is None:
        # 无行业权重控制
        tmp_df_stocks_candidate = pd.concat([df_stockPool.loc[u].to_frame(), df_fmc.loc[u].to_frame(), df_scores.loc[u].to_frame()], axis=1)
        tmp_df_stocks_candidate.columns = ['stockPool', 'floatmktcap', 'score']
        tmp_df_stocks_candidate = tmp_df_stocks_candidate[tmp_df_stocks_candidate['stockPool']>0]
        
        tmp_df_stocks_selected = tmp_df_stocks_candidate.sort_values(by='score').iloc[:int_holdNum]
        if method=='equalWeight':
            tmp_df_stocks_selected['weight'] = 1/len(tmp_df_stocks_selected)
        elif method=='fmcWeight':
            tmp_df_stocks_selected['weight'] = tmp_df_stocks_selected['floatmktcap'] / tmp_df_stocks_selected['floatmktcap'].sum()
                    
        # stgy_df_target_position.loc[list(tmp_df_stocks_selected.index)] = tmp_df_stocks_selected['weight']
    else:
        # 有行业权重控制
        tmp_df_industry = filter_weight(df_target_industry.loc[u], flt_floor).to_frame().rename(columns={0:'weight'})
        tmp_df_industry['holdingNum'] = tmp_df_industry['weight'].apply(lambda x:math.floor(1+x*int_holdNum))
        tmp_df_stocks_candidate = pd.concat([df_stockPool.loc[u].to_frame(), df_industry.loc[u].to_frame(), 
                                             df_fmc.loc[u].to_frame(), df_scores.loc[u].to_frame()], axis=1)
        tmp_df_stocks_candidate.columns = ['stockPool', 'industry', 'floatmktcap', 'score']
        tmp_df_stocks_candidate = tmp_df_stocks_candidate[tmp_df_stocks_candidate['stockPool']>0]
        tmp_df_stocks_candidate['industry'] = tmp_df_stocks_candidate['industry'].apply(lambda x:x if x in tmp_df_industry.index else '其他')
    
        tmp_df_stocks_selected = pd.DataFrame()
        for q in tmp_df_industry.index:
            tmp_df_stocks_compare = tmp_df_stocks_candidate[tmp_df_stocks_candidate['industry']==q].sort_values(by='score')
            # 计算该行业的选股数量；若设置的选股数>10000，则选择该行业全部股票
            tmp_num = len(tmp_df_stocks_compare) if int_holdNum>=10000 else min(tmp_df_industry.loc[q,'holdingNum'], len(tmp_df_stocks_compare))
            if tmp_num==0:
                pass
            else:
                tmp_df_stocks_compare = pd.DataFrame(tmp_df_industry.loc[q,'weight']/tmp_num, 
                                                     index=tmp_df_stocks_compare.index[:tmp_num], columns=['weight'])
                if method=='equalWeight':
                    pass        # 每只股票等权重（不做修改）
                elif method=='fmcWeight':
                                # 每只股票按流通市值比例赋权
                    tmp_fmc = df_fmc.loc[u, tmp_df_stocks_compare.index]
                    tmp_fmc = tmp_fmc * tmp_num / tmp_fmc.sum()
                    tmp_df_stocks_compare['weight'] = tmp_df_stocks_compare['weight'] * tmp_fmc
                        
                tmp_df_stocks_compare['industry'] = q
                tmp_df_stocks_selected = tmp_df_stocks_selected.append(tmp_df_stocks_compare)
    
    stgy_df_target_position.loc[list(tmp_df_stocks_selected.index)] = tmp_df_stocks_selected['weight']
    
    return stgy_df_target_position


# 策略功能分支，处理int_holdNum<1的情况
def func_target_position_day_branch_2(u, df_target_industry, df_stockPool, df_industry, df_scores, df_fmc, int_holdNum, method):
    ''' 策略功能分支，详情见下位函数 '''
    int_holdNum_new = max(1, int(df_stockPool.loc[u].sum() * int_holdNum))
    stgy_df_target_position = pd.Series(0, index=df_stockPool.columns)
        
    if df_target_industry is None:
        # 无行业权重控制
        tmp_df_stocks_candidate = pd.concat([df_stockPool.loc[u].to_frame(), df_fmc.loc[u].to_frame(), df_scores.loc[u].to_frame()], axis=1)
        tmp_df_stocks_candidate.columns = ['stockPool', 'floatmktcap', 'score']
        tmp_df_stocks_candidate = tmp_df_stocks_candidate[tmp_df_stocks_candidate['stockPool']>0]
        
        tmp_df_stocks_selected = tmp_df_stocks_candidate.sort_values(by='score').iloc[:int_holdNum_new]
        if method=='equalWeight':
            tmp_df_stocks_selected['weight'] = 1/len(tmp_df_stocks_selected)
        elif method=='fmcWeight':
            tmp_df_stocks_selected['weight'] = tmp_df_stocks_selected['floatmktcap'] / tmp_df_stocks_selected['floatmktcap'].sum()
                    
        # stgy_df_target_position.loc[list(tmp_df_stocks_selected.index)] = tmp_df_stocks_selected['weight']
    else:
        # 有行业权重控制
        pass
    
    stgy_df_target_position.loc[list(tmp_df_stocks_selected.index)] = tmp_df_stocks_selected['weight']
    
    return stgy_df_target_position


# 根据股票分值选股，获取目标持仓，不进行行业控制
# 注意，是依据策略打分升序排列；暂时只支持所有股票等权重选股
def get_target_position_unlimited(list_days, df_stockPool, df_scores, int_holdNum, flt_holdRate):
    ''' 
    根据股票分值选股，获取目标持仓，不进行行业控制
    注意，是依据策略打分升序排列；暂时只支持所有股票等权重选股
    '''
    print(">>> %s| 根据策略分值获取目标持仓..."%str_hours(0))
    stgy_df_target_position = pd.DataFrame(0, index=list_days, columns=df_stockPool.columns)
    for u in list_days:
        tmp_df_stocks_candidate = pd.concat([df_stockPool.loc[u].to_frame(), df_scores.loc[u].to_frame()], axis=1)
        tmp_df_stocks_candidate.columns = ['stockPool', 'score']
        tmp_df_stocks_candidate = tmp_df_stocks_candidate[tmp_df_stocks_candidate['stockPool']>0]
        
        tmp_int_stocks_limit = int_holdNum if int_holdNum!=None \
                               else (int(len(tmp_df_stocks_candidate)*flt_holdRate) if flt_holdRate!=None else 20)
        tmp_df_stocks_selected = tmp_df_stocks_candidate.sort_values(by='score').iloc[:int_holdNum]
        tmp_df_stocks_selected['weight'] = 1/len(tmp_df_stocks_selected)
    
        stgy_df_target_position.loc[u, list(tmp_df_stocks_selected.index)] = tmp_df_stocks_selected['weight']
    
    return stgy_df_target_position
    



# 将股票池按照因子分值排序进行分组
def get_df_slices(df_scores, list_dates_signal, df_stock_pool, int_layers, int_signal=1):
    ''' 
    将股票池按照因子分值进行分组
    默认信号延迟一天：即当天尾盘按照昨日收盘信息调仓；int_signal=0表示按照当天收盘信息调仓
    '''
    tmp_list_stocks = list(df_stock_pool.sum(axis=0)[df_stock_pool.sum(axis=0)>0].index)
    dict_result = {v:pd.DataFrame(0, index=df_stock_pool.index, columns=tmp_list_stocks) for v in range(int_layers)}
    for u in list_dates_signal:
        tmp_list_rank = list(df_scores.loc[u].dropna().sort_values(ascending=True).index)
        for ii in range(int_layers):
            tmp_head, tmp_tail = math.ceil(ii*len(tmp_list_rank)/int_layers), math.ceil((ii+1)*len(tmp_list_rank)/int_layers)
            dict_result[ii].loc[u+dt.timedelta(days=int_signal):] = 0
            dict_result[ii].loc[u+dt.timedelta(days=int_signal):, tmp_list_rank[tmp_head:tmp_tail]] = 1
    
    return dict_result

    
    
    
if __name__=='__main__':
    
    pass

