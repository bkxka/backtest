# -*- coding: utf-8 -*-
"""
Created on Tue Apr 27 16:58:46 2021

@author: 好鱼
"""
import arch
import py_vollib.black_scholes.implied_volatility as bsiv
import py_vollib.black_scholes.greeks.analytical as greek
import numpy as np
import pandas as pd
from scipy.stats import norm
N = norm.cdf

__theory__ = '''
希腊字母简介：

delta
【定义】期权价格的变动 / 标的价格的变动
【内涵】实际上是期权价格对标的价格的弹性（对冲标的风险的头寸）
【取值】认购期权delta>0, 标的价格上涨，期权价格也相应上涨；认沽期权反之
       delta的绝对值介于0和1之间，越是实值期权delta越接近1，越是虚值期权delta越接近0，平值期权delta为0.5
【与标的价格的关系】
       深度实值期权标的价格变动，期权价格同等程度变动；
       深度虚值期权标的价格变动，期权价格几乎不变动；
       平值期权价格变动，依赖于标的价格变动的方向和幅度（delta的不确定性最大）
       因此可以将delta视作期权到期实值的概率（实值期权到期大概率是实值期权，小概率变成虚值期权；虚值期权反之；平值期权的概率各1/2）
【与到期时间的关系】
       越接近到期，标的波动与期权价格波动联系越紧密，不确定性越小
       深度实值趋近于1，深度虚值趋近于0，平值趋近于0.5; 

gamma
【定义】delta的变动 / 标的价格的变动
【内涵】实际上是delta对标的价格的弹性（对冲风险的难度，gamma越低，delta波动越大，对冲越困难）
【取值】
【与标的价格的关系】
       当标的价格接近行权价时（平值），期权是否被行权的不确定性最大，对冲风险最高，gamma也最大；
       当标的价格为深度虚值的时候，被行权的可能性很低，期权近似无效，对冲风险极低，gamma趋近于0
       当标的价格为深度实值的时候，被行权的可能性极高，期权对冲有效，对冲风险极低，gamma趋近于0
【与到期时间的关系】
       当到期较远时，到期结果不确定性较高，期权价格与标的价格关联度低，delta的变动较小，gamma较低
       随着时间流逝，到期结果不确定性下降，期权价格与标的价格关联度上升，delta的变动增大，gamma增大
       当临近到期时间，深度实值/虚值期权的delta趋于稳定，gamma回落；平值期权的delta变动增大，gamma增大

vega
【定义】期权价格的变动 / 预期波动率的变动
【内涵】实际上标的价格的风险（标的风险越高，则期权价格越高，这个关联关系就是vega）
【取值】认购/认沽期权的vega都大于0
【与标的价格的关系】
       vega随标的价格的变化类似于gamma
       深度实值/虚值期权，行权的确定性很高，期权价格对价格波动敏感度较低（1或0），vega接近0
       平值期权，行权的确定性最低，期权价格对价格波动敏感度较高，vega达到峰值
【与到期时间的关系】
       当到期较远时，预期波动率差异对期权价格影响较大，vega较大
       当临近到期时，预期波动率差异对期权价格影响较小，vega趋于零
       
theta
【定义】期权的时间价值 = 期权权利金 - 行权价值
【内涵】虚值期权的价值不为零，原因在于预期有未来发生变化的可能，时间价值即是对未来有利于自己的可能性的定价
【取值】大多数theta大于零，偶尔也有小于0
'''

if False:
    C = (176 - 87.1503)/2.002 # 期权价格 = （转债交易价格 - 纯债价值）/转股率
    S = 81.825        # 标的价格（股票价格，不复权）
    K = 49.95         # 行权价格，即票面价值按照转股价折算而成的期权  
    T = 4.726         # 到期时间  
    r = 0.01          # 无风险利率
    vol = 0.25        # 波动率
    
    find_vol(C, S, K*1.76, T, r)


# 逼近法计算隐含波动率：输入参数为一列series
def find_vol_row(se_data):
    ''' 逼近法计算隐含波动率：输入参数为一列series '''
    try:
        return bsiv.implied_volatility(se_data['C'], se_data['S'], se_data['K'], se_data['T'], se_data['r'], 'c')
    except:
        return 0

# 逼近法计算隐含波动率：输入参数为一组series
def find_vol_series(se_C, se_S, se_K, se_T, se_r):
    ''' 逼近法计算隐含波动率：输入参数为一组series '''
    df_vol = pd.concat([se_C, se_S, se_K, se_T, se_r], axis=1).fillna(0)
    df_vol.columns = ['C', 'S', 'K', 'T', 'r']
    return df_vol.T.apply(lambda x:find_vol_row(x))
    

# 计算希腊字母之delta
def find_delta_row(se_data):
    np.seterr(invalid='ignore')
    if se_data['S'] * se_data['K'] * se_data['T'] * se_data['r'] * se_data['sigma'] <= 0:
        return 0
    try:
        return greek.delta('c', se_data['S'], se_data['K'], se_data['T'], se_data['r'], se_data['sigma'])
    except:
        return 0


# 计算希腊字母之delta：输入参数为一组series【注意各指标的顺序】
def find_delta_series(se_S, se_K, se_T, se_r, se_sigma):
    ''' 逼近法计算隐含波动率：输入参数为一组series '''
    df_delta = pd.concat([se_S, se_K, se_T, se_r, se_sigma], axis=1).fillna(0)
    df_delta.columns = ['S', 'K', 'T', 'r', 'sigma']
    return df_delta.T.apply(lambda x:find_delta_row(x))
    

# 应用garch(1,1)模型对未来的波动率均值进行预测
def garch_forecast_vol(tmp_df_return_prev, tmp_int_maturity):
    ''' 应用garch(1,1)模型对未来的波动率进行预测 '''
    
    if tmp_df_return_prev is None:
        return 0
    else:
        # tmp_df_return_prev = tmp_df_return.loc[:tmp_dt_date]
        garch_res = arch.arch_model(tmp_df_return_prev, vol='GARCH', p=1, o=0, q=1, dist='Normal', rescale=False)
        forecasts = garch_res.fit(update_freq=1, disp='off')
        sim = forecasts.forecast(horizon=tmp_int_maturity, method='analytic', reindex=False)
        # tmp_df_garch_vol.loc[tmp_dt_date] = sim.variance.apply(lambda x:x**0.5).mean(axis=1).iloc[0]
    
        return sim.variance.apply(lambda x:x**0.5).mean(axis=1).iloc[0]




if __name__=='__main__':
    
    pass

