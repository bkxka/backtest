# -*- coding: utf-8 -*-
"""
Created on Thu Sep 16 09:36:17 2021

@author: 好鱼
"""
import pandas as pd
import datetime as dt

# 导入wind数据接口，登陆账号; 注意 w 有特定含义，不要在后文中用 w 作遍历检索的变量
from WindPy import w
w.start()

import sys
sys.path.append("..")
from tools.tools_func import *


# 参数设置
par_dct_order_type = {'exlarge':'1', 'large':'2', 'middle':'3', 'small':'4'}
par_dct_industry_type = {'industry_sw':'1', 'industry_wind':'2'}
par_dct_shhk = {'sh_buy_amount':'SHHK_BUY_AMT', 'sh_sell_amount':'SHHK_SELL_AMT', 'hk_buy_amount':'HKSH_BUY_AMT', 'hk_sell_amount':'HKSH_SELL_AMT'}
par_dct_szhk = {'sz_buy_amount':'SZHK_BUY_AMT', 'sz_sell_amount':'SZHK_SELL_AMT', 'hk_buy_amount':'HKSZ_BUY_AMT', 'hk_sell_amount':'HKSZ_SELL_AMT'}
par_dct_cb   = {'valueStock':'convvalue', 'valueBond':'strbvalue', 'convPrice':'convprice', 'convDilution':'ldiluterate', 'impliedVol':'impliedvol'}


# 检测API函数是否运转正常
def connect_wind():
    ''' 检测API函数是否运转正常 '''
    if w.isconnected():
        tmp_testA = w.wsd("000002.SZ", "share_HKS", "2020-08-21", "2020-08-30", "unit=1")
        tmp_testB = w.wss("000002.SZ,600606.SH", "share_N","unit=1;tradeDate=20200831")
        tmp_testC = w.wset("indexconstituent",'date=2020-12-31;windcode=000300.SH')
        if tmp_testA.ErrorCode!=0:
            print(">>> Wind wsd 函数错误，请中止数据更新程序...")
            print(tmp_testA.Data[0])
            return False
        elif tmp_testB.ErrorCode!=0:
            print(">>> Wind wss 函数超限，请中止数据更新程序...")
            print(tmp_testB.Data[0])
            return False
        elif tmp_testC.ErrorCode!=0:
            print(">>> Wind wset 函数超限，请中止数据更新程序...")
            print(tmp_testC.Data[0])
            return False
        else:
            print(">>> %s| Wind API 连接成功..."%str_hours(0))
            return True
    else:
        print(">>> %s| Wind API 连接失败，请检查运行环境..."%str_hours(0))
        return False


# 调用wind函数wss
def wind_func_wss(str_tickers_piece, str_metric, str_date=None):
    ''' 调用wind函数wss, 需要把不同的指标映射为正确的查询参数 '''
    if str_date is not None:
        # 可转债指标
        if 'cb_' in str_metric:
            tmp_str_metric = str_metric.split('_')[1]
            if tmp_str_metric in ['close', 'vwap']:
                tmp_raw_price = w.wss(str_tickers_piece, tmp_str_metric,"tradeDate="+str_date+";priceAdj=MP;cycle=D")
            elif tmp_str_metric == 'amount':
                tmp_raw_price = w.wss(str_tickers_piece, "amt","tradeDate="+str_date+";priceAdj=MP;cycle=D")
            elif tmp_str_metric == 'impliedVol':
                tmp_raw_price = w.wss(str_tickers_piece, "impliedvol","tradeDate="+str_date+";rfIndex=1")   # 一年期定存利率为参数
            elif tmp_str_metric == 'grossValue':
                tmp_raw_price = w.wss(str_tickers_piece, "clause_conversion2_bondlot","unit=1;tradeDate="+str_date)
            else:
                tmp_raw_price = w.wss(str_tickers_piece, par_dct_cb[tmp_str_metric],"tradeDate="+str_date)
        # ETF指标     
        elif 'etf_' in str_metric:
            if str_metric == 'etf_close':
                tmp_raw_price = w.wss(str_tickers_piece, "close","tradeDate="+str_date+";priceAdj=U;cycle=D")
            elif str_metric == 'etf_amount':
                tmp_raw_price = w.wss(str_tickers_piece, "amt","tradeDate="+str_date+";cycle=D")
            elif str_metric == 'etf_netvalue':
                tmp_raw_price = w.wss(str_tickers_piece, "nav","tradeDate="+str_date)
        # 汇率指标
        elif str_metric in ['CFE_close', 'hclose']:
            tmp_raw_price = w.wss(str_tickers_piece, "close","tradeDate="+str_date+";priceAdj=U;cycle=D")
        elif '.FX' in str_metric:
            tmp_raw_price = w.wss(str_tickers_piece, str_metric,"tradeDate="+str_date+";priceAdj=U;cycle=D")
        elif str_metric =='mktcap':
            tmp_raw_price = w.wss(str_tickers_piece, "mkt_cap","unit=1;tradeDate="+str_date)
        elif str_metric == 'dayReturn':
            tmp_raw_price = w.wss(str_tickers_piece, "pct_chg", "tradeDate="+str_date+";cycle=D")
        elif str_metric == 'amount':
            tmp_raw_price = w.wss(str_tickers_piece, "amt", "tradeDate="+str_date+";cycle=D")
        elif str_metric == 'floatAmktcap':
            tmp_raw_price = w.wss(str_tickers_piece, "mkt_cap_float", "unit=1;tradeDate="+str_date+";currencyType=")
        elif str_metric == 'shszhkHold':
            tmp_raw_price = w.wss(str_tickers_piece, "share_N", "unit=1;tradeDate="+str_date)
        elif str_metric == 'announceDate':
            tmp_raw_price = w.wss(str_tickers_piece, "stm_issuingdate","rptDate="+str_date)
        elif str_metric == 'netQProfit':
            # 此处净利润为单季度归属于母公司股东的净利润
            tmp_raw_price = w.wss(str_tickers_piece, "qfa_np_belongto_parcomsh","unit=1;rptDate="+str_date+";rptType=1")
        elif str_metric == 'netEquity':
            tmp_raw_price = w.wss(str_tickers_piece, "eqy_belongto_parcomsh","unit=1;rptDate="+str_date+";rptType=1")
        elif 'buy' in str_metric:
            # 注意这里并不是限定性的判断条件
            tmp_raw_price = w.wss(str_tickers_piece, "mfd_buyamt_d","unit=1;tradeDate="+str_date+";traderType="+par_dct_order_type[str_metric.split('_')[-1]])
        elif 'sell' in str_metric:
            tmp_raw_price = w.wss(str_tickers_piece, "mfd_sellamt_d","unit=1;tradeDate="+str_date+";traderType="+par_dct_order_type[str_metric.split('_')[-1]])
        elif str_metric in ['industry_sw', 'industry_wind']:
            tmp_raw_price = w.wss(str_tickers_piece, "industry2","industryType="+par_dct_industry_type[str_metric]+";industryStandard=5;tradeDate="+str_date)
        else:
            tmp_raw_price = w.wss(str_tickers_piece, str_metric,"tradeDate="+str_date+";priceAdj=U;cycle=D")
    else:
        if str_metric == 'cb_redeemNoticeDate':
            tmp_raw_price = w.wss(str_tickers_piece, "clause_calloption_noticedate")
        else:
            tmp_raw_price = w.wss(str_tickers_piece, str_metric)
    
    # 调用成功则返回结果，否则再次调用
    if tmp_raw_price.ErrorCode==0:
        return tmp_raw_price
    else:
        return wind_func_wss(str_tickers_piece, str_metric, str_date)


# 调用万得wsd函数
def wind_func_wsd(u, str_metric, str_start_date, str_end_date):
    ''' 调用wind函数wsd，需要把不同的指标映射为正确的查询参数 '''
    if 'cb_' in str_metric:
        tmp_str_metric = str_metric.split('_')[1]
        if tmp_str_metric in ['close', 'vwap']:
            tmp_raw_price = w.wsd(u, tmp_str_metric, str_start_date, str_end_date, "Fill=Previous;PriceAdj=MP")
        elif tmp_str_metric == "amount":
            tmp_raw_price = w.wsd(u, "amt", str_start_date, str_end_date, "PriceAdj=MP")
        elif tmp_str_metric == "impliedVol":
            tmp_raw_price = w.wsd(u, "impliedvol", str_start_date, str_end_date, "rfIndex=1;PriceAdj=MP")
        elif tmp_str_metric == "grossValue":
            tmp_raw_price = w.wsd(u, "clause_conversion2_bondlot", str_start_date, str_end_date, "unit=1")
        else:
            tmp_raw_price = w.wsd(u, par_dct_cb[tmp_str_metric], str_start_date, str_end_date, "PriceAdj=MP")
    else:
        if str_metric=="mktcap":
            tmp_raw_price = w.wsd(u, "ev", str_start_date, str_end_date, "Fill=Previous")
        elif str_metric == 'dayReturn':
            tmp_raw_price = w.wsd(u, "pct_chg", str_start_date, str_end_date, "")
        elif str_metric == 'amount':
            tmp_raw_price = w.wsd(u, "amt", str_start_date, str_end_date, "")
        elif str_metric == 'floatAmktcap':
            tmp_raw_price = w.wsd(u, "mkt_cap_float", str_start_date, str_end_date, "unit=1;currencyType=;Fill=Previous")
        elif str_metric == 'shszhkHold':
            tmp_raw_price = w.wsd(u, "share_N", str_start_date, str_end_date, "unit=1")
        elif str_metric == 'announceDate':
            tmp_raw_price = w.wsd(u, "stm_issuingdate", str_start_date, str_end_date, "Period=Q;Days=Alldays")
        elif str_metric == 'netQProfit':
            tmp_raw_price = w.wsd(u, "qfa_np_belongto_parcomsh", str_start_date, str_end_date, "unit=1;rptType=1;Period=Q;Days=Alldays")
        elif str_metric == 'netEquity':
            tmp_raw_price = w.wsd(u, "eqy_belongto_parcomsh", str_start_date, str_end_date, "unit=1;rptType=1;Period=Q;Days=Alldays")
        elif 'buy' in str_metric.split('_')[0]:
            tmp_raw_price = w.wsd(u, "mfd_buyamt_d", str_start_date, str_end_date, "unit=1;traderType="+par_dct_order_type[str_metric.split('_')[-1]])
        elif 'sell' in str_metric.split('_')[0]:
            tmp_raw_price = w.wsd(u, "mfd_sellamt_d", str_start_date, str_end_date, "unit=1;traderType="+par_dct_order_type[str_metric.split('_')[-1]])
        else:
            tmp_raw_price = w.wsd(u, str_metric, str_start_date, str_end_date, "Fill=Previous")
        
    # 调用成功则返回结果，否则再次调用
    if tmp_raw_price.ErrorCode==0:
        return tmp_raw_price
    else:
        return wind_func_wsd(u, str_metric, str_start_date, str_end_date)


# 调用万得wset函数
def wind_func_wset(str_metric, str_start_date, str_end_date):
    ''' 
    调用wind函数wset，需要把不同的指标映射为正确的查询参数
    日期为字符串 2021-10-01
    '''
    if str_metric == 'shhk_trade':
        tmp_raw_price = w.wset("shhktransactionstatistics","startdate="+str_start_date+";enddate="+str_end_date+";cycle=day;currency=hkd")
    elif str_metric == 'szhk_trade':
        tmp_raw_price = w.wset("szhktransactionstatistics","startdate="+str_start_date+";enddate="+str_end_date+";cycle=day;currency=hkd")
    elif str_metric == 'bonus':
        tmp_str_year  =     str_start_date.split('-')[0]
        tmp_int_month = int(str_start_date.split('-')[1])
        tmp_str_quarter = 's1' if tmp_int_month == 3 else 'h1' if tmp_int_month == 6 else\
                          's3' if tmp_int_month == 9 else 'y1' if tmp_int_month == 12 else None
        tmp_raw_price = w.wset(str_metric,"orderby=报告期;year="+tmp_str_year+";period="+tmp_str_quarter+";sectorid=a001010100000000")
    elif str_metric in ['000300.SH', '000905.SH', '000852.SH']:
        tmp_raw_price = w.wset("indexconstituent",'date='+str_start_date+';windcode='+str_metric)
    elif str_metric in ['IF.CFE', 'IH.CFE', 'IC.CFE']:
        tmp_raw_price = w.wset("futurecc","startdate="+str_start_date+";enddate="+str_end_date+";wind_code="+str_metric)
    elif str_metric == 'listed_tickers':
        tmp_raw_price = w.wset("listedsecuritygeneralview","sectorid=a001010100000000;field=wind_code,sec_name,ipo_date,sec_type,listing_board,exchange")
    elif str_metric == 'delisted_tickers':
        tmp_raw_price = w.wset("sectorconstituent","date="+str_start_date+";sectorid=a001010m00000000")
    elif str_metric == 'ticker_cb':
        tmp_raw_price = w.wset("newbondissueview","startdate="+str_start_date+";enddate="+str_end_date+";datetype=listingdate;bondtype=convertiblebonds;dealmarket=allmarkets;maingrade=all")
    elif str_metric == 'st':
        tmp_raw_price = w.wset("sectorconstituent","date="+str_start_date+";sectorid=1000006526000000")
    elif str_metric == 'shhkFlow':
        tmp_raw_price = w.wset("shhktransactionstatistics","startdate="+str_start_date+";enddate="+str_end_date+";cycle=day;currency=hkd")
    elif str_metric == 'szhkFlow':
        tmp_raw_price = w.wset("szhktransactionstatistics","startdate="+str_start_date+";enddate="+str_end_date+";cycle=day;currency=hkd")
    elif str_metric == 'insiderTrade':
        tmp_raw_price = w.wset("majorholderdealrecord","startdate="+str_start_date+";enddate="+str_end_date+";sectorid=a001010100000000;type=announcedate")

    # 调用成功则返回结果，否则再次调用
    if tmp_raw_price.ErrorCode == 0:
        return tmp_raw_price
    else:
        print(">>> 调用数据错误 ", tmp_raw_price.ErrorCode)
        return wind_func_wset(str_metric, str_start_date, str_end_date)


# 调用wind分钟序列函数
def wind_func_wsi(str_ticker, str_date_start, str_date_end, str_time_start="09:00:00", str_time_end="15:30:00"):
    '''
    调用wind分钟序列函数; 注意不同的品种可能导出的volume口径不一致，需要在后端进行处理

    Parameters
    ----------
    str_ticker : TYPE
        DESCRIPTION.
    str_date_start : TYPE
        2012-01-01.
    str_date_end : TYPE
        2012-01-01.
    str_time_start : TYPE, optional
        DESCRIPTION. The default is "09:00:00".
    str_time_end : TYPE, optional
        DESCRIPTION. The default is "15:30:00".

    Returns
    -------
    tmp_df_data : dataframe
        DESCRIPTION.

    '''
    tmp_raw_data = w.wsi(str_ticker, "open,close,high,low,volume,amt", 
                         str_date_start+" "+str_time_start, str_date_end+" "+str_time_end, 
                         "periodstart="+str_time_start+";periodend="+str_time_end)
    tmp_df_data = pd.DataFrame(tmp_raw_data.Data, index=tmp_raw_data.Fields,
                               columns=tmp_raw_data.Times).T
    
    return tmp_df_data



if __name__=='__main__':
    
    pass
