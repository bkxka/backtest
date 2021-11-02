# -*- coding: utf-8 -*-
"""
Created on Wed Oct 28 14:09:32 2020

@author: 王笃
"""

__author__ = 'wangdu'
__version__ = 20210916
__structure__ = ['dataset.py', 'dataprocess.py', 'analysis.py', 'trade.py', 'profitNloss.py', 
                 'tracking.py', 'eastmoneyApi.py', 'etfTiming.py', 'mail.py', 'bias.py', 'option.py']
__records__ = '''版本修订记录：
20201019, 第一版回测代码, 将回测程序划分成 读取和处理数据/生成股票池/打分/选股/模拟交易/合成净值曲线/计算收益指标 等多个部分
20201111, 第二版回测代码, 将 生成股票池/选股 部分进行了优化, 支持更多更复杂的模式
20201117, 代码模块化, 将数据源、处理方法、回测代码分别打包成对应子模块，集成在 backtest 模块中, 最终在测试脚本中调用
20201118, 新增分析模块, 包含 处理分层数据分析、无限制交易、遍历调参 等功能
20201124, 修订了计算夏普率的错误和日回报的计算方法; 增加了分层测试的交易限制开关，允许开放/关闭涨跌停板的交易限制
20201125, 修订了 get_stocks_pool() 函数，新增了流通市值排序区间的股票池筛选流程
20201211，修正了 get_stocks_pool() 函数，根据设定的起始日期开始回测
          更新了计算对冲后的净值数据
          代码迁移到 NUC 主机上，以该蓝本为准，减少回测偏差
          新增了计算回报率等的函数
20201215，增加了行业信息（申万一级分类）
20200122, 重写了数据存储格式
20200202, 重写了加载数据的dataset模块
          重写了数据处理dataprocess模块，重写了选股池函数，策略分数处理函数
          增添了trade模块，用于处理交易的模拟和回测功能
20200205, 增添了行业权重控制模块
          重写了模拟交易函数
20200206, 更新了回测系统模块设计
          增添了盈亏指标分析模块
          增添了指令清单模块
          修订了选股程序，支持等权重选股、控制行业权重选股、按照流通市值加权选股等功能
20210304, 增加了东方财富网的API模块，实时查询数据，便于实际交易
20210309, 增加了ETF择时策略模块，用于处理相关策略的回测和交易
20210316, 增加了邮件模块，可以自动生成交易总结，发送至相关人等
20210407, 增加了实盘交易与回测的对比分析的bias模块，分析交易偏差
20210423, 增加了对可转债数据的支持
20210427, 增加了 dataprocess 模块中对买入一定比例的标的功能
          增加了 trade 模块中对无交易限制（ST/涨跌停等）的模拟交易
          增加了 option 模块，用于计算相应的期权价格、隐含波动率等(加载了py_vollib函数)

'''

import sys
# path_pkg = '/'.join(sys.argv[0].split('\\')[:-1])
path_pkg = "C:/InvestmentResearch"

if path_pkg not in sys.path:
    sys.path.append(path_pkg+'/backtest')

import dataset      # 加载数据模块，处理数据读取问题
import dataprocess  # 数据处理模块，处理数据整合，行业权重控制等问题
import analysis     # 数据分析模块，处理各类杂项
import trade        # 交易模拟模块，处理交易的模拟，仓位的变动
import profitNloss  # 收益指标模块，处理盈亏指标分析
import tracking     # 指令清单模块，处理最新策略交易的指令清单等问题
import eastmoneyApi # 东方财富网查询模块，获取实时的价格数据
import etfTiming    # ETF择时策略模块，处理相关策略的回测和交易
import mail         # 邮件模块，将相关信息汇总发送至相关人
import bias         # 处理实盘交易与回测的对比分析
import option       # 期权模块，处理与期权参数计算相关的任务



