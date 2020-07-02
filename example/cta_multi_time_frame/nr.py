# -*- coding:utf-8 -*-

"""
固定数量模式CTA: Normalized Return Model

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import numpy as np
import pandas as pd

from quant import const
from quant.market import Kline
from quant.interface.model_api import ModelAPI
from quant.interface.ah_math import AHMath
from quant.interface.ta_lib import TaLib
from quant.interface.kline_generator import KlineGenerator


class NrModel(object):

    def __init__(self):
        #这个model订阅‘BTC/USDT’
        self.symbols = ['BTC/USDT']

        self.mode_params = {
            'fixed_volume': 0.04, #每次买卖0.04个btc
            'fastWindow': 5,      #快速均线窗口
            'slowWindow': 20,     #慢速均线窗口
            'rsiLong': 50+20,     #RSI买开阈值
            'rsiShort': 50-20,    #RSI卖开阈值    
            'rsiWindow': 14       #RSI窗口
        }

        self.maTrend = 0 #均线趋势，多头1，空头-1

        self.running_status = 'running'
        self.signal = 0 #model返回的信号值，这个值是介于-1.0到1.0之间的一个浮点数
        self.target_position = {'BTC': 0}
        self.last_kline_end_dt = None
        self.latency = 2*60*1000 #两分钟

        self.talib5 = TaLib()
        self.kg5 = KlineGenerator(None, const.MARKET_TYPE_KLINE_5M, self.on_5m_kline_update_callback)

        self.talib15 = TaLib()
        self.kg15 = KlineGenerator(None, const.MARKET_TYPE_KLINE_15M, self.on_15m_kline_update_callback)

    def on_time(self):
        ''' 每5秒定时被驱动，检查k线是否断连'''
        if self.running_status == 'stopping': #如果是停止状态就不工作了
            return
        now = ModelAPI.current_milli_timestamp()
        if self.last_kline_end_dt == None:
            self.last_kline_end_dt = now
        if now - self.last_kline_end_dt > self.latency: #超过2分钟
            self.signal = 0
            self.target_position['BTC'] = 0.0
            self.running_status = 'stopping'

    def on_kline_update_callback(self, kline: Kline):
        if self.running_status == 'stopping': #如果是停止状态就不工作了
            return
        if kline.symbol not in self.symbols:
            return
        self.last_kline_end_dt = kline.end_dt
        self.kg5.update_bar(kline)
        self.kg15.update_bar(kline)

    def on_5m_kline_update_callback(self, kline: Kline):
        ''' 最新5分钟k线来了，我们需要更新此model的signal'''
        self.talib5.kline_update(kline)
        if not self.talib5.inited:
            return
        #如果15分钟数据尚未初始化完毕，则直接返回
        if not self.maTrend:
            return
        #计算指标数值
        rsiValue = self.talib5.rsi(self.mode_params['rsiWindow'])
        #判断是否要进行交易
        #当前无仓位
        if self.signal == 0:
            if self.maTrend > 0 and rsiValue >= self.mode_params['rsiLong']:
                self.signal = 1
            elif self.maTrend < 0 and rsiValue <= self.mode_params['rsiShort']:
                self.signal = -1
        #持有多头仓位
        elif self.signal > 0:
            if self.maTrend < 0 or rsiValue < 50:
                self.signal = 0
        #持有空头仓位
        elif self.signal < 0:
            if self.maTrend > 0 or rsiValue > 50:
                self.signal = 0
        self.generate_target_position()

    def on_15m_kline_update_callback(self, kline: Kline):
        """15分钟K线推送"""
        self.talib15.kline_update(kline)
        if not self.talib15.inited:
            return
        #计算均线并判断趋势
        fastMa = self.talib15.sma(self.mode_params['fastWindow'])
        slowMa = self.talib15.sma(self.mode_params['slowWindow'])
        if fastMa > slowMa:
            self.maTrend = 1
        else:
            self.maTrend = -1

    def generate_target_position(self):
        self.target_position['BTC'] = self.signal * self.mode_params['fixed_volume']