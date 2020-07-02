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


class RsiModel(object):

    def __init__(self):
        #这个model订阅‘BTC/USDT’
        self.symbols = ['BTC/USDT']

        self.mode_params = {
            'fixed_volume': 0.04, #每次买卖0.04个btc
            'rsiWindow': 14,
            'rsiLong': 50+20,
            'rsiShort': 50-20,
        }

        self.running_status = 'running'
        self.factor = np.nan
        self.signal = np.nan #model返回的信号值，这个值是介于-1.0到1.0之间的一个浮点数
        self.target_position = {'BTC': 0}
        self.last_kline_end_dt = None
        self.latency = 2*60*1000 #两分钟

        self.talib = TaLib()

    def on_time(self):
        ''' 每5秒定时被驱动，检查k线是否断连'''
        if self.running_status == 'stopping': #如果是停止状态就不工作了
            return
        now = ModelAPI.current_milli_timestamp()
        if self.last_kline_end_dt == None:
            self.last_kline_end_dt = now
        if now - self.last_kline_end_dt > self.latency: #超过2分钟
            self.factor = np.nan
            self.signal = np.nan
            self.target_position['BTC'] = 0.0
            self.running_status = 'stopping'

    def on_kline_update_callback(self, kline: Kline):
        ''' 最新1分钟k线来了，我们需要更新此model的signal'''
        if self.running_status == 'stopping': #如果是停止状态就不工作了
            return
        if kline.symbol not in self.symbols:
            return
        self.last_kline_end_dt = kline.end_dt
        self.talib.kline_update(kline)
        if not self.talib.inited:
            return
        self.generate_factor() #产生因子
        self.generate_signal() #通过因子生成信号
        self.generate_target_position() #通过信号生成仓位

    def generate_factor(self):
        self.factor = self.talib.rsi(self.mode_params['rsiWindow'])

    def generate_signal(self):
        if self.factor >= self.mode_params['rsiLong']:
            self.signal = 1.0
        elif self.factor <= self.mode_params['rsiShort']:
            self.signal = -1.0
        else:
            self.signal = np.nan

    def generate_target_position(self):
        if self.target_position['BTC'] == 0 and self.signal == 1:
            self.target_position['BTC'] = self.signal * self.mode_params['fixed_volume']
        elif self.target_position['BTC'] == 0 and self.signal == -1:
            self.target_position['BTC'] = self.signal * self.mode_params['fixed_volume']
        elif self.target_position['BTC'] > 0 and self.signal == -1:
            self.target_position['BTC'] = 0
        elif self.target_position['BTC'] < 0 and self.signal == 1:
            self.target_position['BTC'] = 0
        elif self.target_position['BTC'] != 0 and np.isnan(self.signal):
            self.target_position['BTC'] = 0


class CciModel(object):

    def __init__(self):
        #这个model订阅‘BTC/USDT’
        self.symbols = ['BTC/USDT']

        self.mode_params = {
            'fixed_volume': 0.04, #每次买卖0.04个btc
            'cciWindow': 30,
            'cciLong': 10,
            'cciShort': -10,
        }

        self.running_status = 'running'
        self.factor = np.nan
        self.signal = np.nan #model返回的信号值，这个值是介于-1.0到1.0之间的一个浮点数
        self.target_position = {'BTC': 0}
        self.last_kline_end_dt = None
        self.latency = 2*60*1000 #两分钟

        self.talib = TaLib()

    def on_time(self):
        ''' 每5秒定时被驱动，检查k线是否断连'''
        if self.running_status == 'stopping': #如果是停止状态就不工作了
            return
        now = ModelAPI.current_milli_timestamp()
        if self.last_kline_end_dt == None:
            self.last_kline_end_dt = now
        if now - self.last_kline_end_dt > self.latency: #超过2分钟
            self.factor = np.nan
            self.signal = np.nan
            self.target_position['BTC'] = 0.0
            self.running_status = 'stopping'

    def on_kline_update_callback(self, kline: Kline):
        ''' 最新1分钟k线来了，我们需要更新此model的signal'''
        if self.running_status == 'stopping': #如果是停止状态就不工作了
            return
        if kline.symbol not in self.symbols:
            return
        self.last_kline_end_dt = kline.end_dt
        self.talib.kline_update(kline)
        if not self.talib.inited:
            return
        self.generate_factor() #产生因子
        self.generate_signal() #通过因子生成信号
        self.generate_target_position() #通过信号生成仓位

    def generate_factor(self):
        self.factor = self.talib.cci(self.mode_params['cciWindow'])

    def generate_signal(self):
        if self.factor >= self.mode_params['cciLong']:
            self.signal = 1.0
        elif self.factor <= self.mode_params['cciShort']:
            self.signal = -1.0
        else:
            self.signal = np.nan

    def generate_target_position(self):
        if self.target_position['BTC'] == 0 and self.signal == 1:
            self.target_position['BTC'] = self.signal * self.mode_params['fixed_volume']
        elif self.target_position['BTC'] == 0 and self.signal == -1:
            self.target_position['BTC'] = self.signal * self.mode_params['fixed_volume']
        elif self.target_position['BTC'] > 0 and self.signal == -1:
            self.target_position['BTC'] = 0
        elif self.target_position['BTC'] < 0 and self.signal == 1:
            self.target_position['BTC'] = 0
        elif self.target_position['BTC'] != 0 and np.isnan(self.signal):
            self.target_position['BTC'] = 0


class MaModel(object):

    def __init__(self):
        #这个model订阅‘BTC/USDT’
        self.symbols = ['BTC/USDT']

        self.mode_params = {
            'fixed_volume': 0.04, #每次买卖0.04个btc
            'fastWindow': 5,
            'slowWindow': 20,
        }

        self.running_status = 'running'
        self.factor_fastma = np.nan
        self.factor_slowma = np.nan
        self.signal = np.nan #model返回的信号值，这个值是介于-1.0到1.0之间的一个浮点数
        self.target_position = {'BTC': 0}
        self.last_kline_end_dt = None
        self.latency = 2*60*1000 #两分钟

        self.talib = TaLib(24) #5*24=120分钟
        self.kg = KlineGenerator(None, const.MARKET_TYPE_KLINE_5M, self.on_5m_kline_update_callback)

    def on_time(self):
        ''' 每5秒定时被驱动，检查k线是否断连'''
        if self.running_status == 'stopping': #如果是停止状态就不工作了
            return
        now = ModelAPI.current_milli_timestamp()
        if self.last_kline_end_dt == None:
            self.last_kline_end_dt = now
        if now - self.last_kline_end_dt > self.latency: #超过2分钟
            self.factor_fastma = np.nan
            self.factor_slowma = np.nan
            self.signal = np.nan
            self.target_position['BTC'] = 0.0
            self.running_status = 'stopping'

    def on_kline_update_callback(self, kline: Kline):
        if self.running_status == 'stopping': #如果是停止状态就不工作了
            return
        if kline.symbol not in self.symbols:
            return
        self.last_kline_end_dt = kline.end_dt
        self.kg.update_bar(kline)

    def on_5m_kline_update_callback(self, kline: Kline):
        ''' 最新5分钟k线来了，我们需要更新此model的signal'''
        self.talib.kline_update(kline)
        if not self.talib.inited:
            return
        self.generate_factor() #产生因子
        self.generate_signal() #通过因子生成信号
        self.generate_target_position() #通过信号生成仓位

    def generate_factor(self):
        self.factor_fastma = self.talib.sma(self.mode_params['fastWindow'])
        self.factor_slowma = self.talib.sma(self.mode_params['slowWindow'])

    def generate_signal(self):
        if self.factor_fastma > self.factor_slowma:
            self.signal = 1.0
        elif self.factor_fastma < self.factor_slowma:
            self.signal = -1.0
        else:
            self.signal = np.nan

    def generate_target_position(self):
        if self.target_position['BTC'] == 0 and self.signal == 1:
            self.target_position['BTC'] = self.signal * self.mode_params['fixed_volume']
        elif self.target_position['BTC'] == 0 and self.signal == -1:
            self.target_position['BTC'] = self.signal * self.mode_params['fixed_volume']
        elif self.target_position['BTC'] > 0 and self.signal == -1:
            self.target_position['BTC'] = 0
        elif self.target_position['BTC'] < 0 and self.signal == 1:
            self.target_position['BTC'] = 0
        elif self.target_position['BTC'] != 0 and np.isnan(self.signal):
            self.target_position['BTC'] = 0