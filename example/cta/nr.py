# -*- coding:utf-8 -*-

"""
固定数量模式CTA: Normalized Return Model

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import numpy as np
import pandas as pd

from quant.market import Kline
from quant.interface.model_api import ModelAPI
from quant.interface.ah_math import AHMath


class NrModel(object):

    def __init__(self):

        #这个model订阅‘BTC/USDT’
        self.symbols = ['BTC/USDT']

        self.mode_params = {
            'fixed_volume': 0.03, #每次买卖0.03个btc
            'warmup_period': 3, #预热周期三天
            'signal_period': 60, #信号周期60分钟
            'open': 0.42, #做多信号
            'close': -0.42 #做空信号
        }

        self.running_status = 'running'
        self.last_kline = None
        self.last_kline_end_dt = None
        self.last_midnight = None
        self.lag_ret_matrix = []
        self.factor = np.nan
        self.signal = np.nan #model返回的信号值，这个值是介于-1.0到1.0之间的一个浮点数

        self.target_position = {'BTC': 0}

        self.latency = 2*60*1000 #两分钟

    def on_history_kline(self, kline: Kline):
        ''' 加载历史数据'''
        if kline.symbol not in self.symbols:
            return
        midnight = ModelAPI.find_last_datetime_by_time_str(kline.end_dt, reference_time_str='00:00:00.000')
        if self.last_midnight != midnight:
            self.last_midnight = midnight #新的一天来了
            self.lag_ret_matrix.append([]) #为新的一天创建一个空列表用于保存相关数据
        self.lag_ret_matrix[-1].append(kline.lag_ret_fillna)
        if len(self.lag_ret_matrix) > self.mode_params['warmup_period'] + 1: #前三天+'今天'=共四天
            self.lag_ret_matrix.pop(0) #只需要前三天的完整数据,把前第四天的去掉

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

        self.last_kline = kline
        self.last_kline_end_dt = kline.end_dt
        midnight = ModelAPI.find_last_datetime_by_time_str(self.last_kline_end_dt, reference_time_str='00:00:00.000')

        if self.last_midnight != midnight:
            self.last_midnight = midnight #新的一天到了
            self.lag_ret_matrix.append([])

        self.lag_ret_matrix[-1].append(self.last_kline.lag_ret_fillna)

        if len(self.lag_ret_matrix) == self.mode_params['warmup_period'] + 1: #有了三天的数据后,从第四天起就开始工作
            self.generate_factor() #产生因子
            self.generate_signal() #通过因子生成信号
            self.generate_target_position() #通过信号生成仓位
        elif len(self.lag_ret_matrix) > self.mode_params['warmup_period'] + 1: #历史数据超过三天,前四天+'今天'=共五天
            self.lag_ret_matrix.pop(0) #前第四天的数据去掉,只需要保存前三天的数据
            #策略正式工作以后,每当新的一天到来,都将仓位,信号等都清0,重新开始计算
            self.factor = np.nan
            self.signal = np.nan
            self.target_position['BTC'] = 0.0

    def generate_factor(self):
        all_lag_ret = []
        for each in self.lag_ret_matrix: #把前三天+'今天'的数据都放到一个列表里面
            all_lag_ret += each
        all_lag_ret = pd.Series(all_lag_ret)
        x = AHMath.ewma(all_lag_ret, self.mode_params['signal_period'])
        y = AHMath.ewma(np.abs(all_lag_ret), self.mode_params['signal_period'])
        factors = AHMath.zero_divide(x, y)
        count = factors[-self.mode_params['signal_period']-1:].count()
        self.factor = factors.iloc[-1] if count == self.mode_params['signal_period']+1 else np.nan

    def generate_signal(self):
        self.signal = 0.0
        if self.factor > self.mode_params['open']:
            self.signal = 1.0
        elif self.factor < self.mode_params['close']:
            self.signal = -1.0
        elif np.isnan(self.factor):
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