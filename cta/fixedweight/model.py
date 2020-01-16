# -*- coding:utf-8 -*-

"""
固定权重模式CTA Model演示

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""


class CTAFixedWeightModel1(object):

    def __init__(self):

        # 这个model订阅‘btcusdt’
        self.symbols = ['btcusdt']

        # 这个model关心的货的种类
        self.coins = ['btc']

        # 这个model的模式：采用fixed_weight模式
        self.mode = 'fixed_weight'

        # mode_params是这个固定权重模式下的具体参数，字典类型，其中fixed_weight为固定权重的具体数值，max_money为该模型占用资金的最大上限
        self.mode_params = {
            'fixed_weight': 0.3, 
            'max_money': 120
        }

        # model返回的信号值，这个值是介于-1.0到1.0之间的一个浮点数
        self.signal = {'btc': 0.32}

    def on_klines_combined_update_generate_signal(self):
        ''' 最新k线来了，我们需呀更新此model的signal字典'''
        return self.signal

    def on_time_update_generate_signal(self):
        ''' 定时被驱动，按照最新价格更新此model的signal字典'''
        return self.signal
