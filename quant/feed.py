# -*- coding:utf-8 -*-

"""
历史数据输送器

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

from quant.config import config
from quant.tasks import SingleTask


class HistoryDataFeed:
    """
    """

    gw_list = []
    
    def __init__(self, **kwargs):
        self.gw_list.append(self)

    @classmethod
    async def start(cls):
        if config.backtest:
            cls._start_time = config.backtest["start_time"]
            cls._period_day = config.backtest["period_day"]
            cls._drive_type = config.backtest["drive_type"]
        elif config.datamatrix:
            cls._start_time = config.datamatrix["start_time"]
            cls._period_day = config.datamatrix["period_day"]
            cls._drive_type = config.datamatrix["drive_type"]
        
        #1.算出begin_time和end_time
        #2.然后按3小时为一个单位调用 按drive_type 按gw_list里面每个对象的gw.load_data(drive_type, begin_time, end_time)
        #3.将上一步读取到的所有pandas合并成一个大的pandas, 然后按dt进行排序
        #4.循环遍历这个大的pandas,将记录里面的self值，把数据逐条推送给BacktestTrader
        #5.BacktestTrader里面将记录按drive_type转换为相应的结构 然后调用相应on_xxxx
        #6.重复第二步
        #备注：每次把时间dt记录下来 作为回测环境的当前时间
        pass