# -*- coding:utf-8 -*-

"""
实时K线合成器

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

from quant import const
from quant.market import Kline, Trade


ONE_MINUTE = 60*1000

class KlineGenerator(object):
    """
    K线合成器，支持：
    1. 基于trade合成1分钟K线
    2. 基于1分钟K线合成X分钟K线 (X可以是5、15)
    """

    #----------------------------------------------------------------------
    def __init__(self, onBar, bar_type=None, onXminBar=None):
        """Constructor"""
        self.bar = None             #1分钟K线对象
        self.onBar = onBar          #1分钟K线回调函数
        if bar_type == const.MARKET_TYPE_KLINE_5M:
            self.xmin = 5
        elif bar_type == const.MARKET_TYPE_KLINE_15M:
            self.xmin = 15
        self.bar_type = bar_type
        self.xminBar = None         #X分钟K线对象
        self.onXminBar = onXminBar  #X分钟K线的回调函数

    #----------------------------------------------------------------------
    async def update_trade(self, trade):
        """ 逐笔成交更新
        """
        newMinute = False   #默认不是新的一分钟
        #尚未创建对象
        if not self.bar:
            self.bar = Kline()
            newMinute = True
        #新的一分钟
        elif int(self.bar.timestamp//ONE_MINUTE) != int(trade.timestamp//ONE_MINUTE):
            #生成上一分钟K线的时间戳
            self.bar.timestamp = int(self.bar.timestamp//ONE_MINUTE)*ONE_MINUTE
            #推送已经结束的上一分钟K线
            await self.onBar(self.bar)
            #创建新的K线对象
            self.bar = Kline()
            newMinute = True
        #初始化新一分钟的K线数据
        if newMinute:
            self.bar.platform = trade.platform
            self.bar.symbol = trade.symbol
            self.bar.open = trade.price
            self.bar.high = trade.price
            self.bar.low = trade.price
            self.bar.kline_type = const.MARKET_TYPE_KLINE
            self.bar.volume = 0
        #累加更新老一分钟的K线数据
        else:
            self.bar.high = max(self.bar.high, trade.price)
            self.bar.low = min(self.bar.low, trade.price)
        #通用更新部分
        self.bar.close = trade.price
        self.bar.timestamp = trade.timestamp
        self.bar.volume += trade.quantity

    #----------------------------------------------------------------------
    async def update_bar(self, bar):
        """ 1分钟K线更新
        """
        #尚未创建对象
        if not self.xminBar:
            self.xminBar = Kline()
            self.xminBar.kline_type = self.bar_type
            self.xminBar.platform = bar.platform
            self.xminBar.symbol = bar.symbol
            self.xminBar.open = bar.open
            self.xminBar.high = bar.high
            self.xminBar.low = bar.low
            self.xminBar.timestamp = bar.timestamp    #以第一根分钟K线的开始时间戳作为X分钟线的时间戳
            self.xminBar.volume = 0
        #累加老K线
        else:
            self.xminBar.high = max(self.xminBar.high, bar.high)
            self.xminBar.low = min(self.xminBar.low, bar.low)
        #通用部分
        self.xminBar.close = bar.close
        self.xminBar.volume += bar.volume
        #X分钟已经走完
        minute = int(bar.timestamp//ONE_MINUTE)
        if not (minute + 1) % self.xmin:   #可以用X整除
            #推送
            await self.onXminBar(self.xminBar)
            #清空老K线缓存对象
            self.xminBar = None
