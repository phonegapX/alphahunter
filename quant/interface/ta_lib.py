# -*- coding:utf-8 -*-

"""
技术分析库

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import numpy as np
import talib


class TaLib(object):
    """ 技术分析库
    """

    #----------------------------------------------------------------------
    def __init__(self, size=120):
        """Constructor"""
        self._count = 0                      # 缓存计数
        self._size = size                    # 缓存大小
        self._inited = False                 # True if count>=size
        
        self._openArray = np.zeros(size)     # OHLC
        self._highArray = np.zeros(size)
        self._lowArray = np.zeros(size)
        self._closeArray = np.zeros(size)
        self._volumeArray = np.zeros(size)
        
    #----------------------------------------------------------------------
    def kline_update(self, kline):
        """更新K线"""
        self._count += 1
        if not self._inited and self._count >= self._size:
            self._inited = True

        self._openArray[:-1] = self._openArray[1:]
        self._highArray[:-1] = self._highArray[1:]
        self._lowArray[:-1] = self._lowArray[1:]
        self._closeArray[:-1] = self._closeArray[1:]
        self._volumeArray[:-1] = self._volumeArray[1:]

        self._openArray[-1] = kline.open
        self._highArray[-1] = kline.high
        self._lowArray[-1] = kline.low
        self._closeArray[-1] = kline.close
        self._volumeArray[-1] = kline.volume

    #----------------------------------------------------------------------
    @property
    def inited(self):
        """是否可以使用"""
        return self._inited

    #----------------------------------------------------------------------
    @property
    def open(self):
        """获取开盘价序列"""
        return self._openArray

    #----------------------------------------------------------------------
    @property
    def high(self):
        """获取最高价序列"""
        return self._highArray

    #----------------------------------------------------------------------
    @property
    def low(self):
        """获取最低价序列"""
        return self._lowArray

    #----------------------------------------------------------------------
    @property
    def close(self):
        """获取收盘价序列"""
        return self._closeArray

    #----------------------------------------------------------------------
    @property
    def volume(self):
        """获取成交量序列"""
        return self._volumeArray

    #----------------------------------------------------------------------
    def sma(self, n, array=False):
        """简单均线"""
        result = talib.SMA(self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def ema(self, n, array=False):
        """指数平均数指标"""
        result = talib.EMA(self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def std(self, n, array=False):
        """标准差"""
        result = talib.STDDEV(self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def cci(self, n, array=False):
        """CCI指标"""
        result = talib.CCI(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def atr(self, n, array=False):
        """ATR指标"""
        result = talib.ATR(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def rsi(self, n, array=False):
        """RSI指标"""
        result = talib.RSI(self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def macd(self, fastPeriod, slowPeriod, signalPeriod, array=False):
        """MACD指标"""
        macd, signal, hist = talib.MACD(self.close, fastPeriod, slowPeriod, signalPeriod)
        if array:
            return macd, signal, hist
        return macd[-1], signal[-1], hist[-1]

    #----------------------------------------------------------------------
    def adx(self, n, array=False):
        """ADX指标"""
        result = talib.ADX(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    #----------------------------------------------------------------------
    def boll(self, n, dev, array=False):
        """布林通道"""
        mid = self.sma(n, array)
        std = self.std(n, array)
        
        up = mid + std * dev
        down = mid - std * dev
        
        return up, down

    #----------------------------------------------------------------------
    def keltner(self, n, dev, array=False):
        """肯特纳通道"""
        mid = self.sma(n, array)
        atr = self.atr(n, array)
        
        up = mid + atr * dev
        down = mid - atr * dev
        
        return up, down

    #----------------------------------------------------------------------
    def donchian(self, n, array=False):
        """唐奇安通道"""
        up = talib.MAX(self.high, n)
        down = talib.MIN(self.low, n)
        
        if array:
            return up, down
        return up[-1], down[-1]
