# -*- coding:utf-8 -*-

"""
持仓对象

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

from quant.utils import tools


class Position:
    """ 持仓对象
    """

    def __init__(self, platform=None, account=None, strategy=None, symbol=None):
        """ 初始化持仓对象
        @param platform 交易平台
        @param account 账户
        @param strategy 策略名称
        @param symbol 合约名称
        """
        self.platform = platform
        self.account = account
        self.strategy = strategy
        self.symbol = symbol
        self.short_quantity = 0  # 空仓数量
        self.short_avg_price = 0  # 空仓持仓平均价格
        self.long_quantity = 0  # 多仓数量
        self.long_avg_price = 0  # 多仓持仓平均价格
        self.liquid_price = 0  # 预估爆仓价格
        self.utime = None  # 更新时间戳

    def update(self, short_quantity=0, short_avg_price=0, long_quantity=0, long_avg_price=0, liquid_price=0,
               utime=None):
        self.short_quantity = short_quantity
        self.short_avg_price = short_avg_price
        self.long_quantity = long_quantity
        self.long_avg_price = long_avg_price
        self.liquid_price = liquid_price
        self.utime = utime if utime else tools.get_cur_timestamp_ms()

    def __str__(self):
        info = "[platform: {platform}, account: {account}, strategy: {strategy}, symbol: {symbol}, " \
               "short_quantity: {short_quantity}, short_avg_price: {short_avg_price}, " \
               "long_quantity: {long_quantity}, long_avg_price: {long_avg_price}, liquid_price: {liquid_price}, " \
               "utime: {utime}]"\
            .format(platform=self.platform, account=self.account, strategy=self.strategy, symbol=self.symbol,
                    short_quantity=self.short_quantity, short_avg_price=self.short_avg_price,
                    long_quantity=self.long_quantity, long_avg_price=self.long_avg_price,
                    liquid_price=self.liquid_price, utime=self.utime)
        return info

    def __repr__(self):
        return str(self)
