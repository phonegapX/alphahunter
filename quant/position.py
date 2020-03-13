# -*- coding:utf-8 -*-

"""
持仓对象

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

from quant.utils import tools

#保证金模式
MARGIN_MODE_CROSSED = "crossed" #全仓
MARGIN_MODE_FIXED = "fixed" #逐仓


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
        
        self.margin_mode = None         #全仓or逐仓
        
        self.long_quantity = 0          #多仓数量 [不能为负数]
        self.long_avail_qty = 0         #多仓可用数量(平仓挂单会冻结相应数量) [不能为负数]
        self.long_open_price = 0        #多仓开仓平均价格
        self.long_hold_price = 0        #多仓持仓平均价格(与结算有关,某些平台采用周结算制度)
        self.long_liquid_price = 0      #多仓预估爆仓价格,逐仓模式爆仓价格单独计算(OKEX全仓模式多头和空头破产价格合并计算,也就是破产价格相同)
        self.long_unrealised_pnl = 0    #多仓未实现盈亏 [可正可负,正数代表盈利,负数代表亏损]
        self.long_leverage = 0          #多仓杠杠倍数,逐仓模式下可以单独设置,全仓模式下只能统一设置
        self.long_margin = 0            #多仓保证金,本字段逐仓模式下意义更重要
        
        self.short_quantity = 0         #空仓数量 [不能为负数]
        self.short_avail_qty = 0        #空仓可用数量(平仓挂单会冻结相应数量) [不能为负数]
        self.short_open_price = 0       #空仓开仓平均价格
        self.short_hold_price = 0       #空仓持仓平均价格(与结算有关,某些平台采用周结算制度)
        self.short_liquid_price = 0     #空仓预估爆仓价格,逐仓模式爆仓价格单独计算(OKEX全仓模式多头和空头破产价格合并计算,也就是破产价格相同)
        self.short_unrealised_pnl = 0   #空仓未实现盈亏 [可正可负,正数代表盈利,负数代表亏损]
        self.short_leverage = 0         #空仓杠杠倍数,逐仓模式下可以单独设置,全仓模式下只能统一设置
        self.short_margin = 0           #空仓保证金,本字段逐仓模式下意义更重要

        self.utime = None               #更新时间戳

    def __str__(self):
        info = "[platform: {platform}, account: {account}, strategy: {strategy}, symbol: {symbol}, " \
               \
               "margin_mode: {margin_mode}, " \
               \
               "long_quantity: {long_quantity}, " \
               "long_avail_qty: {long_avail_qty}, " \
               "long_open_price: {long_open_price}, " \
               "long_hold_price: {long_hold_price}, " \
               "long_liquid_price: {long_liquid_price}, " \
               "long_unrealised_pnl: {long_unrealised_pnl}, " \
               "long_leverage: {long_leverage}, " \
               "long_margin: {long_margin}, " \
               \
               "short_quantity: {short_quantity}, " \
               "short_avail_qty: {short_avail_qty}, " \
               "short_open_price: {short_open_price}, " \
               "short_hold_price: {short_hold_price}, " \
               "short_liquid_price: {short_liquid_price}, " \
               "short_unrealised_pnl: {short_unrealised_pnl}, " \
               "short_leverage: {short_leverage}, " \
               "short_margin: {short_margin}, " \
               \
               "utime: {utime}]".format(platform=self.platform, account=self.account, strategy=self.strategy, symbol=self.symbol,
                    #
                    margin_mode=self.margin_mode,
                    #
                    long_quantity=self.long_quantity,
                    long_avail_qty=self.long_avail_qty,
                    long_open_price=self.long_open_price,
                    long_hold_price=self.long_hold_price,
                    long_liquid_price=self.long_liquid_price,
                    long_unrealised_pnl=self.long_unrealised_pnl,
                    long_leverage=self.long_leverage,
                    long_margin=self.long_margin,
                    #
                    short_quantity=self.short_quantity,
                    short_avail_qty=self.short_avail_qty,
                    short_open_price=self.short_open_price,
                    short_hold_price=self.short_hold_price,
                    short_liquid_price=self.short_liquid_price,
                    short_unrealised_pnl=self.short_unrealised_pnl,
                    short_leverage=self.short_leverage,
                    short_margin=self.short_margin,
                    #
                    utime=self.utime)
        return info

    def __repr__(self):
        return str(self)
