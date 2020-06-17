# -*- coding:utf-8 -*-

"""
虚拟交易所,用于策略回测,可参考文档中的策略回测架构图

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import time
import zlib
import json
import copy
import hmac
import base64
import numpy as np
import pandas as pd
from urllib.parse import urljoin
from collections import defaultdict, deque
from typing import DefaultDict, Deque, List, Dict, Tuple, Optional, Any
from itertools import zip_longest
from abc import ABCMeta, abstractmethod
from six import with_metaclass
from decimal import Decimal

from quant.config import config
from quant.state import State
from quant.order import Order, Fill, SymbolInfo
from quant.tasks import SingleTask, LoopRunTask
from quant.position import Position
from quant.asset import Asset
from quant.const import MARKET_TYPE_KLINE, MARKET_TYPE_KLINE_5M
from quant.utils import tools, logger
from quant.utils.decorator import async_method_locker
from quant.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL
from quant.order import ORDER_TYPE_LIMIT, ORDER_TYPE_MARKET, ORDER_TYPE_IOC
from quant.order import LIQUIDITY_TYPE_MAKER, LIQUIDITY_TYPE_TAKER
from quant.order import ORDER_STATUS_SUBMITTED, ORDER_STATUS_PARTIAL_FILLED, ORDER_STATUS_FILLED, ORDER_STATUS_CANCELED, ORDER_STATUS_FAILED
from quant.market import Kline, Orderbook, Trade, Ticker
from quant.history import VirtualTrader
from quant.infra_api import InfraAPI
from quant.interface.model_api import ModelAPI
from quant.trader import Trader


__all__ = ("BacktestTrader",)


class SequenceGenerator(object):
    """ 生成唯一序列号
    """
    def __init__(self):
        self.__d = defaultdict(int)
    
    def get_next(self, key):
        self.__d[key] += 1
        return self.__d[key]


class BaseMatchEngine(with_metaclass(ABCMeta)):
    """ 撮合引擎基类
    """

    def __init__(self, symbol, trader, **kwargs):
        """Initialize."""
        self._platform = kwargs.get("databind")
        self._symbol = symbol #绑定的交易对符号
        self._seq_gen = SequenceGenerator()
        self._maker_commission_rate = config.backtest["feature"][self._platform]["maker_commission_rate"] #maker手续费
        self._taker_commission_rate = config.backtest["feature"][self._platform]["taker_commission_rate"] #taker手续费

    def next_fill_no(self):
        return "trade_{}_{}_{:0>8d}".format(self._platform, self._symbol, self._seq_gen.get_next('trade_id'))

    def next_order_no(self):
        return "order_{}_{}_{:0>8d}".format(self._platform, self._symbol, self._seq_gen.get_next('order_id'))

    @property
    def maker_commission_rate(self):
        """ maker交易手续费
        """
        return self._maker_commission_rate

    @property
    def taker_commission_rate(self):
        """ taker交易手续费
        """
        return self._taker_commission_rate

    @abstractmethod
    async def on_kline_update_callback(self, kline: Kline):
        """ K线方式驱动回测引擎
        """

    @abstractmethod
    async def on_orderbook_update_callback(self, orderbook: Orderbook):
        """ 订单薄方式驱动回测引擎
        """

    @abstractmethod
    async def on_trade_update_callback(self, trade: Trade):
        """ 市场成交方式驱动回测引擎
        """

    @abstractmethod
    async def create_order(self, action, price, quantity, order_type=ORDER_TYPE_LIMIT):
        """ 下单
        """

    @abstractmethod
    async def revoke_order(self, *order_nos):
        """ 撤单
        """

    @abstractmethod    
    async def get_orders(self):
        """ 获取挂单列表
        """

    @abstractmethod
    async def get_position(self):
        """ 获取当前仓位
        """

    @abstractmethod
    async def get_symbol_info(self):
        """ 获取符号信息
        """

    @abstractmethod
    async def invalid_indicate(self, indicate_type):
        """ 强制刷新指定回调函数
        """


class SimpleFutureMatchEngine(BaseMatchEngine):
    """ 简单版合约回测撮合引擎
    """

    def __init__(self, symbol, trader, **kwargs):
        """Initialize."""
        super(SimpleFutureMatchEngine, self).__init__(symbol, trader, **kwargs)
        self.cb = kwargs["cb"]
        self._platform = kwargs.get("databind")
        self._strategy = kwargs.get("strategy")
        self._account = kwargs.get("account")
        self._symbol = symbol #绑定的交易对符号
        self._trader = trader
        self._orders = dict() #订单列表,模拟订单薄
        self._last_kline = None

    async def on_kline_update_callback(self, kline: Kline):
        """ K线方式驱动回测引擎
        """
        self._last_kline = kline #保存最新一根K线
        await self.make_trade() #尝试和订单列表中的订单进行撮合成交

    async def on_orderbook_update_callback(self, orderbook: Orderbook):
        """ 订单薄方式驱动回测引擎
        """
        pass

    async def on_trade_update_callback(self, trade: Trade):
        """ 市场成交方式驱动回测引擎
        """
        pass

    async def make_trade(self):
        """ 尝试和订单列表中的订单进行撮合成交
        """
        pass

    async def create_order(self, action, price, quantity, order_type=ORDER_TYPE_LIMIT):
        """ 下单
        """
        return None, None

    async def revoke_order(self, *order_nos):
        """ 撤单
        """
        return [], None

    async def get_orders(self):
        """ 获取挂单列表
        """
        return [], None

    async def get_position(self):
        """ 获取当前仓位
        """
        return None, None

    async def get_symbol_info(self):
        """ 获取符号信息
        """
        info = config.backtest["feature"][self._platform]["syminfo"][self._symbol]
        price_tick = info["price_tick"]
        size_tick = info["size_tick"]
        size_limit = info["size_limit"]
        value_tick = info["value_tick"]
        value_limit = info["value_limit"]
        base_currency = info["base_currency"]
        quote_currency = info["quote_currency"]
        settlement_currency = info["settlement_currency"]
        syminfo = SymbolInfo(self._platform, self._symbol, price_tick, size_tick, size_limit, value_tick, value_limit, base_currency, quote_currency, settlement_currency)
        return syminfo, None

    async def invalid_indicate(self, indicate_type):
        """ 强制刷新指定回调函数
        """
        return False, None


class SimpleSpotMatchEngine(BaseMatchEngine):
    """ 简单版现货回测撮合引擎
    """

    def __init__(self, symbol, trader, **kwargs):
        """Initialize."""
        super(SimpleSpotMatchEngine, self).__init__(symbol, trader, **kwargs)
        self.cb = kwargs["cb"]
        self._platform = kwargs.get("databind")
        self._strategy = kwargs.get("strategy")
        self._account = kwargs.get("account")
        self._symbol = symbol #绑定的交易对符号
        self._trader = trader
        self._orders = dict() #订单列表,模拟订单薄
        self._last_kline = None

    async def on_kline_update_callback(self, kline: Kline):
        """ K线方式驱动回测引擎
        """
        self._last_kline = kline #保存最新一根K线
        await self.make_trade() #尝试和订单列表中的订单进行撮合成交

    async def on_orderbook_update_callback(self, orderbook: Orderbook):
        """ 订单薄方式驱动回测引擎
        """
        pass

    async def on_trade_update_callback(self, trade: Trade):
        """ 市场成交方式驱动回测引擎
        """
        pass

    async def make_trade(self):
        """ 尝试和订单列表中的订单进行撮合成交
        """
        #遍历订单簿里面所有挂单
        os = copy.copy(self._orders)
        for (k, o) in os.items():
            if o.action == ORDER_ACTION_BUY: #买单
                if o.price >= self._last_kline.close_avg_fillna: #当前价格可以成交
                    ts = ModelAPI.current_milli_timestamp()
                    #收盘均价模拟成交价
                    tradeprice = self._last_kline.close_avg_fillna
                    tradevolmue = quantity #直接模拟全部成交
                    trademoney = tradeprice*tradevolmue #成交金额
                    #对于现货交易,手续费是从接收币种里面扣除
                    fee = tradevolmue*self.maker_commission_rate
                    tradevolmue -= fee
                    #订单通知
                    o.remain = 0
                    o.status = ORDER_STATUS_FILLED
                    o.utime = ts
                    if self.cb.on_order_update_callback:
                        await self.cb.on_order_update_callback(o)
                    #成交通知
                    fill_no = self.next_fill_no()
                    f = {
                        "platform": self._platform,
                        "account": self._account,
                        "strategy": self._strategy,
                        "fill_no": fill_no,
                        "order_no": o.order_no,
                        "side": o.action, #成交方向,买还是卖
                        "symbol": self._symbol,
                        "price": tradeprice, #成交价格
                        "quantity": tradevolmue, #成交数量
                        "liquidity": LIQUIDITY_TYPE_MAKER, #maker成交还是taker成交
                        "fee": fee,
                        "ctime": ts
                    }
                    fill = Fill(**f)
                    if self.cb.on_fill_update_callback:
                        await self.cb.on_fill_update_callback(fill)
                    #账户资产通知
                    #'货'增加
                    bc['free'] += tradevolmue
                    bc['total'] = bc['free'] + bc['locked']
                    #释放挂单占用的'钱'
                    sc['locked'] -= o.quantity*o.price
                    sc['free'] = sc['total'] - sc['locked']
                    #'钱'减少
                    sc['free'] -= trademoney
                    sc['total'] = sc['free'] + sc['locked']
                    #
                    ast = Asset(self._platform, self._account, self._trader._assets, ts, True)
                    if self.cb.on_asset_update_callback:
                        await self.cb.on_asset_update_callback(ast)
                    #删除订单簿中的订单
                    del self._orders[o.order_no]
            elif o.action == ORDER_ACTION_SELL: #卖单
                if o.price <= self._last_kline.close_avg_fillna: #当前价格可以成交
                    ts = ModelAPI.current_milli_timestamp()
                    #收盘均价模拟成交价
                    tradeprice = self._last_kline.close_avg_fillna
                    trademoney = o.quantity*tradeprice #模拟全部成交
                    #对于现货交易,手续费是从接收币种里面扣除
                    fee = trademoney*self.maker_commission_rate
                    trademoney -= fee
                    #订单通知
                    o.remain = 0
                    o.status = ORDER_STATUS_FILLED
                    o.utime = ts
                    if self.cb.on_order_update_callback:
                        await self.cb.on_order_update_callback(o)
                    #成交通知
                    fill_no = self.next_fill_no()
                    f = {
                        "platform": self._platform,
                        "account": self._account,
                        "strategy": self._strategy,
                        "fill_no": fill_no,
                        "order_no": o.order_no,
                        "side": o.action, #成交方向,买还是卖
                        "symbol": self._symbol,
                        "price": tradeprice, #成交价格
                        "quantity": o.quantity, #成交数量
                        "liquidity": LIQUIDITY_TYPE_MAKER, #maker成交还是taker成交
                        "fee": fee,
                        "ctime": ts
                    }
                    fill = Fill(**f)
                    if self.cb.on_fill_update_callback:
                        await self.cb.on_fill_update_callback(fill)
                    #账户资产通知
                    #释放挂单占用的'货'
                    bc['locked'] -= o.quantity
                    bc['free'] = bc['total'] - bc['locked']
                    #'货'减少
                    bc['free'] -= o.quantity
                    bc['total'] = bc['free'] + bc['locked']
                    #'钱'增加
                    sc['free'] += trademoney
                    sc['total'] = sc['free'] + sc['locked']
                    #
                    ast = Asset(self._platform, self._account, self._trader._assets, ts, True)
                    if self.cb.on_asset_update_callback:
                        await self.cb.on_asset_update_callback(ast)
                    #删除订单簿中的订单
                    del self._orders[o.order_no]

    def precision_verify(self, src:float, t:float):
        #src和t不能超出浮点数有效精度范围
        #t为0代表不检测,直接返回True
        if not t or Decimal(str(src))%Decimal(str(t)) == 0:
            return True
        else:
            return False

    async def create_order(self, action, price, quantity, order_type=ORDER_TYPE_LIMIT):
        """ 下单
        """
        if not self._last_kline or not self._last_kline.usable:
            return None, "无法创建订单"
        #获取符号相关信息
        syminfo = config.backtest["feature"][self._platform]["syminfo"][self._symbol]
        price_tick = syminfo["price_tick"]   #价格变动最小精度
        size_tick = syminfo["size_tick"]     #下单数量变动最小精度
        size_limit = syminfo["size_limit"]   #下单数量最小限制
        value_tick = syminfo["value_tick"]   #下单金额变动最小精度
        value_limit = syminfo["value_limit"] #下单金额最小限制
        base_currency = syminfo["base_currency"] #基础币种,交易标的,或者说就是'货'
        settlement_currency = syminfo["settlement_currency"] #结算币种,或者说就是'钱'
        #输入参数验证
        if order_type == ORDER_TYPE_MARKET:
            if price:
                return None, "无法创建订单,市价单价格必须填0"
            if action == ORDER_ACTION_BUY:
                #市价买单quantity代表的是下单金额
                if quantity < value_limit:
                    return None, "无法创建订单,下单金额太少"
                if not self.precision_verify(quantity, value_tick):
                    return None, "无法创建订单,下单金额精度错误"
            else:
                if quantity < size_limit:
                    return None, "无法创建订单,下单数量太少"
                if not self.precision_verify(quantity, size_tick):
                    return None, "无法创建订单,下单数量精度错误"
        else:
            if price <= 0:
                return None, "无法创建订单,价格必须大于0"
            if not self.precision_verify(price, price_tick):
                return None, "无法创建订单,价格精度错误"
            if quantity < size_limit:
                return None, "无法创建订单,下单数量太少"
            if not self.precision_verify(quantity, size_tick):
                return None, "无法创建订单,下单数量精度错误"
        #获取当前时间
        ts = ModelAPI.current_milli_timestamp()
        #
        if order_type == ORDER_TYPE_MARKET: #市价单
            if action == ORDER_ACTION_BUY: #买
                bc = self._trader._assets[base_currency]
                sc = self._trader._assets[settlement_currency]
                if quantity > sc['free']:
                    return None, "账户余额不够"
                #收盘均价模拟成交价
                tradeprice = self._last_kline.close_avg_fillna
                #市价买单quantity指的是'钱'
                tradevolmue = quantity/tradeprice
                #对于现货交易,手续费是从接收币种里面扣除
                fee = tradevolmue*self.taker_commission_rate
                tradevolmue -= fee
                #订单通知
                order_no = self.next_order_no()
                o = {
                    "platform": self._platform,
                    "account": self._account,
                    "strategy": self._strategy,
                    "order_no": order_no,
                    "action": action,
                    "symbol": self._symbol,
                    "price": 0,
                    "quantity": quantity,
                    "remain": 0,
                    "status": ORDER_STATUS_FILLED,
                    "order_type": order_type,
                    "ctime": ts,
                    "utime": ts
                    #avg_price
                    #trade_type
                }
                order = Order(**o)
                if self.cb.on_order_update_callback:
                    await self.cb.on_order_update_callback(order)
                #成交通知
                fill_no = self.next_fill_no()
                f = {
                    "platform": self._platform,
                    "account": self._account,
                    "strategy": self._strategy,
                    "fill_no": fill_no,
                    "order_no": order_no,
                    "side": action, #成交方向,买还是卖
                    "symbol": self._symbol,
                    "price": tradeprice, #成交价格
                    "quantity": tradevolmue, #成交数量
                    "liquidity": LIQUIDITY_TYPE_TAKER, #maker成交还是taker成交
                    "fee": fee,
                    "ctime": ts
                }
                fill = Fill(**f)
                if self.cb.on_fill_update_callback:
                    await self.cb.on_fill_update_callback(fill)
                #账户资产通知
                #'货'增加
                bc['free'] += tradevolmue
                bc['total'] = bc['free'] + bc['locked']
                #'钱'减少
                sc['free'] -= quantity #市价买单quantity指的是'钱'
                sc['total'] = sc['free'] + sc['locked']
                #
                ast = Asset(self._platform, self._account, self._trader._assets, ts, True)
                if self.cb.on_asset_update_callback:
                    await self.cb.on_asset_update_callback(ast)
            elif action == ORDER_ACTION_SELL: #卖
                bc = self._trader._assets[base_currency]
                sc = self._trader._assets[settlement_currency]
                if quantity > bc['free']:
                    return None, "账户币不足"
                #收盘均价模拟成交价
                tradeprice = self._last_kline.close_avg_fillna
                trademoney = quantity*tradeprice
                #对于现货交易,手续费是从接收币种里面扣除
                fee = trademoney*self.taker_commission_rate
                trademoney -= fee
                #订单通知
                order_no = self.next_order_no()
                o = {
                    "platform": self._platform,
                    "account": self._account,
                    "strategy": self._strategy,
                    "order_no": order_no,
                    "action": action,
                    "symbol": self._symbol,
                    "price": 0,
                    "quantity": quantity,
                    "remain": 0,
                    "status": ORDER_STATUS_FILLED,
                    "order_type": order_type,
                    "ctime": ts,
                    "utime": ts
                    #avg_price
                    #trade_type
                }
                order = Order(**o)
                if self.cb.on_order_update_callback:
                    await self.cb.on_order_update_callback(order)
                #成交通知
                fill_no = self.next_fill_no()
                f = {
                    "platform": self._platform,
                    "account": self._account,
                    "strategy": self._strategy,
                    "fill_no": fill_no,
                    "order_no": order_no,
                    "side": action, #成交方向,买还是卖
                    "symbol": self._symbol,
                    "price": tradeprice, #成交价格
                    "quantity": quantity, #成交数量
                    "liquidity": LIQUIDITY_TYPE_TAKER, #maker成交还是taker成交
                    "fee": fee,
                    "ctime": ts
                }
                fill = Fill(**f)
                if self.cb.on_fill_update_callback:
                    await self.cb.on_fill_update_callback(fill)
                #账户资产通知
                #'货'减少
                bc['free'] -= quantity
                bc['total'] = bc['free'] + bc['locked']
                #'钱'增加
                sc['free'] += trademoney
                sc['total'] = sc['free'] + sc['locked']
                #
                ast = Asset(self._platform, self._account, self._trader._assets, ts, True)
                if self.cb.on_asset_update_callback:
                    await self.cb.on_asset_update_callback(ast)
        elif order_type == ORDER_TYPE_LIMIT: #限价单
            if action == ORDER_ACTION_BUY: #买
                bc = self._trader._assets[base_currency]
                sc = self._trader._assets[settlement_currency]
                if quantity*price > sc['free']:
                    return None, "账户余额不够"
                #如果下单价格小于当前价格,那意味着无法成交,订单将进入订单薄挂着
                if price < self._last_kline.close_avg_fillna:
                    #订单通知
                    order_no = self.next_order_no()
                    o = {
                        "platform": self._platform,
                        "account": self._account,
                        "strategy": self._strategy,
                        "order_no": order_no,
                        "action": action,
                        "symbol": self._symbol,
                        "price": price,
                        "quantity": quantity,
                        "remain": quantity,
                        "status": ORDER_STATUS_SUBMITTED,
                        "order_type": order_type,
                        "ctime": ts,
                        "utime": ts
                        #avg_price
                        #trade_type
                    }
                    order = Order(**o)
                    self._orders[order_no] = order #进入订单簿
                    if self.cb.on_order_update_callback:
                        await self.cb.on_order_update_callback(order)
                    #账户资产通知
                    #'钱'需要被锁定一部分
                    sc['locked'] += quantity*price #挂单部分所占用的资金需要被锁定
                    sc['free'] = sc['total'] - sc['locked']
                    #
                    ast = Asset(self._platform, self._account, self._trader._assets, ts, True)
                    if self.cb.on_asset_update_callback:
                        await self.cb.on_asset_update_callback(ast)
                else: #直接成交
                    #收盘均价模拟成交价
                    tradeprice = self._last_kline.close_avg_fillna
                    tradevolmue = quantity #直接模拟全部成交
                    trademoney = tradeprice*tradevolmue #成交金额
                    #对于现货交易,手续费是从接收币种里面扣除
                    fee = tradevolmue*self.taker_commission_rate
                    tradevolmue -= fee
                    #订单通知
                    order_no = self.next_order_no()
                    o = {
                        "platform": self._platform,
                        "account": self._account,
                        "strategy": self._strategy,
                        "order_no": order_no,
                        "action": action,
                        "symbol": self._symbol,
                        "price": price,
                        "quantity": quantity,
                        "remain": 0,
                        "status": ORDER_STATUS_FILLED,
                        "order_type": order_type,
                        "ctime": ts,
                        "utime": ts
                        #avg_price
                        #trade_type
                    }
                    order = Order(**o)
                    if self.cb.on_order_update_callback:
                        await self.cb.on_order_update_callback(order)
                    #成交通知
                    fill_no = self.next_fill_no()
                    f = {
                        "platform": self._platform,
                        "account": self._account,
                        "strategy": self._strategy,
                        "fill_no": fill_no,
                        "order_no": order_no,
                        "side": action, #成交方向,买还是卖
                        "symbol": self._symbol,
                        "price": tradeprice, #成交价格
                        "quantity": tradevolmue, #成交数量
                        "liquidity": LIQUIDITY_TYPE_TAKER, #maker成交还是taker成交
                        "fee": fee,
                        "ctime": ts
                    }
                    fill = Fill(**f)
                    if self.cb.on_fill_update_callback:
                        await self.cb.on_fill_update_callback(fill)
                    #账户资产通知
                    #'货'增加
                    bc['free'] += tradevolmue
                    bc['total'] = bc['free'] + bc['locked']
                    #'钱'减少
                    sc['free'] -= trademoney
                    sc['total'] = sc['free'] + sc['locked']
                    #
                    ast = Asset(self._platform, self._account, self._trader._assets, ts, True)
                    if self.cb.on_asset_update_callback:
                        await self.cb.on_asset_update_callback(ast)
            elif action == ORDER_ACTION_SELL: #卖
                bc = self._trader._assets[base_currency]
                sc = self._trader._assets[settlement_currency]
                if quantity > bc['free']:
                    return None, "账户币不足"
                #如果下单价格大于当前价格,那意味着无法成交,订单将进入订单薄挂着
                if price > self._last_kline.close_avg_fillna:
                    #订单通知
                    order_no = self.next_order_no()
                    o = {
                        "platform": self._platform,
                        "account": self._account,
                        "strategy": self._strategy,
                        "order_no": order_no,
                        "action": action,
                        "symbol": self._symbol,
                        "price": price,
                        "quantity": quantity,
                        "remain": quantity,
                        "status": ORDER_STATUS_SUBMITTED,
                        "order_type": order_type,
                        "ctime": ts,
                        "utime": ts
                        #avg_price
                        #trade_type
                    }
                    order = Order(**o)
                    self._orders[order_no] = order #进入订单簿
                    if self.cb.on_order_update_callback:
                        await self.cb.on_order_update_callback(order)
                    #账户资产通知
                    #'货'需要被锁定一部分
                    bc['locked'] += quantity #挂单部分所占用的'货'需要被锁定
                    bc['free'] = bc['total'] - bc['locked']
                    #
                    ast = Asset(self._platform, self._account, self._trader._assets, ts, True)
                    if self.cb.on_asset_update_callback:
                        await self.cb.on_asset_update_callback(ast)
                else: #直接成交
                    #收盘均价模拟成交价
                    tradeprice = self._last_kline.close_avg_fillna
                    trademoney = quantity*tradeprice
                    #对于现货交易,手续费是从接收币种里面扣除
                    fee = trademoney*self.taker_commission_rate
                    trademoney -= fee
                    #订单通知
                    order_no = self.next_order_no()
                    o = {
                        "platform": self._platform,
                        "account": self._account,
                        "strategy": self._strategy,
                        "order_no": order_no,
                        "action": action,
                        "symbol": self._symbol,
                        "price": price,
                        "quantity": quantity,
                        "remain": 0,
                        "status": ORDER_STATUS_FILLED,
                        "order_type": order_type,
                        "ctime": ts,
                        "utime": ts
                        #avg_price
                        #trade_type
                    }
                    order = Order(**o)
                    if self.cb.on_order_update_callback:
                        await self.cb.on_order_update_callback(order)
                    #成交通知
                    fill_no = self.next_fill_no()
                    f = {
                        "platform": self._platform,
                        "account": self._account,
                        "strategy": self._strategy,
                        "fill_no": fill_no,
                        "order_no": order_no,
                        "side": action, #成交方向,买还是卖
                        "symbol": self._symbol,
                        "price": tradeprice, #成交价格
                        "quantity": quantity, #成交数量
                        "liquidity": LIQUIDITY_TYPE_TAKER, #maker成交还是taker成交
                        "fee": fee,
                        "ctime": ts
                    }
                    fill = Fill(**f)
                    if self.cb.on_fill_update_callback:
                        await self.cb.on_fill_update_callback(fill)
                    #账户资产通知
                    #'货'减少
                    bc['free'] -= quantity
                    bc['total'] = bc['free'] + bc['locked']
                    #'钱'增加
                    sc['free'] += trademoney
                    sc['total'] = sc['free'] + sc['locked']
                    #
                    ast = Asset(self._platform, self._account, self._trader._assets, ts, True)
                    if self.cb.on_asset_update_callback:
                        await self.cb.on_asset_update_callback(ast)
        elif order_type == ORDER_TYPE_IOC:
            raise NotImplementedError
        #返回订单号
        return order_no, None

    async def revoke_order(self, *order_nos):
        """ 撤单
        """
        #如果传入order_nos为空，即撤销全部委托单
        if len(order_nos) == 0:
            order_nos = [o.order_no for o in self._orders.values()]
            if not order_nos:
                return [], None
        #如果传入order_nos为一个委托单号，那么只撤销一个委托单
        if len(order_nos) == 1:
            if self._orders.get(order_nos[0]):
                del self._orders[order_nos[0]]
                return order_nos[0], None
            else:
                return order_nos[0], "没有找到指定订单"
        #如果传入order_nos数量大于1，那么就批量撤销传入的委托单
        if len(order_nos) > 1:
            result = []
            for oid in order_nos:
                if self._orders.get(oid):
                    del self._orders[oid]
                    result.append((oid, None))
                else:
                    result.append((oid, "没有找到指定订单"))
            return result, None

    async def get_orders(self):
        """ 获取挂单列表
        """
        return list(self._orders.values()), None

    async def get_position(self):
        """ 获取当前仓位
        """
        raise NotImplementedError #现货模式不需要此功能

    async def get_symbol_info(self):
        """ 获取符号信息
        """
        info = config.backtest["feature"][self._platform]["syminfo"][self._symbol]
        price_tick = info["price_tick"]
        size_tick = info["size_tick"]
        size_limit = info["size_limit"]
        value_tick = info["value_tick"]
        value_limit = info["value_limit"]
        base_currency = info["base_currency"]
        quote_currency = info["quote_currency"]
        settlement_currency = info["settlement_currency"]
        syminfo = SymbolInfo(self._platform, self._symbol, price_tick, size_tick, size_limit, value_tick, value_limit, base_currency, quote_currency, settlement_currency)
        return syminfo, None

    async def invalid_indicate(self, indicate_type):
        """ 强制刷新指定回调函数
        """
        return False, None


class BacktestTrader(VirtualTrader):
    """ BacktestTrader module. You can initialize trader object with some attributes in kwargs.
    """

    def __init__(self, **kwargs):
        """Initialize."""
        self.cb = kwargs["cb"]

        self._platform = kwargs.get("databind")
        self._symbols = kwargs.get("symbols")
        self._strategy = kwargs.get("strategy")
        self._account = kwargs.get("account")

        state = None
        if not self._platform:
            state = State(self._platform, self._account, "param platform miss")
        elif not self._symbols:
            state = State(self._platform, self._account, "param symbols miss")
        elif not self._strategy:
            state = State(self._platform, self._account, "param strategy miss")
        if state:
            logger.error(state, caller=self)
            return

        #资产列表
        self._assets: DefaultDict[str: Dict[str, float]] = defaultdict(lambda: {k: 0.0 for k in {'free', 'locked', 'total'}})

        #替换k线回调函数(K线方式驱动回测引擎)
        self._original_on_kline_update_callback = self.cb.on_kline_update_callback
        self.cb.on_kline_update_callback = self.on_kline_update_callback

        #替换订单簿回调函数(订单薄方式驱动回测引擎)
        self._original_on_orderbook_update_callback = self.cb.on_orderbook_update_callback
        self.cb.on_orderbook_update_callback = self.on_orderbook_update_callback

        #替换市场成交回调函数(市场成交方式驱动回测引擎)
        self._original_on_trade_update_callback = self.cb.on_trade_update_callback
        self.cb.on_trade_update_callback = self.on_trade_update_callback

        #为每个交易对绑定回测撮合引擎
        self.bind_match_engine(**kwargs)

        super(BacktestTrader, self).__init__(**kwargs)

    def bind_match_engine(self, **kwargs):
        """ 为每个交易对绑定回测撮合引擎
        """
        self.match_engine_dict = {}
        for sym in self._symbols:
            symtype = config.backtest["feature"][self._platform]["syminfo"][sym]["type"]
            if symtype == "spot": #如果是现货就绑定现货撮合引擎
                self.match_engine_dict[sym] = SimpleSpotMatchEngine(sym, self, **kwargs)
            elif symtype == "future": #如果是合约就绑定合约撮合引擎
                #self.match_engine_dict[sym] = SimpleFutureMatchEngine(sym, self, **kwargs)
                pass

    async def on_kline_update_callback(self, kline: Kline):
        """ K线方式驱动回测引擎
        """
        #通过K线所属的交易对,找到对应的撮合引擎,并且驱动它
        match_engine = self.match_engine_dict[kline.symbol]
        await match_engine.on_kline_update_callback(kline)
        #调用原K线回调函数(上层策略)
        if self._original_on_kline_update_callback:
            await self._original_on_kline_update_callback(kline)

    async def on_orderbook_update_callback(self, orderbook: Orderbook):
        """ 订单薄方式驱动回测引擎
        """
        #通过订单薄所属的交易对,找到对应的撮合引擎,并且驱动它
        match_engine = self.match_engine_dict[orderbook.symbol]
        await match_engine.on_orderbook_update_callback(orderbook)
        #调用原订单薄回调函数(上层策略)
        if self._original_on_orderbook_update_callback:
            await self._original_on_orderbook_update_callback(orderbook)

    async def on_trade_update_callback(self, trade: Trade):
        """ 市场成交方式驱动回测引擎
        """
        #通过市场成交所属的交易对,找到对应的撮合引擎,并且驱动它
        match_engine = self.match_engine_dict[trade.symbol]
        await match_engine.on_trade_update_callback(trade)
        #调用原市场成交回调函数(上层策略)
        if self._original_on_trade_update_callback:
            await self._original_on_trade_update_callback(trade)

    async def init_asset(self):
        """ 读取回测配置信息中的初始化资产,通知上层策略
        """
        ts = ModelAPI.current_milli_timestamp()
        d = config.backtest["feature"][self._platform]["asset"]
        for (k, v) in d.items():
            self._assets[k]["free"] = float(v)
            self._assets[k]["total"] = float(v)
        #通知上层策略
        ast = Asset(self._platform, self._account, self._assets, ts, True)
        if self.cb.on_asset_update_callback:
            await self.cb.on_asset_update_callback(ast)

    async def launch(self):
        """ 模拟交易接口连接初始化成功
        """
        state = State(self._platform, self._account, "connect to server success", State.STATE_CODE_CONNECT_SUCCESS)
        await self.cb.on_state_update_callback(state)
        await self.init_asset()
        state = State(self._platform, self._account, "Environment ready", State.STATE_CODE_READY)
        await self.cb.on_state_update_callback(state)

    async def done(self):
        """ 回测完成
        """
        pass

    async def create_order(self, symbol, action, price, quantity, order_type=ORDER_TYPE_LIMIT, *args, **kwargs):
        """ Create an order.

        Args:
            symbol: Trade target
            action: Trade direction, `BUY` or `SELL`.
            price: Price of each contract.
            quantity: The buying or selling quantity.
            order_type: Order type, `MARKET` or `LIMIT`.

        Returns:
            order_no: Order ID if created successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        match_engine = self.match_engine_dict[symbol] #通过交易对符号找到相应回测撮合引擎
        return await match_engine.create_order(action, price, quantity, order_type)

    async def revoke_order(self, symbol, *order_nos):
        """ 撤销订单
        @param symbol 交易对
        @param order_nos 订单号列表，可传入任意多个，如果不传入，那么就撤销所有订单
        备注:关于批量删除订单函数返回值格式,如果函数调用失败了那肯定是return None, error
        如果函数调用成功,但是多个订单有成功有失败的情况,比如输入3个订单id,成功2个,失败1个,那么
        返回值统一都类似: 
        return [(成功订单ID, None),(成功订单ID, None),(失败订单ID, "失败原因")], None
        """
        match_engine = self.match_engine_dict[symbol] #通过交易对符号找到相应回测撮合引擎
        return await match_engine.revoke_order(*order_nos)

    async def get_assets(self):
        """ 获取交易账户资产信息

        Args:
            None

        Returns:
            assets: Asset if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        ts = ModelAPI.current_milli_timestamp()
        ast = Asset(self._platform, self._account, self._assets, ts, True)
        return ast, None

    async def get_orders(self, symbol):
        """ 获取当前挂单列表

        Args:
            symbol: Trade target

        Returns:
            orders: Order list if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        match_engine = self.match_engine_dict[symbol] #通过交易对符号找到相应回测撮合引擎
        return await match_engine.get_orders()

    async def get_position(self, symbol):
        """ 获取当前持仓

        Args:
            symbol: Trade target

        Returns:
            position: Position if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        match_engine = self.match_engine_dict[symbol] #通过交易对符号找到相应回测撮合引擎
        return await match_engine.get_position()

    async def get_symbol_info(self, symbol):
        """ 获取指定符号相关信息

        Args:
            symbol: Trade target

        Returns:
            symbol_info: SymbolInfo if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        match_engine = self.match_engine_dict[symbol] #通过交易对符号找到相应回测撮合引擎
        return await match_engine.get_symbol_info()

    async def invalid_indicate(self, symbol, indicate_type):
        """ update (an) callback function.

        Args:
            symbol: Trade target
            indicate_type: INDICATE_ORDER, INDICATE_ASSET, INDICATE_POSITION

        Returns:
            success: If execute successfully, return True, otherwise it's False.
            error: If execute failed, return error information, otherwise it's None.
        """
        match_engine = self.match_engine_dict[symbol] #通过交易对符号找到相应回测撮合引擎
        return await match_engine.invalid_indicate(indicate_type)

    @staticmethod
    def mapping_layer():
        """ 获取符号映射关系.
        Returns:
            layer: 符号映射关系
        """
        return None #回测模块不需要符号映射