# -*- coding:utf-8 -*-

"""
策略对应的相关资产,仓位,订单等数据统一管理.

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import hashlib
from typing import DefaultDict, Dict
from collections import defaultdict

from quant.asset import Asset
from quant.position import Position
from quant.order import Order, Fill


def sha256(s):
    """
    """
    x = hashlib.sha256()
    x.update(s.encode())
    return x.hexdigest()


class PortfolioManager(object):
    """策略数据管理器,比如资产,仓位,订单,成交等
    """
    
    def __init__(self):
        """ 初始化
        """
        self._assets: DefaultDict[str, Asset] = defaultdict(Asset)
        self._positions: DefaultDict[str, Position] = defaultdict(Position)
        self._orders: DefaultDict[str, Dict[str, Order]] = defaultdict(dict) #两级字典
        #self._orders: DefaultDict[str, DefaultDict[str, Order]] = defaultdict(lambda:defaultdict(Order)) #两级字典
        self._fills: DefaultDict[str, Dict[str, Dict[str, Fill]]] = defaultdict(dict) #三级字典
        #self._fills: DefaultDict[str, DefaultDict[str, DefaultDict[str, Fill]]] = defaultdict(lambda:defaultdict(lambda:defaultdict(Fill))) #三级字典

    def on_asset_update(self, asset: Asset):
        """资产变化
        """
        key = sha256(asset.platform + asset.account)
        self._assets[key] = asset

    def on_position_update(self, position: Position):
        """仓位变化
        """
        key = sha256(position.platform + position.account + position.symbol)
        self._positions[key] = position

    def on_order_update(self, order: Order):
        """订单变化
        """
        key = sha256(order.platform + order.account + order.symbol)
        self._orders[key][order.order_no] = order

    def on_fill_update(self, fill: Fill):
        """订单成交
        """
        key = sha256(fill.platform + fill.account + fill.symbol)
        if not self._fills[key].get(fill.order_no):
            self._fills[key][fill.order_no] = {}
        self._fills[key][fill.order_no][fill.fill_no] = fill
        
    def get_asset(platform, account) -> Asset:
        """从本地获取账户的资产信息
        """
        key = sha256(platform + account)
        return self._assets[key]
        
    def get_position(platform, account, symbol) -> Position:
        """从本地获取指定的持仓信息
        """
        key = sha256(platform + account + symbol)
        return self._positions[key]
    
    def get_order(platform, account, symbol, order_no) -> Order:
        """从本地获取指定的挂单信息
        """
        key = sha256(platform + account + symbol)
        return self._orders[key][order_no]
    
    def get_orders(platform, account, symbol):
        """从本地获取某个符号下的所有挂单
        """
        key = sha256(platform + account + symbol)
        d = self._orders[key]
        for v in d.values():
            yield v

    def get_fills_by_order_no(platform, account, symbol, order_no) -> DefaultDict[str, Fill]:
        """从本地获取指定挂单的所有成交
        """
        key = sha256(platform + account + symbol)
        return self._fills[key][order_no]

    def get_fills_by_symbol(platform, account, symbol):
        """从本地获取指定符号的所有成交
        """
        key = sha256(platform + account + symbol)
        d = self._fills[key]
        for v in d.values(): #v是一个dict,包含了某一个订单的所有成交(一个订单可以包含多个成交)
            for vv in v.values():
                yield vv