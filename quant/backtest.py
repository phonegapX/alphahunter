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

from quant.gateway import ExchangeGateway
from quant.state import State
from quant.order import Order, Fill, SymbolInfo
from quant.tasks import SingleTask, LoopRunTask
from quant.position import Position
from quant.asset import Asset
from quant.const import MARKET_TYPE_KLINE, MARKET_TYPE_KLINE_5M
from quant.utils import tools, logger
from quant.utils.decorator import async_method_locker
from quant.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL
from quant.order import ORDER_TYPE_LIMIT, ORDER_TYPE_MARKET
from quant.order import LIQUIDITY_TYPE_MAKER, LIQUIDITY_TYPE_TAKER
from quant.order import ORDER_STATUS_SUBMITTED, ORDER_STATUS_PARTIAL_FILLED, ORDER_STATUS_FILLED, ORDER_STATUS_CANCELED, ORDER_STATUS_FAILED
from quant.market import Kline, Orderbook, Trade, Ticker
from quant.history import HistoryAdapter
from quant.infra_api import InfraAPI


__all__ = ("BacktestTrader",)


class BacktestTrader(HistoryAdapter, ExchangeGateway):
    """ BacktestTrader module. You can initialize trader object with some attributes in kwargs.
    """

    def __init__(self, **kwargs):
        """Initialize."""
        self.cb = kwargs["cb"]
        state = None

        self._platform = kwargs.get("databind")
        self._symbols = kwargs.get("symbols")
        self._strategy = kwargs.get("strategy")
        self._account = kwargs.get("account")

        if not self._platform:
            state = State(self._platform, self._account, "param platform miss")
        elif not self._symbols:
            state = State(self._platform, self._account, "param symbols miss")
        elif not self._strategy:
            state = State(self._platform, self._account, "param strategy miss")

        super(BacktestTrader, self).__init__(**kwargs)

    async def load_data(self, drive_type, begin_time, end_time):
        """ 从数据库中读取历史数据
        """
        try:
            if drive_type == "kline":
                pd_list = []
                for symbol in self._symbols:
                    r = await InfraAPI.get_klines_between(self._platform, symbol, begin_time, end_time)
                    if r:
                        #1.将r转换成pandas
                        #2.然后添加3列,一列为drive_type,一列为symbol,一列为当前类的self值,然后将从begin_dt这一列复制一个新列,名字叫做dt,方便以后统一排序
                        #3.pd_list.append(pandas)
                        df = pd.DataFrame(r)
                        df["drive_type"] = drive_type
                        df["symbol"] = symbol
                        df["gw"] = self
                        df["dt"] = df["begin_dt"]
                        del df["_id"]
                        pd_list.append(df)
                #将pd_list的所有pandas按行合并成一个大的pandas
                #然后return这个大的pandas
                if pd_list:
                    return pd.concat(pd_list)
                else:
                    return pd.DataFrame()
            elif drive_type == "trade":
                pd_list = []
                for symbol in self._symbols:
                    r = await InfraAPI.get_trades_between(self._platform, symbol, begin_time, end_time)
                    if r:
                        #1.将r转换成pandas
                        #2.然后添加3列,一列为drive_type,一列为symbol,一列为当前类的self值
                        #3.pd_list.append(pandas)
                        df = pd.DataFrame(r)
                        df["drive_type"] = drive_type
                        df["symbol"] = symbol
                        df["gw"] = self
                        del df["_id"]
                        pd_list.append(df)
                #将pd_list的所有pandas按行合并成一个大的pandas
                #然后return这个大的pandas
                if pd_list:
                    return pd.concat(pd_list)
                else:
                    return pd.DataFrame()
            elif drive_type == "orderbook":
                pd_list = []
                for symbol in self._symbols:
                    r = await InfraAPI.get_orderbooks_between(self._platform, symbol, begin_time, end_time)
                    if r:
                        #1.将r转换成pandas
                        #2.然后添加3列,一列为drive_type,一列为symbol,一列为当前类的self值
                        #3.pd_list.append(pandas)
                        df = pd.DataFrame(r)
                        df["drive_type"] = drive_type
                        df["symbol"] = symbol
                        df["gw"] = self
                        del df["_id"]
                        pd_list.append(df)
                #将pd_list的所有pandas按行合并成一个大的pandas
                #然后return这个大的pandas
                if pd_list:
                    return pd.concat(pd_list)
                else:
                    return pd.DataFrame()
        except Exception as e:
            return pd.DataFrame() #发生异常就返回空df

    async def feed(self, row):
        """ 通过历史数据驱动策略进行回测
        """
        drive_type = row["drive_type"] #数据驱动方式
        if drive_type == "kline" and self.cb.on_kline_update_callback:
            row = row.dropna()
            kw = row.to_dict()
            del kw["drive_type"]
            del kw["gw"]
            del kw["dt"]
            kw["platform"] = self._platform
            kw["timestamp"] = int(kw["begin_dt"])
            kw["kline_type"] = MARKET_TYPE_KLINE
            kline = Kline(**kw)
            await self.cb.on_kline_update_callback(kline)
        elif drive_type == "trade" and self.cb.on_trade_update_callback:
            kw = {
                "platform": self._platform,
                "symbol": row["symbol"],
                "action": row["direction"],
                "price": row["tradeprice"],
                "quantity": row["volume"],
                "timestamp": int(row["tradedt"])
            }
            trade = Trade(**kw)
            await self.cb.on_trade_update_callback(trade)
        elif drive_type == "orderbook" and self.cb.on_orderbook_update_callback:
            asks = []
            bids = []
            for i in range(1, 20+1):
                asks.append([row[f'askprice{i}'], row[f'asksize{i}']])
                bids.append([row[f'bidprice{i}'], row[f'bidsize{i}']])
            kw = {
                "platform": self._platform,
                "symbol": row["symbol"],
                "asks": asks,
                "bids": bids,
                "timestamp": int(row["pubdt"])
            }
            ob = Orderbook(**kw)
            await self.cb.on_orderbook_update_callback(ob)

    async def launch(self):
        """ 模拟交易接口连接初始化成功
        """
        state = State(self._platform, self._account, "connect to server success", State.STATE_CODE_CONNECT_SUCCESS)
        await self.cb.on_state_update_callback(state)
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

    async def revoke_order(self, symbol, *order_nos):
        """ Revoke (an) order(s).

        Args:
            symbol: Trade target
            order_nos: Order id list, you can set this param to 0 or multiple items. If you set 0 param, you can cancel all orders for 
            this symbol. If you set 1 or multiple param, you can cancel an or multiple order.

        Returns:
            删除全部订单情况: 成功=(True, None), 失败=(False, error information)
            删除单个或多个订单情况: (删除成功的订单id[], 删除失败的订单id及错误信息[]),比如删除三个都成功那么结果为([1xx,2xx,3xx], [])
        """

    async def get_assets(self):
        """ 获取交易账户资产信息

        Args:
            None

        Returns:
            assets: Asset if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """

    async def get_orders(self, symbol):
        """ 获取当前挂单列表

        Args:
            symbol: Trade target

        Returns:
            orders: Order list if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """

    async def get_position(self, symbol):
        """ 获取当前持仓

        Args:
            symbol: Trade target

        Returns:
            position: Position if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """

    async def get_symbol_info(self, symbol):
        """ 获取指定符号相关信息

        Args:
            symbol: Trade target

        Returns:
            symbol_info: SymbolInfo if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """

    async def invalid_indicate(self, symbol, indicate_type):
        """ update (an) callback function.

        Args:
            symbol: Trade target
            indicate_type: INDICATE_ORDER, INDICATE_ASSET, INDICATE_POSITION

        Returns:
            success: If execute successfully, return True, otherwise it's False.
            error: If execute failed, return error information, otherwise it's None.
        """