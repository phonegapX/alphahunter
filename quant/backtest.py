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
from quant.feed import HistoryDataFeed


__all__ = ("BacktestTrader",)


class BacktestTrader(HistoryDataFeed, ExchangeGateway):
    """ BacktestTrader module. You can initialize trader object with some attributes in kwargs.
    """

    def __init__(self, **kwargs):
        """Initialize."""
        self.cb = kwargs["cb"]
        state = None
        if not kwargs.get("strategy"):
            state = State("param strategy miss")
        elif not kwargs.get("symbols"):
            state = State("param symbols miss")
        elif not kwargs.get("platform"):
            state = State("param platform miss")
        elif not kwargs.get("databind"):
            state = State("param databind miss")
            
        if state:
            logger.error(state, caller=self)
            SingleTask.run(self.cb.on_state_update_callback, state)
            return

        self._strategy = kwargs["strategy"]
        self._databind = kwargs["databind"]
        self._symbols = kwargs["symbols"]

        super(BacktestTrader, self).__init__(**kwargs)
      
        

    
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
