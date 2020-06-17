# -*- coding:utf-8 -*-

"""
交易所接口基类

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

from abc import ABCMeta, abstractmethod
from six import with_metaclass

from quant.market import Kline, Orderbook, Trade, Ticker
from quant.asset import Asset
from quant.position import Position
from quant.order import Order, Fill, ORDER_TYPE_LIMIT
from quant.state import State


class ExchangeGateway(with_metaclass(ABCMeta)):
    """
    交易所接口基类
    """

    @abstractmethod
    async def get_orders(self, symbol):
        """ 获取当前挂单列表

        Args:
            symbol: Trade target

        Returns:
            orders: Order list if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
    
    @abstractmethod
    async def get_assets(self):
        """ 获取交易账户资产信息

        Args:
            None

        Returns:
            assets: Asset if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """

    @abstractmethod
    async def get_position(self, symbol):
        """ 获取当前持仓

        Args:
            symbol: Trade target

        Returns:
            position: Position if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """

    @abstractmethod
    async def get_symbol_info(self, symbol):
        """ 获取指定符号相关信息

        Args:
            symbol: Trade target

        Returns:
            symbol_info: SymbolInfo if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """

    @abstractmethod
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
    
    @abstractmethod
    async def revoke_order(self, symbol, *order_nos):
        """ Revoke (an) order(s).

        Args:
            symbol: Trade target
            order_nos: Order id list, you can set this param to 0 or multiple items. If you set 0 param, you can cancel
                all orders for this symbol(initialized in Trade object). If you set 1 param, you can cancel an order.
                If you set multiple param, you can cancel multiple orders. Do not set param length more than 100.

        Returns:
            success: If execute successfully, return success information, otherwise it's None.
            error: If execute failed, return error information, otherwise it's None.
        """

    @abstractmethod
    async def invalid_indicate(self, symbol, indicate_type):
        """ update (an) callback function.

        Args:
            symbol: Trade target
            indicate_type: INDICATE_ORDER, INDICATE_ASSET, INDICATE_POSITION

        Returns:
            success: If execute successfully, return True, otherwise it's False.
            error: If execute failed, return error information, otherwise it's None.
        """

    @staticmethod
    def mapping_layer(self):
        """ 获取符号映射关系.
        Returns:
            layer: 符号映射关系
        """


    class ICallBack(with_metaclass(ABCMeta)):
        """
        交易所信息通知回调接口
        """
        
        @abstractmethod
        async def on_kline_update_callback(self, kline: Kline):
            """
            市场公共数据: K线数据 (也可以通过采集Ticker数据计算组合出来)
            """
        
        @abstractmethod
        async def on_orderbook_update_callback(self, orderbook: Orderbook):
            """
            市场公共数据: 订单簿,深度数据,市场的买卖挂单深度
            """
        
        @abstractmethod
        async def on_trade_update_callback(self, trade: Trade):
            """
            市场公共数据: 市场成交列表,最新成交价本质就是最后一笔成交的价格
            """
        
        @abstractmethod
        async def on_ticker_update_callback(self, ticker: Ticker):
            """
            市场公共数据: 市场最新tick行情
            """
        
        #=====================================================================
        
        @abstractmethod
        async def on_asset_update_callback(self, asset: Asset): 
            """
            用户私有数据: 账户资产数据更新
            """
        
        @abstractmethod
        async def on_position_update_callback(self, position: Position): 
            """
            用户私有数据: 用户持仓更新
            """

        @abstractmethod
        async def on_order_update_callback(self, order: Order): 
            """
            用户私有数据: 用户挂单更新
            """

        @abstractmethod
        async def on_fill_update_callback(self, fill: Fill): 
            """
            用户私有数据: 用户挂单成交更新
            """
        
        #=====================================================================
        
        @abstractmethod
        async def on_state_update_callback(self, state: State, **kwargs): 
            """
            状态变化(底层交易所接口,框架等)通知回调函数
            """