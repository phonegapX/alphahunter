# -*- coding:utf-8 -*-

"""
Trader Module.

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import copy

from quant import const
from quant.error import Error
from quant.utils import logger
from quant.tasks import SingleTask
from quant.order import Order, ORDER_TYPE_LIMIT
from quant.position import Position
from quant.event import EventOrder
from quant.gateway import ExchangeGateway
from quant.market import Kline, Orderbook, Trade
from quant.asset import Asset


def gateway_class(platform):
    """获取对应的交易网关类
    
    Args:
        platform: 交易平台名称
    
    Returns:
        交易网关类
    """
    if platform == const.DATAMATRIX:
        from quant.datamatrix import DataMatrixTrader as T
        return T
    elif platform == const.BACKTEST:
        from quant.backtest import BacktestTrader as T
        return T
    elif platform == const.OKEX:
        from quant.platform.okex import OKExTrader as T
        return T
    elif platform == const.OKEX_MARGIN:
        from quant.platform.okex_margin import OKExMarginTrader as T
        return T
    elif platform == const.OKEX_FUTURE:
        from quant.platform.okex_future import OKExFutureTrader as T
        return T
    elif platform == const.OKEX_SWAP:
        from quant.platform.okex_swap import OKExSwapTrader as T
        return T
    elif platform == const.BITMEX:
        from quant.platform.bitmex import BitmexTrader as T
        return T
    elif platform == const.BINANCE:
        from quant.platform.binance import BinanceTrader as T
        return T
    elif platform == const.BINANCE_FUTURE:
        from quant.platform.binance_future import BinanceFutureTrader as T
        return T
    elif platform == const.HUOBI:
        from quant.platform.huobi import HuobiTrader as T
        return T
    elif platform == const.HUOBI_FUTURE:
        from quant.platform.huobi_future import HuobiFutureTrader as T
        return T
    elif platform == const.GATE:
        from quant.platform.gate import GateTrader as T
        return T
    elif platform == const.FTX:
        from quant.platform.ftx import FTXTrader as T
        return T
    else:
        return None


class Trader(ExchangeGateway):
    """ Trader Module.
    """

    def __init__(self, **kwargs):
        """initialize trader object.
        
        Args:
            strategy: 策略名称,由哪个策略发起
            platform: 交易平台
            databind: 这个字段只有在platform等于datamatrix或backtest的时候才有用,代表为矩阵操作或策略回测提供历史数据的交易所
            symbols: 策略需要订阅和交易的币种
            account: 交易所登陆账号,如果为空就只是订阅市场公共行情数据,不进行登录认证,所以也无法进行交易等
            access_key: 登录令牌
            secret_key: 令牌密钥
            cb: ExchangeGateway.ICallBack {
                on_init_success_callback: `初始化是否成功`回调通知函数
                on_kline_update_callback: `K线数据`回调通知函数 (值为None就不启用此通知回调)
                on_orderbook_update_callback: `订单簿深度数据`回调通知函数 (值为None就不启用此通知回调)
                on_trade_update_callback: `市场最新成交`回调通知函数 (值为None就不启用此通知回调)
                on_ticker_update_callback: `市场行情tick`回调通知函数 (值为None就不启用此通知回调)
                on_order_update_callback: `用户挂单`回调通知函数 (值为None就不启用此通知回调)
                on_fill_update_callback: `用户挂单成交`回调通知函数 (值为None就不启用此通知回调)
                on_position_update_callback: `用户持仓`回调通知函数 (值为None就不启用此通知回调)
                on_asset_update_callback: `用户资产`回调通知函数 (值为None就不启用此通知回调)
            }
        """
        T = gateway_class(kwargs["platform"])
        if T == None:
            logger.error("platform not found:", kwargs["platform"], caller=self)
            cb = kwargs["cb"]
            SingleTask.run(cb.on_init_success_callback, False, Error("platform not found"))
            return
        self._t = T(**kwargs)

    @property
    def rest_api(self):
        return self._t.rest_api

    async def get_orders(self, symbol):
        """ 获取当前挂单列表

        Args:
            symbol: Trade target

        Returns:
            orders: Order list if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        return await self._t.get_orders(symbol)
    
    async def get_assets(self):
        """ 获取交易账户资产信息

        Args:
            None

        Returns:
            assets: Asset if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        return await self._t.get_assets()
    
    async def get_position(self, symbol):
        """ 获取当前持仓

        Args:
            symbol: Trade target

        Returns:
            position: Position if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        return await self._t.get_position(symbol)

    async def get_symbol_info(self, symbol):
        """ 获取指定符号相关信息

        Args:
            symbol: Trade target

        Returns:
            symbol_info: SymbolInfo if successfully, otherwise it's None.
            error: Error information, otherwise it's None.
        """
        return await self._t.get_symbol_info(symbol)

    async def create_order(self, symbol, action, price, quantity, order_type=ORDER_TYPE_LIMIT, **kwargs):
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
        order_no, error = await self._t.create_order(symbol, action, price, quantity, order_type, **kwargs)
        return order_no, error

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
        success, error = await self._t.revoke_order(symbol, *order_nos)
        return success, error