# -*- coding:utf-8 -*-

"""
策略回测演示

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import sys
import asyncio

from quant import const
from quant.state import State
from quant.utils import tools, logger
from quant.config import config
from quant.market import Market, Kline, Orderbook, Trade, Ticker
from quant.order import Order, Fill, ORDER_ACTION_BUY, ORDER_ACTION_SELL, ORDER_STATUS_FILLED, ORDER_TYPE_MARKET
from quant.position import Position
from quant.asset import Asset
from quant.tasks import LoopRunTask
from quant.gateway import ExchangeGateway
from quant.trader import Trader
from quant.strategy import Strategy
from quant.utils.decorator import async_method_locker
from quant.startup import default_main
from quant.interface.model_api import ModelAPI


class DemoStrategy(Strategy):

    def __init__(self):
        """ 初始化
        """
        super(DemoStrategy, self).__init__()
        
        #=====================================================
        #创建第一个交易接口
        platform = config.platforms[0]["platform"] #交易所
        account = config.platforms[0]["account"] #此参数回测盘不用,不过为了统一都写上不影响
        access_key = config.platforms[0]["access_key"]  #此参数回测盘不用,不过为了统一都写上不影响
        secret_key = config.platforms[0]["secret_key"]  #此参数回测盘不用,不过为了统一都写上不影响
        symbols = config.platforms[0]["symbols"]
        # 交易模块参数
        params = {
            "strategy": config.strategy,
            "platform": platform,
            "symbols": symbols,
            "account": account,
            "access_key": access_key,
            "secret_key": secret_key,

            "enable_kline_update": True,
            "enable_orderbook_update": True,
            "enable_trade_update": True,
            "enable_ticker_update": True,
            "enable_order_update": True,
            "enable_fill_update": True,
            "enable_position_update": True,
            "enable_asset_update": True,

            "direct_kline_update": False,
            "direct_orderbook_update": False,
            "direct_trade_update": False,
            "direct_ticker_update": False
        }
        self.gw = self.create_gateway(**params)
        
        #=====================================================
        #创建第二个交易接口
        #platform = config.platforms[1]["platform"] #交易所
        #account = config.platforms[1]["account"] #此参数回测盘不用,不过为了统一都写上不影响
        #access_key = config.platforms[1]["access_key"] #此参数回测盘不用,不过为了统一都写上不影响
        #secret_key = config.platforms[1]["secret_key"] #此参数回测盘不用,不过为了统一都写上不影响
        #symbols = config.platforms[1]["symbols"]
        # 交易模块参数
        #params = {
        #    "strategy": config.strategy,
        #    "platform": platform,
        #    "symbols": symbols,
        #    "account": account,
        #    "access_key": access_key,
        #    "secret_key": secret_key,

        #    "enable_kline_update": True,
        #    "enable_orderbook_update": True,
        #    "enable_trade_update": True,
        #    "enable_ticker_update": True,
        #    "enable_order_update": True,
        #    "enable_fill_update": True,
        #    "enable_position_update": True,
        #    "enable_asset_update": True,

        #    "direct_kline_update": False,
        #    "direct_orderbook_update": False,
        #    "direct_trade_update": False,
        #    "direct_ticker_update": False
        #}
        #self.gw1 = self.create_gateway(**params)

        # 注册定时器
        #self.enable_timer()  # 每隔1秒执行一次回调

    async def on_time(self):
        """ 每秒钟执行一次. 因为是异步并发架构,这个函数执行的时候交易通道链接不一定已经建立好
        """
        logger.info("on_time ...", caller=self)

    async def on_state_update_callback(self, state: State, **kwargs):
        """ 状态变化(底层交易所接口,框架等)通知回调函数
        """
        logger.info("on_state_update_callback:", state, caller=self)

    async def on_kline_update_callback(self, kline: Kline):
        """ 市场K线更新
        """
        logger.info("kline:", kline, caller=self)
        x = ModelAPI.current_datetime()
        print(x)
        x = ModelAPI.current_milli_timestamp()
        print(x)
        x = ModelAPI.today()
        print(x)

    async def on_orderbook_update_callback(self, orderbook: Orderbook):
        """ 订单薄更新
        """
        logger.info("orderbook:", orderbook, caller=self)

    async def on_trade_update_callback(self, trade: Trade):
        """ 市场最新成交更新
        """
        logger.info("trade:", trade, caller=self)

    async def on_ticker_update_callback(self, ticker: Ticker):
        """ 市场行情tick更新
        """
        logger.info("ticker:", ticker, caller=self)

    async def on_order_update_callback(self, order: Order):
        """ 订单状态更新
        """
        logger.info("order:", order, caller=self)

    async def on_fill_update_callback(self, fill: Fill):
        """ 订单成交通知
        """
        logger.info("fill:", fill, caller=self)

    async def on_position_update_callback(self, position: Position):
        """ 持仓更新
        """
        logger.info("position:", position, caller=self)

    async def on_asset_update_callback(self, asset: Asset):
        """ 账户资产更新
        """
        logger.info("asset:", asset, caller=self)


if __name__ == '__main__':
    default_main(DemoStrategy)
