# -*- coding:utf-8 -*-

"""
DataMatrix样例演示

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
from quant.tasks import LoopRunTask
from quant.trader import Trader
from quant.strategy import Strategy
from quant.utils.decorator import async_method_locker
from quant.order import Order, Fill, ORDER_ACTION_BUY, ORDER_ACTION_SELL, ORDER_STATUS_FILLED, ORDER_TYPE_MARKET
from quant.position import Position
from quant.asset import Asset
from quant.startup import default_main
from quant.interface.datamatrix_api import DataMatrixAPI


class DataMatrixDemo(Strategy):

    def __init__(self):
        """ 初始化
        """
        super(DataMatrixDemo, self).__init__()

        self.platform = config.platforms[0]["platform"] #交易所
        self.symbols = config.platforms[0]["symbols"]
        # 交易模块参数
        params = {
            "strategy": config.strategy,
            "platform": self.platform,
            "symbols": self.symbols,

            "enable_kline_update": True,
            "enable_orderbook_update": True,
            "enable_trade_update": True,
            "enable_ticker_update": True,
            "enable_order_update": False,
            "enable_fill_update": False,
            "enable_position_update": False,
            "enable_asset_update": False,

            "direct_kline_update": True,
            "direct_orderbook_update": True,
            "direct_trade_update": True,
            "direct_ticker_update": True
        }
        self.gw = self.create_gateway(**params)

    async def on_state_update_callback(self, state: State, **kwargs):
        """ 状态变化(底层交易所接口,框架等)通知回调函数
        """
        logger.info("on_state_update_callback:", state, caller=self)

    async def on_kline_update_callback(self, kline: Kline):
        """ 市场K线更新
        """
        logger.info("kline:", kline, caller=self)
        #ts = DataMatrixAPI.current_milli_timestamp()
        #r = await DataMatrixAPI.get_klines_between(self.platform, self.symbols[0], ts, ts+10*60*1000)
        #print(r)
        # add some logic and calculations here.
        # await add_row(Row)

    async def on_orderbook_update_callback(self, orderbook: Orderbook):
        """ 订单薄更新
        """
        logger.info("orderbook:", orderbook, caller=self)
        # add some logic and calculations here.
        # await add_row(Row)

    async def on_trade_update_callback(self, trade: Trade):
        """ 市场最新成交更新
        """
        logger.info("trade:", trade, caller=self)
        # add some logic and calculations here.
        # await add_row(Row)

    async def on_ticker_update_callback(self, ticker: Ticker):
        """ 市场行情tick更新
        """
        logger.info("ticker:", ticker, caller=self)
        # add some logic and calculations here.
        # await add_row(Row)

    async def add_row(self, Row):
        # add this Row to an existing csv file
        pass

    async def on_order_update_callback(self, order: Order): ...
    async def on_fill_update_callback(self, fill: Fill): ...
    async def on_position_update_callback(self, position: Position): ...
    async def on_asset_update_callback(self, asset: Asset): ...


if __name__ == '__main__':
    default_main(DataMatrixDemo)
