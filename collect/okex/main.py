# -*- coding:utf-8 -*-

"""
okex市场数据采集模块

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import sys

from quant import const
from quant.state import State
from quant.utils import tools, logger
from quant.config import config
from quant.market import Market, Kline, Orderbook, Trade, Ticker
from quant.order import Order, Fill
from quant.position import Position
from quant.asset import Asset
from quant.tasks import LoopRunTask
from quant.gateway import ExchangeGateway
from quant.trader import Trader
from quant.strategy import Strategy
from quant.event import EventOrderbook, EventKline, EventTrade, EventTicker


class Collect(Strategy):

    def __init__(self):
        """ 初始化
        """
        super(Collect, self).__init__()
        
        self.strategy = config.strategy
        self.platform = config.platforms[0]["platform"]
        self.symbols = config.markets[self.platform]["symbols"]
        # 接口参数
        params = {
            "strategy": self.strategy,
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
        self.trader = self.create_gateway(**params)

    async def on_state_update_callback(self, state: State, **kwargs):
        """ 状态变化(底层交易所接口,框架等)通知回调函数
        """
        logger.info("on_state_update_callback:", state, caller=self)

    async def on_kline_update_callback(self, kline: Kline):
        """ 市场K线更新
        """
        logger.info("kline:", kline, caller=self)
        kwargs = {
            "platform": kline.platform,
            "symbol": kline.symbol,
            "open": kline.open,
            "high": kline.high,
            "low": kline.low,
            "close": kline.close,
            "volume": kline.volume,
            "timestamp": kline.timestamp,
            "kline_type": kline.kline_type
        }
        EventKline(**kwargs).publish()

    async def on_orderbook_update_callback(self, orderbook: Orderbook):
        """ 订单薄更新
        """
        logger.info("orderbook:", orderbook, caller=self)
        kwargs = {
            "platform": orderbook.platform,
            "symbol": orderbook.symbol,
            "asks": orderbook.asks,
            "bids": orderbook.bids,
            "timestamp": orderbook.timestamp
        }
        EventOrderbook(**kwargs).publish()

    async def on_trade_update_callback(self, trade: Trade):
        """ 市场最新成交更新
        """
        logger.info("trade:", trade, caller=self)
        kwargs = {
            "platform": trade.platform,
            "symbol": trade.symbol,
            "action": trade.action,
            "price": trade.price,
            "quantity": trade.quantity,
            "timestamp": trade.timestamp
        }
        EventTrade(**kwargs).publish()

    async def on_ticker_update_callback(self, ticker: Ticker):
        """ 市场行情tick更新
        """
        logger.info("ticker:", ticker, caller=self)
        kwargs = {
            "platform": ticker.platform,
            "symbol": ticker.symbol,
            "ask": ticker.ask,
            "bid": ticker.bid,
            "last": ticker.last,
            "timestamp": ticker.timestamp
        }
        EventTicker(**kwargs).publish()

    async def on_order_update_callback(self, order: Order): ...
    async def on_fill_update_callback(self, fill: Fill): ...
    async def on_position_update_callback(self, position: Position): ...
    async def on_asset_update_callback(self, asset: Asset): ...


def main():
    if len(sys.argv) <= 1:
        logger.error("config.json miss")
        return
    config_file = sys.argv[1]
    if not config_file.lower().endswith("config.json"):
        logger.error("config.json miss")
        return

    from quant.quant import quant
    quant.initialize(config_file)
    Collect()
    quant.start()


if __name__ == '__main__':
    main()
