# -*- coding:utf-8 -*-

"""
根据实际需要为数据库建立查询索引,加快数据库查询速度

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import sys
import time
import math
import asyncio

from collections import defaultdict

from quant import const
from quant.state import State
from quant.utils import tools, logger
from quant.utils.mongo import MongoDB
from quant.config import config
from quant.market import Market, Kline, Orderbook, Trade, Ticker
from quant.order import Order, Fill
from quant.position import Position
from quant.asset import Asset
from quant.tasks import LoopRunTask, SingleTask
from quant.gateway import ExchangeGateway
from quant.trader import Trader
from quant.strategy import Strategy
from quant.event import EventOrderbook, EventKline, EventTrade, EventTicker
from quant.startup import default_main


class CreateIndex(Strategy):

    def __init__(self):
        """ 初始化
        """
        super(CreateIndex, self).__init__()
        
        self.strategy = config.strategy
        self.platform = config.platforms[0]["platform"]
        self.symbols = config.markets[self.platform]["symbols"]

        #连接数据库
        self.t_trade_map = defaultdict(lambda:None)
        self.t_kline_map = defaultdict(lambda:None)
        if config.mongodb:
            for sym in self.symbols:
                postfix = sym.replace('-','').replace('_','').replace('/','').lower() #将所有可能的情况转换为我们自定义的数据库表名规则
                #逐笔成交
                name = "t_trade_{}_{}".format(self.platform, postfix).lower()
                self.t_trade_map[sym] = MongoDB("db_market", name)
                #K线
                name = "t_kline_{}_{}".format(self.platform, postfix).lower()
                self.t_kline_map[sym] = MongoDB("db_custom_kline", name)
        #开始任务
        SingleTask.run(self._do_work)

    async def _do_work(self):
        while not MongoDB.is_connected(): #等待数据库连接稳定
            await asyncio.sleep(1)
        for sym in self.symbols: #开始建立索引
            t_trade = self.t_trade_map[sym]
            s, e = await t_trade.create_index({'dt':1})
            if e:
                logger.error("create_index trade:", e, caller=self)
            t_kline = self.t_kline_map[sym]
            s, e = await t_kline.create_index({'begin_dt':1})
            if e:
                logger.error("create_index kline:", e, caller=self)
        #结束进程
        self.stop()

    async def on_state_update_callback(self, state: State, **kwargs): ...
    async def on_kline_update_callback(self, kline: Kline): ...
    async def on_orderbook_update_callback(self, orderbook: Orderbook): ...
    async def on_trade_update_callback(self, trade: Trade): ...
    async def on_ticker_update_callback(self, ticker: Ticker): ...
    async def on_order_update_callback(self, order: Order): ...
    async def on_fill_update_callback(self, fill: Fill): ...
    async def on_position_update_callback(self, position: Position): ...
    async def on_asset_update_callback(self, asset: Asset): ...


if __name__ == '__main__':
    default_main(CreateIndex)
