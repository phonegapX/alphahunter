# -*- coding:utf-8 -*-

"""
行情采集模块

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import sys

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
        self.gw = self.create_gateway(**params)
        #为数据库保存行情做准备
        self.t_orderbook_map = defaultdict(lambda:None)
        self.t_trade_map = defaultdict(lambda:None)
        self.t_kline_map = defaultdict(lambda:None)
        self.d_orderbook_map = defaultdict(lambda:[])
        self.d_trade_map = defaultdict(lambda:[])
        if config.mongodb:
            for sym in self.symbols:
                postfix = sym.replace('-','').replace('_','').replace('/','').lower() #将所有可能的情况转换为我们自定义的数据库表名规则
                #订单薄
                name = "t_orderbook_{}_{}".format(self.platform, postfix).lower()
                self.t_orderbook_map[sym] = MongoDB("db_market", name)
                #逐笔成交
                name = "t_trade_{}_{}".format(self.platform, postfix).lower()
                self.t_trade_map[sym] = MongoDB("db_market", name)
                #K线
                name = "t_kline_{}_{}".format(self.platform, postfix).lower()
                self.t_kline_map[sym] = MongoDB("db_market", name)

    async def on_state_update_callback(self, state: State, **kwargs):
        """ 状态变化(底层交易所接口,框架等)通知回调函数
        """
        logger.info("on_state_update_callback:", state, caller=self)

    async def on_kline_update_callback(self, kline: Kline):
        """ 市场K线更新
        """
        logger.info("kline:", kline, caller=self)
        #行情保存进数据库
        kwargs = {
            "open": kline.open,
            "high": kline.high,
            "low": kline.low,
            "close": kline.close,
            "volume": kline.volume,
            "begin_dt": kline.timestamp,
            "end_dt": kline.timestamp+60*1000-1
        }
        async def save(kwargs):
            t_kline = self.t_kline_map[kline.symbol]
            if t_kline:
                s, e = await t_kline.insert(kwargs)
                if e:
                    logger.error("insert kline:", e, caller=self)
        SingleTask.run(save, kwargs)
        #发布行情到消息队列
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
        #行情保存进数据库
        kwargs = {}
        i = 1
        for ask in orderbook.asks:
            kwargs[f'askprice{i}'] = ask[0]
            kwargs[f'asksize{i}'] = ask[1]
            i = i + 1
            if i > 20: break
        i = 1
        for bid in orderbook.bids:
            kwargs[f'bidprice{i}'] = bid[0]
            kwargs[f'bidsize{i}'] = bid[1]
            i = i + 1
            if i > 20: break
        kwargs["dt"] = orderbook.timestamp
        async def save(kwargs):
            #一秒内会有多次通知,将一秒内的通知都收集在一起,一次性写入数据库,约一秒写一次,提高数据库性能
            dlist = self.d_orderbook_map[orderbook.symbol]
            dlist.append(kwargs)
            if dlist[len(dlist)-1]["dt"]-dlist[0]["dt"] > 1000: #每秒写一次数据库
                #因为是异步并发,所以先清空列表,重新收集新的一秒内的所有通知,而不是等待数据库IO完成再清空(python的变量只是对象的引用)
                #xxx = copy.deepcopy(dlist)
                #dlist.clear()
                #insert xxx
                self.d_orderbook_map[orderbook.symbol] = []
                #写数据库
                t_orderbook = self.t_orderbook_map[orderbook.symbol]
                if t_orderbook:
                    s, e = await t_orderbook.insert(dlist)
                    if e:
                        logger.error("insert orderbook:", e, caller=self)
        SingleTask.run(save, kwargs)
        #发布行情到消息队列
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
        #行情保存进数据库
        kwargs = {
            "direction": trade.action,
            "tradeprice": trade.price,
            "volume": trade.quantity,
            "tradedt": trade.timestamp,
            "dt": tools.get_cur_timestamp_ms()
        }
        async def save(kwargs):
            #一秒内会有多次通知,将一秒内的通知都收集在一起,一次性写入数据库,约一秒写一次,提高数据库性能
            dlist = self.d_trade_map[trade.symbol]
            dlist.append(kwargs)
            if dlist[len(dlist)-1]["dt"]-dlist[0]["dt"] > 1000: #每秒写一次数据库
                #因为是异步并发,所以先清空列表,重新收集新的一秒内的所有通知,而不是等待数据库IO完成再清空(python的变量只是对象的引用)
                #xxx = copy.deepcopy(dlist)
                #dlist.clear()
                #insert xxx
                self.d_trade_map[trade.symbol] = []
                #写数据库
                t_trade = self.t_trade_map[trade.symbol]
                if t_trade:
                    s, e = await t_trade.insert(dlist)
                    if e:
                        logger.error("insert trade:", e, caller=self)
        SingleTask.run(save, kwargs)
        #发布行情到消息队列
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
