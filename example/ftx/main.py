# -*- coding:utf-8 -*-

"""
FTX 模块使用演示

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


class DemoStrategy(Strategy):

    def __init__(self):
        """ 初始化
        """
        super(DemoStrategy, self).__init__()
        
        self.strategy = config.strategy
        
        #=====================================================
        #创建第一个交易接口
        self.platform = config.accounts[0]["platform"]
        self.account = config.accounts[0]["account"]
        self.access_key = config.accounts[0]["access_key"]
        self.secret_key = config.accounts[0]["secret_key"]
        target = config.markets[self.platform]
        self.symbols = target["symbols"]
        # 交易模块参数
        params = {
            "strategy": self.strategy,
            "platform": self.platform,
            "symbols": self.symbols,
            "account": self.account,
            "access_key": self.access_key,
            "secret_key": self.secret_key,

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
        #self.trader = self.create_gateway(**params)
        
        #=====================================================
        #创建第二个交易接口
        self.platform1 = config.accounts[1]["platform"]
        self.account1 = config.accounts[1]["account"]
        self.access_key1 = config.accounts[1]["access_key"]
        self.secret_key1 = config.accounts[1]["secret_key"]
        self.subaccount_name1 = config.accounts[1]["subaccount_name"]
        target = config.markets[self.platform1]
        self.symbols1 = target["symbols"]
        # 交易模块参数
        params1 = {
            "strategy": self.strategy,
            "platform": self.platform1,
            "symbols": self.symbols1,
            "account": self.account1,
            "access_key": self.access_key1,
            "secret_key": self.secret_key1,
            "subaccount_name": self.subaccount_name1,

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
        self.trader1 = self.create_gateway(**params1)

        # 注册定时器
        self.enable_timer()  # 每隔1秒执行一次回调

    async def on_time(self):
        """ 每秒钟执行一次. 因为是异步并发架构,这个函数执行的时候交易通道链接不一定已经建立好
        """
        if not hasattr(self, "just_once"):
            self.just_once = 1
            #xx = self.get_orders(self.trader, "ETH-PERP")
            xx = self.get_position(self.trader1, "ETH-PERP")
            #xx = self.get_assets(self.trader)
            #xx = self.create_order(self.trader1, "ETH-PERP", ORDER_ACTION_SELL, "51", "-0.002")
            #xx = self.create_order(self.trader1, "ETH-PERP", ORDER_ACTION_SELL, "0", "-0.002", ORDER_TYPE_MARKET)
            #xx = self.revoke_order(self.trader, "ETH-PERP", "1017521392")
            #order1 = Strategy.TOrder(self.trader, "ETH-PERP", ORDER_ACTION_SELL, "351", "-0.02")
            #order2 = Strategy.TOrder(self.trader1, "ETH-PERP", ORDER_ACTION_SELL, "352", "-0.03")
            #xx = self.create_pair_order(order1, order2)
            #xx = self.get_symbol_info(self.trader, "ETH-PERP")
            yy, zz = await xx
        
        logger.info("on_time ...", caller=self)
        #new_price = tools.float_to_str(price)  # 将价格转换为字符串，保持精度

    async def on_state_update_callback(self, state: State, **kwargs):
        """ 状态变化(底层交易所接口,框架等)通知回调函数
        """
        logger.info("on_state_update_callback:", state, caller=self)

    async def on_kline_update_callback(self, kline: Kline):
        """ 市场K线更新
        """
        logger.info("kline:", kline, caller=self)

    @async_method_locker("DemoStrategy.can_do_open_close_pos_demo.locker", False)
    async def can_do_open_close_pos_demo(self):
        """
        开平仓逻辑应该独立放到一个函数里面,并且加上'不等待类型的锁',就像本函数演示的这样.
        因为为了最大的时效性,框架采用的是异步架构,假如这里还在处理过程中,新的通知回调来了,那样就会
        引起重复开平仓,所以就把开平仓的过程加上'不等待类型的锁',这样新的通知回调来了,这里又被调用的情况下,
        因为有'不等待类型的锁',所以会直接跳过(忽略)本函数,这样就不会导致重复执行开平仓的动作.
        记住这里是'不等待类型的锁'(装饰器第二个参数为False),而不是`等待类型的锁`,因为我们不需要等待,假如等待的话还是会重复开平仓(而且行情也过期了)
        比如下面模拟要处理3秒,现实中是有可能发生的,比如网络或者交易所繁忙的时候.
        """
        await asyncio.sleep(3)

    async def on_orderbook_update_callback(self, orderbook: Orderbook):
        """ 订单薄更新
        """
        logger.info("orderbook:", orderbook, caller=self)
        #ask1_price = float(orderbook.asks[0][0])  # 卖一价格
        #bid1_price = float(orderbook.bids[0][0])  # 买一价格
        #self.current_price = (ask1_price + bid1_price) / 2  # 为了方便，这里假设盘口价格为 卖一 和 买一 的平均值
        """
        假设策略在本回调函数里面判断开平仓条件,并且条件达到可以进行开平仓的情况下,最好是把接下来的开平仓逻辑单独
        放在一个函数里面,并且加上'不等待类型的锁',比如下面这个函数这样.
        """
        #if 开平仓条件达到:
        await self.can_do_open_close_pos_demo()
        print("##################################")

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
    DemoStrategy()
    quant.start()


if __name__ == '__main__':
    main()
