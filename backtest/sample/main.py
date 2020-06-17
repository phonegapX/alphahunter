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
        self.platform = config.platforms[0]["platform"] #交易所
        self.symbols = config.platforms[0]["symbols"]
        self.account = config.platforms[0]["account"] #此参数回测盘不用,不过为了统一都写上不影响
        access_key = config.platforms[0]["access_key"]  #此参数回测盘不用,不过为了统一都写上不影响
        secret_key = config.platforms[0]["secret_key"]  #此参数回测盘不用,不过为了统一都写上不影响
        #交易模块参数
        params = {
            "strategy": config.strategy,
            "platform": self.platform,
            "symbols": self.symbols,
            "account": self.account,
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
        #交易模块参数
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

        #注册定时器
        #self.enable_timer()  # 每隔1秒执行一次回调

    async def on_time(self):
        """ 每秒钟执行一次. 因为是异步并发架构,这个函数执行的时候交易通道链接不一定已经建立好
        """
        logger.info("on_time ...", caller=self)

    async def on_state_update_callback(self, state: State, **kwargs):
        """ 状态变化(底层交易所接口,框架等)通知回调函数
        """
        logger.info("on_state_update_callback:", state, caller=self)
        
        if state.code == State.STATE_CODE_READY: #交易接口准备好
            s, e = await self.get_symbol_info(self.gw, self.symbols[0])
            s, e = await self.get_assets(self.gw)
            s, e = await self.get_orders(self.gw, self.symbols[0])

        elif state.code == State.STATE_CODE_CONNECT_SUCCESS:    #交易接口连接成功
            pass #仅仅是通知一下,实际策略可以不用过于关注此状态
        elif state.code == State.STATE_CODE_CONNECT_FAILED:     #交易接口连接失败
            pass #不需要过于关注此状态,因为底层接口会自动重新连接
        elif state.code == State.STATE_CODE_DISCONNECT:         #交易接口连接断开
            pass #不需要过于关注此状态,因为底层接口会自动重新连接
        elif state.code == State.STATE_CODE_RECONNECTING:       #交易接口重新连接中
            pass #比如说可以记录重连次数,如果一段时间内一直在重连可能交易所出问题,可以酌情处理,如结束本策略进程等
        elif state.code == State.STATE_CODE_PARAM_MISS:         #交易接口初始化过程缺少参数
            pass #收到此状态通知,证明无法正常初始化,应该结束本策略进程
        elif state.code == State.STATE_CODE_GENERAL_ERROR:      #交易接口常规错误
            ... #策略进程运行过程中如果收到某些错误通知,可以根据实际情况判断,比如可以做一些策略善后工作,然后结束本策略进程
            return

    async def on_kline_update_callback(self, kline: Kline):
        """ 市场K线更新
        """
        logger.info("kline:", kline, caller=self)
        #x = ModelAPI.current_datetime()
        #print(x)
        #x = ModelAPI.current_milli_timestamp()
        #print(x)
        #x = ModelAPI.today()
        #print(x)
        #ts = ModelAPI.current_milli_timestamp()
        #r = await ModelAPI.get_klines_between(self.platform, self.symbols[0], ts, ts+10*60*1000)
        #print(r)
        if kline.symbol == self.symbols[0]:
            s, e = await self.create_order(self.gw, self.symbols[0], ORDER_ACTION_BUY, 0, 1000, ORDER_TYPE_MARKET)
            s, e = await self.create_order(self.gw, self.symbols[0], ORDER_ACTION_SELL, 0, 2.3, ORDER_TYPE_MARKET)
            s, e = await self.create_order(self.gw, self.symbols[0], ORDER_ACTION_BUY, 10000, 1.2)
            s, e = await self.create_order(self.gw, self.symbols[0], ORDER_ACTION_SELL, 6500, 0.5)
            s, e = await self.create_order(self.gw, self.symbols[0], ORDER_ACTION_BUY, 5000, 1)
            s, e = await self.create_order(self.gw, self.symbols[0], ORDER_ACTION_SELL, 15000, 1)
            s, e = await self.revoke_order(self.gw, self.symbols[0], s, "123", "234")

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
