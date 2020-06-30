# -*- coding:utf-8 -*-

"""
采集符号信息,写入数据库

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
from quant.trader import Trader
from quant.strategy import Strategy
from quant.utils.decorator import async_method_locker
from quant.startup import default_main
from quant.utils.mongo import MongoDB


class SymInfoWriter(Strategy):

    def __init__(self):
        """ 初始化
        """
        super(SymInfoWriter, self).__init__()
        
        self.strategy = config.strategy
        
        #=====================================================
        #创建第一个交易接口
        self.platform = config.platforms[0]["platform"]
        self.account = config.platforms[0]["account"]
        self.access_key = config.platforms[0]["access_key"]
        self.secret_key = config.platforms[0]["secret_key"]
        self.symbols = config.platforms[0]["symbols"]
        # 交易模块参数
        params = {
            "strategy": self.strategy,
            "platform": self.platform,
            "symbols": self.symbols,
            "account": self.account,
            "access_key": self.access_key,
            "secret_key": self.secret_key,

            "enable_kline_update": False,
            "enable_orderbook_update": False,
            "enable_trade_update": False,
            "enable_ticker_update": False,
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
        self.platform2 = config.platforms[1]["platform"]
        self.account2 = config.platforms[1]["account"]
        self.access_key2 = config.platforms[1]["access_key"]
        self.secret_key2 = config.platforms[1]["secret_key"]
        self.symbols2 = config.platforms[1]["symbols"]
        # 交易模块参数
        params2 = {
            "strategy": self.strategy,
            "platform": self.platform2,
            "symbols": self.symbols2,
            "account": self.account2,
            "access_key": self.access_key2,
            "secret_key": self.secret_key2,

            "enable_kline_update": False,
            "enable_orderbook_update": False,
            "enable_trade_update": False,
            "enable_ticker_update": False,
            "enable_order_update": True,
            "enable_fill_update": True,
            "enable_position_update": True,
            "enable_asset_update": True,

            "direct_kline_update": False,
            "direct_orderbook_update": False,
            "direct_trade_update": False,
            "direct_ticker_update": False
        }
        self.gw2 = self.create_gateway(**params2)

        #=====================================================
        #创建第三个交易接口
        self.platform3 = config.platforms[2]["platform"]
        self.account3 = config.platforms[2]["account"]
        self.access_key3 = config.platforms[2]["access_key"]
        self.secret_key3 = config.platforms[2]["secret_key"]
        self.passphrase = config.platforms[2]["passphrase"]
        self.symbols3 = config.platforms[2]["symbols"]
        # 交易模块参数
        params3 = {
            "strategy": self.strategy,
            "platform": self.platform3,
            "symbols": self.symbols3,
            "account": self.account3,
            "access_key": self.access_key3,
            "secret_key": self.secret_key3,
            "passphrase": self.passphrase,

            "enable_kline_update": False,
            "enable_orderbook_update": False,
            "enable_trade_update": False,
            "enable_ticker_update": False,
            "enable_order_update": True,
            "enable_fill_update": True,
            "enable_position_update": True,
            "enable_asset_update": True,

            "direct_kline_update": False,
            "direct_orderbook_update": False,
            "direct_trade_update": False,
            "direct_ticker_update": False
        }
        self.gw3 = self.create_gateway(**params3)

        #=====================================================
        #创建第四个交易接口
        self.platform4 = config.platforms[3]["platform"]
        self.account4 = config.platforms[3]["account"]
        self.access_key4 = config.platforms[3]["access_key"]
        self.secret_key4 = config.platforms[3]["secret_key"]
        self.symbols4 = config.platforms[3]["symbols"]
        # 交易模块参数
        params4 = {
            "strategy": self.strategy,
            "platform": self.platform4,
            "symbols": self.symbols4,
            "account": self.account4,
            "access_key": self.access_key4,
            "secret_key": self.secret_key4,

            "enable_kline_update": False,
            "enable_orderbook_update": False,
            "enable_trade_update": False,
            "enable_ticker_update": False,
            "enable_order_update": True,
            "enable_fill_update": True,
            "enable_position_update": True,
            "enable_asset_update": True,

            "direct_kline_update": False,
            "direct_orderbook_update": False,
            "direct_trade_update": False,
            "direct_ticker_update": False
        }
        self.gw4 = self.create_gateway(**params4)

        self.t_symbol_info = MongoDB("db_market", "t_symbol_info")

    async def write_db(self, syminfo):
        update_fields = vars(syminfo)
        #s, e = await self.t_symbol_info.insert(update_fields) 
        s, e = await self.t_symbol_info.update({'platform':syminfo.platform, 'symbol':syminfo.symbol}, {'$set':update_fields}, True)

    async def on_state_update_callback(self, state: State, **kwargs):
        """ 状态变化(底层交易所接口,框架等)通知回调函数
        """
        logger.info("on_state_update_callback:", state, caller=self)
        
        if state.code == State.STATE_CODE_READY: #交易接口准备好
            if state.platform == const.HUOBI:
                for sym in self.symbols:
                    s, e = await self.get_symbol_info(self.gw, sym)
                    if not e and s:
                        await self.write_db(s)
            if state.platform == const.FTX:
                for sym in self.symbols2:
                    s, e = await self.get_symbol_info(self.gw2, sym)
                    if not e and s:
                        await self.write_db(s)
            if state.platform == const.OKEX:
                for sym in self.symbols3:
                    s, e = await self.get_symbol_info(self.gw3, sym)
                    if not e and s:
                        await self.write_db(s)
            if state.platform == const.HUOBI_FUTURE:
                for sym in self.symbols4:
                    s, e = await self.get_symbol_info(self.gw4, sym)
                    if not e and s:
                        await self.write_db(s)

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

    async def on_kline_update_callback(self, kline: Kline): ...
    async def on_orderbook_update_callback(self, orderbook: Orderbook): ...
    async def on_trade_update_callback(self, trade: Trade): ...
    async def on_ticker_update_callback(self, ticker: Ticker): ...
    async def on_order_update_callback(self, order: Order): ...
    async def on_fill_update_callback(self, fill: Fill): ...
    async def on_position_update_callback(self, position: Position): ...
    async def on_asset_update_callback(self, asset: Asset): ...


if __name__ == '__main__':
    default_main(SymInfoWriter)
