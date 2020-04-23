# -*- coding:utf-8 -*-

"""
HUOBI_FUTURE 模块使用演示

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
from quant.order import Order, Fill, ORDER_ACTION_BUY, ORDER_ACTION_SELL, ORDER_STATUS_FILLED, ORDER_TYPE_MARKET, ORDER_TYPE_IOC
from quant.position import Position
from quant.asset import Asset
from quant.tasks import LoopRunTask
from quant.gateway import ExchangeGateway
from quant.trader import Trader
from quant.strategy import Strategy
from quant.utils.decorator import async_method_locker
from quant.startup import default_main


class DemoStrategy(Strategy):

    def __init__(self):
        """ 初始化
        """
        super(DemoStrategy, self).__init__()
        
        self.strategy = config.strategy
        
        #=====================================================
        #创建交易网关
        self.platform = config.accounts[0]["platform"]
        self.account = config.accounts[0]["account"]
        self.access_key = config.accounts[0]["access_key"]
        self.secret_key = config.accounts[0]["secret_key"]
        self.symbols = config.markets[self.platform]["symbols"]
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
        self.gw = self.create_gateway(**params)
        # 注册定时器
        self.enable_timer()  # 每隔1秒执行一次回调

    async def on_time(self):
        """ 每秒钟执行一次. 因为是异步并发架构,这个函数执行的时候交易通道链接不一定已经建立好
        """
        logger.info("on_time ...", caller=self)

    async def on_state_update_callback(self, state: State, **kwargs):
        """ 状态变化(底层交易所接口,框架等)通知回调函数
        """
        logger.info("on_state_update_callback:", state, caller=self)
        
        if state.code == State.STATE_CODE_READY: #收到此状态通知,证明底层环境一切准备就绪,策略可以开始工作
            #s, e = await self.create_order(self.gw, "ETH200327", ORDER_ACTION_SELL, 307, -7) #卖出开空(限价单)
            #s, e = await self.create_order(self.gw, "ETH200327", ORDER_ACTION_SELL, 0, -1, ORDER_TYPE_MARKET) #卖出开空(市价单)
            #s, e = await self.create_order(self.gw, "ETH200327", ORDER_ACTION_SELL, 131, -1, ORDER_TYPE_IOC) #卖出开空(IOC单)
            
            #s, e = await self.create_order(self.gw, "ETH200327", ORDER_ACTION_BUY, 136, 2) #买入开多(限价单)
            #s, e = await self.create_order(self.gw, "ETH200327", ORDER_ACTION_BUY, 0, 1, ORDER_TYPE_MARKET) #买入开多(市价单)
            #s, e = await self.create_order(self.gw, "ETH200327", ORDER_ACTION_BUY, 150, 1, ORDER_TYPE_IOC) #买入开多(IOC单)

            #s, e = await self.create_order(self.gw, "ETH200327", ORDER_ACTION_SELL, 160, 2) #卖出平多(限价单)
            #s, e = await self.create_order(self.gw, "ETH200327", ORDER_ACTION_SELL, 0, 1, ORDER_TYPE_MARKET) #卖出平多(市价单)
            #s, e = await self.create_order(self.gw, "ETH200327", ORDER_ACTION_SELL, 130, 1, ORDER_TYPE_IOC) #卖出平多(IOC单)
            
            #s, e = await self.create_order(self.gw, "ETH200327", ORDER_ACTION_BUY, 105, -1) #买入平空(限价单)
            #s, e = await self.create_order(self.gw, "ETH200327", ORDER_ACTION_BUY, 0, -1, ORDER_TYPE_MARKET) #买入平空(市价单)
            #s, e = await self.create_order(self.gw, "ETH200327", ORDER_ACTION_BUY, 130, -1, ORDER_TYPE_IOC) #买入平空(IOC单)
            
            #s, e = await self.revoke_order(self.gw, "ETH200327") #撤销此合约下的所有挂单
            #s, e = await self.revoke_order(self.gw, "ETH200327", "688011256896319488") #撤销单个挂单
            #s, e = await self.revoke_order(self.gw, "ETH200327", "70873572342", "688009872595656704") #撤销多个挂单
            
            s, e = await self.get_symbol_info(self.gw, "ETH200327")
            #s, e = await self.get_position(self.gw, "ETH200327")
            #s, e = await self.get_assets(self.gw)
            #s, e = await self.get_orders(self.gw, "ETH200327")
            #s, e = await self.invalid_indicate(self.gw, "ETH200327", const.INDICATE_ASSET)
            #s, e = await self.invalid_indicate(self.gw, "ETH200327", const.INDICATE_ORDER)
            #s, e = await self.invalid_indicate(self.gw, "ETH200327", const.INDICATE_POSITION)

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
        """
        假设策略在本回调函数里面判断开平仓条件,并且条件达到可以进行开平仓的情况下,最好是把接下来的开平仓逻辑单独
        放在一个函数里面,并且加上'不等待类型的锁',比如下面这个函数这样.
        """
        #if 开平仓条件达到:
        await self.can_do_open_close_pos_demo()
        #print("##################################")

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
