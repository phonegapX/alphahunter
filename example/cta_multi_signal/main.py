# -*- coding:utf-8 -*-

"""
CTA多信号策略
RSI（1分钟）：大于70为多头、低于30为空头
CCI（1分钟）：大于10为多头、低于-10为空头
MA（5分钟）：快速大于慢速为多头、低于慢速为空头

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
from quant.tasks import LoopRunTask, SingleTask
from quant.trader import Trader
from quant.strategy import Strategy
from quant.utils.decorator import async_method_locker
from quant.startup import default_main
from cta_controller import CTAController


class CTAMultiSignalStrategy(CTAController):

    def __init__(self):
        """ 初始化
        """
        super(CTAMultiSignalStrategy, self).__init__()
        
        self.strategy = config.strategy
        
        #=====================================================
        #创建交易网关
        self.platform = config.platforms[0]["platform"]
        self.account = config.platforms[0]["account"]
        self.access_key = config.platforms[0]["access_key"]
        self.secret_key = config.platforms[0]["secret_key"]
        self.symbols = config.platforms[0]["symbols"]
        #交易模块参数
        params = {
            "strategy": self.strategy,
            "platform": self.platform,
            "symbols": self.symbols,
            "account": self.account,
            "access_key": self.access_key,
            "secret_key": self.secret_key,

            "enable_kline_update": True,
            "enable_orderbook_update": False,
            "enable_trade_update": False,
            "enable_ticker_update": False,
            "enable_order_update": True,
            "enable_fill_update": True,
            "enable_position_update": False,
            "enable_asset_update": True,

            "direct_kline_update": False,
            "direct_orderbook_update": False,
            "direct_trade_update": False,
            "direct_ticker_update": False
        }
        self.gw = self.create_gateway(**params)
        #注册定时器
        self.enable_timer(5)  #每隔5秒执行一次回调

        self.last_kline = None

    async def on_time(self):
        """ 每5秒钟执行一次.
        """
        await super(CTAMultiSignalStrategy, self).on_time()

    async def on_state_update_callback(self, state: State, **kwargs):
        """ 状态变化(底层交易所接口,框架等)通知回调函数
        """
        logger.info("on_state_update_callback:", state, caller=self)

        if state.code == State.STATE_CODE_DB_SUCCESS: #数据库连接成功
            pass
        elif state.code == State.STATE_CODE_READY: #交易接口准备好
            #收到此状态通知,证明指定交易接口准备就绪,可以对其进行操作,比如下单
            pass
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
        #logger.info("kline:", kline, caller=self)
        self.last_kline = kline

        #如果收到的是'交易所提供的K线'而不是'自合成K线'的话就直接忽略
        if not kline.is_custom_and_usable():
            return
        await super(CTAMultiSignalStrategy, self).on_kline_update_callback(kline)

    async def on_asset_update_callback(self, asset: Asset):
        """ 账户资产更新
        """
        logger.info("asset:", asset, caller=self)
        await super(CTAMultiSignalStrategy, self).on_asset_update_callback(asset)

    async def on_orderbook_update_callback(self, orderbook: Orderbook): ...
    async def on_trade_update_callback(self, trade: Trade): ...
    async def on_ticker_update_callback(self, ticker: Ticker): ...
    async def on_position_update_callback(self, position: Position): ...
    
    async def on_order_update_callback(self, order: Order):
        """ 订单状态更新
        """
        logger.info("order:", order, caller=self)

    async def on_fill_update_callback(self, fill: Fill):
        """ 订单成交通知
        """
        logger.info("fill:", fill, caller=self)

    async def submit_orders(self, delta_position):
        """ 根据当前最新的delta_position来执行下单操作
        """
        x = delta_position['BTC'] #下单量
        vol = abs(x)
        if vol > 0.001: #如果下单量太小就不下单
            if x > 0: #做多
                vol = tools.decimal_truncate(vol, 4) #保留4位小数
                s, e = await self.create_order(self.gw, self.symbols[0], ORDER_ACTION_BUY, self.last_kline.close+100, vol) #限价单模拟市价单
                if e:
                    logger.error("error:", e, caller=self)
            elif x < 0: #做空
                vol = tools.decimal_truncate(vol, 4) #保留4位小数
                s, e = await self.create_order(self.gw, self.symbols[0], ORDER_ACTION_SELL, self.last_kline.close-100, vol) #限价单模拟市价单
                if e:
                    logger.error("error:", e, caller=self)


if __name__ == '__main__':
    default_main(CTAMultiSignalStrategy)
