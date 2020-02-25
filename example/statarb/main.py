# -*- coding:utf-8 -*-

"""
HUOBI 模块使用演示

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
from quant.interface.model_api import ModelAPI


class DemoStrategy(Strategy):

    def __init__(self):
        """ 初始化
        """
        super(DemoStrategy, self).__init__()
        
        self.strategy = config.strategy
        
        #=====================================================
        #创建第一个交易接口
        self.platformHB = config.accounts[0]["platform"]
        self.accountHB = config.accounts[0]["account"]
        access_key = config.accounts[0]["access_key"]
        secret_key = config.accounts[0]["secret_key"]
        
        # 交易模块参数
        params = {
            "strategy": self.strategy,
            "platform": self.platformHB,
            "symbols": "ethusdt",
            "account": self.accountHB,
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

            "direct_kline_update": True,
            "direct_orderbook_update": True,
            "direct_trade_update": True,
            "direct_ticker_update": True
        }
        self.traderHB = self.create_gateway(**params)
        
        #=====================================================
        #创建第二个交易接口
        self.platformHBF = config.accounts[1]["platform"]
        self.accountHBF = config.accounts[1]["account"]
        access_key = config.accounts[1]["access_key"]
        secret_key = config.accounts[1]["secret_key"]
        
        # 交易模块参数
        params = {
            "strategy": self.strategy,
            "platform": self.platformHBF,
            "symbols": "ETH191227",
            "account": self.accountHBF,
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

            "direct_kline_update": True,
            "direct_orderbook_update": True,
            "direct_trade_update": True,
            "direct_ticker_update": True
        }
        self.traderHBF = self.create_gateway(**params)
        

        # 注册定时器
        self.enable_timer()  # 每隔1秒执行一次回调
        
        #==================================================================
        self._orderbookHB = None
        self._orderbookHBF = None
        self._last_ts = 0

    async def on_time(self):
        """ 每秒钟执行一次. 因为是异步并发架构,这个函数执行的时候交易通道链接不一定已经建立好
        """
        #if not hasattr(self, "just_once"):
        #    self.just_once = 1
            #xx = self.get_orders(self.trader, "ETH-PERP")
            #xx = self.get_position(self.trader1, "ETH-PERP")
            #xx = self.get_assets(self.trader)
            #xx = self.create_order(self.trader, "ETH-PERP", ORDER_ACTION_SELL, "51", "-0.002")
            #xx = self.create_order(self.trader, "ETH-PERP", ORDER_ACTION_SELL, "0", "-0.002", ORDER_TYPE_MARKET)
            #xx = self.revoke_order(self.trader, "ETH-PERP", "1017521392")
            #order1 = Strategy.TOrder(self.trader, "ETH-PERP", ORDER_ACTION_SELL, "351", "-0.02")
            #order2 = Strategy.TOrder(self.trader1, "ETH-PERP", ORDER_ACTION_SELL, "352", "-0.03")
            #xx = self.create_pair_order(order1, order2)
        #    xx = self.get_symbol_info(self.trader, "trxeth")
        #    yy, zz = await xx
        
        #logger.info("on_time ...", caller=self)
        #new_price = tools.float_to_str(price)  # 将价格转换为字符串，保持精度

    async def on_state_update_callback(self, state: State, **kwargs):
        """ 状态变化(底层交易所接口,框架等)通知回调函数
        """
        logger.info("on_state_update_callback:", state, caller=self)



    def datactrl(self):
        self.mpHB.pop(0)
        self.mpHBF.pop(0)

    def fit_parameters(self, length):
        z = None
        if len(self.mpHB) >= length and len(self.mpHBF) >= length:
                mpHB = np.array(self.mpHB)
                mpHBF = np.array(self.mpHBF)
                spread = mpHBF - mpHB
                
                
                #asset = self.pm.get_asset(self.platformHB, self.accountHB)
                #free_eth = asset.assets["eth"]["free"]
                #free_usdt = asset.assets["usdt"]["free"]
                #posHBF = self.pm.get_position(self.platformHBF, self.accountHBF, "ETH191227")
                
                #if abs(posSpot) > 0.2 and abs(posSwap) > 0.2:
                #    z   = (spread[-1] - self.mu)/self.sigma
                #    self.datactrl()
                #    return z
                if sm.tsa.stattools.adfuller(spread)[1] < 0.05 and \
                   sm.tsa.stattools.adfuller(spread[int(length/2):])[1] < 0.05 and \
                   sm.tsa.stattools.adfuller(spread[int(length/3):])[1] < 0.05:
                    model     = AR(spread)
                    model_fit = model.fit(1)
                    a, b      = model_fit.params
                    if b < 1:
                        self.theta     = 1-b
                        self.mu        = a/(1-b)
                        self.epsilon   = spread[1:]-(a+b*spread[:-1])
                        self.sigma     = np.sqrt((np.std(self.epsilon)**2)/(2*self.theta))
                        z              = (spread[-1] - self.mu)/self.sigma
                        #print(z, len(spread))
                self.datactrl()
        return z





  




    async def on_orderbook_update_callback(self, orderbook: Orderbook):
        """ 订单薄更新
        """
        logger.info("orderbook:", orderbook, caller=self)
        
        
        #if self.pm.get_orders(self.platformHB, self.accountHB, self.symbolsHB) or self.pm.get_orders(self.platformHBF, self.accountHBF, self.symbolsHBF):
        #    return        
        
        if orderbook.platform == self.platformHB:
            self._orderbookHB = orderbook
        elif orderbook.platform == self.platformHBF:
            self._orderbookHBF = orderbook
            
            
        nowts = ModelAPI.timenow_unix_time()
        
        if (nowTicks-self._last_ts > 1000):
            if self._orderbookHB and self._orderbookHBF and \
               (nowts - self._orderbookHB.timestamp < 2000) and (nowts - self._orderbookHBF.timestamp < 2000):
                apHB = self._orderbookHB.asks[0][0]
                bpHB = self._orderbookHB.bids[0][0]
                self.mpHB.append((apHB+bpHB)/2)
                apHBF = self._orderbookHBF.asks[0][0]
                bpHBF = self._orderbookHBF.bids[0][0]
                self.mpHBF.append((apHBF+bpHBF)/2)

                z = self.fit_parameters(3600)
                self.trade(z, 10, 6, 1.96)
                
                self._last_ts = nowts
                
                
                
            
            
    async def on_kline_update_callback(self, kline: Kline): ...
    async def on_trade_update_callback(self, trade: Trade): ...
    async def on_ticker_update_callback(self, ticker: Ticker): ...

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
