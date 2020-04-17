# -*- coding:utf-8 -*-

"""
huobi自合成K线服务

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import sys
import time
import math

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


class klinesrv(Strategy):

    def __init__(self):
        """ 初始化
        """
        super(klinesrv, self).__init__()
        
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

        # 注册定时器
        self.enable_timer()  # 每隔1秒执行一次回调

        self.last_ts_min = int(tools.get_cur_timestamp_ms()//60000*60000) #以分钟为刻度进行对齐的毫秒时间戳
        self.prev_kline_map = defaultdict(lambda:None)
        self.interval = 60*1000 #一分钟

    async def on_time(self):
        """ 每秒钟执行一次.
        """
        logger.info("on_time ...", caller=self)
        
        cur_ts_min = int(tools.get_cur_timestamp_ms()//60000*60000) #以分钟为刻度进行对齐的毫秒时间戳
        
        if self.last_ts_min != cur_ts_min: #新的一分钟到来了
            for sym in self.symbols:
                SingleTask.call_later(self._task_delay, 2, sym, self.last_ts_min) #等待两秒后再工作
            self.last_ts_min = cur_ts_min

    async def _publish_kline(self, symbol, new_kline):
        #发布自合成K线到消息队列
        kwargs = {
            "platform": self.platform,
            "symbol": symbol,
            "timestamp": new_kline["begin_dt"],
            "kline_type": const.MARKET_TYPE_KLINE
        }
        kwargs.update(new_kline) #填充剩余字段
        EventKline(**kwargs).publish()

    async def _task_delay(self, symbol, begin_dt):
        """ 等待两秒后被调用
        """
        trades = await self.db_read_trades(symbol, begin_dt) #读取刚过去的一分钟的所有的逐笔成交
        prev_kline = self.prev_kline_map[symbol] #获取前一根K线
        new_kline, prev_kline = self.generate_kline(begin_dt, trades, prev_kline) #生成新K线,同时更新老K线的若干字段
        self.prev_kline_map[symbol] = new_kline
        await self._publish_kline(symbol, new_kline) #发布
        await self.db_write_kline(symbol, new_kline, prev_kline) #保存K线

    async def db_read_trades(self, symbol, begin_dt):
        """ 读取指定一分钟的所有的逐笔成交
        """
        t_trade = self.t_trade_map[symbol]
        if t_trade:
            end_dt = begin_dt + 60*1000
            s, e = await t_trade.get_list({'dt':{'$gte':begin_dt,'$lt':end_dt}})
            if e:
                logger.error("get trades:", e, caller=self)
                return []
            return s

    async def db_write_kline(self, symbol, new_kline, prev_kline):
        """ 将新K线插入数据库,同时更新数据库前一根K线
        """
        t_kline = self.t_kline_map[symbol]
        if t_kline:
            s, e = await t_kline.insert(new_kline)
            if e:
                logger.error("insert kline:", e, caller=self)
            if prev_kline: #如果存在前一根K线就更新
                update_fields = {}
                update_fields.update({
                    "next_price": prev_kline["next_price"],
                    "next_price_fillna": prev_kline["next_price_fillna"],
                    "lead_ret": prev_kline["lead_ret"],
                    "lead_ret_fillna": prev_kline["lead_ret_fillna"]
                })
                s, e = await t_kline.update({'begin_dt':prev_kline["begin_dt"]}, {'$set':update_fields})
                if e:
                    logger.error("update kline:", e, caller=self)

    def generate_kline(self, begin_dt, trades, prev_kline):
        """ 生成新K线
        注意事项: 关于成交额的计算,成交额(amount)=成交量(volume)*成交价(tradeprice),但是成交量(volume)在反向合约中表示是成交的合约(张)数量,
        不能用这个公式计算,所以我们一定要注意区分我们要处理的是现货还是正向合约或者反向合约,如果是反向合约就单独处理,不能用这个公式计算成交额.
        """
        prev_kline = prev_kline or {}
        new_kline = {
            "begin_dt": begin_dt,
            "end_dt": self.interval + begin_dt - 1,
            "open": 0.0,
            "high": 0.0,
            "low": 0.0,
            "close": 0.0,
            "avg_price": 0.0,
            "buy_avg_price": 0.0,
            "sell_avg_price": 0.0,
            "open_avg": 0.0,
            "open_avg_fillna": 0.0,
            "close_avg": 0.0,
            "close_avg_fillna": 0.0,
            "volume": 0.0,
            "amount": 0.0,
            "buy_volume": 0.0,
            "buy_amount": 0.0,
            "sell_volume": 0.0,
            "sell_amount": 0.0,
            "sectional_high": 0.0,
            "sectional_low": 0.0,
            "sectional_volume": 0.0,
            "sectional_amount": 0.0,
            "sectional_avg_price": 0.0,
            "sectional_buy_avg_price": 0.0,
            "sectional_sell_avg_price": 0.0,
            "sectional_book_count": 0,
            "sectional_buy_book_count": 0,
            "sectional_sell_book_count": 0,
            "sectional_buy_volume": 0.0,
            "sectional_buy_amount": 0.0,
            "sectional_sell_volume": 0.0,
            "sectional_sell_amount": 0.0,
            "prev_close_price": 0.0,
            "next_price": 0.0,
            "next_price_fillna": 0.0,
            "prev_price": prev_kline.get("close_avg", 0.0),
            "prev_price_fillna": prev_kline.get("close_avg_fillna", 0.0),
            "lead_ret": None,
            "lead_ret_fillna": None,
            "lag_ret": None,
            "usable": False
        }
        if trades:
            new_kline["open"] = trades[0]["tradeprice"]
            new_kline["close"] = trades[-1]["tradeprice"]
            results = self.handle_documents(trades)

            new_kline["avg_price"] = results["avg_price"]
            new_kline["volume"] = results["sum_volume"]

            new_kline["amount"] = results["sum_amount"]
            new_kline["usable"] = True if results["sum_volume"] > 0 else False
        sell_trades = []
        buy_trades = []
        open_trades = []
        close_trades = []
        tradeprices = []

        duration = self.interval * 0.2

        for t_document in trades:
            tradeprices.append(t_document["tradeprice"])

            if t_document["direction"] == "BUY":
                buy_trades.append(t_document)
            else:
                sell_trades.append(t_document)

            if t_document["dt"] - begin_dt < duration:
                open_trades.append(t_document)
            elif (begin_dt + self.interval) - t_document["dt"] <= duration:
                close_trades.append(t_document)

        high = max(tradeprices) if tradeprices else 0.0
        low = min(tradeprices) if tradeprices else 0.0
        new_kline["high"] = high
        new_kline["low"] = low

        new_kline["book_count"] = len(trades)
        new_kline["buy_book_count"] = len(buy_trades)
        new_kline["sell_book_count"] = len(sell_trades)

        # 主买
        if buy_trades:
            results = self.handle_documents(buy_trades)
            new_kline["buy_avg_price"] = results["avg_price"]
            new_kline["buy_volume"] = results["sum_volume"]
            new_kline["buy_amount"] = results["sum_amount"]

        # 主卖
        if sell_trades:
            results = self.handle_documents(sell_trades)
            new_kline["sell_avg_price"] = results["avg_price"]
            new_kline["sell_volume"] = results["sum_volume"]
            new_kline["sell_amount"] = results["sum_amount"]

        if open_trades:
            results = self.handle_documents(open_trades)
            new_kline["open_avg"] = results["avg_price"]

        if close_trades:
            results = self.handle_documents(close_trades)
            new_kline["close_avg"] = results["avg_price"]

        open_avg = new_kline["open_avg"]
        close_avg = new_kline["close_avg"]
        new_kline["open_avg_fillna"] = open_avg if open_avg else new_kline["open"]
        close_avg_fillna = close_avg if close_avg else new_kline["close"]
        new_kline["close_avg_fillna"] = close_avg_fillna

        prev_close_avg_fillna = prev_kline.get("close_avg_fillna", 0.0)
        new_kline["lag_ret"] = math.log(new_kline["close_avg"] / prev_kline["close_avg"]) \
            if prev_kline.get("close_avg", 0.0) and new_kline["close_avg"] else None
        new_kline["lag_ret_fillna"] = math.log(close_avg_fillna / prev_close_avg_fillna) \
            if prev_close_avg_fillna and close_avg_fillna else None

        # "lead_ret": 0.0  # math.log(next_price / open_avg),
        # "lead_ret_fillna": 0.0  # math.log(next_price_fillna / open_avg_fillna),
        open_avg = new_kline["open_avg"]
        open_avg_fillna = new_kline["open_avg_fillna"]
        lead_ret = math.log(open_avg / prev_kline["open_avg"]) if prev_kline.get("open_avg", 0.0) and open_avg else None
        lead_ret_fillna = math.log(open_avg_fillna / prev_kline["open_avg_fillna"])\
            if prev_kline.get("open_avg_fillna", 0.0) and open_avg_fillna else None
        
        if prev_kline:
            prev_kline.update({
                "next_price": open_avg,
                "next_price_fillna": open_avg_fillna if open_avg_fillna else prev_kline["close_avg_fillna"],
                "lead_ret": lead_ret,
                "lead_ret_fillna": lead_ret_fillna,
            })
        else:
            prev_kline = None

        return new_kline, prev_kline

    def handle_documents(self, documents):
        sum_amount = 0
        sum_volume = 0

        for document in documents:
            sum_amount += document["volume"]*document["tradeprice"]
            sum_volume += document["volume"]
        return {
            "avg_price": sum_amount / sum_volume,
            "sum_volume": sum_volume,
            "sum_amount": sum_amount,
        }

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
    default_main(klinesrv)
