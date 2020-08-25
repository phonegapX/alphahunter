# -*- coding:utf-8 -*-

"""
DataMatrix样例演示

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""

import sys
import asyncio
import numpy as np
import pandas as pd

from quant import const
from quant.state import State
from quant.utils import tools, logger
from quant.config import config
from quant.market import Market, Kline, Orderbook, Trade, Ticker
from quant.tasks import LoopRunTask
from quant.trader import Trader
from quant.strategy import Strategy
from quant.utils.decorator import async_method_locker
from quant.order import Order, Fill, ORDER_ACTION_BUY, ORDER_ACTION_SELL, ORDER_STATUS_FILLED, ORDER_TYPE_MARKET
from quant.position import Position
from quant.asset import Asset
from quant.startup import default_main
from quant.interface.datamatrix_api import DataMatrixAPI
from quant.interface.ah_math import AHMath


class DataMatrixDemo(Strategy):

    def __init__(self):
        """ 初始化
        """
        super(DataMatrixDemo, self).__init__()

        self.platform = config.platforms[0]["platform"] #交易所
        self.symbols = config.platforms[0]["symbols"]
        #交易模块参数
        params = {
            "strategy": config.strategy,
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
        
        self.init()

    async def on_state_update_callback(self, state: State, **kwargs):
        """ 状态变化(底层交易所接口,框架等)通知回调函数
        """
        logger.info("on_state_update_callback:", state, caller=self)

    def init(self):
        #self.feature_row表示csv文件字段名,此变量系统内部会被使用到,按下面这种列表形式填充该变量
        self.feature_row = ['dt', 'date_str', 'next_1min_ret', 'next_2min_ret', 'next_5min_ret', 'next_10min_ret', 'next_30min_ret', 'next_60min_ret']
        for x in ['arc', 'vrc']:
            for xho in [30, 60, 120, 240]:
                self.feature_row.append(x + str(xho))

    async def on_kline_update_callback(self, kline: Kline):
        """ 市场K线更新
        """
        #logger.info("kline:", kline, caller=self)

        recent_days = []
        moment = DataMatrixAPI.current_datetime()
        for i in range(7):
            recent_days.append(DataMatrixAPI.datetime_delta_time(moment, delta_day=-1*(i+1)))
        eod_klines = []
        for each in recent_days:
            lk = await DataMatrixAPI.get_last_kline_oneday(self.platform, self.symbols[0], each)
            eod_klines.append(lk)

        ts = DataMatrixAPI.current_milli_timestamp()
        prev_klines = await DataMatrixAPI.get_prev_klines(self.platform, self.symbols[0], ts, 241)
        next_klines = await DataMatrixAPI.get_next_klines(self.platform, self.symbols[0], ts, 61)

        avg_daily_volume = np.mean([each['sectional_volume'] for each in eod_klines])

        turnover_rate = [each['volume'] / avg_daily_volume if each else 0.0 for each in prev_klines]
        relative_ret = []
        for i in range(1,241):
            kline_start = prev_klines[i]
            kline_end = prev_klines[0]
            if not kline_start or not kline_end or kline_start['close_avg_fillna'] == 0.0:
                relative_ret.append(0.0)
            else:
                temp_ret = (kline_end['close_avg_fillna'] - kline_start['close_avg_fillna']) / kline_start['close_avg_fillna']
                if not temp_ret or temp_ret == -1.0:
                    relative_ret.append(0.0)
                else:
                    relative_ret.append(temp_ret / (temp_ret + 1.0))

        adjusted_turnover_rate = []
        for i in range(1,241):
            atr = turnover_rate[i]
            for k in range(i):
                atr *= (1 - turnover_rate[k])
            adjusted_turnover_rate.append(atr)

        arc30 = AHMath.weighted_mean(relative_ret[:30], adjusted_turnover_rate[:30])
        arc60 = AHMath.weighted_mean(relative_ret[:60], adjusted_turnover_rate[:60])
        arc120 = AHMath.weighted_mean(relative_ret[:120], adjusted_turnover_rate[:120])
        arc240 = AHMath.weighted_mean(relative_ret[:240], adjusted_turnover_rate[:240])
        if pd.isnull(arc30):
            vrc30 = np.nan
        else:
            vrc30 = (30.0/29.0) * AHMath.weighted_mean(np.power(np.array(relative_ret[:30]) - arc30, 2.0), adjusted_turnover_rate[:30])
        if pd.isnull(arc60):
            vrc60 = np.nan
        else:
            vrc60 = (60.0/59.0) * AHMath.weighted_mean(np.power(np.array(relative_ret[:60]) - arc60, 2.0), adjusted_turnover_rate[:60])
        if pd.isnull(arc120):
            vrc120 = np.nan
        else:
            vrc120 = (120.0/119.0) * AHMath.weighted_mean(np.power(np.array(relative_ret[:120]) - arc120, 2.0), adjusted_turnover_rate[:120])
        if pd.isnull(arc240):
            vrc240 = np.nan
        else:
            vrc240 = (240.0/239.0) * AHMath.weighted_mean(np.power(np.array(relative_ret[:240]) - arc240, 2.0), adjusted_turnover_rate[:240])

        lead_rets = []
        kline_start = prev_klines[-1]
        for i in [1, 2, 5, 10, 30, 60]:
            kline_end = next_klines[i]
            if not kline_start or not kline_end:
                lead_rets.append(np.nan)
            elif pd.isnull(kline_start['next_price_fillna']) or pd.isnull(kline_end['next_price_fillna']) or kline_start['next_price_fillna'] == 0 or kline_end['next_price_fillna'] == 0:
                lead_rets.append(np.nan)
            else:
                lead_rets.append(kline_end['next_price_fillna'] / kline_start['next_price_fillna'] - 1.0)

        new_row = { "dt": kline_start['end_dt'],
                    "date_str": DataMatrixAPI.datetime_to_str(DataMatrixAPI.milli_timestamp_to_datetime(kline_start['end_dt'])),
                    "next_1min_ret": lead_rets[0],
                    "next_2min_ret": lead_rets[1],
                    "next_5min_ret": lead_rets[2],
                    "next_10min_ret": lead_rets[3],
                    "next_30min_ret": lead_rets[4],
                    "next_60min_ret": lead_rets[5],
                    "arc30": arc30,
                    "arc60": arc60,
                    "arc120": arc120,
                    "arc240": arc240,
                    "vrc30": vrc30,
                    "vrc60": vrc60,
                    "vrc120": vrc120,
                    "vrc240": vrc240 }
        await self.add_row(new_row)

    async def on_orderbook_update_callback(self, orderbook: Orderbook): ...
    async def on_trade_update_callback(self, trade: Trade): ...
    async def on_ticker_update_callback(self, ticker: Ticker): ...
    async def on_order_update_callback(self, order: Order): ...
    async def on_fill_update_callback(self, fill: Fill): ...
    async def on_position_update_callback(self, position: Position): ...
    async def on_asset_update_callback(self, asset: Asset): ...


if __name__ == '__main__':
    default_main(DataMatrixDemo)
