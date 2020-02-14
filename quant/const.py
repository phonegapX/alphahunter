# -*- coding:utf-8 -*-

"""
some constants

Project: alphahunter
Author: HJQuant
Description: Asynchronous driven quantitative trading framework
"""


# Exchange Names
DATAMATRIX = "datamatrix" #用于策略研究的数据矩阵平台
BACKTEST = "backtest"     #用于策略回测的量化回测平台

BINANCE = "binance"  # Binance https://www.binance.com
BINANCE_FUTURE = "binance_future"  # https://www.binance-cn.com/cn/futures/BTCUSDT
OKEX = "okex"  # OKEx SPOT https://www.okex.me/spot/trade
OKEX_MARGIN = "okex_margin"  # OKEx MARGIN https://www.okex.me/spot/marginTrade
OKEX_FUTURE = "okex_future"  # OKEx FUTURE https://www.okex.me/future/trade
OKEX_SWAP = "okex_swap"  # OKEx SWAP https://www.okex.me/future/swap
BITMEX = "bitmex"  # BitMEX https://www.bitmex.com/
HUOBI = "huobi"  # Huobi https://www.hbg.com/zh-cn/
HUOBI_FUTURE = "huobi_future"  # Huobi Future https://www.hbdm.com/en-us/contract/exchange/
GATE = "gate"  # Gate.io https://gateio.news/
FTX = "ftx"

# Market Types
MARKET_TYPE_TRADE = "trade"
MARKET_TYPE_ORDERBOOK = "orderbook"
MARKET_TYPE_KLINE = "kline"
MARKET_TYPE_KLINE_5M = "kline_5m"
MARKET_TYPE_KLINE_15M = "kline_15m"
MARKET_TYPE_TICKER = "ticker"

#
INDICATE_ORDER = 1
INDICATE_ASSET = 2
INDICATE_POSITION = 3