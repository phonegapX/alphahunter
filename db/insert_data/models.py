# -*- coding: utf-8 -*-
import os
import sys
import re

abspath = os.path.abspath(os.path.join("../.."))  # NOQA
sys.path.append(abspath)  # NOQA
from scripts.insert_data.mongo_utils import get_mongo_conn


class Exchange(object):
    COLUMNS = []

    def __init__(self):
        self.collection = get_mongo_conn()["t_exchange"]


class Symbol(object):
    FOCUS_SYMBOLS = [
        "btcusdt", "ethusdt", "ltcusdt", "etcusdt", "xrpusdt", "eosusdt", "bchusdt", "bsvusdt", "trxusdt", "adausdt",
        "ethbtc", "ltcbtc", "etcbtc", "xrpbtc", "eosbtc", "bchbtc", "bsvbtc", "trxbtc", "adabtc"]

    RE_FOCUS_SYMBOLS = [
        "btc([^a-zA-Z]?)usdt", "eth([^a-zA-Z]?)usdt", "ltc([^a-zA-Z]?)usdt", "etc([^a-zA-Z]?)usdt",
        "xrp([^a-zA-Z]?)usdt", "eos([^a-zA-Z]?)usdt", "bch([^a-zA-Z]?)usdt", "bsv([^a-zA-Z]?)usdt",
        "trx([^a-zA-Z]?)usdt", "ada([^a-zA-Z]?)usdt", "eth([^a-zA-Z]?)btc", "ltc([^a-zA-Z]?)btc", "etc([^a-zA-Z]?)btc",
        "xrp([^a-zA-Z]?)btc", "eos([^a-zA-Z]?)btc", "bch([^a-zA-Z]?)btc", "bsv([^a-zA-Z]?)btc", "trx([^a-zA-Z]?)btc",
        "ada([^a-zA-Z]?)btc"]

    COLUMNS = ["exchange", "name", "research_usable", "trade_usable"]

    def __init__(self):
        self.collection = get_mongo_conn()["t_symbol"]

    @classmethod
    def is_focus(cls, symbol_name):
        # 先按照正常模式过滤
        if symbol_name in cls.FOCUS_SYMBOLS:
            return symbol_name

        # 考虑特殊情况
        for focus_symbol in cls.RE_FOCUS_SYMBOLS:
            if re.match(focus_symbol, symbol_name, flags=re.I):
                return re.sub("\(\[\^a\-zA\-Z\]\?\)", "", focus_symbol)  # NOQA
        return


class OrderBook(object):
    """
    市场订单簿表
    创建索引
    db.t_orderbook_binance_btcusdt.createIndex({dt:1},{background:1})
    db.t_orderbook_binance_ethusdt.createIndex({dt:1},{background:1})
    db.t_orderbook_binance_ltcusdt.createIndex({dt:1},{background:1})
    db.t_orderbook_binance_etcusdt.createIndex({dt:1},{background:1})
    db.t_orderbook_binance_xrpusdt.createIndex({dt:1},{background:1})
    db.t_orderbook_binance_eosusdt.createIndex({dt:1},{background:1})
    db.t_orderbook_binance_bchusdt.createIndex({dt:1},{background:1})
    db.t_orderbook_binance_bsvusdt.createIndex({dt:1},{background:1})
    db.t_orderbook_binance_trxusdt.createIndex({dt:1},{background:1})
    db.t_orderbook_binance_adausdt.createIndex({dt:1},{background:1})
    db.t_orderbook_binance_ethbtc.createIndex({dt:1},{background:1})
    db.t_orderbook_binance_ltcbtc.createIndex({dt:1},{background:1})
    db.t_orderbook_binance_etcbtc.createIndex({dt:1},{background:1})
    db.t_orderbook_binance_xrpbtc.createIndex({dt:1},{background:1})
    db.t_orderbook_binance_eosbtc.createIndex({dt:1},{background:1})
    db.t_orderbook_binance_bchbtc.createIndex({dt:1},{background:1})

    db.t_orderbook_binance_bsvbtc.createIndex({dt:1},{background:1})
    db.t_orderbook_binance_trxbtc.createIndex({dt:1},{background:1})
    db.t_orderbook_binance_adabtc.createIndex({dt:1},{background:1})
    """
    COLUMNS = ["dt",
                "askprice1", "askprice2", "askprice3", "askprice4", "askprice5", "askprice6", "askprice7", "askprice8", "askprice9", "askprice10", "askprice11", "askprice12", "askprice13", "askprice14", "askprice15", "askprice16", "askprice17", "askprice18", "askprice19", "askprice20",  # NOQA
                "bidprice1", "bidprice2", "bidprice3", "bidprice4", "bidprice5", "bidprice6", "bidprice7", "bidprice8", "bidprice9", "bidprice10", "bidprice11", "bidprice12", "bidprice13", "bidprice14", "bidprice15", "bidprice16", "bidprice17", "bidprice18", "bidprice19", "bidprice20",  # NOQA
                "asksize1", "asksize2", "asksize3", "asksize4", "asksize5", "asksize6", "asksize7", "asksize8", "asksize9", "asksize10", "asksize11", "asksize12", "asksize13", "asksize14", "asksize15", "asksize16", "asksize17", "asksize18", "asksize19", "asksize20",  # NOQA
                "bidsize1", "bidsize2", "bidsize3", "bidsize4", "bidsize5", "bidsize6", "bidsize7", "bidsize8", "bidsize9", "bidsize10", "bidsize11", "bidsize12", "bidsize13", "bidsize14", "bidsize15", "bidsize16", "bidsize17", "bidsize18", "bidsize19", "bidsize20",  # NOQA
            ]

    def __init__(self, exchange_name, symbol_name):
        collection_name = "t_orderbook_{exchange_name}_{symbol_name}".format(exchange_name=exchange_name,
                                                                             symbol_name=symbol_name)
        self.collection = get_mongo_conn()[collection_name]


class Trade(object):
    """
    市场逐笔成交记录表
    """
    COLUMNS = ["dt", "tradedt", "tradeprice", "volume", "amount", "direction"]

    def __init__(self, exchange_name, symbol_name):
        collection_name = "t_trade_{exchange_name}_{symbol_name}".format(exchange_name=exchange_name,
                                                                         symbol_name=symbol_name)
        self.collection = get_mongo_conn()[collection_name]
