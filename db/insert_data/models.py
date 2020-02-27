# -*- coding: utf-8 -*-
import re
import asyncio
import datetime
import time
import pandas as pd

from pandas import DataFrame

from mongo_utils import get_mongo_conn


class Base(object):

    def get_all(self, sort=None, **kwargs):
        pass

    async def get_df_from_table(self, query={}, sort=None, skip=0, limit=1000):
        """
        use me like this:

        kline = Kline(exchange_name="binance", symbol_name="btcusdt")
        df = await kline.get_df_from_table()
        """
        if sort:
            cursor = self.collection.find(query).sort(sort).skip(skip).limit(limit)
        else:
            cursor = self.collection.find(query).skip(skip).limit(limit)
        documents = [t_document for t_document in await cursor.to_list(length=limit)]
        return DataFrame(documents)

    async def to_daily(self, begin_dt_str, end_dt_str, key, lookback_hour, lookahead_hour, save_path):
        """
        示例:

        async def test():
            trade = Trade(exchange_name="binance", symbol_name="btcusdt")
            begin_str = "2019-12-01 11:23:56.123"
            end_str = "2019-12-01 14:24:56.123"
            save_path = "想要存放的路径"
            lookback_hour = 2
            lookahead_hour = 2
            await trade.to_daily(begin_str, end_str, "tradedt", lookback_hour, lookahead_hour, save_path)
        """
        if begin_dt_str > end_dt_str:
            raise ValueError("开始时间不能大于结束时间")

        days = self.divide_days(begin_dt_str, end_dt_str)

        for i in range(0, len(days) - 1):
            begin_dt = days[i]
            begin_timestamp = datetime_2_timestamp(begin_dt)
            end_timestamp = datetime_2_timestamp(days[i + 1])
            # 查找该时间间隔内
            now_df = await self.get_df(key, begin_timestamp, end_timestamp)
            if now_df.empty:
                continue

            # 历史
            back_microsecond = lookback_hour * 60 * 60 * 1000
            lookback_df = await self.get_df(key, begin_timestamp - back_microsecond, begin_timestamp, False)

            # 未来
            ahead_microsecond = lookahead_hour * 60 * 60 * 1000
            lookahead_df = await self.get_df(key, end_timestamp, end_timestamp + ahead_microsecond, False)

            df = pd.concat([lookback_df, now_df, lookahead_df], axis=0, sort=False)
            df.insert(0, "local_time", df[key])
            df["local_time"] = df.apply(mic_timestamp_2_datetime, axis=1)

            file_name = save_path + "/" + begin_dt.strftime("%Y%m%d") + ".pkl"
            df.to_pickle(file_name)
            file_name_csv = save_path + "/" + begin_dt.strftime("%Y%m%d") + ".csv"
            df.to_csv(file_name_csv)

    async def get_df(self, key, begin_timestamp, end_timestamp, good=True):
        cursor = self.collection.find({key: {"$gte": begin_timestamp, "$lt": end_timestamp}}, {"_id": 0}).sort(key)
        documents = []
        async for document in cursor:
            documents.append(document)
        # 若没有数据, 则跳过
        if not documents:
            return DataFrame([])
        df = DataFrame(documents)
        df["good"] = good
        return df

    def divide_days(self, begin_dt_str, end_dt_str):
        begin_dt = str_2_datetime(begin_dt_str)
        end_dt = str_2_datetime(end_dt_str)
        # begin_dt 当天结束
        one_day_end = datetime.datetime(begin_dt.year, begin_dt.month, begin_dt.day + 1)
        interval = end_dt - one_day_end
        # 按时间划分
        days = [one_day_end + datetime.timedelta(days=i) for i in range(0, interval.days+1)]
        days.insert(0, begin_dt)
        days.append(end_dt)
        return days


class Exchange(Base):
    COLUMNS = []

    def __init__(self):
        super(Base, self).__init__()
        self.collection = get_mongo_conn()["t_exchange"]


class Symbol(Base):
    """
    research_usable: 做datamatrix研究时， 是否可用于分析
    trade_usable: 跑策略时，是否可用于交易
    """
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
        super(Base, self).__init__()
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


class OrderBook(Base):
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


class Trade(Base):
    """
    市场逐笔成交记录表
    """
    COLUMNS = ["dt", "tradedt", "tradeprice", "volume", "amount", "direction"]

    def __init__(self, exchange_name, symbol_name):
        super(Base, self).__init__()
        collection_name = "t_trade_{exchange_name}_{symbol_name}".format(exchange_name=exchange_name,
                                                                         symbol_name=symbol_name)
        self.collection = get_mongo_conn()[collection_name]


class Kline(Base):
    """
    K线
    """
    COLUMNS = ["begin_dt", "end_dt", "open", "high", "low", "close", "avg_price", "buy_avg_price", "sell_avg_price",
               "open_avg", "close_avg", "volume", "amount", "book_count", "buy_book_count", "sell_book_count",
               "buy_volume", "sell_volume", "sell_aomunt", "sectional_high", "sectional_low", "sectional_volume",
               "sectional_aomunt", "sectional_avg_price", "sectional_buy_avg_price", "sectional_sell_avg_price",
               "sectional_book_count" "sectional_buy_book_count", "sectional_sell_book_count", "sectional_buy_volume",
               "sectional_sell_aomunt", "sectional_sell_volume", "sectional_sell_aomunt", "prev_close_price",
               "next_price", "prev_price", "lead_ret", "lag_ret", "usable"
               ]
    INTERVAL_DIRECTION = {
        "10s": 10 * 1000,
        "1min": 60 * 1000,
        "5min": 60 * 5 * 1000,
    }

    def __init__(self, exchange_name, symbol_name, interval_str="1min"):
        super(Base, self).__init__()
        self.collection_name = "t_kline_1min_{exchange_name}_{symbol_name}".format(exchange_name=exchange_name,
                                                                                   symbol_name=symbol_name)
        self.collection = get_mongo_conn()[self.collection_name]
        self.interval = self.INTERVAL_DIRECTION.get(interval_str, "1min")

    @asyncio.coroutine
    def insert_many(self, klines):
        result = yield from self.collection.bulk_write(klines)
        return result


def str_2_datetime(date_str):
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f")
    return dt


def datetime_2_timestamp(dt):
    t = dt.timetuple()
    timestamp = int(time.mktime(t)) * 1000 + round(dt.microsecond / 1000)
    return timestamp


def mic_timestamp_2_datetime(series):
    # 毫秒时间戳转时间
    mic_timestamp = series.get("local_time")
    timestamp = mic_timestamp / 1000
    dt = datetime.datetime.fromtimestamp(timestamp)
    return dt
