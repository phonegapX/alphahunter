# -*- coding: utf-8 -*-
import re
import datetime
import time
import pandas as pd
import math

from pandas import DataFrame

from mongo_utils import get_mongo_conn


class Base(object):
    DATABASE = "db_market"
    LIMIT = 100
    HALF_MINUTE = 30 * 1000

    def get_all(self, sort=None, **kwargs):
        pass

    def get_key(self):
        key = ""
        if self.__class__.__name__ in ["Trade", "OrderBook"]:
            key = "dt"
        elif self.__class__.__name__ == "Kline":
            key = "begin_dt"
        return key

    def get_df_from_table(self, begin_timestamp, end_timestamp):
        """
        查询一个时间段的数据
        use me like this:

        trade = Trade(exchange_name="binance", symbol_name="btcusdt")
        df = trade.get_df_from_table(1575158400000, 1575258400000)
        """
        documents = []
        key = self.get_key()

        for i in range(begin_timestamp, end_timestamp, self.HALF_MINUTE):
            end = (i + self.HALF_MINUTE) if i + \
                self.HALF_MINUTE < end_timestamp else end_timestamp
            cursor = self.collection.find(
                {key: {"$gte": i, "$lt": end}},
                {"_id": 0},
                sort=[("dt", 1)]
            )
            documents.extend(list(cursor))

        df = pd.DataFrame(documents)
        return df

    def to_daily(self, begin_dt_str, end_dt_str, lookback_hour, lookahead_hour, save_path):
        """
        示例:

        def test():
            trade = Trade(exchange_name="binance", symbol_name="btcusdt")
            begin_str = "2019-12-01 11:23:56.123"
            end_str = "2019-12-01 14:24:56.123"
            save_path = "想要存放的路径"
            lookback_hour = 2
            lookahead_hour = 2
            trade.to_daily(begin_str, end_str, lookback_hour, lookahead_hour, save_path)
        """
        if begin_dt_str > end_dt_str:
            raise ValueError("开始时间不能大于结束时间")

        days = self.divide_days(begin_dt_str, end_dt_str)
        key = self.get_key()

        for i in range(0, len(days) - 1):
            begin_dt = days[i]
            begin_timestamp = datetime_2_timestamp(begin_dt)
            end_timestamp = datetime_2_timestamp(days[i + 1])
            # 查找该时间间隔内
            now_df = self.get_df(key, begin_timestamp, end_timestamp)
            if now_df.empty:
                continue

            # 历史
            back_microsecond = lookback_hour * 60 * 60 * 1000
            lookback_df = self.get_df(
                key, begin_timestamp - back_microsecond, begin_timestamp, False)

            # 未来
            ahead_microsecond = lookahead_hour * 60 * 60 * 1000
            lookahead_df = self.get_df(
                key, end_timestamp, end_timestamp + ahead_microsecond, False)

            df = pd.concat([lookback_df, now_df, lookahead_df],
                           axis=0, sort=False)
            df.insert(0, "local_time", df[key])
            df["local_time"] = df.apply(mic_timestamp_2_datetime, axis=1)

            file_name = save_path + "/" + begin_dt.strftime("%Y%m%d") + ".pkl"
            df.to_pickle(file_name)

    def get_df(self, key, begin_timestamp, end_timestamp, good=True):
        documents = []
        for i in range(begin_timestamp, end_timestamp, self.HALF_MINUTE):
            end = (i + self.HALF_MINUTE) if i + \
                self.HALF_MINUTE < end_timestamp else end_timestamp
            cursor = self.collection.find(
                {key: {"$gte": i, "$lt": end}},
                {"_id": 0},
                sort=[("dt", 1)]
            )
            documents.extend(list(cursor))
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
        one_day_end = datetime.datetime(
            begin_dt.year, begin_dt.month, begin_dt.day + 1)
        interval = end_dt - one_day_end
        # 按时间划分
        days = [one_day_end +
                datetime.timedelta(days=i) for i in range(0, interval.days+1)]
        days.insert(0, begin_dt)
        days.append(end_dt)
        return days


class Exchange(Base):
    COLUMNS = []

    def __init__(self):
        super(Base, self).__init__()
        self.collection = get_mongo_conn(self.DATABASE)["t_exchange"]

    def insert(self, exchange_name):
        return self.collection.insert_one({"name": exchange_name})


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
        self.collection = get_mongo_conn(self.DATABASE)["t_symbol"]

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
    索引 dt
    """
    COLUMNS = ["dt",
                "askprice1", "askprice2", "askprice3", "askprice4", "askprice5", "askprice6", "askprice7", "askprice8", "askprice9", "askprice10", "askprice11", "askprice12", "askprice13", "askprice14", "askprice15", "askprice16", "askprice17", "askprice18", "askprice19", "askprice20",  # NOQA
                "bidprice1", "bidprice2", "bidprice3", "bidprice4", "bidprice5", "bidprice6", "bidprice7", "bidprice8", "bidprice9", "bidprice10", "bidprice11", "bidprice12", "bidprice13", "bidprice14", "bidprice15", "bidprice16", "bidprice17", "bidprice18", "bidprice19", "bidprice20",  # NOQA
                "asksize1", "asksize2", "asksize3", "asksize4", "asksize5", "asksize6", "asksize7", "asksize8", "asksize9", "asksize10", "asksize11", "asksize12", "asksize13", "asksize14", "asksize15", "asksize16", "asksize17", "asksize18", "asksize19", "asksize20",  # NOQA
                "bidsize1", "bidsize2", "bidsize3", "bidsize4", "bidsize5", "bidsize6", "bidsize7", "bidsize8", "bidsize9", "bidsize10", "bidsize11", "bidsize12", "bidsize13", "bidsize14", "bidsize15", "bidsize16", "bidsize17", "bidsize18", "bidsize19", "bidsize20",  # NOQA
            ]

    def __init__(self, exchange_name, symbol_name):
        super(Base, self).__init__()
        self.collection_name = "t_orderbook_{exchange_name}_{symbol_name}".format(exchange_name=exchange_name,
                                                                                  symbol_name=symbol_name)
        self.collection = get_mongo_conn(self.DATABASE)[self.collection_name]


class Trade(Base):
    """
    市场逐笔成交记录表
    """
    COLUMNS = ["dt", "tradedt", "tradeprice", "volume", "amount", "direction"]

    def __init__(self, exchange_name, symbol_name):
        super(Base, self).__init__()
        self.collection_name = "t_trade_{exchange_name}_{symbol_name}".format(exchange_name=exchange_name,
                                                                              symbol_name=symbol_name)
        self.collection = get_mongo_conn(self.DATABASE)[self.collection_name]


class Kline(Base):
    """
    K线
    """
    DATABASE = "db_custom_kline"
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
        
        if interval_str=="1min":
            self.collection_name = "t_kline_{exchange_name}_{symbol_name}".format(exchange_name=exchange_name, symbol_name=symbol_name)
        else:
            self.collection_name = "t_kline_{interval_str}_{exchange_name}_{symbol_name}".format(
                interval_str=interval_str, exchange_name=exchange_name, symbol_name=symbol_name)

        self.collection = get_mongo_conn(self.DATABASE)[self.collection_name]
        self.interval = self.INTERVAL_DIRECTION.get(interval_str, "1min")

    def insert_many(self, klines):
        result = self.collection.bulk_write(klines)
        return result

    def generate_kline(self, begin_dt, trades, prev_kline):
        """
        生成kline, 不包括当天累计字段
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
            "lag_ret": None,
            "lead_ret_fillna": None,
            "lag_ret_fillna": None,
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

            if t_document["direction"] == "buy":
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
        lead_ret = math.log(
            open_avg / prev_kline["open_avg"]) if prev_kline.get("open_avg", 0.0) and open_avg else None
        lead_ret_fillna = math.log(open_avg_fillna / prev_kline["open_avg_fillna"])\
            if prev_kline.get("open_avg_fillna", 0.0) and open_avg_fillna else None
        prev_kline.update({
            "next_price": open_avg,
            "next_price_fillna": open_avg_fillna if open_avg_fillna else prev_kline["close_avg_fillna"],
            "lead_ret": lead_ret,
            "lead_ret_fillna": lead_ret_fillna,
        })

        return new_kline, prev_kline

    def handle_documents(self, documents):
        sum_amount = 0
        sum_volume = 0

        for document in documents:
            sum_amount += document["volume"]*document["tradeprice"] #如果是反向合约就不能这样计算
            sum_volume += document["volume"]
        return {
            "avg_price": sum_amount / sum_volume,
            "sum_volume": sum_volume,
            "sum_amount": sum_amount,
        }

    def generate_kline_ex(self, begin_dt, trades, prev_kline, yestdy_last_kline, yestdy_last_trade_price=0.0):
        """
        生成一根完整kline
        """
        if prev_kline == yestdy_last_kline:
            prev_kline = { #这样做的目的是因为所有sectional_xxx的字段都需要每天从头开始累加
                "open_avg": prev_kline.get("open_avg", 0.0),
                "open_avg_fillna": prev_kline.get("open_avg_fillna", 0.0),
                "close_avg": prev_kline.get("close_avg", 0.0),
                "close_avg_fillna": prev_kline.get("close_avg_fillna", 0.0),
            }
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
            "prev_close_price": yestdy_last_kline.get("close", 0.0) or yestdy_last_trade_price,
            "next_price": 0.0,
            "next_price_fillna": 0.0,
            "prev_price": prev_kline.get("close_avg", 0.0),
            "prev_price_fillna": prev_kline.get("close_avg_fillna", 0.0),
            "lead_ret": None,
            "lag_ret": None,
            "lead_ret_fillna": None,
            "lag_ret_fillna": None,
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

            if t_document["dt"] - begin_dt < duration: #对于一分钟K线就是前12秒的成交
                open_trades.append(t_document)
            elif (begin_dt + self.interval) - t_document["dt"] <= duration: #对于一分钟K线就是后12秒的成交
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

        new_kline["sectional_high"] = max([prev_kline.get("sectional_high", 0.0), high])
        prev_kline_sectional_low = prev_kline.get("sectional_low", 0.0)
        min_low = min([low, prev_kline_sectional_low])
        new_kline["sectional_low"] = min_low if min_low else max([low, prev_kline_sectional_low])

        sectional_volume = prev_kline.get("sectional_volume", 0.0) + new_kline["volume"]
        sectional_amount = prev_kline.get("sectional_amount", 0.0) + new_kline["amount"]
        new_kline["sectional_volume"] = sectional_volume
        new_kline["sectional_amount"] = sectional_amount
        new_kline["sectional_avg_price"] = sectional_amount / sectional_volume if sectional_amount else 0.0
        new_kline["sectional_book_count"] = prev_kline.get("sectional_book_count", 0.0) + new_kline["book_count"]

        sectional_buy_volume = prev_kline.get("sectional_buy_volume", 0.0) + new_kline["buy_volume"]
        sectional_buy_amount = prev_kline.get("sectional_buy_amount", 0.0) + new_kline["buy_amount"]
        new_kline["sectional_buy_volume"] = sectional_buy_volume
        new_kline["sectional_buy_amount"] = sectional_buy_amount
        new_kline["sectional_buy_avg_price"] = sectional_buy_amount / sectional_buy_volume if sectional_buy_amount else 0.0
        new_kline["sectional_buy_book_count"] = prev_kline.get("sectional_buy_book_count", 0.0) + new_kline["buy_book_count"]

        sectional_sell_volume = prev_kline.get("sectional_sell_volume", 0.0) + new_kline["sell_volume"]
        sectional_sell_amount = prev_kline.get("sectional_sell_amount", 0.0) + new_kline["sell_amount"]
        new_kline["sectional_sell_volume"] = sectional_sell_volume
        new_kline["sectional_sell_amount"] = sectional_sell_amount
        new_kline["sectional_sell_avg_price"] = sectional_sell_amount / sectional_sell_volume if sectional_sell_amount else 0.0
        new_kline["sectional_sell_book_count"] = prev_kline.get("sectional_sell_book_count", 0.0) + new_kline["sell_book_count"]

        open_avg = new_kline["open_avg"]
        close_avg = new_kline["close_avg"]
        new_kline["open_avg_fillna"] = open_avg if open_avg else new_kline["open"]
        close_avg_fillna = close_avg if close_avg else new_kline["close"]
        new_kline["close_avg_fillna"] = close_avg_fillna

        new_kline["lag_ret"] = math.log(new_kline["close_avg"]/prev_kline["close_avg"]) if prev_kline.get("close_avg", 0.0) and new_kline["close_avg"] else None

        prev_close_avg_fillna = prev_kline.get("close_avg_fillna", 0.0)
        new_kline["lag_ret_fillna"] = math.log(close_avg_fillna/prev_close_avg_fillna) if prev_close_avg_fillna and close_avg_fillna else None

        # "lead_ret": 0.0  # math.log(next_price / open_avg),
        # "lead_ret_fillna": 0.0  # math.log(next_price_fillna / open_avg_fillna),
        open_avg = new_kline["open_avg"]
        open_avg_fillna = new_kline["open_avg_fillna"]
        lead_ret = math.log(open_avg/prev_kline["open_avg"]) if prev_kline.get("open_avg", 0.0) and open_avg else None
        lead_ret_fillna = math.log(open_avg_fillna/prev_kline["open_avg_fillna"]) if prev_kline.get("open_avg_fillna", 0.0) and open_avg_fillna else None
        prev_kline.update({
            "next_price": open_avg,
            "next_price_fillna": open_avg_fillna if open_avg_fillna else prev_kline["close_avg_fillna"],
            "lead_ret": lead_ret,
            "lead_ret_fillna": lead_ret_fillna,
        })

        return new_kline, prev_kline


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


if __name__ == '__main__':

    trade = Trade(exchange_name="binance", symbol_name="btcusdt")
    df = trade.get_df_from_table(1575158400000, 1575258400000)

    begin_str = "2019-12-01 11:23:56.123"
    end_str = "2019-12-01 14:24:56.123"
    save_path = "./"
    lookback_hour = 2
    lookahead_hour = 2
    trade.to_daily(begin_str, end_str, lookback_hour,
                   lookahead_hour, save_path)
